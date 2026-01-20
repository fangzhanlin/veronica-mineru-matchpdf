"""
批量PDF转Markdown处理脚本
使用MinerU API处理pdfs文件夹下的所有PDF文件

输出结构:
    outputs_api/
    ├── DSS/
    │   └── 文件名1/
    │       ├── 文件名1.md
    │       └── images/
    ├── EJIS/
    │   └── 文件名2/
    │       └── ...
    └── ...

使用方法:
    python batch_convert_api.py
    python batch_convert_api.py --input-dir pdfs --output-dir outputs_api
    python batch_convert_api.py --language en --no-ocr
"""

import argparse
import asyncio
import json
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

from mineru_api_base import (
    MinerUAPIClient,
    BaseBatchProcessor,
    TaskState,
    TaskResult,
    logger,
)


class PDFBatchProcessor(BaseBatchProcessor):
    """
    PDF批量处理器
    
    处理pdfs文件夹下所有子文件夹中的PDF文件，
    保持相同的目录结构输出到outputs_api文件夹。
    """
    
    # 支持的文件扩展名
    SUPPORTED_EXTENSIONS = {'.pdf', '.PDF'}
    
    def __init__(
        self,
        input_dir: str = "pdfs",
        output_dir: str = "outputs_api",
        client: Optional[MinerUAPIClient] = None,
        **client_kwargs
    ):
        """
        初始化PDF批量处理器
        
        Args:
            input_dir: 输入目录（包含PDF文件的根目录）
            output_dir: 输出目录
            client: MinerUAPIClient实例
            **client_kwargs: 传递给MinerUAPIClient的参数
        """
        super().__init__(client, **client_kwargs)
        
        self.input_dir = Path(input_dir).resolve()
        self.output_dir = Path(output_dir).resolve()
        
        if not self.input_dir.exists():
            raise ValueError(f"输入目录不存在: {self.input_dir}")
        
        # 确保输出目录存在
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"PDF批量处理器初始化完成")
        logger.info(f"  输入目录: {self.input_dir}")
        logger.info(f"  输出目录: {self.output_dir}")
    
    def find_files(self) -> List[Dict[str, Any]]:
        """
        查找所有需要处理的PDF文件
        
        Returns:
            文件信息列表，每个字典包含:
            - path: 文件完整路径
            - output_dir: 对应的输出目录
            - subfolder: 相对于input_dir的子文件夹路径
            - filename: 文件名（不含扩展名）
        """
        files = []
        
        try:
            # 递归查找所有PDF文件
            for pdf_path in self.input_dir.rglob("*"):
                # 跳过非文件
                if not pdf_path.is_file():
                    continue
                
                # 检查扩展名
                if pdf_path.suffix not in self.SUPPORTED_EXTENSIONS:
                    continue
                
                # 跳过zip文件内的内容
                if '.zip' in str(pdf_path):
                    continue
                
                try:
                    # 计算相对路径
                    relative_path = pdf_path.relative_to(self.input_dir)
                    subfolder = relative_path.parent
                    filename = pdf_path.stem
                    
                    # 计算输出目录: outputs_api/子文件夹/文件名/
                    file_output_dir = self.output_dir / subfolder / filename
                    
                    files.append({
                        'path': pdf_path,
                        'output_dir': file_output_dir,
                        'subfolder': subfolder,
                        'filename': filename,
                    })
                except Exception as e:
                    logger.warning(f"处理文件路径时出错 {pdf_path}: {e}")
                    continue
        
        except Exception as e:
            logger.error(f"查找文件时出错: {e}")
        
        logger.info(f"找到 {len(files)} 个PDF文件")
        return files
    
    def is_processed(self, file_info: Dict[str, Any]) -> bool:
        """
        检查文件是否已处理
        
        判断依据：输出目录下存在.md文件
        """
        try:
            output_dir = Path(file_info['output_dir'])
            
            if not output_dir.exists():
                return False
            
            # 检查是否存在.md文件
            md_files = list(output_dir.rglob("*.md"))
            if md_files:
                # 检查md文件是否有内容
                for md_file in md_files:
                    try:
                        if md_file.stat().st_size > 0:
                            return True
                    except:
                        continue
            
            return False
        
        except Exception as e:
            logger.warning(f"检查处理状态时出错 {file_info['path']}: {e}")
            return False
    
    def on_file_success(self, file_info: Dict[str, Any], result: TaskResult) -> None:
        """文件处理成功回调"""
        try:
            # 统计图片数量
            output_dir = Path(file_info['output_dir'])
            images_dir = output_dir / "images"
            img_count = 0
            
            if images_dir.exists():
                img_count = len(list(images_dir.glob("*")))
            else:
                # 可能在子目录中
                for subdir in output_dir.rglob("images"):
                    if subdir.is_dir():
                        img_count += len(list(subdir.glob("*")))
            
            logger.info(
                f"✅ 成功: {file_info['filename']}.pdf "
                f"(子目录: {file_info['subfolder']}, 图片: {img_count})"
            )
        except Exception as e:
            logger.info(f"✅ 成功: {file_info['filename']}.pdf")
    
    def on_file_error(self, file_info: Dict[str, Any], error: Exception) -> None:
        """文件处理失败回调"""
        logger.error(
            f"❌ 失败: {file_info['filename']}.pdf "
            f"(子目录: {file_info['subfolder']}) - {error}"
        )
    
    def cleanup_partial_output(self, file_info: Dict[str, Any]) -> None:
        """清理部分完成的输出"""
        try:
            output_dir = Path(file_info['output_dir'])
            if output_dir.exists():
                shutil.rmtree(output_dir)
                logger.debug(f"已清理部分输出: {output_dir}")
        except Exception as e:
            logger.warning(f"清理输出失败 {file_info['output_dir']}: {e}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取当前处理统计信息"""
        files = self.find_files()
        processed = sum(1 for f in files if self.is_processed(f))
        
        # 按子文件夹统计
        by_subfolder = {}
        for file_info in files:
            subfolder = str(file_info['subfolder'])
            if subfolder not in by_subfolder:
                by_subfolder[subfolder] = {'total': 0, 'processed': 0}
            by_subfolder[subfolder]['total'] += 1
            if self.is_processed(file_info):
                by_subfolder[subfolder]['processed'] += 1
        
        return {
            'total': len(files),
            'processed': processed,
            'remaining': len(files) - processed,
            'by_subfolder': by_subfolder,
        }
    
    def process_all_sync(
        self,
        enable_ocr: bool = True,
        language: str = "ch",
        skip_processed: bool = True,
        batch_size: int = 1,
        delay_between_batches: float = 1.0,
    ) -> Dict[str, Any]:
        """
        同步处理所有文件（带批次控制）
        
        Args:
            enable_ocr: 是否启用OCR
            language: 文档语言
            skip_processed: 是否跳过已处理文件
            batch_size: 每批处理的文件数（目前API限制为1）
            delay_between_batches: 批次之间的延迟(秒)
            
        Returns:
            处理统计信息
        """
        files = self.find_files()
        
        stats = {
            "total": len(files),
            "skipped": 0,
            "success": 0,
            "failed": 0,
            "errors": [],
            "start_time": datetime.now().isoformat(),
            "end_time": None,
        }
        
        # 过滤已处理的文件
        files_to_process = []
        for file_info in files:
            try:
                if skip_processed and self.is_processed(file_info):
                    stats["skipped"] += 1
                    logger.info(f"⏭️ 跳过已处理: {file_info['subfolder']}/{file_info['filename']}.pdf")
                    continue
                files_to_process.append(file_info)
            except Exception as e:
                logger.warning(f"检查文件状态时出错: {e}")
                files_to_process.append(file_info)
        
        logger.info(f"需要处理: {len(files_to_process)} 个文件")
        
        # 逐个处理（API当前限制）
        for idx, file_info in enumerate(files_to_process):
            try:
                logger.info(f"📄 处理 [{idx+1}/{len(files_to_process)}]: "
                           f"{file_info['subfolder']}/{file_info['filename']}.pdf")
                
                # 清理可能存在的部分输出
                self.cleanup_partial_output(file_info)
                
                # 处理文件
                task_info = self.client.process_file_sync(
                    str(file_info['path']),
                    str(file_info['output_dir']),
                    enable_ocr=enable_ocr,
                    language=language,
                )
                
                # 检查结果
                for result in task_info.results:
                    if result.status == TaskState.DONE:
                        stats["success"] += 1
                        self.on_file_success(file_info, result)
                    else:
                        stats["failed"] += 1
                        error_msg = result.error_message or "未知错误"
                        self.on_file_error(file_info, Exception(error_msg))
                        stats["errors"].append({
                            "file": str(file_info['path']),
                            "subfolder": str(file_info['subfolder']),
                            "error": error_msg,
                        })
                
                # 批次间延迟
                if idx < len(files_to_process) - 1 and delay_between_batches > 0:
                    time.sleep(delay_between_batches)
                    
            except Exception as e:
                stats["failed"] += 1
                self.on_file_error(file_info, e)
                stats["errors"].append({
                    "file": str(file_info['path']),
                    "subfolder": str(file_info['subfolder']),
                    "error": str(e),
                })
                
                # 清理失败的输出
                self.cleanup_partial_output(file_info)
        
        stats["end_time"] = datetime.now().isoformat()
        
        # 打印摘要
        self._print_summary(stats)
        
        return stats
    
    async def process_all_async(
        self,
        enable_ocr: bool = True,
        language: str = "ch",
        skip_processed: bool = True,
        delay_between_batches: float = 1.0,
        batch_size: int = 1,
    ) -> Dict[str, Any]:
        """
        异步处理所有文件（支持并发）
        
        Args:
            enable_ocr: 是否启用OCR
            language: 文档语言
            skip_processed: 是否跳过已处理文件
            delay_between_batches: 批次之间的延迟(秒)
            batch_size: 并发处理的文件数量
            
        Returns:
            处理统计信息
        """
        files = self.find_files()
        
        stats = {
            "total": len(files),
            "skipped": 0,
            "success": 0,
            "failed": 0,
            "errors": [],
            "start_time": datetime.now().isoformat(),
            "end_time": None,
            "batch_size": batch_size,
        }
        
        # 过滤已处理的文件
        files_to_process = []
        for file_info in files:
            try:
                if skip_processed and self.is_processed(file_info):
                    stats["skipped"] += 1
                    logger.info(f"⏭️ 跳过已处理: {file_info['subfolder']}/{file_info['filename']}.pdf")
                    continue
                files_to_process.append(file_info)
            except Exception as e:
                logger.warning(f"检查文件状态时出错: {e}")
                files_to_process.append(file_info)
        
        total_to_process = len(files_to_process)
        logger.info(f"需要处理: {total_to_process} 个文件，并发数: {batch_size}")
        
        # 按batch_size分批处理
        for batch_start in range(0, total_to_process, batch_size):
            batch_end = min(batch_start + batch_size, total_to_process)
            batch_files = files_to_process[batch_start:batch_end]
            batch_num = batch_start // batch_size + 1
            total_batches = (total_to_process + batch_size - 1) // batch_size
            
            logger.info(f"🚀 批次 [{batch_num}/{total_batches}]: 并发处理 {len(batch_files)} 个文件")
            
            # 并发处理当前批次
            tasks = []
            for file_info in batch_files:
                task = self._process_single_file_async(
                    file_info, enable_ocr, language, stats
                )
                tasks.append(task)
            
            # 等待所有任务完成
            await asyncio.gather(*tasks, return_exceptions=True)
            
            # 批次间延迟
            if batch_end < total_to_process and delay_between_batches > 0:
                logger.info(f"⏳ 等待 {delay_between_batches} 秒后处理下一批次...")
                await asyncio.sleep(delay_between_batches)
        
        stats["end_time"] = datetime.now().isoformat()
        
        # 打印摘要
        self._print_summary(stats)
        
        return stats
    
    async def _process_single_file_async(
        self,
        file_info: Dict[str, Any],
        enable_ocr: bool,
        language: str,
        stats: Dict[str, Any],
    ) -> None:
        """
        异步处理单个文件（用于并发调用）
        
        Args:
            file_info: 文件信息
            enable_ocr: 是否启用OCR
            language: 文档语言
            stats: 统计信息（会原地修改）
        """
        try:
            logger.info(f"📄 开始处理: {file_info['subfolder']}/{file_info['filename']}.pdf")
            
            # 清理可能存在的部分输出
            self.cleanup_partial_output(file_info)
            
            # 处理文件
            task_info = await self.client.process_file(
                str(file_info['path']),
                str(file_info['output_dir']),
                enable_ocr=enable_ocr,
                language=language,
            )
            
            # 检查结果
            for result in task_info.results:
                if result.status == TaskState.DONE:
                    stats["success"] += 1
                    self.on_file_success(file_info, result)
                else:
                    stats["failed"] += 1
                    error_msg = result.error_message or "未知错误"
                    self.on_file_error(file_info, Exception(error_msg))
                    stats["errors"].append({
                        "file": str(file_info['path']),
                        "subfolder": str(file_info['subfolder']),
                        "error": error_msg,
                    })
                    
        except Exception as e:
            stats["failed"] += 1
            self.on_file_error(file_info, e)
            stats["errors"].append({
                "file": str(file_info['path']),
                "subfolder": str(file_info['subfolder']),
                "error": str(e),
            })
            
            # 清理失败的输出
            self.cleanup_partial_output(file_info)
    
    def _print_summary(self, stats: Dict[str, Any]) -> None:
        """打印处理摘要"""
        logger.info("=" * 60)
        logger.info("处理完成摘要")
        logger.info("=" * 60)
        logger.info(f"  总文件数: {stats['total']}")
        logger.info(f"  跳过(已处理): {stats['skipped']}")
        logger.info(f"  成功: {stats['success']}")
        logger.info(f"  失败: {stats['failed']}")
        logger.info(f"  开始时间: {stats['start_time']}")
        logger.info(f"  结束时间: {stats['end_time']}")
        
        if stats['errors']:
            logger.info("-" * 60)
            logger.info("错误详情:")
            for error in stats['errors'][:10]:  # 只显示前10个错误
                logger.info(f"  - {error['file']}: {error['error']}")
            if len(stats['errors']) > 10:
                logger.info(f"  ... 还有 {len(stats['errors']) - 10} 个错误")
        
        logger.info("=" * 60)


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="使用MinerU API批量处理PDF文件转换为Markdown",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s                          # 使用默认设置处理
  %(prog)s --input-dir pdfs         # 指定输入目录
  %(prog)s --output-dir outputs     # 指定输出目录
  %(prog)s --language en            # 设置文档语言为英文
  %(prog)s --no-ocr                 # 禁用OCR
  %(prog)s --no-skip                # 不跳过已处理的文件
  %(prog)s --stats                  # 只显示统计信息
  %(prog)s --async                  # 使用异步模式
        """
    )
    
    parser.add_argument(
        '--input-dir', '-i',
        type=str,
        default='pdfs',
        help='输入目录路径 (默认: pdfs)'
    )
    
    parser.add_argument(
        '--output-dir', '-o',
        type=str,
        default='outputs_api',
        help='输出目录路径 (默认: outputs_api)'
    )
    
    parser.add_argument(
        '--language', '-l',
        type=str,
        default='en',
        choices=['ch', 'en', 'korean', 'japan', 'chinese_cht', 'ta', 'te', 'ka', 'th', 'el', 'latin', 'arabic'],
        help='文档语言 (默认: en)'
    )
    
    parser.add_argument(
        '--no-ocr',
        action='store_true',
        help='禁用OCR'
    )
    
    parser.add_argument(
        '--no-skip',
        action='store_true',
        help='不跳过已处理的文件'
    )
    
    parser.add_argument(
        '--stats',
        action='store_true',
        help='只显示统计信息，不进行处理'
    )
    
    parser.add_argument(
        '--async',
        dest='use_async',
        action='store_true',
        help='使用异步模式处理（支持并发）'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1,
        help='并发处理的文件数量 (默认: 1, 需配合--async使用)'
    )
    
    parser.add_argument(
        '--delay',
        type=float,
        default=1.0,
        help='批次之间的延迟秒数 (默认: 1.0)'
    )
    
    parser.add_argument(
        '--api-key',
        type=str,
        help='MinerU API密钥 (默认从token.txt或环境变量读取)'
    )
    
    parser.add_argument(
        '--max-retries',
        type=int,
        default=180,
        help='最大重试次数 (默认: 180)'
    )
    
    parser.add_argument(
        '--retry-interval',
        type=int,
        default=10,
        help='重试间隔秒数 (默认: 10)'
    )
    
    return parser.parse_args()


def show_statistics(processor: PDFBatchProcessor) -> None:
    """显示处理统计信息"""
    stats = processor.get_statistics()
    
    print("\n" + "=" * 60)
    print("PDF处理统计信息")
    print("=" * 60)
    print(f"  总文件数: {stats['total']}")
    print(f"  已处理: {stats['processed']}")
    print(f"  待处理: {stats['remaining']}")
    print()
    print("按文件夹统计:")
    print("-" * 60)
    
    for subfolder, counts in sorted(stats['by_subfolder'].items()):
        print(f"  {subfolder or '(根目录)'}: "
              f"{counts['processed']}/{counts['total']} 已处理")
    
    print("=" * 60)


def main():
    """主函数"""
    args = parse_arguments()
    
    try:
        # 构建客户端参数
        client_kwargs = {
            'max_retries': args.max_retries,
            'retry_interval': args.retry_interval,
        }
        if args.api_key:
            client_kwargs['api_key'] = args.api_key
        
        # 创建处理器
        processor = PDFBatchProcessor(
            input_dir=args.input_dir,
            output_dir=args.output_dir,
            **client_kwargs
        )
        
        # 只显示统计
        if args.stats:
            show_statistics(processor)
            return 0
        
        # 处理参数
        process_kwargs = {
            'enable_ocr': not args.no_ocr,
            'language': args.language,
            'skip_processed': not args.no_skip,
        }
        
        # 开始处理
        logger.info("开始批量处理...")
        
        if args.use_async:
            # 异步模式（支持并发）
            process_kwargs['delay_between_batches'] = args.delay
            process_kwargs['batch_size'] = args.batch_size
            stats = asyncio.run(processor.process_all_async(**process_kwargs))
        else:
            # 同步模式
            process_kwargs['delay_between_batches'] = args.delay
            stats = processor.process_all_sync(**process_kwargs)
        
        # 保存处理结果
        result_file = Path(args.output_dir) / 'processing_result.json'
        try:
            with open(result_file, 'w', encoding='utf-8') as f:
                json.dump(stats, f, ensure_ascii=False, indent=2)
            logger.info(f"处理结果已保存到: {result_file}")
        except Exception as e:
            logger.warning(f"保存处理结果失败: {e}")
        
        # 返回状态码
        return 0 if stats['failed'] == 0 else 1
        
    except KeyboardInterrupt:
        logger.info("\n用户中断处理")
        return 130
    except Exception as e:
        logger.error(f"处理过程中发生错误: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
