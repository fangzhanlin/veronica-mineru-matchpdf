#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
文献数据匹配脚本 (简化版 v3.0)

根据 PDF 文件名匹配数据源（CSV/MongoDB）中的文献数据。
自动检测文件名格式，无需按期刊区分。

主要特性:
- 自动处理特殊编码（#x3f; 等）
- 自动检测年份格式和 DOI 格式
- 同时尝试 Title 和 DOI 匹配
- 支持 MongoDB 数据源（doi/label/uuid 字段）
- 成功匹配后可复制 PDF 并重命名为 uuid

Author: GitHub Copilot
Date: 2025-01-20
Version: 3.0.0
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from data_sources import (
    CSVDataSource,
    MongoDBDataSource,
    FieldMapping,
    CSV_FIELD_MAPPING,
    MONGODB_FIELD_MAPPING,
)
from exporters import CSVExporter, PDFCopier, SummaryGenerator
from matcher import BatchMatchResult, PDFMatcher


def setup_logging(log_dir: Path) -> logging.Logger:
    """设置日志记录器"""
    log_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"match_log_{timestamp}.log"
    
    logger = logging.getLogger("match_records")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    
    # 文件处理器
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


class MatchingApplication:
    """
    文献匹配应用程序主类 (简化版)
    """
    
    def __init__(
        self,
        pdfs_dir: Path,
        output_dir: Path,
        logger: logging.Logger,
        copy_pdfs: bool = False,
        copy_dir: Optional[Path] = None,
        recursive_scan: bool = True,
        clean_results: bool = False,
    ):
        """
        初始化应用程序
        
        Args:
            pdfs_dir: PDF 文件目录
            output_dir: 输出目录
            logger: 日志记录器
            copy_pdfs: 是否复制成功匹配的 PDF
            copy_dir: PDF 复制目标目录
            recursive_scan: 是否递归扫描子目录中的 PDF
            clean_results: 是否在运行前清空历史结果
        """
        self.pdfs_dir = Path(pdfs_dir)
        self.output_dir = Path(output_dir)
        self.logger = logger
        self.copy_pdfs = copy_pdfs
        self.copy_dir = Path(copy_dir) if copy_dir else None
        self.recursive_scan = recursive_scan
        self.clean_results = clean_results
        
        # 清空历史结果
        if self.clean_results:
            self._clean_output_directory()
        
        # 组件
        self.exporter = CSVExporter(output_dir, logger=logger)
        self.summary_generator = SummaryGenerator(output_dir, logger=logger)
        
        # 结果
        self.result: Optional[BatchMatchResult] = None
    
    def _clean_output_directory(self):
        """清空输出目录中的历史结果"""
        import shutil
        
        dirs_to_clean = ['matched', 'unmatched', 'multi_matched']
        files_to_clean = ['ALL_MATCHED.csv', 'ALL_UNMATCHED.csv', 'ALL_MULTI_MATCHED.csv']
        
        cleaned = False
        for dir_name in dirs_to_clean:
            dir_path = self.output_dir / dir_name
            if dir_path.exists():
                shutil.rmtree(dir_path)
                cleaned = True
        
        for file_name in files_to_clean:
            file_path = self.output_dir / file_name
            if file_path.exists():
                file_path.unlink()
                cleaned = True
        
        if cleaned:
            self.logger.info("已清空历史匹配结果")
    
    def run_csv(
        self,
        csv_file: Optional[Path] = None,
        csv_dir: Optional[Path] = None,
        csv_pattern: str = "*.csv",
    ) -> BatchMatchResult:
        """
        使用 CSV 数据源运行匹配
        """
        self.logger.info("=" * 60)
        self.logger.info("文献数据匹配 (CSV 模式) v3.0")
        self.logger.info("=" * 60)
        
        # 创建数据源
        from data_sources import CSVDataSource, CSV_FIELD_MAPPING
        
        data_source = CSVDataSource(
            csv_file=csv_file,
            csv_dir=csv_dir,
            file_pattern=csv_pattern,
            field_mapping=CSV_FIELD_MAPPING,
            logger=self.logger
        )
        
        # 创建匹配器
        matcher = PDFMatcher(
            logger=self.logger,
            title_column=CSV_FIELD_MAPPING.title,
            doi_column=CSV_FIELD_MAPPING.doi
        )
        
        return self._run_matching(data_source, matcher, CSV_FIELD_MAPPING)
    
    def run_mongodb(
        self,
        connection_string: str,
        database: str,
        collection: str,
        query: Optional[Dict] = None,
    ) -> BatchMatchResult:
        """
        使用 MongoDB 数据源运行匹配
        """
        self.logger.info("=" * 60)
        self.logger.info("文献数据匹配 (MongoDB 模式) v3.0")
        self.logger.info("=" * 60)
        
        # 创建数据源
        from data_sources import MongoDBDataSource, MONGODB_FIELD_MAPPING
        
        data_source = MongoDBDataSource(
            connection_string=connection_string,
            database=database,
            collection=collection,
            field_mapping=MONGODB_FIELD_MAPPING,
            logger=self.logger
        )
        
        # 创建匹配器
        matcher = PDFMatcher(
            logger=self.logger,
            title_column=MONGODB_FIELD_MAPPING.title,  # 'label'
            doi_column=MONGODB_FIELD_MAPPING.doi       # 'doi'
        )
        
        return self._run_matching(
            data_source, matcher, MONGODB_FIELD_MAPPING,
            collection_name=collection, query=query
        )
    
    def _run_matching(
        self,
        data_source,
        matcher: PDFMatcher,
        field_mapping: FieldMapping,
        collection_name: str = "",
        query: Optional[Dict] = None,
    ) -> BatchMatchResult:
        """执行匹配流程"""
        
        self.logger.info(f"PDF 目录: {self.pdfs_dir}")
        self.logger.info(f"输出目录: {self.output_dir}")
        
        # 连接数据源
        if not data_source.connect():
            self.logger.error("无法连接到数据源")
            raise RuntimeError("数据源连接失败")
        
        try:
            # 获取记录
            data_result = data_source.get_records(collection_name, query)
            self.logger.info(f"数据源: {data_result.source_name}")
            
            # 执行匹配
            self.result = matcher.match_all(
                pdfs_dir=self.pdfs_dir,
                data_result=data_result,
                interactive=False,
                recursive_scan=self.recursive_scan
            )
            
            # 导出结果
            self.exporter.export_all(
                self.result,
                data_result.headers,
                field_mapping
            )
            
            # 生成汇总
            self.logger.info("\n" + "=" * 60)
            self.logger.info("生成汇总文件")
            self.logger.info("=" * 60)
            self.summary_generator.generate_all_summaries()
            
            # 复制 PDF 文件
            if self.copy_pdfs and self.copy_dir and self.result.matched_count > 0:
                self.logger.info("\n" + "=" * 60)
                self.logger.info("复制匹配的 PDF 文件")
                self.logger.info("=" * 60)
                
                copier = PDFCopier(self.copy_dir, logger=self.logger)
                copy_stats = copier.copy_matched_pdfs(
                    self.result,
                    uuid_field=field_mapping.uuid if field_mapping.uuid else ''
                )
                
                self.logger.info(
                    f"复制完成: {copy_stats['copied']} 个文件已复制到 {self.copy_dir}"
                )
            
            self.logger.info("\n处理完成！")
            
        finally:
            data_source.disconnect()
        
        return self.result


def create_argument_parser() -> argparse.ArgumentParser:
    """创建命令行参数解析器"""
    parser = argparse.ArgumentParser(
        description='文献数据匹配工具 - 将 PDF 文件名与数据源记录匹配',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # CSV 模式 - 使用单个 CSV 文件
  python match_records.py --pdfs-dir ./pdfs --csv-file ./data.csv

  # CSV 模式 - 使用 CSV 目录
  python match_records.py --pdfs-dir ./pdfs --csv-dir ./csv_files

  # MongoDB 模式
  python match_records.py --pdfs-dir ./pdfs \\
      --source mongodb \\
      --mongo-uri "mongodb://localhost:27017" \\
      --mongo-db literature \\
      --mongo-collection papers

  # 复制匹配的 PDF 到指定目录（MongoDB 模式会使用 uuid 重命名）
  python match_records.py --pdfs-dir ./pdfs --csv-file ./data.csv \\
      --copy-pdfs --copy-dir ./matched_pdfs

  # 清空历史结果后重新运行
  python match_records.py --pdfs-dir ./pdfs --csv-file ./data.csv --clean
        """
    )
    
    # 必需参数
    parser.add_argument(
        '--pdfs-dir',
        type=str,
        required=True,
        help='PDF 文件目录'
    )
    
    # 数据源选择
    parser.add_argument(
        '--source',
        type=str,
        default='csv',
        choices=['csv', 'mongodb'],
        help='数据源类型 (默认: csv)'
    )
    
    # CSV 参数
    csv_group = parser.add_argument_group('CSV 数据源参数')
    csv_group.add_argument(
        '--csv-file',
        type=str,
        help='CSV 文件路径（与 --csv-dir 二选一）'
    )
    csv_group.add_argument(
        '--csv-dir',
        type=str,
        help='CSV 文件目录（与 --csv-file 二选一）'
    )
    csv_group.add_argument(
        '--csv-pattern',
        type=str,
        default='*.csv',
        help='CSV 文件名匹配模式 (默认: *.csv)'
    )
    
    # MongoDB 参数
    mongo_group = parser.add_argument_group('MongoDB 数据源参数')
    mongo_group.add_argument(
        '--mongo-uri',
        type=str,
        default='mongodb://localhost:27017',
        help='MongoDB 连接字符串'
    )
    mongo_group.add_argument(
        '--mongo-db',
        type=str,
        help='MongoDB 数据库名称'
    )
    mongo_group.add_argument(
        '--mongo-collection',
        type=str,
        help='MongoDB 集合名称'
    )
    
    # 输出参数
    output_group = parser.add_argument_group('输出参数')
    output_group.add_argument(
        '--output-dir',
        type=str,
        default='./match_results',
        help='输出目录 (默认: ./match_results)'
    )
    output_group.add_argument(
        '--log-dir',
        type=str,
        default='./logs',
        help='日志目录 (默认: ./logs)'
    )
    
    # 扫描参数
    scan_group = parser.add_argument_group('扫描参数')
    scan_group.add_argument(
        '--recursive',
        action='store_true',
        default=True,
        help='递归扫描子目录中的 PDF 文件 (默认: True)'
    )
    scan_group.add_argument(
        '--no-recursive',
        action='store_true',
        help='不递归扫描子目录'
    )
    
    # PDF 复制参数
    copy_group = parser.add_argument_group('PDF 复制参数')
    copy_group.add_argument(
        '--copy-pdfs',
        action='store_true',
        help='复制成功匹配的 PDF 到指定目录'
    )
    copy_group.add_argument(
        '--copy-dir',
        type=str,
        default='./pdfs',
        help='PDF 复制目标目录 (默认: ./pdfs)'
    )
    
    # 清理参数
    clean_group = parser.add_argument_group('清理参数')
    clean_group.add_argument(
        '--clean',
        action='store_true',
        help='运行前清空历史匹配结果 (删除 match_results 目录下的历史数据)'
    )
    
    return parser


def resolve_path(path_str: str, base_dir: Path) -> Path:
    """解析路径"""
    path = Path(path_str)
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    return path


def main():
    """主函数"""
    parser = create_argument_parser()
    args = parser.parse_args()
    
    script_dir = Path(__file__).parent
    
    # 解析路径
    pdfs_dir = resolve_path(args.pdfs_dir, script_dir)
    output_dir = resolve_path(args.output_dir, script_dir)
    log_dir = resolve_path(args.log_dir, script_dir)
    copy_dir = resolve_path(args.copy_dir, script_dir) if args.copy_dir else None
    
    # 设置日志
    logger = setup_logging(log_dir)
    
    # 验证 PDF 目录
    if not pdfs_dir.exists():
        logger.error(f"PDF 目录不存在: {pdfs_dir}")
        sys.exit(1)
    
    # 确定是否递归扫描
    recursive_scan = not args.no_recursive
    
    # 创建应用程序
    app = MatchingApplication(
        pdfs_dir=pdfs_dir,
        output_dir=output_dir,
        logger=logger,
        copy_pdfs=args.copy_pdfs,
        copy_dir=copy_dir,
        recursive_scan=recursive_scan,
        clean_results=args.clean
    )
    
    # 根据数据源类型运行
    if args.source == 'csv':
        # CSV 模式
        csv_file = resolve_path(args.csv_file, script_dir) if args.csv_file else None
        csv_dir = resolve_path(args.csv_dir, script_dir) if args.csv_dir else None
        
        if not csv_file and not csv_dir:
            logger.error("必须指定 --csv-file 或 --csv-dir")
            sys.exit(1)
        
        if csv_file and not csv_file.exists():
            logger.error(f"CSV 文件不存在: {csv_file}")
            sys.exit(1)
        
        if csv_dir and not csv_dir.exists():
            logger.error(f"CSV 目录不存在: {csv_dir}")
            sys.exit(1)
        
        app.run_csv(
            csv_file=csv_file,
            csv_dir=csv_dir,
            csv_pattern=args.csv_pattern
        )
        
    else:
        # MongoDB 模式
        if not args.mongo_db:
            logger.error("MongoDB 模式必须指定 --mongo-db")
            sys.exit(1)
        
        if not args.mongo_collection:
            logger.error("MongoDB 模式必须指定 --mongo-collection")
            sys.exit(1)
        
        app.run_mongodb(
            connection_string=args.mongo_uri,
            database=args.mongo_db,
            collection=args.mongo_collection
        )


if __name__ == '__main__':
    main()
