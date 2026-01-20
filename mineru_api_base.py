"""
MinerU API 基类模块
参考官方文档: https://mineru.net/apiManage/docs
参考官方MCP实现: https://github.com/opendatalab/MinerU/tree/main/projects/mcp

提供统一的API调用接口，支持：
- 本地文件上传解析
- URL文件解析
- 批量任务提交与状态轮询
- 结果下载与解压
"""

import asyncio
import aiohttp
import requests
import zipfile
import logging
import os
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, Union, List, Dict, Any
from dataclasses import dataclass, field
from enum import Enum


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mineru_api.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class TaskState(Enum):
    """任务状态枚举"""
    PENDING = "pending"
    PROCESSING = "processing"
    DONE = "done"
    FAILED = "failed"


@dataclass
class FileConfig:
    """文件配置数据类"""
    path: Optional[Path] = None
    name: str = ""
    is_ocr: bool = True
    page_ranges: Optional[str] = None
    
    def to_payload(self) -> Dict[str, Any]:
        """转换为API payload格式"""
        payload = {
            "name": self.name,
            "is_ocr": self.is_ocr,
        }
        if self.page_ranges:
            payload["page_ranges"] = self.page_ranges
        return payload


@dataclass
class TaskResult:
    """任务结果数据类"""
    file_name: str
    status: TaskState
    download_url: Optional[str] = None
    error_message: Optional[str] = None
    markdown_content: Optional[str] = None
    extract_path: Optional[Path] = None


@dataclass
class BatchTaskInfo:
    """批量任务信息数据类"""
    batch_id: str
    uploaded_files: List[str] = field(default_factory=list)
    results: List[TaskResult] = field(default_factory=list)


class MinerUAPIClient:
    """
    MinerU API 客户端基类
    
    用于与 MinerU 云API 交互，将文档转换为 Markdown。
    
    API 端点 (v4):
    - POST /api/v4/file-urls/batch: 获取文件上传URL
    - POST /api/v4/extract/task/batch: 提交URL批量任务
    - GET /api/v4/extract-results/batch/{batch_id}: 获取批量任务状态
    
    使用示例:
    ```python
    client = MinerUAPIClient(api_key="your_api_key")
    
    # 同步方式
    result = client.process_file_sync("path/to/file.pdf", output_dir="./output")
    
    # 异步方式
    result = await client.process_file("path/to/file.pdf", output_dir="./output")
    ```
    """
    
    # 默认API配置
    DEFAULT_API_BASE = "https://mineru.net"
    DEFAULT_MAX_RETRIES = 180
    DEFAULT_RETRY_INTERVAL = 10
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        api_base: Optional[str] = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_interval: int = DEFAULT_RETRY_INTERVAL,
    ):
        """
        初始化 MinerU API 客户端
        
        Args:
            api_key: API密钥，如果不提供则从环境变量MINERU_API_KEY或token.txt读取
            api_base: API基础URL，默认为 https://mineru.net
            max_retries: 最大重试次数
            retry_interval: 重试间隔(秒)
        """
        self.api_key = api_key or self._load_api_key()
        self.api_base = api_base or os.getenv("MINERU_API_BASE", self.DEFAULT_API_BASE)
        self.max_retries = max_retries
        self.retry_interval = retry_interval
        
        if not self.api_key:
            raise ValueError(
                "未找到API密钥。请设置MINERU_API_KEY环境变量，"
                "或在当前目录创建token.txt文件，"
                "或在初始化时传入api_key参数。"
            )
        
        logger.info(f"MinerU API客户端初始化完成，API基础URL: {self.api_base}")
    
    def _load_api_key(self) -> Optional[str]:
        """从环境变量或token.txt文件加载API密钥"""
        # 首先尝试环境变量
        api_key = os.getenv("MINERU_API_KEY")
        if api_key:
            logger.debug("从环境变量加载API密钥")
            return api_key.strip()
        
        # 尝试从当前目录的token.txt读取
        token_paths = [
            Path("token.txt"),
            Path(__file__).parent / "token.txt",
        ]
        
        for token_path in token_paths:
            try:
                if token_path.exists():
                    api_key = token_path.read_text(encoding='utf-8').strip()
                    if api_key:
                        logger.debug(f"从{token_path}加载API密钥")
                        return api_key
            except Exception as e:
                logger.warning(f"读取{token_path}失败: {e}")
        
        return None
    
    def _get_headers(self) -> Dict[str, str]:
        """获取API请求头"""
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        }
    
    # ==================== 异步API方法 ====================
    
    async def _async_request(
        self,
        method: str,
        endpoint: str,
        **kwargs
    ) -> Dict[str, Any]:
        """
        发起异步HTTP请求
        
        Args:
            method: HTTP方法 (GET, POST等)
            endpoint: API端点路径
            **kwargs: 传递给aiohttp的额外参数
            
        Returns:
            API响应JSON
        """
        url = f"{self.api_base}{endpoint}"
        headers = self._get_headers()
        
        if "headers" in kwargs:
            kwargs["headers"].update(headers)
        else:
            kwargs["headers"] = headers
        
        logger.debug(f"API请求: {method} {url}")
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.request(method, url, **kwargs) as response:
                    response.raise_for_status()
                    result = await response.json()
                    logger.debug(f"API响应: {result}")
                    return result
        except aiohttp.ClientResponseError as e:
            logger.error(f"API请求失败: {e.status} - {e.message}")
            raise
        except Exception as e:
            logger.error(f"API请求异常: {e}")
            raise
    
    async def submit_file_task(
        self,
        files: Union[str, Path, List[Union[str, Path]], List[FileConfig]],
        enable_ocr: bool = True,
        language: str = "ch",
        page_ranges: Optional[str] = None,
    ) -> BatchTaskInfo:
        """
        提交本地文件转换任务（异步）
        
        流程：
        1. 调用 /api/v4/file-urls/batch 获取上传URL
        2. 上传文件到OSS
        3. 返回batch_id用于后续状态查询
        
        Args:
            files: 文件路径或FileConfig列表
            enable_ocr: 是否启用OCR
            language: 文档语言 (ch/en/korean/japan等)
            page_ranges: 页码范围，如 "1-5,8,10-12"
            
        Returns:
            BatchTaskInfo: 包含batch_id和上传文件列表
        """
        # 标准化文件配置
        files_config = self._normalize_file_config(files, enable_ocr, page_ranges)
        
        if not files_config:
            raise ValueError("未提供有效的文件")
        
        logger.info(f"准备上传 {len(files_config)} 个文件")
        
        # 步骤1: 构建payload并获取上传URL
        files_payload = [fc.to_payload() for fc in files_config]
        payload = {
            "language": language,
            "files": files_payload,
        }
        
        try:
            response = await self._async_request(
                "POST", "/api/v4/file-urls/batch", json=payload
            )
        except Exception as e:
            logger.error(f"获取上传URL失败: {e}")
            raise
        
        # 检查响应
        if "data" not in response or "batch_id" not in response["data"]:
            raise ValueError(f"获取上传URL失败: {response}")
        
        batch_id = response["data"]["batch_id"]
        file_urls = response["data"].get("file_urls", [])
        
        if len(file_urls) != len(files_config):
            raise ValueError(
                f"上传URL数量 ({len(file_urls)}) 与文件数量 ({len(files_config)}) 不匹配"
            )
        
        logger.info(f"获取上传URL成功，批次ID: {batch_id}")
        
        # 步骤2: 上传文件
        uploaded_files = []
        for file_config, upload_url in zip(files_config, file_urls):
            try:
                await self._upload_file(file_config.path, upload_url)
                uploaded_files.append(file_config.name)
                logger.debug(f"文件 {file_config.name} 上传成功")
            except Exception as e:
                logger.error(f"文件 {file_config.name} 上传失败: {e}")
                raise
        
        logger.info(f"文件上传完成，共 {len(uploaded_files)} 个文件")
        
        return BatchTaskInfo(batch_id=batch_id, uploaded_files=uploaded_files)
    
    async def submit_url_task(
        self,
        urls: Union[str, List[str], List[Dict[str, Any]]],
        enable_ocr: bool = True,
        language: str = "ch",
        page_ranges: Optional[str] = None,
    ) -> BatchTaskInfo:
        """
        提交URL文件转换任务（异步）
        
        Args:
            urls: 文件URL或URL配置列表
            enable_ocr: 是否启用OCR
            language: 文档语言
            page_ranges: 页码范围
            
        Returns:
            BatchTaskInfo: 包含batch_id
        """
        # 标准化URL配置
        urls_config = self._normalize_url_config(urls, enable_ocr, page_ranges)
        
        if not urls_config:
            raise ValueError("未提供有效的URL")
        
        logger.info(f"准备提交 {len(urls_config)} 个URL任务")
        
        payload = {
            "language": language,
            "files": urls_config,
        }
        
        try:
            response = await self._async_request(
                "POST", "/api/v4/extract/task/batch", json=payload
            )
        except Exception as e:
            logger.error(f"提交URL任务失败: {e}")
            raise
        
        if "data" not in response or "batch_id" not in response["data"]:
            raise ValueError(f"提交URL任务失败: {response}")
        
        batch_id = response["data"]["batch_id"]
        uploaded_files = [uc.get("url", "").split("/")[-1] for uc in urls_config]
        
        logger.info(f"URL任务提交成功，批次ID: {batch_id}")
        
        return BatchTaskInfo(batch_id=batch_id, uploaded_files=uploaded_files)
    
    async def get_batch_status(self, batch_id: str) -> Dict[str, Any]:
        """
        获取批量任务状态（异步）
        
        Args:
            batch_id: 批次ID
            
        Returns:
            任务状态信息
        """
        return await self._async_request(
            "GET", f"/api/v4/extract-results/batch/{batch_id}"
        )
    
    async def wait_for_completion(
        self,
        batch_id: str,
        max_retries: Optional[int] = None,
        retry_interval: Optional[int] = None,
    ) -> Dict[str, TaskResult]:
        """
        等待批量任务完成（异步）
        
        Args:
            batch_id: 批次ID
            max_retries: 最大重试次数
            retry_interval: 重试间隔(秒)
            
        Returns:
            文件名到TaskResult的映射
        """
        max_retries = max_retries or self.max_retries
        retry_interval = retry_interval or self.retry_interval
        
        files_status: Dict[str, TaskResult] = {}
        
        logger.info(f"开始轮询任务状态，批次ID: {batch_id}")
        
        for i in range(max_retries):
            try:
                status_info = await self.get_batch_status(batch_id)
            except Exception as e:
                logger.warning(f"获取状态失败 (重试 {i+1}/{max_retries}): {e}")
                await asyncio.sleep(retry_interval)
                continue
            
            extract_results = status_info.get("data", {}).get("extract_result", [])
            
            all_done = True
            for result in extract_results:
                file_name = result.get("file_name")
                if not file_name:
                    continue
                
                state = result.get("state", "pending")
                
                if file_name not in files_status:
                    files_status[file_name] = TaskResult(
                        file_name=file_name,
                        status=TaskState.PENDING
                    )
                
                try:
                    files_status[file_name].status = TaskState(state)
                except ValueError:
                    files_status[file_name].status = TaskState.PENDING
                
                if state == "done":
                    files_status[file_name].download_url = result.get("full_zip_url")
                elif state == "failed":
                    files_status[file_name].error_message = result.get("err_msg", "未知错误")
                
                if state not in ("done", "failed"):
                    all_done = False
            
            # 打印进度
            done_count = sum(1 for r in files_status.values() if r.status in (TaskState.DONE, TaskState.FAILED))
            total_count = len(files_status)
            logger.info(f"任务进度: {done_count}/{total_count} ({i+1}/{max_retries})")
            
            if all_done and files_status:
                logger.info("所有任务已完成")
                break
            
            await asyncio.sleep(retry_interval)
        else:
            logger.warning(f"等待超时，部分任务可能未完成")
        
        return files_status
    
    async def download_and_extract(
        self,
        task_result: TaskResult,
        output_dir: Union[str, Path],
    ) -> Optional[Path]:
        """
        下载并解压任务结果（异步）
        
        Args:
            task_result: 任务结果
            output_dir: 输出目录
            
        Returns:
            解压后的目录路径，失败返回None
        """
        if not task_result.download_url:
            logger.error(f"文件 {task_result.file_name} 没有下载链接")
            return None
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # 从文件名提取目录名（去掉扩展名）
        file_stem = Path(task_result.file_name).stem
        extract_dir = output_path / file_stem
        extract_dir.mkdir(parents=True, exist_ok=True)
        
        zip_path = output_path / f"{file_stem}.zip"
        
        logger.info(f"下载文件: {task_result.file_name}")
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    task_result.download_url,
                    headers=self._get_headers()
                ) as response:
                    response.raise_for_status()
                    content = await response.read()
                    
                    with open(zip_path, "wb") as f:
                        f.write(content)
            
            # 解压
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(extract_dir)
            
            # 删除zip文件
            zip_path.unlink()
            
            # 读取Markdown内容
            md_files = list(extract_dir.rglob("*.md"))
            if md_files:
                task_result.markdown_content = md_files[0].read_text(encoding='utf-8')
            
            task_result.extract_path = extract_dir
            
            logger.info(f"文件 {task_result.file_name} 解压完成: {extract_dir}")
            return extract_dir
            
        except Exception as e:
            logger.error(f"下载或解压失败 {task_result.file_name}: {e}")
            # 清理临时文件
            if zip_path.exists():
                try:
                    zip_path.unlink()
                except:
                    pass
            return None
    
    async def process_file(
        self,
        files: Union[str, Path, List[Union[str, Path]]],
        output_dir: Union[str, Path],
        enable_ocr: bool = True,
        language: str = "ch",
        page_ranges: Optional[str] = None,
    ) -> BatchTaskInfo:
        """
        完整处理文件：上传 -> 等待 -> 下载（异步）
        
        Args:
            files: 文件路径或路径列表
            output_dir: 输出目录
            enable_ocr: 是否启用OCR
            language: 文档语言
            page_ranges: 页码范围
            
        Returns:
            BatchTaskInfo: 包含所有处理结果
        """
        # 提交任务
        task_info = await self.submit_file_task(
            files, enable_ocr, language, page_ranges
        )
        
        # 等待完成
        results = await self.wait_for_completion(task_info.batch_id)
        
        # 下载并解压
        for file_name, task_result in results.items():
            if task_result.status == TaskState.DONE:
                await self.download_and_extract(task_result, output_dir)
            task_info.results.append(task_result)
        
        return task_info
    
    # ==================== 同步API方法 ====================
    
    def _sync_request(
        self,
        method: str,
        endpoint: str,
        **kwargs
    ) -> Dict[str, Any]:
        """发起同步HTTP请求"""
        url = f"{self.api_base}{endpoint}"
        headers = self._get_headers()
        
        if "headers" in kwargs:
            kwargs["headers"].update(headers)
        else:
            kwargs["headers"] = headers
        
        logger.debug(f"API请求: {method} {url}")
        
        try:
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
            result = response.json()
            logger.debug(f"API响应: {result}")
            return result
        except requests.RequestException as e:
            logger.error(f"API请求失败: {e}")
            raise
    
    def submit_file_task_sync(
        self,
        files: Union[str, Path, List[Union[str, Path]], List[FileConfig]],
        enable_ocr: bool = True,
        language: str = "ch",
        page_ranges: Optional[str] = None,
    ) -> BatchTaskInfo:
        """提交本地文件转换任务（同步）"""
        files_config = self._normalize_file_config(files, enable_ocr, page_ranges)
        
        if not files_config:
            raise ValueError("未提供有效的文件")
        
        logger.info(f"准备上传 {len(files_config)} 个文件")
        
        files_payload = [fc.to_payload() for fc in files_config]
        payload = {
            "language": language,
            "files": files_payload,
        }
        
        try:
            response = self._sync_request(
                "POST", "/api/v4/file-urls/batch", json=payload
            )
        except Exception as e:
            logger.error(f"获取上传URL失败: {e}")
            raise
        
        if "data" not in response or "batch_id" not in response["data"]:
            raise ValueError(f"获取上传URL失败: {response}")
        
        batch_id = response["data"]["batch_id"]
        file_urls = response["data"].get("file_urls", [])
        
        if len(file_urls) != len(files_config):
            raise ValueError(
                f"上传URL数量 ({len(file_urls)}) 与文件数量 ({len(files_config)}) 不匹配"
            )
        
        logger.info(f"获取上传URL成功，批次ID: {batch_id}")
        
        uploaded_files = []
        for file_config, upload_url in zip(files_config, file_urls):
            try:
                self._upload_file_sync(file_config.path, upload_url)
                uploaded_files.append(file_config.name)
                logger.debug(f"文件 {file_config.name} 上传成功")
            except Exception as e:
                logger.error(f"文件 {file_config.name} 上传失败: {e}")
                raise
        
        logger.info(f"文件上传完成，共 {len(uploaded_files)} 个文件")
        
        return BatchTaskInfo(batch_id=batch_id, uploaded_files=uploaded_files)
    
    def get_batch_status_sync(self, batch_id: str) -> Dict[str, Any]:
        """获取批量任务状态（同步）"""
        return self._sync_request(
            "GET", f"/api/v4/extract-results/batch/{batch_id}"
        )
    
    def wait_for_completion_sync(
        self,
        batch_id: str,
        max_retries: Optional[int] = None,
        retry_interval: Optional[int] = None,
    ) -> Dict[str, TaskResult]:
        """等待批量任务完成（同步）"""
        max_retries = max_retries or self.max_retries
        retry_interval = retry_interval or self.retry_interval
        
        files_status: Dict[str, TaskResult] = {}
        
        logger.info(f"开始轮询任务状态，批次ID: {batch_id}")
        
        for i in range(max_retries):
            try:
                status_info = self.get_batch_status_sync(batch_id)
            except Exception as e:
                logger.warning(f"获取状态失败 (重试 {i+1}/{max_retries}): {e}")
                time.sleep(retry_interval)
                continue
            
            extract_results = status_info.get("data", {}).get("extract_result", [])
            
            all_done = True
            for result in extract_results:
                file_name = result.get("file_name")
                if not file_name:
                    continue
                
                state = result.get("state", "pending")
                
                if file_name not in files_status:
                    files_status[file_name] = TaskResult(
                        file_name=file_name,
                        status=TaskState.PENDING
                    )
                
                try:
                    files_status[file_name].status = TaskState(state)
                except ValueError:
                    files_status[file_name].status = TaskState.PENDING
                
                if state == "done":
                    files_status[file_name].download_url = result.get("full_zip_url")
                elif state == "failed":
                    files_status[file_name].error_message = result.get("err_msg", "未知错误")
                
                if state not in ("done", "failed"):
                    all_done = False
            
            done_count = sum(1 for r in files_status.values() if r.status in (TaskState.DONE, TaskState.FAILED))
            total_count = len(files_status)
            logger.info(f"任务进度: {done_count}/{total_count} ({i+1}/{max_retries})")
            
            if all_done and files_status:
                logger.info("所有任务已完成")
                break
            
            time.sleep(retry_interval)
        else:
            logger.warning(f"等待超时，部分任务可能未完成")
        
        return files_status
    
    def download_and_extract_sync(
        self,
        task_result: TaskResult,
        output_dir: Union[str, Path],
    ) -> Optional[Path]:
        """下载并解压任务结果（同步）"""
        if not task_result.download_url:
            logger.error(f"文件 {task_result.file_name} 没有下载链接")
            return None
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        file_stem = Path(task_result.file_name).stem
        extract_dir = output_path / file_stem
        extract_dir.mkdir(parents=True, exist_ok=True)
        
        zip_path = output_path / f"{file_stem}.zip"
        
        logger.info(f"下载文件: {task_result.file_name}")
        
        try:
            response = requests.get(
                task_result.download_url,
                headers=self._get_headers()
            )
            response.raise_for_status()
            
            with open(zip_path, "wb") as f:
                f.write(response.content)
            
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(extract_dir)
            
            zip_path.unlink()
            
            md_files = list(extract_dir.rglob("*.md"))
            if md_files:
                task_result.markdown_content = md_files[0].read_text(encoding='utf-8')
            
            task_result.extract_path = extract_dir
            
            logger.info(f"文件 {task_result.file_name} 解压完成: {extract_dir}")
            return extract_dir
            
        except Exception as e:
            logger.error(f"下载或解压失败 {task_result.file_name}: {e}")
            if zip_path.exists():
                try:
                    zip_path.unlink()
                except:
                    pass
            return None
    
    def process_file_sync(
        self,
        files: Union[str, Path, List[Union[str, Path]]],
        output_dir: Union[str, Path],
        enable_ocr: bool = True,
        language: str = "ch",
        page_ranges: Optional[str] = None,
    ) -> BatchTaskInfo:
        """完整处理文件（同步）"""
        task_info = self.submit_file_task_sync(
            files, enable_ocr, language, page_ranges
        )
        
        results = self.wait_for_completion_sync(task_info.batch_id)
        
        for file_name, task_result in results.items():
            if task_result.status == TaskState.DONE:
                self.download_and_extract_sync(task_result, output_dir)
            task_info.results.append(task_result)
        
        return task_info
    
    # ==================== 辅助方法 ====================
    
    def _normalize_file_config(
        self,
        files: Union[str, Path, List[Union[str, Path]], List[FileConfig]],
        enable_ocr: bool,
        page_ranges: Optional[str],
    ) -> List[FileConfig]:
        """将各种文件输入格式标准化为FileConfig列表"""
        configs = []
        
        if isinstance(files, (str, Path)):
            files = [files]
        
        for f in files:
            if isinstance(f, FileConfig):
                configs.append(f)
            elif isinstance(f, (str, Path)):
                path = Path(f)
                if not path.exists():
                    raise FileNotFoundError(f"文件不存在: {path}")
                configs.append(FileConfig(
                    path=path,
                    name=path.name,
                    is_ocr=enable_ocr,
                    page_ranges=page_ranges,
                ))
            else:
                raise TypeError(f"不支持的文件类型: {type(f)}")
        
        return configs
    
    def _normalize_url_config(
        self,
        urls: Union[str, List[str], List[Dict[str, Any]]],
        enable_ocr: bool,
        page_ranges: Optional[str],
    ) -> List[Dict[str, Any]]:
        """将各种URL输入格式标准化"""
        configs = []
        
        if isinstance(urls, str):
            urls = [urls]
        
        for url in urls:
            if isinstance(url, str):
                config = {"url": url, "is_ocr": enable_ocr}
                if page_ranges:
                    config["page_ranges"] = page_ranges
                configs.append(config)
            elif isinstance(url, dict):
                if "url" not in url:
                    raise ValueError(f"URL配置缺少'url'字段: {url}")
                config = {
                    "url": url["url"],
                    "is_ocr": url.get("is_ocr", enable_ocr),
                }
                if url.get("page_ranges") or page_ranges:
                    config["page_ranges"] = url.get("page_ranges", page_ranges)
                configs.append(config)
            else:
                raise TypeError(f"不支持的URL类型: {type(url)}")
        
        return configs
    
    async def _upload_file(self, file_path: Path, upload_url: str) -> None:
        """异步上传文件到OSS"""
        try:
            import httpx
            
            with open(file_path, "rb") as f:
                content = f.read()
            
            # 使用httpx替代aiohttp，行为更接近requests
            # aiohttp会自动添加headers导致OSS签名验证失败
            async with httpx.AsyncClient() as client:
                response = await client.put(upload_url, content=content)
                if response.status_code != 200:
                    raise ValueError(f"上传失败 ({response.status_code}): {response.text}")
        except Exception as e:
            logger.error(f"文件 {file_path.name} 上传失败: {e}")
            raise

    def _upload_file_sync(self, file_path: Path, upload_url: str) -> None:
        """同步上传文件到OSS"""
        try:
            # 重要：不设置Content-Type，让OSS根据预签名URL自动处理
            with open(file_path, "rb") as f:
                response = requests.put(upload_url, data=f)
                if response.status_code != 200:
                    raise ValueError(f"上传失败 ({response.status_code}): {response.text}")
        except Exception as e:
            logger.error(f"文件上传异常: {e}")
            raise


class BaseBatchProcessor(ABC):
    """
    批量处理器抽象基类
    
    用于实现不同场景的批量PDF转Markdown处理逻辑。
    继承此类并实现抽象方法来自定义处理行为。
    """
    
    def __init__(
        self,
        client: Optional[MinerUAPIClient] = None,
        **client_kwargs
    ):
        """
        初始化批量处理器
        
        Args:
            client: MinerUAPIClient实例，如果不提供则创建新实例
            **client_kwargs: 传递给MinerUAPIClient的参数
        """
        self.client = client or MinerUAPIClient(**client_kwargs)
    
    @abstractmethod
    def find_files(self) -> List[Dict[str, Any]]:
        """
        查找需要处理的文件
        
        Returns:
            文件信息字典列表，每个字典至少包含:
            - 'path': 文件完整路径
            - 'output_dir': 输出目录
        """
        pass
    
    @abstractmethod
    def is_processed(self, file_info: Dict[str, Any]) -> bool:
        """
        检查文件是否已处理
        
        Args:
            file_info: find_files返回的文件信息字典
            
        Returns:
            是否已处理
        """
        pass
    
    def on_file_success(self, file_info: Dict[str, Any], result: TaskResult) -> None:
        """文件处理成功回调，子类可覆盖"""
        logger.info(f"✅ 处理成功: {file_info['path']}")
    
    def on_file_error(self, file_info: Dict[str, Any], error: Exception) -> None:
        """文件处理失败回调，子类可覆盖"""
        logger.error(f"❌ 处理失败: {file_info['path']} - {error}")
    
    def process_all_sync(
        self,
        enable_ocr: bool = True,
        language: str = "ch",
        skip_processed: bool = True,
    ) -> Dict[str, Any]:
        """
        同步处理所有文件
        
        Args:
            enable_ocr: 是否启用OCR
            language: 文档语言
            skip_processed: 是否跳过已处理的文件
            
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
        }
        
        for file_info in files:
            try:
                if skip_processed and self.is_processed(file_info):
                    stats["skipped"] += 1
                    logger.info(f"⏭️ 跳过已处理: {file_info['path']}")
                    continue
                
                task_info = self.client.process_file_sync(
                    file_info['path'],
                    file_info['output_dir'],
                    enable_ocr=enable_ocr,
                    language=language,
                )
                
                for result in task_info.results:
                    if result.status == TaskState.DONE:
                        stats["success"] += 1
                        self.on_file_success(file_info, result)
                    else:
                        stats["failed"] += 1
                        self.on_file_error(file_info, Exception(result.error_message))
                        stats["errors"].append({
                            "file": str(file_info['path']),
                            "error": result.error_message,
                        })
                        
            except Exception as e:
                stats["failed"] += 1
                self.on_file_error(file_info, e)
                stats["errors"].append({
                    "file": str(file_info['path']),
                    "error": str(e),
                })
        
        return stats
    
    async def process_all_async(
        self,
        enable_ocr: bool = True,
        language: str = "ch",
        skip_processed: bool = True,
    ) -> Dict[str, Any]:
        """
        异步处理所有文件
        
        Args:
            enable_ocr: 是否启用OCR
            language: 文档语言
            skip_processed: 是否跳过已处理的文件
            
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
        }
        
        for file_info in files:
            try:
                if skip_processed and self.is_processed(file_info):
                    stats["skipped"] += 1
                    logger.info(f"⏭️ 跳过已处理: {file_info['path']}")
                    continue
                
                task_info = await self.client.process_file(
                    file_info['path'],
                    file_info['output_dir'],
                    enable_ocr=enable_ocr,
                    language=language,
                )
                
                for result in task_info.results:
                    if result.status == TaskState.DONE:
                        stats["success"] += 1
                        self.on_file_success(file_info, result)
                    else:
                        stats["failed"] += 1
                        self.on_file_error(file_info, Exception(result.error_message))
                        stats["errors"].append({
                            "file": str(file_info['path']),
                            "error": result.error_message,
                        })
                        
            except Exception as e:
                stats["failed"] += 1
                self.on_file_error(file_info, e)
                stats["errors"].append({
                    "file": str(file_info['path']),
                    "error": str(e),
                })
        
        return stats


# 测试代码
if __name__ == "__main__":
    # 简单测试
    try:
        client = MinerUAPIClient()
        print(f"客户端初始化成功")
        print(f"API基础URL: {client.api_base}")
    except Exception as e:
        print(f"初始化失败: {e}")
