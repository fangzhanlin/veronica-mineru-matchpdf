#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PDF 文献匹配引擎 (简化版 v3.0)

核心匹配逻辑，将 PDF 文件与文献记录进行匹配。
自动检测文件名格式，无需按期刊区分。

Author: GitHub Copilot
Date: 2025-01-20
"""

import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from .data_sources import DataSourceResult, Record
except ImportError:
    from data_sources import DataSourceResult, Record


class MatchStatus(Enum):
    """匹配状态"""
    MATCHED = auto()
    UNMATCHED = auto()
    MULTI_MATCHED = auto()


@dataclass
class MatchResult:
    """
    单条记录的匹配结果
    """
    record_index: int
    record: Record
    status: MatchStatus
    matched_pdfs: List[Path] = field(default_factory=list)
    reason: str = ""
    
    @property
    def is_matched(self) -> bool:
        return self.status == MatchStatus.MATCHED
    
    @property
    def is_multi_matched(self) -> bool:
        return self.status == MatchStatus.MULTI_MATCHED
    
    @property
    def matched_pdf(self) -> Optional[Path]:
        """返回唯一匹配的 PDF（仅当 status 为 MATCHED 时有效）"""
        if self.status == MatchStatus.MATCHED and self.matched_pdfs:
            return self.matched_pdfs[0]
        return None


@dataclass
class BatchMatchResult:
    """
    批量匹配结果汇总
    """
    source_name: str
    total_records: int
    total_pdfs: int
    results: List[MatchResult] = field(default_factory=list)
    
    @property
    def matched_results(self) -> List[MatchResult]:
        return [r for r in self.results if r.status == MatchStatus.MATCHED]
    
    @property
    def unmatched_results(self) -> List[MatchResult]:
        return [r for r in self.results if r.status == MatchStatus.UNMATCHED]
    
    @property
    def multi_matched_results(self) -> List[MatchResult]:
        return [r for r in self.results if r.status == MatchStatus.MULTI_MATCHED]
    
    @property
    def matched_count(self) -> int:
        return len(self.matched_results)
    
    @property
    def unmatched_count(self) -> int:
        return len(self.unmatched_results)
    
    @property
    def multi_matched_count(self) -> int:
        return len(self.multi_matched_results)
    
    @property
    def match_rate(self) -> float:
        """匹配率（相对于记录数量）"""
        if self.total_records == 0:
            return 0.0
        return self.matched_count / self.total_records
    
    def to_stats_dict(self) -> Dict[str, int]:
        """转换为统计字典"""
        return {
            'total_records': self.total_records,
            'total_pdfs': self.total_pdfs,
            'matched': self.matched_count,
            'unmatched': self.unmatched_count,
            'multi_matched': self.multi_matched_count,
        }


class TextNormalizer:
    """
    文本标准化工具类
    """
    
    @staticmethod
    def normalize(text: str, remove_numbers: bool = True) -> str:
        """
        标准化文本用于匹配
        
        Args:
            text: 原始文本
            remove_numbers: 是否移除数字
            
        Returns:
            标准化后的文本（纯小写字母，可选保留数字）
        """
        if not text:
            return ""
        
        text = text.lower()
        
        if remove_numbers:
            return re.sub(r'[^a-z]', '', text)
        else:
            return re.sub(r'[^a-z0-9]', '', text)
    
    @staticmethod
    def remove_special_encoding(filename: str) -> str:
        """
        移除文件名中的特殊编码（如 #x3a; #x3f; 等）
        
        编码格式: #x 后紧跟十六进制字符，以 ; 结尾
        """
        return re.sub(r'#x[0-9a-fA-F]+;', '', filename)


class PDFNameAnalyzer:
    """
    PDF 文件名分析器
    
    自动检测文件名格式并提取用于匹配的部分。
    """
    
    # DOI 前缀模式
    DOI_PREFIXES = [
        'isj.',           # ISJ 期刊
        'j.1365-2575',    # ISJ 旧格式
        '10.',            # 标准 DOI
    ]
    
    @classmethod
    def analyze(cls, pdf_name: str) -> Tuple[str, str, bool]:
        """
        分析 PDF 文件名，返回用于匹配的文本
        
        Args:
            pdf_name: PDF 文件名（不含扩展名）
            
        Returns:
            (normalized_for_title, normalized_for_doi, is_doi_format)
            - normalized_for_title: 用于 Title 匹配的标准化文本
            - normalized_for_doi: 用于 DOI 匹配的标准化文本
            - is_doi_format: 是否为 DOI 格式的文件名
        """
        # 1. 先移除特殊编码
        cleaned_name = TextNormalizer.remove_special_encoding(pdf_name)
        
        # 2. 检查是否为 DOI 格式
        is_doi = cls._is_doi_format(cleaned_name)
        
        if is_doi:
            # DOI 格式：构建完整 DOI 并标准化
            full_doi = cls._build_full_doi(cleaned_name)
            normalized_doi = TextNormalizer.normalize(full_doi, remove_numbers=False)
            # 对于 DOI 格式，Title 匹配用原文件名
            normalized_title = TextNormalizer.normalize(cleaned_name, remove_numbers=True)
            return normalized_title, normalized_doi, True
        else:
            # 非 DOI 格式：提取标题部分
            title_part = cls._extract_title_part(cleaned_name)
            normalized_title = TextNormalizer.normalize(title_part, remove_numbers=True)
            # 非 DOI 格式也生成 DOI 匹配文本（以防万一）
            normalized_doi = TextNormalizer.normalize(cleaned_name, remove_numbers=False)
            return normalized_title, normalized_doi, False
    
    @classmethod
    def _is_doi_format(cls, name: str) -> bool:
        """检查是否为 DOI 格式的文件名"""
        name_lower = name.lower()
        for prefix in cls.DOI_PREFIXES:
            if name_lower.startswith(prefix):
                return True
        return False
    
    @classmethod
    def _build_full_doi(cls, name: str) -> str:
        """从文件名构建完整 DOI"""
        if name.startswith('isj.'):
            return f"10.1111/{name}"
        if name.startswith('j.1365-2575'):
            return f"10.1111/{name}"
        if name.startswith('10.'):
            return name
        return name
    
    @classmethod
    def _extract_title_part(cls, name: str) -> str:
        """
        从文件名中提取标题部分
        
        自动检测是否包含年份模式（如 _2024_期刊名）
        """
        # 查找 _年份_ 模式（4位数字前后有下划线）
        match = re.search(r'_(\d{4})_', name)
        if match:
            return name[:match.start()]
        
        # 查找 _年份 结尾模式
        match = re.search(r'_(\d{4})$', name)
        if match:
            return name[:match.start()]
        
        # 无年份模式，返回原名称
        return name


class PDFScanner:
    """
    PDF 文件扫描器
    """
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
    
    def scan_directory(self, directory: Path, recursive: bool = False) -> Dict[str, Path]:
        """
        扫描目录获取所有 PDF 文件
        
        Args:
            directory: 要扫描的目录
            recursive: 是否递归扫描子目录
            
        Returns:
            {文件名（不含扩展名）: 完整路径} 的字典
        """
        pdf_files = {}
        
        if not directory.exists():
            self.logger.warning(f"目录不存在: {directory}")
            return pdf_files
        
        try:
            pattern = '**/*.pdf' if recursive else '*.pdf'
            for item in directory.glob(pattern):
                if item.is_file():
                    pdf_files[item.stem] = item
        except Exception as e:
            self.logger.error(f"扫描目录 {directory} 时出错: {e}")
        
        return pdf_files


class PDFMatcher:
    """
    PDF 文献匹配引擎 (简化版)
    
    核心匹配逻辑，自动检测文件名格式进行匹配。
    同时尝试 Title 和 DOI 匹配，优先使用匹配成功的方式。
    """
    
    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        title_column: str = 'Title',
        doi_column: str = 'DOI'
    ):
        """
        初始化匹配引擎
        
        Args:
            logger: 日志记录器
            title_column: 数据源中的标题列名（CSV 用 'Title'，MongoDB 用 'label'）
            doi_column: 数据源中的 DOI 列名（CSV 用 'DOI'，MongoDB 用 'doi'）
        """
        self.logger = logger or logging.getLogger(__name__)
        self.pdf_scanner = PDFScanner(logger)
        self.title_column = title_column
        self.doi_column = doi_column
    
    def match_all(
        self,
        pdfs_dir: Path,
        data_result: DataSourceResult,
        interactive: bool = False,
        recursive_scan: bool = False
    ) -> BatchMatchResult:
        """
        匹配所有记录
        
        Args:
            pdfs_dir: PDF 文件目录
            data_result: 数据源查询结果
            interactive: 是否交互模式
            recursive_scan: 是否递归扫描子目录
            
        Returns:
            BatchMatchResult 匹配结果
        """
        self.logger.info(f"\n{'='*60}")
        self.logger.info("开始匹配")
        self.logger.info(f"{'='*60}")
        self.logger.info(f"PDF 目录: {pdfs_dir}")
        self.logger.info(f"Title 列: {self.title_column}, DOI 列: {self.doi_column}")
        
        # 扫描 PDF 文件
        pdf_files = self.pdf_scanner.scan_directory(pdfs_dir, recursive=recursive_scan)
        self.logger.info(f"找到 {len(pdf_files)} 个 PDF 文件")
        self.logger.info(f"读取到 {len(data_result.records)} 条记录")
        
        # 创建结果对象
        result = BatchMatchResult(
            source_name=data_result.source_name,
            total_records=len(data_result.records),
            total_pdfs=len(pdf_files)
        )
        
        if not pdf_files:
            self.logger.warning("未找到任何 PDF 文件")
            for idx, record in enumerate(data_result.records):
                result.results.append(MatchResult(
                    record_index=idx,
                    record=record,
                    status=MatchStatus.UNMATCHED,
                    reason="目录中没有 PDF 文件"
                ))
            return result
        
        # 预处理 PDF 文件名：同时生成 Title 和 DOI 匹配的索引
        title_index: Dict[str, List[Tuple[str, Path]]] = defaultdict(list)
        doi_index: Dict[str, List[Tuple[str, Path]]] = defaultdict(list)
        
        self.logger.info("分析 PDF 文件名...")
        for pdf_name, pdf_path in pdf_files.items():
            norm_title, norm_doi, is_doi = PDFNameAnalyzer.analyze(pdf_name)
            
            # DOI 格式的文件名不应参与 Title 匹配（移除数字后可能太短，容易误匹配）
            # 同时要求 Title 索引的字符串至少有 10 个字符
            if norm_title and not is_doi and len(norm_title) >= 10:
                title_index[norm_title].append((pdf_name, pdf_path))
            if norm_doi:
                doi_index[norm_doi].append((pdf_name, pdf_path))
            
            self.logger.debug(
                f"PDF: {pdf_name} -> Title索引: {norm_title[:30]}..., "
                f"DOI索引: {norm_doi[:30]}..., DOI格式: {is_doi}"
            )
        
        # 遍历记录进行匹配
        self.logger.info("开始匹配记录...")
        for idx, record in enumerate(data_result.records):
            match_result = self._match_single_record(
                idx, record, title_index, doi_index
            )
            result.results.append(match_result)
        
        # 打印统计
        self._log_statistics(result)
        
        return result
    
    def _match_single_record(
        self,
        idx: int,
        record: Record,
        title_index: Dict[str, List[Tuple[str, Path]]],
        doi_index: Dict[str, List[Tuple[str, Path]]]
    ) -> MatchResult:
        """匹配单条记录"""
        
        # 获取 Title 和 DOI 值
        title_value = record.get(self.title_column, "")
        doi_value = record.get(self.doi_column, "")
        
        matching_pdfs = []
        match_method = ""
        
        # 1. 优先尝试 DOI 匹配（更精确）
        if doi_value:
            norm_doi = TextNormalizer.normalize(str(doi_value), remove_numbers=False)
            # DOI 必须有足够长度才进行匹配（避免短字符串误匹配）
            if len(norm_doi) >= 5:
                for pdf_norm_doi, pdf_list in doi_index.items():
                    # DOI 完全匹配，或 PDF 的 DOI 包含在记录的 DOI 中（而非反过来）
                    if pdf_norm_doi and len(pdf_norm_doi) >= 5 and (
                        norm_doi == pdf_norm_doi or 
                        pdf_norm_doi in norm_doi
                    ):
                        matching_pdfs.extend([path for _, path in pdf_list])
                        match_method = "DOI"
        
        # 2. 如果 DOI 未匹配到，尝试 Title 匹配
        if not matching_pdfs and title_value:
            norm_title = TextNormalizer.normalize(str(title_value), remove_numbers=True)
            for pdf_norm_title, pdf_list in title_index.items():
                # Title 匹配：记录标题以 PDF 文件名开头
                if pdf_norm_title and norm_title.startswith(pdf_norm_title):
                    matching_pdfs.extend([path for _, path in pdf_list])
                    match_method = "Title"
        
        # 去重（同一个 PDF 可能在两个索引中都存在）
        matching_pdfs = list(dict.fromkeys(matching_pdfs))
        
        # 处理匹配结果
        if len(matching_pdfs) == 0:
            self.logger.debug(f"记录 {idx + 1}: 未找到匹配的 PDF")
            return MatchResult(
                record_index=idx,
                record=record,
                status=MatchStatus.UNMATCHED,
                reason="未找到匹配的 PDF 文件"
            )
        elif len(matching_pdfs) == 1:
            self.logger.info(
                f"记录 {idx + 1}: 成功匹配 ({match_method}) -> '{matching_pdfs[0].name}'"
            )
            return MatchResult(
                record_index=idx,
                record=record,
                status=MatchStatus.MATCHED,
                matched_pdfs=matching_pdfs
            )
        else:
            self.logger.warning(
                f"记录 {idx + 1}: 匹配到多个 PDF: {[f.name for f in matching_pdfs]}"
            )
            return MatchResult(
                record_index=idx,
                record=record,
                status=MatchStatus.MULTI_MATCHED,
                matched_pdfs=matching_pdfs,
                reason=f"匹配到 {len(matching_pdfs)} 个 PDF 文件"
            )
    
    def _log_statistics(self, result: BatchMatchResult):
        """输出统计信息"""
        self.logger.info(f"\n匹配统计:")
        self.logger.info(f"  记录数: {result.total_records}")
        self.logger.info(f"  PDF 文件数: {result.total_pdfs}")
        self.logger.info(f"  成功匹配: {result.matched_count}")
        self.logger.info(f"  未匹配:   {result.unmatched_count}")
        self.logger.info(f"  多重匹配: {result.multi_matched_count}")
        if result.total_records > 0:
            self.logger.info(f"  匹配率:   {result.match_rate*100:.1f}%")
