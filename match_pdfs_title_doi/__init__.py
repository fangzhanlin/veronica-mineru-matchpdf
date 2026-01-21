#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
match_pdfs_title_doi 包 (v3.0)

文献数据匹配工具包，支持多种数据源（CSV、MongoDB）。

主要特性:
- 自动检测 PDF 文件名格式（DOI 格式 / 年份格式）
- 自动处理特殊编码（#x3f; 等）
- 统一的字段映射（CSV: Title/DOI, MongoDB: label/doi/uuid）
- 支持复制匹配的 PDF 并以 uuid 重命名
"""

from .data_sources import (
    CSVDataSource,
    DataSource,
    DataSourceResult,
    FieldMapping,
    MongoDBDataSource,
    Record,
    create_data_source,
    CSV_FIELD_MAPPING,
    MONGODB_FIELD_MAPPING,
)
from .exporters import (
    CSVExporter,
    CSVMerger,
    PDFCopier,
    ResultExporter,
    SummaryGenerator,
    generate_doi_url,
)
from .matcher import (
    BatchMatchResult,
    MatchResult,
    MatchStatus,
    PDFMatcher,
    PDFNameAnalyzer,
    TextNormalizer,
)

__all__ = [
    # 数据源
    'DataSource',
    'CSVDataSource',
    'MongoDBDataSource',
    'DataSourceResult',
    'Record',
    'FieldMapping',
    'CSV_FIELD_MAPPING',
    'MONGODB_FIELD_MAPPING',
    'create_data_source',
    # 匹配器
    'PDFMatcher',
    'PDFNameAnalyzer',
    'BatchMatchResult',
    'MatchResult',
    'MatchStatus',
    'TextNormalizer',
    # 导出器
    'ResultExporter',
    'CSVExporter',
    'CSVMerger',
    'SummaryGenerator',
    'PDFCopier',
    'generate_doi_url',
]

__version__ = '3.0.1'
