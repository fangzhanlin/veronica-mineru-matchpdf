#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
结果导出器 (v3.0)

将匹配结果导出为各种格式，支持 PDF 文件复制和重命名。

Author: GitHub Copilot
Date: 2025-01-20
"""

from __future__ import annotations

import csv
import logging
import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING

if TYPE_CHECKING:
    from .matcher import BatchMatchResult, MatchResult
    from .data_sources import FieldMapping


def generate_doi_url(doi: str) -> str:
    """根据 DOI 生成下载链接"""
    if not doi or not doi.strip():
        return ""
    doi = doi.strip()
    if doi.startswith('http'):
        return doi
    return f"https://doi.org/{doi}"


class ResultExporter(ABC):
    """结果导出器抽象基类"""
    
    def __init__(self, output_dir: Path, logger: Optional[logging.Logger] = None):
        self.output_dir = Path(output_dir)
        self.logger = logger or logging.getLogger(__name__)
    
    @abstractmethod
    def export_matched(
        self,
        result: BatchMatchResult,
        headers: List[str],
        field_mapping: FieldMapping
    ) -> Optional[Path]:
        """导出成功匹配的记录"""
        pass
    
    @abstractmethod
    def export_unmatched(
        self,
        result: BatchMatchResult,
        headers: List[str],
        field_mapping: FieldMapping
    ) -> Optional[Path]:
        """导出未匹配的记录"""
        pass
    
    @abstractmethod
    def export_multi_matched(
        self,
        result: BatchMatchResult,
        headers: List[str],
        field_mapping: FieldMapping
    ) -> Optional[Path]:
        """导出多重匹配的记录"""
        pass
    
    def export_all(
        self,
        result: BatchMatchResult,
        headers: List[str],
        field_mapping: FieldMapping
    ) -> Dict[str, Optional[Path]]:
        """导出所有结果"""
        return {
            'matched': self.export_matched(result, headers, field_mapping),
            'unmatched': self.export_unmatched(result, headers, field_mapping),
            'multi_matched': self.export_multi_matched(result, headers, field_mapping),
        }


class CSVExporter(ResultExporter):
    """CSV 格式导出器"""
    
    def __init__(
        self,
        output_dir: Path,
        encoding: str = 'utf-8-sig',
        logger: Optional[logging.Logger] = None
    ):
        super().__init__(output_dir, logger)
        self.encoding = encoding
    
    def export_matched(
        self,
        result: BatchMatchResult,
        headers: List[str],
        field_mapping: FieldMapping
    ) -> Optional[Path]:
        """导出成功匹配的记录"""
        matched_results = result.matched_results
        if not matched_results:
            return None
        
        output_path = self.output_dir / "matched" / f"{result.source_name}_matched.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        new_headers = headers + ['Matched_PDF_Path']
        
        try:
            with open(output_path, 'w', encoding=self.encoding, newline='') as f:
                writer = csv.DictWriter(f, fieldnames=new_headers)
                writer.writeheader()
                
                for match_result in matched_results:
                    row = match_result.record.to_dict()
                    row['Matched_PDF_Path'] = str(match_result.matched_pdf)
                    writer.writerow(row)
            
            self.logger.info(f"保存匹配结果: {output_path} ({len(matched_results)} 条)")
            return output_path
            
        except Exception as e:
            self.logger.error(f"保存匹配 CSV 时出错: {e}")
            raise
    
    def export_unmatched(
        self,
        result: BatchMatchResult,
        headers: List[str],
        field_mapping: FieldMapping
    ) -> Optional[Path]:
        """导出未匹配的记录"""
        unmatched_results = result.unmatched_results
        if not unmatched_results:
            return None
        
        output_path = self.output_dir / "unmatched" / f"{result.source_name}_unmatched.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        new_headers = headers + ['Unmatch_Reason']
        
        try:
            with open(output_path, 'w', encoding=self.encoding, newline='') as f:
                writer = csv.DictWriter(f, fieldnames=new_headers)
                writer.writeheader()
                
                for match_result in unmatched_results:
                    row = match_result.record.to_dict()
                    row['Unmatch_Reason'] = match_result.reason
                    writer.writerow(row)
            
            self.logger.info(f"保存未匹配结果: {output_path} ({len(unmatched_results)} 条)")
            return output_path
            
        except Exception as e:
            self.logger.error(f"保存未匹配 CSV 时出错: {e}")
            raise
    
    def export_multi_matched(
        self,
        result: BatchMatchResult,
        headers: List[str],
        field_mapping: FieldMapping
    ) -> Optional[Path]:
        """导出多重匹配的记录"""
        multi_results = result.multi_matched_results
        if not multi_results:
            return None
        
        output_path = self.output_dir / "multi_matched" / f"{result.source_name}_multi_matched.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        new_headers = headers + ['Matched_PDF_Paths', 'Match_Count']
        
        try:
            with open(output_path, 'w', encoding=self.encoding, newline='') as f:
                writer = csv.DictWriter(f, fieldnames=new_headers)
                writer.writeheader()
                
                for match_result in multi_results:
                    row = match_result.record.to_dict()
                    row['Matched_PDF_Paths'] = '; '.join(
                        str(p) for p in match_result.matched_pdfs
                    )
                    row['Match_Count'] = len(match_result.matched_pdfs)
                    writer.writerow(row)
            
            self.logger.info(f"保存多重匹配结果: {output_path} ({len(multi_results)} 条)")
            return output_path
            
        except Exception as e:
            self.logger.error(f"保存多重匹配 CSV 时出错: {e}")
            raise


class PDFCopier:
    """
    PDF 文件复制器
    
    将成功匹配的 PDF 复制到指定目录，并可按 uuid 重命名。
    """
    
    def __init__(
        self,
        output_dir: Path,
        logger: Optional[logging.Logger] = None
    ):
        self.output_dir = Path(output_dir)
        self.logger = logger or logging.getLogger(__name__)
    
    def copy_matched_pdfs(
        self,
        result: BatchMatchResult,
        uuid_field: str = '',
        overwrite: bool = False
    ) -> Dict[str, Any]:
        """
        复制成功匹配的 PDF 文件
        
        Args:
            result: 匹配结果
            uuid_field: UUID 字段名，如果提供则使用 uuid 作为新文件名
            overwrite: 是否覆盖已存在的文件
            
        Returns:
            复制统计信息
        """
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        stats = {
            'total': 0,
            'copied': 0,
            'skipped': 0,
            'failed': 0,
            'copied_files': [],
            'failed_files': [],
        }
        
        matched_results = result.matched_results
        stats['total'] = len(matched_results)
        
        self.logger.info(f"开始复制 PDF 文件到: {self.output_dir}")
        self.logger.info(f"待复制文件数: {stats['total']}")
        
        for match_result in matched_results:
            src_path = match_result.matched_pdf
            if not src_path or not src_path.exists():
                stats['failed'] += 1
                stats['failed_files'].append((str(src_path), "源文件不存在"))
                continue
            
            # 确定目标文件名
            if uuid_field:
                uuid_value = match_result.record.get(uuid_field, "")
                if uuid_value:
                    # 使用 uuid 作为文件名
                    dst_name = f"{uuid_value}.pdf"
                else:
                    # uuid 为空，使用原文件名
                    dst_name = src_path.name
                    self.logger.warning(
                        f"记录缺少 {uuid_field} 字段，使用原文件名: {dst_name}"
                    )
            else:
                # 不使用 uuid，保持原文件名
                dst_name = src_path.name
            
            dst_path = self.output_dir / dst_name
            
            # 检查是否已存在
            if dst_path.exists() and not overwrite:
                stats['skipped'] += 1
                self.logger.debug(f"跳过已存在文件: {dst_name}")
                continue
            
            # 复制文件
            try:
                shutil.copy2(src_path, dst_path)
                stats['copied'] += 1
                stats['copied_files'].append(str(dst_path))
                self.logger.debug(f"已复制: {src_path.name} -> {dst_name}")
            except Exception as e:
                stats['failed'] += 1
                stats['failed_files'].append((str(src_path), str(e)))
                self.logger.error(f"复制失败 {src_path.name}: {e}")
        
        self.logger.info(
            f"PDF 复制完成: 成功 {stats['copied']}, "
            f"跳过 {stats['skipped']}, 失败 {stats['failed']}"
        )
        
        return stats


class CSVMerger:
    """CSV 文件合并器"""
    
    def __init__(
        self,
        encoding: str = 'utf-8-sig',
        logger: Optional[logging.Logger] = None
    ):
        self.encoding = encoding
        self.logger = logger or logging.getLogger(__name__)
    
    def merge(
        self,
        input_dir: Path,
        output_path: Path,
        add_source_column: bool = True,
        add_doi_link: bool = False,
        deduplicate: bool = False,
        dedup_key: str = 'DOI',
        exclude_keys: Optional[Set[str]] = None
    ) -> int:
        """
        合并目录下所有 CSV 文件
        
        Returns:
            合并的记录总数
        """
        if exclude_keys is None:
            exclude_keys = set()
        
        all_records = []
        all_headers = None
        seen_keys = set()
        
        csv_files = sorted(input_dir.glob("*.csv"))
        
        for csv_file in csv_files:
            try:
                with open(csv_file, 'r', encoding=self.encoding) as f:
                    reader = csv.DictReader(f)
                    headers = list(reader.fieldnames or [])
                    
                    source_name = csv_file.stem.split('_')[0]
                    
                    for row in reader:
                        # 支持大小写不同的字段名
                        key = row.get(dedup_key, '') or row.get(dedup_key.lower(), '')
                        
                        if key in exclude_keys:
                            continue
                        
                        if deduplicate:
                            if key in seen_keys:
                                continue
                            seen_keys.add(key)
                        
                        if add_source_column:
                            row['Source'] = source_name
                        
                        if add_doi_link:
                            doi = row.get('DOI', '') or row.get('doi', '')
                            row['DOI_Download_Link'] = generate_doi_url(doi)
                        
                        all_records.append(row)
                    
                    if all_headers is None:
                        all_headers = list(headers)
                        if add_source_column:
                            all_headers.insert(0, 'Source')
                        if add_doi_link:
                            all_headers.append('DOI_Download_Link')
                            
            except Exception as e:
                self.logger.warning(f"读取 {csv_file} 失败: {e}")
        
        if not all_records:
            self.logger.warning("没有找到任何记录可合并")
            return 0
        
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w', encoding=self.encoding, newline='') as f:
                writer = csv.DictWriter(f, fieldnames=all_headers, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(all_records)
            
            self.logger.info(f"合并完成: {output_path} ({len(all_records)} 条记录)")
            return len(all_records)
            
        except Exception as e:
            self.logger.error(f"保存合并 CSV 时出错: {e}")
            return 0
    
    def collect_matched_keys(
        self,
        matched_csv_path: Path,
        key_column: str = 'DOI'
    ) -> Set[str]:
        """从已匹配 CSV 中收集键值"""
        keys = set()
        
        if not matched_csv_path.exists():
            return keys
        
        try:
            with open(matched_csv_path, 'r', encoding=self.encoding) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # 支持大小写不同的字段名
                    key = row.get(key_column, '') or row.get(key_column.lower(), '')
                    if key:
                        keys.add(key)
            
            self.logger.info(f"从 {matched_csv_path} 收集到 {len(keys)} 个键值")
            
        except Exception as e:
            self.logger.warning(f"收集键值时出错: {e}")
        
        return keys


class SummaryGenerator:
    """汇总报告生成器"""
    
    def __init__(
        self,
        output_dir: Path,
        logger: Optional[logging.Logger] = None
    ):
        self.output_dir = Path(output_dir)
        self.logger = logger or logging.getLogger(__name__)
        self.merger = CSVMerger(logger=logger)
    
    def generate_all_summaries(self) -> Dict[str, int]:
        """生成所有汇总文件"""
        results = {}
        
        matched_dir = self.output_dir / "matched"
        unmatched_dir = self.output_dir / "unmatched"
        multi_matched_dir = self.output_dir / "multi_matched"
        
        # 合并匹配成功的记录
        matched_dois = set()
        if matched_dir.exists():
            all_matched_path = self.output_dir / "ALL_MATCHED.csv"
            matched_count = self.merger.merge(
                input_dir=matched_dir,
                output_path=all_matched_path,
                add_source_column=True,
                add_doi_link=False
            )
            results['matched'] = matched_count
            
            if matched_count > 0:
                self.logger.info(f"✅ 已合并匹配记录: {all_matched_path}")
                matched_dois = self.merger.collect_matched_keys(all_matched_path)
        
        # 合并未匹配的记录
        if unmatched_dir.exists():
            all_unmatched_path = self.output_dir / "ALL_UNMATCHED.csv"
            unmatched_count = self.merger.merge(
                input_dir=unmatched_dir,
                output_path=all_unmatched_path,
                add_source_column=False,
                add_doi_link=True,
                deduplicate=True,
                dedup_key='DOI',
                exclude_keys=matched_dois
            )
            results['unmatched'] = unmatched_count
            
            if unmatched_count > 0:
                self.logger.info(f"❌ 已合并未匹配记录: {all_unmatched_path}")
        
        # 合并多重匹配的记录
        if multi_matched_dir.exists() and list(multi_matched_dir.glob("*.csv")):
            all_multi_path = self.output_dir / "ALL_MULTI_MATCHED.csv"
            multi_count = self.merger.merge(
                input_dir=multi_matched_dir,
                output_path=all_multi_path,
                add_source_column=True,
                add_doi_link=True
            )
            results['multi_matched'] = multi_count
            
            if multi_count > 0:
                self.logger.info(f"⚠️ 已合并多重匹配记录: {all_multi_path}")
        
        return results
