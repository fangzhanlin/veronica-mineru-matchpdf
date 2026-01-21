#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
单元测试脚本 - 测试 match_pdfs_title_doi 模块

测试内容:
1. TextNormalizer - 文本标准化
2. PDFNameAnalyzer - PDF 文件名分析
3. FieldMapping - 字段映射
4. Record / DataSourceResult - 数据结构
5. CSVDataSource - CSV 数据源（需要测试文件）
6. PDFMatcher - 核心匹配逻辑
7. CSVExporter - 结果导出
"""

import sys
import tempfile
import csv
from pathlib import Path

# 确保可以导入模块
sys.path.insert(0, str(Path(__file__).parent))

from data_sources import (
    FieldMapping,
    Record,
    DataSourceResult,
    CSVDataSource,
    CSV_FIELD_MAPPING,
    MONGODB_FIELD_MAPPING,
)
from matcher import (
    TextNormalizer,
    PDFNameAnalyzer,
    PDFMatcher,
    MatchStatus,
    BatchMatchResult,
)
from exporters import (
    generate_doi_url,
    CSVExporter,
    PDFCopier,
    CSVMerger,
    SummaryGenerator,
)


class TestRunner:
    """简单的测试运行器"""
    
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []
    
    def assert_equal(self, actual, expected, msg=""):
        """断言相等"""
        if actual == expected:
            self.passed += 1
            print(f"  ✅ {msg}" if msg else "  ✅ PASS")
        else:
            self.failed += 1
            error_msg = f"  ❌ {msg}: 期望 {expected!r}, 实际 {actual!r}"
            self.errors.append(error_msg)
            print(error_msg)
    
    def assert_true(self, condition, msg=""):
        """断言为真"""
        if condition:
            self.passed += 1
            print(f"  ✅ {msg}" if msg else "  ✅ PASS")
        else:
            self.failed += 1
            error_msg = f"  ❌ {msg}: 期望 True, 实际 False"
            self.errors.append(error_msg)
            print(error_msg)
    
    def assert_false(self, condition, msg=""):
        """断言为假"""
        self.assert_true(not condition, msg)
    
    def assert_raises(self, exception_type, func, msg=""):
        """断言抛出异常"""
        try:
            func()
            self.failed += 1
            error_msg = f"  ❌ {msg}: 期望抛出 {exception_type.__name__}, 但未抛出"
            self.errors.append(error_msg)
            print(error_msg)
        except exception_type:
            self.passed += 1
            print(f"  ✅ {msg}" if msg else "  ✅ PASS")
        except Exception as e:
            self.failed += 1
            error_msg = f"  ❌ {msg}: 期望抛出 {exception_type.__name__}, 实际抛出 {type(e).__name__}"
            self.errors.append(error_msg)
            print(error_msg)
    
    def summary(self):
        """输出测试摘要"""
        print("\n" + "=" * 60)
        print(f"测试摘要: 通过 {self.passed}, 失败 {self.failed}")
        print("=" * 60)
        
        if self.errors:
            print("\n失败的测试:")
            for error in self.errors:
                print(error)
        
        return self.failed == 0


def test_text_normalizer(t: TestRunner):
    """测试 TextNormalizer"""
    print("\n📋 测试 TextNormalizer")
    
    # 测试 normalize 方法 - 移除数字
    t.assert_equal(
        TextNormalizer.normalize("Hello World 123", remove_numbers=True),
        "helloworld",
        "normalize() 移除数字"
    )
    
    # 测试 normalize 方法 - 保留数字
    t.assert_equal(
        TextNormalizer.normalize("Hello World 123", remove_numbers=False),
        "helloworld123",
        "normalize() 保留数字"
    )
    
    # 测试空字符串
    t.assert_equal(
        TextNormalizer.normalize("", remove_numbers=True),
        "",
        "normalize() 空字符串"
    )
    
    # 测试特殊字符
    t.assert_equal(
        TextNormalizer.normalize("A-B_C.D", remove_numbers=True),
        "abcd",
        "normalize() 特殊字符"
    )
    
    # 测试 remove_special_encoding 方法
    t.assert_equal(
        TextNormalizer.remove_special_encoding("file#x3f;name.pdf"),
        "filename.pdf",
        "remove_special_encoding() 移除 #x3f;"
    )
    
    t.assert_equal(
        TextNormalizer.remove_special_encoding("test#x3a;#x2f;file"),
        "testfile",
        "remove_special_encoding() 移除多个编码"
    )


def test_pdf_name_analyzer(t: TestRunner):
    """测试 PDFNameAnalyzer"""
    print("\n📋 测试 PDFNameAnalyzer")
    
    # 测试 DOI 格式 - isj.
    norm_title, norm_doi, is_doi = PDFNameAnalyzer.analyze("isj.12345")
    t.assert_true(is_doi, "isj. 格式识别为 DOI")
    t.assert_true("101111isj12345" in norm_doi, "isj. DOI 构建正确")
    
    # 测试 DOI 格式 - 10.
    norm_title, norm_doi, is_doi = PDFNameAnalyzer.analyze("10.1016/j.dss.2024.001")
    t.assert_true(is_doi, "10. 格式识别为 DOI")
    
    # 测试年份格式
    norm_title, norm_doi, is_doi = PDFNameAnalyzer.analyze("A-computer-vision-based_2024_DSS")
    t.assert_false(is_doi, "年份格式不应识别为 DOI")
    t.assert_equal(norm_title, "acomputervisionbased", "年份格式提取标题")
    
    # 测试无年份的普通格式
    norm_title, norm_doi, is_doi = PDFNameAnalyzer.analyze("Some-Article-Title")
    t.assert_false(is_doi, "普通格式不应识别为 DOI")
    t.assert_equal(norm_title, "somearticletitle", "普通格式标准化")
    
    # 测试特殊编码移除
    norm_title, norm_doi, is_doi = PDFNameAnalyzer.analyze("Title#x3f;With_2024_DSS")
    t.assert_equal(norm_title, "titlewith", "特殊编码被移除")


def test_field_mapping(t: TestRunner):
    """测试 FieldMapping"""
    print("\n📋 测试 FieldMapping")
    
    # 测试默认值
    mapping = FieldMapping()
    t.assert_equal(mapping.title, "Title", "默认 title 字段")
    t.assert_equal(mapping.doi, "DOI", "默认 doi 字段")
    t.assert_equal(mapping.uuid, "", "默认 uuid 为空")
    
    # 测试预定义映射
    t.assert_equal(CSV_FIELD_MAPPING.title, "Title", "CSV 映射 title")
    t.assert_equal(MONGODB_FIELD_MAPPING.title, "label", "MongoDB 映射 title")
    t.assert_equal(MONGODB_FIELD_MAPPING.uuid, "uuid", "MongoDB 映射 uuid")
    
    # 测试 to_dict
    d = CSV_FIELD_MAPPING.to_dict()
    t.assert_equal(d['title'], "Title", "to_dict() 正确")


def test_record(t: TestRunner):
    """测试 Record 类"""
    print("\n📋 测试 Record")
    
    record = Record(
        data={'Title': 'Test Article', 'DOI': '10.1234/test'},
        source_id='test_source'
    )
    
    # 测试 get 方法
    t.assert_equal(record.get('Title'), 'Test Article', "get() 方法")
    t.assert_equal(record.get('missing', 'default'), 'default', "get() 默认值")
    
    # 测试 __getitem__
    t.assert_equal(record['Title'], 'Test Article', "__getitem__ 方法")
    
    # 测试 __contains__
    t.assert_true('Title' in record, "__contains__ 存在的键")
    t.assert_false('missing' in record, "__contains__ 不存在的键")
    
    # 测试 copy
    record_copy = record.copy()
    t.assert_equal(record_copy.get('Title'), 'Test Article', "copy() 方法")
    
    # 测试 to_dict
    d = record.to_dict()
    t.assert_equal(d['Title'], 'Test Article', "to_dict() 方法")


def test_data_source_result(t: TestRunner):
    """测试 DataSourceResult"""
    print("\n📋 测试 DataSourceResult")
    
    records = [
        Record(data={'Title': 'Article 1'}),
        Record(data={'Title': 'Article 2'}),
    ]
    
    result = DataSourceResult(
        records=records,
        headers=['Title'],
        source_name='test'
    )
    
    t.assert_equal(result.total_count, 2, "自动计算 total_count")
    t.assert_equal(len(result.records), 2, "记录数量正确")


def test_csv_data_source(t: TestRunner):
    """测试 CSVDataSource"""
    print("\n📋 测试 CSVDataSource")
    
    # 创建临时 CSV 文件
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, 
                                       newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(['Title', 'DOI', 'Year'])
        writer.writerow(['Test Article 1', '10.1234/test1', '2024'])
        writer.writerow(['Test Article 2', '10.1234/test2', '2023'])
        csv_path = Path(f.name)
    
    try:
        # 测试单文件模式
        source = CSVDataSource(csv_file=csv_path)
        t.assert_equal(source.source_type, 'csv', "source_type 正确")
        
        # 测试连接
        t.assert_true(source.connect(), "connect() 成功")
        
        # 测试获取记录
        result = source.get_records()
        t.assert_equal(len(result.records), 2, "读取记录数量正确")
        t.assert_true('Title' in result.headers, "headers 包含 Title")
        
        # 验证记录内容
        t.assert_equal(
            result.records[0].get('Title'), 
            'Test Article 1', 
            "记录内容正确"
        )
        
        # 测试断开连接
        source.disconnect()
        
        # 测试不存在的文件
        bad_source = CSVDataSource(csv_file=Path('nonexistent.csv'))
        t.assert_false(bad_source.connect(), "不存在的文件返回 False")
        
    finally:
        # 清理临时文件
        csv_path.unlink()


def test_generate_doi_url(t: TestRunner):
    """测试 generate_doi_url"""
    print("\n📋 测试 generate_doi_url")
    
    t.assert_equal(
        generate_doi_url("10.1234/test"),
        "https://doi.org/10.1234/test",
        "普通 DOI"
    )
    
    t.assert_equal(
        generate_doi_url("https://doi.org/10.1234/test"),
        "https://doi.org/10.1234/test",
        "已有 http 前缀"
    )
    
    t.assert_equal(
        generate_doi_url(""),
        "",
        "空 DOI"
    )
    
    t.assert_equal(
        generate_doi_url("  10.1234/test  "),
        "https://doi.org/10.1234/test",
        "带空格的 DOI"
    )


def test_pdf_matcher(t: TestRunner):
    """测试 PDFMatcher 核心匹配逻辑"""
    print("\n📋 测试 PDFMatcher")
    
    # 创建临时目录和 PDF 文件
    import tempfile
    import shutil
    
    temp_dir = Path(tempfile.mkdtemp())
    pdf_dir = temp_dir / "pdfs"
    pdf_dir.mkdir()
    
    # 创建模拟的 PDF 文件（只需要存在，不需要真正是 PDF）
    (pdf_dir / "A-computer-vision-based_2024_DSS.pdf").write_text("")
    (pdf_dir / "isj.12345.pdf").write_text("")
    
    # 创建数据记录
    records = [
        Record(data={
            'Title': 'A computer vision based concept model to recommend...',
            'DOI': '10.1016/j.dss.2024.001'
        }),
        Record(data={
            'Title': 'Some other article',
            'DOI': '10.1111/isj.12345'
        }),
        Record(data={
            'Title': 'Unmatched article',
            'DOI': '10.9999/notfound'
        }),
    ]
    
    data_result = DataSourceResult(
        records=records,
        headers=['Title', 'DOI'],
        source_name='test'
    )
    
    try:
        # 创建匹配器
        matcher = PDFMatcher(title_column='Title', doi_column='DOI')
        
        # 执行匹配
        result = matcher.match_all(pdfs_dir=pdf_dir, data_result=data_result)
        
        t.assert_equal(result.total_records, 3, "总记录数")
        t.assert_equal(result.total_pdfs, 2, "总 PDF 数")
        
        # 检查匹配结果（至少应该有一些匹配）
        t.assert_true(
            result.matched_count + result.unmatched_count + result.multi_matched_count == 3,
            "结果分类完整"
        )
        
        # 检查 match_rate 计算
        if result.total_records > 0:
            expected_rate = result.matched_count / result.total_records
            t.assert_equal(result.match_rate, expected_rate, "match_rate 计算正确")
        
    finally:
        # 清理
        shutil.rmtree(temp_dir)


def test_csv_exporter(t: TestRunner):
    """测试 CSVExporter"""
    print("\n📋 测试 CSVExporter")
    
    import tempfile
    import shutil
    
    temp_dir = Path(tempfile.mkdtemp())
    
    try:
        # 创建模拟的 BatchMatchResult
        from matcher import MatchResult
        
        records = [
            Record(data={'Title': 'Article 1', 'DOI': '10.1234/a'}),
            Record(data={'Title': 'Article 2', 'DOI': '10.1234/b'}),
        ]
        
        match_results = [
            MatchResult(
                record_index=0,
                record=records[0],
                status=MatchStatus.MATCHED,
                matched_pdfs=[Path('/test/article1.pdf')]
            ),
            MatchResult(
                record_index=1,
                record=records[1],
                status=MatchStatus.UNMATCHED,
                reason="未找到匹配"
            ),
        ]
        
        batch_result = BatchMatchResult(
            source_name='test',
            total_records=2,
            total_pdfs=1,
            results=match_results
        )
        
        # 测试导出
        exporter = CSVExporter(output_dir=temp_dir)
        paths = exporter.export_all(
            batch_result,
            headers=['Title', 'DOI'],
            field_mapping=CSV_FIELD_MAPPING
        )
        
        t.assert_true(paths['matched'] is not None, "导出匹配结果")
        t.assert_true(paths['unmatched'] is not None, "导出未匹配结果")
        
        # 验证文件存在
        if paths['matched']:
            t.assert_true(paths['matched'].exists(), "匹配文件存在")
            
    finally:
        shutil.rmtree(temp_dir)


def test_match_result_properties(t: TestRunner):
    """测试 MatchResult 的属性方法"""
    print("\n📋 测试 MatchResult 属性")
    
    from matcher import MatchResult
    
    # 测试 MATCHED 状态
    matched = MatchResult(
        record_index=0,
        record=Record(data={}),
        status=MatchStatus.MATCHED,
        matched_pdfs=[Path('/test/file.pdf')]
    )
    t.assert_true(matched.is_matched, "is_matched 为 True")
    t.assert_false(matched.is_multi_matched, "is_multi_matched 为 False")
    t.assert_equal(matched.matched_pdf, Path('/test/file.pdf'), "matched_pdf 正确")
    
    # 测试 UNMATCHED 状态
    unmatched = MatchResult(
        record_index=1,
        record=Record(data={}),
        status=MatchStatus.UNMATCHED,
        reason="未找到"
    )
    t.assert_false(unmatched.is_matched, "is_matched 为 False")
    t.assert_true(unmatched.matched_pdf is None, "matched_pdf 为 None")
    
    # 测试 MULTI_MATCHED 状态
    multi = MatchResult(
        record_index=2,
        record=Record(data={}),
        status=MatchStatus.MULTI_MATCHED,
        matched_pdfs=[Path('/test/file1.pdf'), Path('/test/file2.pdf')]
    )
    t.assert_true(multi.is_multi_matched, "is_multi_matched 为 True")


def test_batch_match_result_properties(t: TestRunner):
    """测试 BatchMatchResult 的属性方法"""
    print("\n📋 测试 BatchMatchResult 属性")
    
    from matcher import MatchResult
    
    results = [
        MatchResult(0, Record(data={}), MatchStatus.MATCHED, [Path('/a.pdf')]),
        MatchResult(1, Record(data={}), MatchStatus.MATCHED, [Path('/b.pdf')]),
        MatchResult(2, Record(data={}), MatchStatus.UNMATCHED, reason="未找到"),
        MatchResult(3, Record(data={}), MatchStatus.MULTI_MATCHED, [Path('/c.pdf'), Path('/d.pdf')]),
    ]
    
    batch = BatchMatchResult(
        source_name='test',
        total_records=4,
        total_pdfs=4,
        results=results
    )
    
    t.assert_equal(batch.matched_count, 2, "matched_count")
    t.assert_equal(batch.unmatched_count, 1, "unmatched_count")
    t.assert_equal(batch.multi_matched_count, 1, "multi_matched_count")
    t.assert_equal(batch.match_rate, 0.5, "match_rate (2/4)")
    
    stats = batch.to_stats_dict()
    t.assert_equal(stats['matched'], 2, "to_stats_dict matched")
    t.assert_equal(stats['unmatched'], 1, "to_stats_dict unmatched")


def test_import_all(t: TestRunner):
    """测试从 __init__.py 导入所有公共 API"""
    print("\n📋 测试模块导入")
    
    # 当直接运行脚本时，使用相对导入已经成功
    # 这里验证 __init__.py 中声明的 __all__ 包含正确的导出
    import importlib.util
    
    init_path = Path(__file__).parent / '__init__.py'
    spec = importlib.util.spec_from_file_location('match_pdfs_title_doi', init_path)
    module = importlib.util.module_from_spec(spec)
    
    # 设置子模块路径
    sys.modules['match_pdfs_title_doi'] = module
    sys.modules['match_pdfs_title_doi.data_sources'] = __import__('data_sources')
    sys.modules['match_pdfs_title_doi.matcher'] = __import__('matcher')
    sys.modules['match_pdfs_title_doi.exporters'] = __import__('exporters')
    
    try:
        spec.loader.exec_module(module)
        
        # 验证 __all__ 中声明的所有导出
        expected_exports = [
            'DataSource', 'CSVDataSource', 'MongoDBDataSource',
            'DataSourceResult', 'Record', 'FieldMapping',
            'CSV_FIELD_MAPPING', 'MONGODB_FIELD_MAPPING', 'create_data_source',
            'PDFMatcher', 'PDFNameAnalyzer', 'BatchMatchResult',
            'MatchResult', 'MatchStatus', 'TextNormalizer',
            'ResultExporter', 'CSVExporter', 'CSVMerger',
            'SummaryGenerator', 'PDFCopier', 'generate_doi_url',
        ]
        
        missing = [name for name in expected_exports if not hasattr(module, name)]
        if missing:
            t.assert_true(False, f"缺少导出: {missing}")
        else:
            t.assert_true(True, "所有公共 API 可正常导入")
    except Exception as e:
        t.assert_true(False, f"导入失败: {e}")


def main():
    """运行所有测试"""
    print("=" * 60)
    print("match_pdfs_title_doi 模块测试")
    print("=" * 60)
    
    t = TestRunner()
    
    # 运行所有测试
    test_text_normalizer(t)
    test_pdf_name_analyzer(t)
    test_field_mapping(t)
    test_record(t)
    test_data_source_result(t)
    test_csv_data_source(t)
    test_generate_doi_url(t)
    test_match_result_properties(t)
    test_batch_match_result_properties(t)
    test_pdf_matcher(t)
    test_csv_exporter(t)
    test_import_all(t)
    
    # 输出摘要
    success = t.summary()
    
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
