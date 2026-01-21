# 文献数据匹配工具使用指南 (v3.0)

## 概述

本工具用于将 PDF 文件与数据源（CSV 或 MongoDB）中的文献记录进行匹配，建立文献数据与 PDF 文件之间的关联。

**v3.0 新特性**：
- **自动检测 PDF 格式**：无需配置期刊模式，自动识别 DOI 格式和年份格式
- **统一处理特殊编码**：自动移除 `#x3f;` 等特殊编码
- **MongoDB 字段映射**：支持 `doi`/`label`/`uuid` 字段
- **PDF 复制功能**：成功匹配后可复制 PDF 并以 uuid 重命名

## 目录结构

```
match_pdfs_title_doi/
├── __init__.py               # 包入口，导出所有公共 API
├── data_sources.py           # 数据源抽象层（CSV、MongoDB）
├── matcher.py                # 核心匹配引擎（自动检测）
├── exporters.py              # 结果导出器 + PDF 复制器
├── match_records.py          # 主入口脚本
├── scopus_csv_records/       # 存放 Scopus 导出的 CSV 文件
│   └── scopus_*.csv
├── match_results/            # 匹配结果输出目录
│   ├── matched/              # 成功匹配的记录
│   ├── unmatched/            # 未匹配的记录
│   ├── multi_matched/        # 多重匹配的记录
│   ├── ALL_MATCHED.csv       # 所有匹配成功记录合并
│   ├── ALL_UNMATCHED.csv     # 所有未匹配记录合并
│   └── ALL_MULTI_MATCHED.csv # 所有多重匹配记录合并
├── pdfs/                     # 复制匹配 PDF 的目标目录
├── logs/                     # 日志文件
└── MATCH_README.md           # 本文档
```

## 架构设计

### 模块说明

| 模块 | 说明 |
|------|------|
| `data_sources.py` | 数据源抽象层，支持 CSV（Title/DOI）和 MongoDB（label/doi/uuid）字段映射 |
| `matcher.py` | 核心匹配逻辑，`PDFNameAnalyzer` 自动检测文件名格式 |
| `exporters.py` | 结果导出器 + `PDFCopier` 复制匹配文件 |
| `match_records.py` | 命令行入口，简化的参数设计 |

### 核心类

```
┌─────────────────────────────────────────┐
│            PDFNameAnalyzer              │
│  - 自动检测 DOI 格式（如 isj.12026）      │
│  - 自动检测年份格式（如 title_2024_DSS）  │
│  - 自动移除特殊编码（#x3f; 等）           │
└─────────────────────────────────────────┘

┌─────────────────────────────────────────┐
│             FieldMapping                │
│  CSV:     title='Title', doi='DOI'      │
│  MongoDB: title='label', doi='doi',     │
│           uuid='uuid'                   │
└─────────────────────────────────────────┘

┌─────────────────────────────────────────┐
│              PDFCopier                  │
│  - 复制成功匹配的 PDF                    │
│  - 使用 uuid 重命名（MongoDB 模式）       │
└─────────────────────────────────────────┘
```

## 使用方法

### 基本用法（CSV 数据源）

```powershell
# 激活虚拟环境
& .venv\Scripts\Activate.ps1

# 使用单个 CSV 文件
uv run match_pdfs_title_doi/match_records.py \
    --pdfs-dir ./pdfs \
    --csv-file ./scopus_csv_records/scopus_dsr.csv

# 使用 CSV 目录（匹配所有 CSV 文件）
uv run match_pdfs_title_doi/match_records.py \
    --pdfs-dir ./pdfs \
    --csv-dir ./scopus_csv_records
```

### 使用 MongoDB 数据源

```powershell
# 先安装 pymongo（如果尚未安装）
uv add pymongo

# 使用 MongoDB 数据源
uv run match_pdfs_title_doi/match_records.py \
    --pdfs-dir ./pdfs \
    --source mongodb \
    --mongo-uri "mongodb://localhost:27017" \
    --mongo-db literature \
    --mongo-collection papers
```

### 复制匹配的 PDF

```powershell
# CSV 模式 - 复制匹配的 PDF（保持原文件名）
uv run match_pdfs_title_doi/match_records.py \
    --pdfs-dir ./pdfs \
    --csv-file ./data.csv \
    --copy-pdfs \
    --copy-dir ./matched_pdfs

# MongoDB 模式 - 复制并以 uuid 重命名
uv run match_pdfs_title_doi/match_records.py \
    --pdfs-dir ./pdfs \
    --source mongodb \
    --mongo-uri "mongodb://localhost:27017" \
    --mongo-db literature \
    --mongo-collection papers \
    --copy-pdfs \
    --copy-dir ./matched_pdfs
```

### 命令行参数

#### 必需参数

| 参数 | 说明 |
|------|------|
| `--pdfs-dir` | PDF 文件目录（会递归搜索所有 PDF）|

#### 数据源参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--source` | `csv` | 数据源类型：`csv` 或 `mongodb` |

#### CSV 参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--csv-file` | - | CSV 文件路径（与 --csv-dir 二选一）|
| `--csv-dir` | - | CSV 文件目录（与 --csv-file 二选一）|
| `--csv-pattern` | `*.csv` | CSV 文件名匹配模式 |

#### MongoDB 参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--mongo-uri` | `mongodb://localhost:27017` | MongoDB 连接字符串 |
| `--mongo-db` | - | MongoDB 数据库名称（必需）|
| `--mongo-collection` | - | MongoDB 集合名称（必需）|

#### 输出参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--output-dir` | `./match_results` | 匹配结果输出目录 |
| `--log-dir` | `./logs` | 日志目录 |

#### PDF 复制参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--copy-pdfs` | False | 是否复制成功匹配的 PDF |
| `--copy-dir` | `./pdfs` | PDF 复制目标目录 |

## 字段映射

### CSV 数据源
- `Title` → 用于标题匹配
- `DOI` → 用于 DOI 匹配

### MongoDB 数据源
- `label` → 用于标题匹配（对应 CSV 的 Title）
- `doi` → 用于 DOI 匹配（对应 CSV 的 DOI）
- `uuid` → 复制 PDF 时的新文件名

## 自动检测逻辑

### PDF 文件名分析

`PDFNameAnalyzer` 会自动分析每个 PDF 文件名，判断其格式：

| 格式 | 特征 | 处理方式 |
|------|------|----------|
| DOI 格式 | 如 `isj.12026.pdf` | 提取 DOI 后缀进行匹配 |
| 年份格式 | 如 `Article-title_2024_Journal.pdf` | 提取年份前的标题部分 |
| 普通格式 | 其他情况 | 整体作为标题处理 |

### 特殊字符处理

自动移除的特殊编码：
- `#x3f;` → 删除
- URL 编码 → 解码
- 下划线 → 空格（用于标准化）

### 匹配优先级

1. **DOI 精确匹配**（优先级最高）
2. **标题前缀匹配**（记录标题以 PDF 文件名开头）

## 扩展指南

### 添加新的数据源

继承 `DataSource` 基类：

```python
from data_sources import DataSource, DataSourceResult, Record, FieldMapping

# 定义字段映射
MY_FIELD_MAPPING = FieldMapping(
    title='name',     # 你的数据源中的标题字段
    doi='identifier', # 你的数据源中的 DOI 字段
    uuid='id'         # 你的数据源中的唯一标识字段
)

class MyCustomDataSource(DataSource):
    def __init__(self, field_mapping: FieldMapping = MY_FIELD_MAPPING, **kwargs):
        super().__init__(field_mapping=field_mapping, **kwargs)
    
    @property
    def source_type(self) -> str:
        return "custom"
    
    def connect(self) -> bool:
        # 实现连接逻辑
        return True
    
    def disconnect(self) -> None:
        # 实现断开连接逻辑
        pass
    
    def get_records(self, source_identifier: str, query=None) -> DataSourceResult:
        # 实现数据获取逻辑
        records = [Record(data={'name': '...', 'identifier': '...', 'id': '...'})]
        return DataSourceResult(
            records=records,
            headers=['name', 'identifier', 'id'],
            source_name=source_identifier
        )
```

### 编程方式使用

```python
from pathlib import Path
import logging

from match_pdfs_title_doi import (
    CSVDataSource,
    MongoDBDataSource,
    PDFMatcher,
    PDFCopier,
    CSV_FIELD_MAPPING,
    MONGODB_FIELD_MAPPING,
)

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 方式1：使用 CSV 数据源
csv_source = CSVDataSource(
    csv_file=Path('./data.csv'),
    field_mapping=CSV_FIELD_MAPPING,
    logger=logger
)

# 方式2：使用 MongoDB 数据源
mongo_source = MongoDBDataSource(
    connection_string='mongodb://localhost:27017',
    database='literature',
    collection='papers',
    field_mapping=MONGODB_FIELD_MAPPING,
    logger=logger
)

# 使用上下文管理器
with csv_source as source:
    # 获取记录
    result = source.get_records()
    print(f"读取到 {len(result.records)} 条记录")
    
    # 创建匹配器
    matcher = PDFMatcher(
        logger=logger,
        title_column=CSV_FIELD_MAPPING.title,
        doi_column=CSV_FIELD_MAPPING.doi
    )
    
    # 执行批量匹配
    match_result = matcher.match_all(
        pdfs_dir=Path('./pdfs'),
        data_result=result
    )
    
    print(f"匹配成功: {match_result.matched_count}")
    print(f"未匹配: {match_result.unmatched_count}")
    
    # 复制匹配的 PDF
    copier = PDFCopier(Path('./matched_pdfs'), logger=logger)
    copier.copy_matched_pdfs(match_result, uuid_field='')
```

## 匹配逻辑

### 文本标准化

匹配前，对 PDF 文件名和记录内容进行标准化处理：
- 转换为纯小写字母
- 移除所有符号（空格、连字符、下划线等）
- Title 匹配时移除数字，DOI 匹配时保留数字

### 匹配规则

**核心匹配逻辑**：记录的标准化内容必须**从头开始**包含 PDF 文件名的标准化内容。

```
CSV Title (标准化后): acomputervisionbasedconceptmodeltorecommenddomesticoverseaslike...
PDF 文件名 (标准化后): acomputervisionbasedconceptmodeltorecommenddomestic
                       ↑ 从头开始完全匹配 ✓
```

## 输出文件

### 匹配结果文件

| 文件类型 | 新增列 | 说明 |
|---------|--------|------|
| `matched.csv` | `Matched_PDF_Path` | 匹配到的 PDF 完整路径 |
| `unmatched.csv` | `Unmatch_Reason` | 未匹配原因说明 |
| `multi_matched.csv` | `Matched_PDF_Paths`, `Match_Count` | 多个匹配路径和数量 |

### 汇总文件

| 文件 | 说明 | 特殊列 |
|------|------|--------|
| `ALL_MATCHED.csv` | 所有匹配成功记录 | - |
| `ALL_UNMATCHED.csv` | 所有未匹配记录（去重） | `DOI_Download_Link` |
| `ALL_MULTI_MATCHED.csv` | 所有多重匹配记录 | `DOI_Download_Link` |

## MongoDB 数据结构

MongoDB 集合中的文档结构：

```json
{
  "_id": ObjectId("..."),
  "label": "A computer vision based concept model...",
  "doi": "10.1016/j.dss.2024.114123",
  "uuid": "550e8400-e29b-41d4-a716-446655440000",
  "authors": "Author1; Author2",
  "year": 2024,
  // 其他字段...
}
```

关键字段说明：
- `label`: 文献标题，用于标题匹配
- `doi`: 数字对象标识符，用于 DOI 匹配
- `uuid`: 唯一标识符，复制 PDF 时作为新文件名

## 常见问题


### Q: MongoDB 连接失败怎么办？

A: 检查以下几点：
1. 确保已安装 pymongo：`uv add pymongo`
2. 确认 MongoDB 服务正在运行
3. 检查连接字符串格式是否正确
4. 验证用户权限和网络访问

### Q: 如何处理特殊文件名？

A: v3.0 自动处理以下情况：
- `#x3f;` 等 URL 编码会被自动移除
- 年份格式（如 `_2024_`）会被自动检测
- DOI 格式（如 `isj.12026`）会被自动识别

## 版本历史

### v3.0.0 (2025-01-20)
- 简化 API：移除期刊特定配置
- 新增 `PDFNameAnalyzer`：自动检测文件名格式
- 新增 `FieldMapping`：支持不同数据源的字段映射
- 新增 `PDFCopier`：复制匹配 PDF 并以 uuid 重命名
- MongoDB 支持：`label`/`doi`/`uuid` 字段映射

### v2.0.0 (2025-01-20)
- 重构为面向对象架构
- 新增 MongoDB 数据源支持
- 模块化设计

### v1.0.0 (2025-01-20)
- 初始版本
- 支持 CSV 数据源
- Title 和 DOI 匹配模式
