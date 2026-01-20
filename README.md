# MinerU API 批量PDF转Markdown工具

使用 [MinerU](https://mineru.net/) 云API将PDF文档批量转换为Markdown格式。

## 功能特性

- 🚀 支持批量处理多个PDF文件
- 📁 保持原有目录结构输出
- ⏭️ 自动跳过已处理的文件（可配置）
- 🔄 支持同步和异步两种处理模式
- 📊 处理进度实时显示
- 🛡️ 完善的错误处理和重试机制

## 目录结构

```
pdfs/                       # 输入目录（存放PDF文件）
├── DSS/
│   └── paper1.pdf
├── EJIS/
│   └── paper2.pdf
└── ...

outputs_api/                # 输出目录（自动生成）
├── DSS/
│   └── paper1/
│       ├── paper1.md       # 转换后的Markdown
│       └── images/         # 提取的图片
├── EJIS/
│   └── paper2/
│       └── ...
└── processing_result.json  # 处理结果汇总
```

## 安装依赖

```bash
uv sync
.venv\Scripts\Activate.ps1
```

## 配置API密钥

在项目根目录创建 `token.txt` 文件，写入你的MinerU API密钥（从 https://mineru.net/apiManage 获取）：

```
你的API密钥
```

或者设置环境变量：

见env.example

## 使用方法

### 基本命令

```bash
# 显示统计信息，注意重复警告（不执行处理）
uv run batch_convert_api.py --stats

# 使用异步设置处理所有PDF
uv run batch_convert_api.py --async --batch-size 3 --delay 5

# 查看帮助
uv run batch_convert_api.py --help
```

### 命令行参数详解

| 参数 | 简写 | 默认值 | 说明 |
|------|------|--------|------|
| `--input-dir` | `-i` | `pdfs` | 输入目录路径 |
| `--output-dir` | `-o` | `outputs_api` | 输出目录路径 |
| `--language` | `-l` | `ch` | 文档语言 |
| `--no-ocr` | - | False | 禁用OCR识别 |
| `--no-skip` | - | False | 不跳过已处理文件 |
| `--stats` | - | False | 只显示统计，不处理 |
| `--async` | - | False | 使用异步模式（支持并发） |
| `--batch-size` | - | `1` | 并发处理的文件数量（需配合`--async`） |
| `--delay` | - | `1.0` | 批次之间的间隔(秒) |
| `--max-retries` | - | `180` | 最大重试次数 |
| `--retry-interval` | - | `10` | 重试间隔(秒) |
| `--api-key` | - | 自动检测 | 手动指定API密钥 |

### 参数详细说明

#### `--language` 语言选项

指定文档的主要语言，用于优化OCR识别效果：

| 值 | 语言 |
|----|------|
| `ch` | 中文（含英文） |
| `en` | 英文（默认） |
| `korean` | 韩文 |
| `japan` | 日文 |
| `chinese_cht` | 繁体中文 |
| `ta` | 泰米尔语 |
| `te` | 泰卢固语 |
| `ka` | 卡纳达语 |
| `th` | 泰语 |
| `el` | 希腊语 |
| `latin` | 拉丁语系 |
| `arabic` | 阿拉伯语 |

```bash
# 处理英文文档
uv run batch_convert_api.py --language en

# 处理日文文档
uv run batch_convert_api.py --language japan
```

#### `--no-ocr` 禁用OCR

- **启用OCR（默认）**：对扫描版PDF或图片型PDF进行文字识别
- **禁用OCR**：仅提取原生文本，速度更快，适用于纯文本PDF

```bash
# 对于纯文本PDF，禁用OCR可以加快处理速度
uv run batch_convert_api.py --no-ocr
```

#### `--no-skip` 强制重新处理

默认情况下，如果输出目录已存在`.md`文件，会跳过该PDF。使用此参数可强制重新处理：

```bash
# 重新处理所有文件（覆盖已有结果）
uv run batch_convert_api.py --no-skip
```

#### `--async` 异步模式 vs 同步模式

| 特性 | 同步模式（默认） | 异步模式（`--async`） |
|------|-----------------|---------------------|
| 实现方式 | `requests` 库 | `aiohttp` 库 |
| 并发支持 | ❌ 不支持 | ✅ 支持（通过`--batch-size`） |
| HTTP请求 | 阻塞式等待 | 非阻塞式等待 |
| 适用场景 | 简单任务、调试 | 大批量处理 |

```bash
# 使用异步模式，逐个处理（默认）
uv run batch_convert_api.py --async

# 使用异步模式，同时处理3个文件
uv run batch_convert_api.py --async --batch-size 3

# 使用异步模式，同时处理5个文件，批次间隔2秒
uv run batch_convert_api.py --async --batch-size 5 --delay 2
```

#### `--batch-size` 并发数量

设置同时处理的文件数量（**必须配合 `--async` 使用**）：

```bash
# 同时处理3个文件
uv run batch_convert_api.py --async --batch-size 3

# 同时处理5个文件
uv run batch_convert_api.py --async --batch-size 5
```

⚠️ **注意**：
- MinerU API有速率限制，建议 `--batch-size` 不要超过 **5**
- 如果遇到 API 限流错误，请降低 `--batch-size` 或增加 `--delay`

#### `--delay` 批次间隔

设置批次之间的等待时间（秒），用于避免API速率限制：

```bash
# 设置2秒间隔
uv run batch_convert_api.py --async --batch-size 3 --delay 2.0

# 不使用间隔（可能触发速率限制）
uv run batch_convert_api.py --async --batch-size 3 --delay 0
```

#### `--max-retries` 和 `--retry-interval`

控制等待任务完成时的轮询行为：

- `--max-retries`：最多查询多少次任务状态（默认180次）
- `--retry-interval`：每次查询的间隔秒数（默认10秒）

默认设置下，最长等待时间 = 180 × 10 = 1800秒 = 30分钟

```bash
# 对于大文件，增加等待时间
uv run batch_convert_api.py --max-retries 360 --retry-interval 15
```

### 使用示例

```bash
# 1. 先查看有多少文件待处理
uv run batch_convert_api.py --stats

# 2. 处理英文学术论文（推荐设置）
uv run batch_convert_api.py --language en

# 3. 快速处理纯文本PDF
uv run batch_convert_api.py --no-ocr --delay 0.5

# 4. 处理指定目录
uv run batch_convert_api.py -i my_pdfs -o my_outputs

# 5. 重新处理所有文件
uv run batch_convert_api.py --no-skip

# 6. 完整参数示例
uv run batch_convert_api.py \
    --input-dir pdfs \
    --output-dir outputs_api \
    --language en \
    --delay 1.5 \
    --max-retries 200
```

## 处理流程

每个PDF文件的处理流程：

1. **上传文件** → 调用 `/api/v4/file-urls/batch` 获取上传URL，然后上传到OSS
2. **等待处理** → 轮询 `/api/v4/extract-results/batch/{batch_id}` 查询状态
3. **下载结果** → 处理完成后下载ZIP包并解压到输出目录

## 输出文件

处理完成后，每个PDF会在对应的输出目录生成：

- `文件名.md` - Markdown格式的文档内容
- `images/` - 提取的图片文件夹
- 其他可能的文件（如中间JSON等）

## 日志文件

- `mineru_api.log` - 详细的API调用日志
- `outputs_api/processing_result.json` - 处理结果汇总（成功/失败统计）

## 项目文件说明

| 文件 | 说明 |
|------|------|
| `mineru_api_base.py` | API客户端基类，封装MinerU API调用 |
| `batch_convert_api.py` | 批量处理脚本，继承基类实现批量处理 |
| `token.txt` | API密钥文件 |
| `.env.example` | 环境变量配置模板 |

## 扩展开发

如需自定义处理逻辑，可以继承 `BaseBatchProcessor` 基类：

```python
from mineru_api_base import BaseBatchProcessor, MinerUAPIClient

class MyProcessor(BaseBatchProcessor):
    def find_files(self):
        # 自定义文件查找逻辑
        pass
    
    def is_processed(self, file_info):
        # 自定义已处理判断逻辑
        pass
```

## API参数详细说明

本脚本使用 MinerU Cloud API v4，以下是所有调用的API参数及其默认值：

### 🔧 调用的API端点

| 端点 | 方法 | 用途 |
|------|------|------|
| `/api/v4/file-urls/batch` | POST | 获取文件上传URL |
| `/api/v4/extract-results/batch/{batch_id}` | GET | 查询任务状态 |

### 📋 API请求参数

提交任务时发送的参数（在 `mineru_api_base.py` 中定义）：

```json
{
    "language": "en",           // 文档语言，可通过 --language 修改
    "files": [
        {
            "name": "文件名.pdf",
            "is_ocr": true        // 是否启用OCR，可通过 --no-ocr 修改
        }
    ]
}
```

### 🤖 使用的模型

**MinerU云API使用的是服务端托管模型，用户无需选择也无法更改。** 云API内部使用的模型包括：

| 功能 | 模型 | 说明 |
|------|------|------|
| 文档版面分析 | DocLayout-YOLO | 检测文本、图片、表格等布局元素 |
| OCR识别 | PaddleOCR | 光学字符识别 |
| 公式检测 | YOLOv8-MFD | 公式区域检测 |
| 公式识别 | UnimerNet / PP-FormulaNet | 公式LaTeX转换 |
| 表格识别 | RapidTable / SLANet | 表格结构解析 |

> ⚠️ **注意**：与本地部署不同，云API不支持选择或配置模型参数。如需更多控制，请考虑本地部署 MinerU。

### ⚙️ 代码中的默认参数

以下参数在代码中硬编码，如需修改需要直接编辑源文件：

**`mineru_api_base.py` 中的默认值：**

| 参数 | 默认值 | 位置 | 说明 |
|------|--------|------|------|
| `DEFAULT_API_BASE` | `https://mineru.net` | 第92行 | API服务器地址 |
| `DEFAULT_MAX_RETRIES` | `180` | 第93行 | 最大轮询次数 |
| `DEFAULT_RETRY_INTERVAL` | `10` | 第94行 | 轮询间隔(秒) |
| `enable_ocr` | `True` | 多处 | 默认启用OCR |
| `language` | `"ch"` | 多处 | 默认中文 |

**`batch_convert_api.py` 中的默认值：**

| 参数 | 默认值 | 说明 |
|------|--------|------|
| 输入目录 | `pdfs` | 命令行可修改 |
| 输出目录 | `outputs_api` | 命令行可修改 |
| 处理间隔 | `1.0`秒 | 命令行可修改 |
| 批量大小 | `1` | 当前逐个处理 |

### 🔄 跳过已处理文件的机制

默认情况下，脚本会**自动跳过已经处理过的文件**：

1. **判断条件**：检查输出目录 `outputs_api/<子目录>/<文件名>/` 下是否存在 `.md` 文件
2. **实现位置**：`batch_convert_api.py` 的 `is_processed()` 方法（第133-163行）
3. **禁用方式**：使用 `--no-skip` 参数强制重新处理所有文件

```python
# is_processed() 的判断逻辑
def is_processed(self, file_info):
    output_dir = file_info['output_dir']
    if not output_dir.exists():
        return False
    # 检查是否存在非空的.md文件
    md_files = list(output_dir.rglob("*.md"))
    return any(f.stat().st_size > 0 for f in md_files)
```

### 🔀 关于异步和并发处理

使用 `--async --batch-size N` 可以同时处理多个文件：

```bash
# 同时处理3个文件，批次间隔1秒
uv run batch_convert_api.py --async --batch-size 3 --delay 1
```

**工作原理：**
1. 将所有待处理文件按 `batch_size` 分批
2. 每批文件使用 `asyncio.gather()` **并发**上传和处理
3. 等待当前批次所有文件完成
4. 延迟后处理下一批

**推荐设置：**

| 场景 | 推荐命令 |
|------|----------|
| 少量文件（<20） | `--async --batch-size 3` |
| 中等数量（20-100） | `--async --batch-size 5 --delay 2` |
| 大量文件（>100） | `--async --batch-size 3 --delay 3` |

⚠️ **注意**：`--batch-size` 过大可能触发API速率限制，建议不超过5。

## 修改配置的方法

### 方法1：通过命令行参数

```bash
# 修改语言、禁用OCR、不跳过已处理文件
uv run batch_convert_api.py --language en --no-ocr --no-skip
```

### 方法2：修改代码默认值

编辑 `mineru_api_base.py` 第92-94行：

```python
class MinerUAPIClient:
    DEFAULT_API_BASE = "https://mineru.net"
    DEFAULT_MAX_RETRIES = 180    # 修改此值增加等待时间
    DEFAULT_RETRY_INTERVAL = 10  # 修改此值改变轮询频率
```

### 方法3：创建配置文件（高级）

如需更灵活的配置，可以创建 `config.json`：

```json
{
    "api_base": "https://mineru.net",
    "language": "en",
    "enable_ocr": true,
    "max_retries": 200,
    "retry_interval": 15
}
```

然后修改代码读取此配置（需自行实现）。

## 注意事项

1. **API额度**：MinerU API有使用额度限制，请在 https://mineru.net/apiManage 查看
2. **文件大小**：大文件处理时间较长，请耐心等待
3. **网络要求**：需要稳定的网络连接
4. **断点续传**：脚本默认跳过已处理文件，中断后可直接重新运行继续处理
5. **模型选择**：云API不支持自定义模型，如需更多控制请使用本地部署