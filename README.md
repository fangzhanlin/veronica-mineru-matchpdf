# Veronica MineRU MatchPDF

本项目是一个用于管理学术文献 PDF 并将其批量转换为 Markdown 的工具集。主要包含两个功能模块：

1. **文献匹配 (`match_pdfs_title_doi`)**: 将本地 PDF 文件与 Scopus CSV 索引记录进行匹配，建立关联。
2. **格式转换 (`mineru_pdf2md`)**: 使用 MinerU API 高效地将 PDF 批量转换为 Markdown 格式。

## 准备工作

1. **环境配置**:
   - 确保已安装 `uv` 包管理器。
   - 同步项目依赖并激活虚拟环境：
     ```powershell
     uv sync
     .venv\Scripts\Activate.ps1
     ```

2. **文件准备**:
   - **PDF**: 将 PDF 文件按期刊分类放入 `pdfs/` 文件夹下的对应子目录（例如 `pdfs/MISQ/`）。
   - **CSV**: 将 Scopus 导出的 CSV 文件放入 `scopus_csv_records/`。
   - **Token**: 在项目根目录创建 `token.txt` 文件，并填入你的 [MinerU API Key](https://mineru.net/apiManage)。或者使用环境变量。

## 快速开始

### 1. 匹配文献 (Validating & Matching)
用于检查现有的 PDF 是否与 Scopus 记录匹配。

```powershell
uv run match_pdfs_title_doi/match_records.py
```
> 更多匹配规则、参数说明及结果解读，请参考 [MATCH_README.md](match_pdfs_title_doi/MATCH_README.md)。

### 2. PDF 转 Markdown (Converting)
使用 MinerU API 进行批量转换。推荐使用异步模式以提高速度。默认采用pdfs文件夹，可自行修改。

```powershell
# 使用异步模式，并发处理 3 个文件
uv run mineru_pdf2md/batch_convert_api.py --async --batch-size 3
```

> 更多关于并发控制、语言设置及 OCR 选项的说明，请参考 [MINERU_README.md](mineru_pdf2md/MINERU_README.md)。
