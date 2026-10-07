# 上手示例与常见问答

## 30 秒上手

把一份 Word 和一份 PDF 整理到默认的 `markdown-output` 目录：

```bash
python3 "<Skill目录>/scripts/convert.py" "项目周报.docx" "产品手册.pdf"
```

输出目录里会得到每个输入对应的 Markdown，以及 `markitdown-batch-receipt.json`。Agent 应向用户报告成功数、未处理数和每个输出路径，不要只说“转换完成”。

## 按场景选择

### 把会议资料交给 AI 总结

```bash
python3 "<Skill目录>/scripts/convert.py" --output-dir "./会议资料" "议程.docx" "数据.xlsx" "录音.m4a"
```

Word 和表格会保留可读结构，录音会在本机分段转写成带时间戳的文字。人名、产品名和多人重叠片段仍需校对；视频输入只提取音轨文字，不做画面理解。

### 识别扫描 PDF

```bash
python3 "<Skill目录>/scripts/convert_batch.py" --output-dir "./扫描件" --ocr-mode always --pdf-pages "1-20" -- "合同扫描件.pdf"
```

只处理第 1–20 页，避免无意运行整本大文档。表格、印章、手写和双栏版面可能需要人工复核。

### 整理网页内容，而不是搜索网页

```bash
python3 "<Skill目录>/scripts/convert.py" "https://example.com/article"
```

此用法只转换用户已经给出的 URL。若用户要“找资料、比较来源或研究某个主题”，应改用搜索或研究工具。

### 大批文件中断后继续

```bash
python3 "<Skill目录>/scripts/convert_batch.py" --output-dir "./资料库" --resume --progress jsonl -- "报告.pdf" "表格.xlsx" "访谈.wav"
```

本地文件按内容和转换选项复用；已保存网页快照也可复用。若上次已完成原子输出、但来不及更新回执，本次会校验哈希并恢复为成功，不产生重复文件名。

### 整理整个资料目录和邮件包

```bash
python3 "<Skill目录>/scripts/convert_batch.py" --output-dir "./尽调材料包" --exclude "**/~$*" --exclude "**/*.tmp" -- "./尽调资料" "往来邮件.eml" "补充附件.zip"
```

目录按稳定顺序递归读取且不跟随符号链接；ZIP 条目、邮件正文和附件各自保留来源关系。最终以批量回执为材料清单，可直接交给支持该回执的 Office 工程导入器。

### 把 2D/3D 工程文件交给 LLM 阅读

Windows x64 第一次使用且未安装工程 Runtime 时，Agent 应先说明安装器清单中的下载大小，并在用户同意后运行包内安装器：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File "<Skill目录>\scripts\install-runtime.ps1" -ConfirmedByUser
```

```bash
python3 "<Skill目录>/scripts/convert.py" --output-dir "./工程材料" "总装.step" "车间布局.dwg" "设备场景.glb"
```

每项输出固定八段 Markdown、`engineering-material-v3` JSON 侧车和解析器实际生成的 SVG/PNG 预览。CAD 侧车还可携带经过重开与语义指纹验证的 ASCII DXF 文本；没有公开表示的厂商对象和 3DSOLID 模型器载荷只有在摘要绑定胶囊逐条恢复一致时才允许标为 `reconstructable`。公开图元可编辑，胶囊内容仅保证可逆保留，不声称第三方可编辑私有语义。详细边界见 [工程文件材料包](engineering-files.md)。

### 提前准备离线音频转写

在仍可联网时运行一次：

```bash
python3 "<Skill目录>/scripts/convert_batch.py" --prepare-runtime audio
```

依赖和默认 `base` 模型会下载、校验并缓存。之后即使断网，仍按普通命令转换音频，无需填写模型目录；已有的 MarkItDown、ModelScope 或 Hugging Face 标准缓存也会自动发现。

## 何时不该使用

| 用户目标 | 应该怎么做 |
| --- | --- |
| 把 PNG 改成 JPG、压缩视频或裁剪音频 | 使用媒体格式转换工具；本 Skill 不做媒体转码。 |
| 搜索全网、比较多个来源或写研究报告 | 使用搜索或研究工具；本 Skill 只处理用户已给出的 URL。 |
| 精确还原含未支持 CAD 实体、手写表格或像素级版式 | 要求 `--engineering-min-capability reconstructable` 并独立验收，或使用专业 CAD/视觉工具；不得从摘要推断缺失几何。 |
| 破解加密、密码保护或损坏的文件 | 请用户提供可读副本；不要绕过访问控制。 |
| 需要说话人分离、法律级逐字稿或医学转写 | 使用相应专业服务并人工审核；本地 Whisper 结果不是专业认证稿。 |
| 生成或编辑 PDF、Word、Excel、图片、音频 | 使用对应创作工具；本 Skill 只提取和整理内容。 |
| 修改 CAD/SOLIDWORKS、证明制造可行性或完成工程审批 | 使用对应专业 Skill 和工程审核；本 Skill 只生成只读材料包。 |

## 平台与依赖

| 平台 | 基础文档 | 本地 OCR | 本地音频转写 | 官方工程 Runtime |
| --- | --- | --- | --- | --- |
| Windows x64 | 支持 | 支持 | 支持 | 支持，用户确认后安装 |
| Windows ARM64 | 原生支持 | 自动使用已安装的 x64 CPython 和系统仿真 | 自动使用已安装的 x64 CPython 和系统仿真 | 暂无；可显式提供兼容二进制 |
| macOS Intel/Apple Silicon | 支持 | macOS 13+ | 支持 | 暂无；可显式提供兼容二进制 |
| glibc 2.27+ Linux x64/ARM64 | 支持 | 支持 | 支持 | 暂无；可显式提供兼容二进制 |

Windows ARM 原生路径需要 CPython 3.11–3.13。若没有 x64 CPython，基础文档仍可处理，OCR/转写会返回明确的降级说明，不会假装成功。安装 CPython x64 3.10–3.13 后再次运行原命令即可，控制器会自动选择独立的 x64 运行时，无需改参数，也不会与 ARM 原生依赖混用。

## 常见问答

### 第一次运行为什么比之后慢？

控制器会在用户缓存目录创建隔离运行时，并只按本次格式安装所需依赖。OCR 和音频包含较大的原生轮子；音频第一次还要下载并校验模型。后续运行会复用已完成的运行时和模型。

### 可以完全离线使用吗？

可以，但依赖和语音模型必须曾经缓存。联网时先执行 `--prepare-runtime documents`、`ocr`、`audio` 或 `all`；准备完成后不需要手工配置模型路径。用户已有的标准 pip 离线源和模型缓存也会优先复用。

### 文件会上传吗？

本地文件的 OCR 和音视频转写都在本机完成。转换用户给出的 URL 会访问该 URL 并在输出目录保存受限快照；版本检查访问 SkillHub；首次安装访问已配置的软件源；首次语音模型准备访问 ModelScope。

工程文件也只在本机只读处理，不触发 llmrouter 规划或付费模型。Windows x64 的固定 `industrial-corpus` 官方 Runtime 只有在用户明确同意后才下载并做完整性校验。处理 DWG/DXF/DWT 的默认 `auto` 会匿名检查并自动安装最新兼容 `cad-cli`；只传 Runtime 元数据请求，不上传图纸，且用 `builtin` 可完全禁止这次联网。

### `cad-cli` 会不会把 MarkItDown 锁在旧版本？

不会。MarkItDown 不固定 `cad-cli` 产品版本，而是按公开稳定解析器选择最新制品，并以 `--version` 和离线 `capabilities --json` 协商 `inspect` 命令协议。更新检查缓存 24 小时、每批只执行一次；新版不兼容或下载失败时不会覆盖旧版，`auto` 会沿用已验证版本或内置解析。用户通过 `--cad-cli`/`MARKITDOWN_CAD_CLI` 明确提供的路径只做协商，不由 MarkItDown 更新。

### 为什么图片只有引用，没有识别文字？

查看回执的 `quality`。`local-ocr` 表示已识别文字；`image-preserved` 只表示保留了图片和基础信息。优先使用方向正确、清晰、对比度高的原图，必要时再使用专用视觉工具。

### 一个文件失败会影响整批吗？

不会。失败项返回 `outcome: "not-applied"` 及错误码、原因和建议，其他输入继续执行。修正后使用同一输出目录加 `--resume`，可跳过已经成功且未变化的本地文件。

### 输出在哪里？

简洁入口默认写入当前目录的 `markdown-output`。用 `--output-dir` 可指定目录；最终 JSON 回执和持久化回执都会列出每个文件的实际输出路径。

### 怎么判断该不该升级？

每次使用会匿名检查 SkillHub 公共版本。发现新版本时先征得用户同意；用户同意后通过当前 Agent 的 Skill 更新入口升级，不能静默替换。
