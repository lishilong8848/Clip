---
name: markitdown
description: 把文档、媒体、网页、邮件、压缩包及授权的 2D/3D 工程文件整理成结构清晰、可搜索、带来源证据的 Markdown 材料包。用户要批量摄取资料、OCR、转写音视频，或把 STEP、Mesh、CAD、SOLIDWORKS、IFC 等工程文件转成 LLM 可读摘要时使用；不是媒体格式转换器，也不代替专业工程审批与网页研究。
display_name: "MarkItDown"
display_name_en: "MarkItDown"
description_zh: "批量摄取文档、网页、邮件、图片、音视频、压缩包及授权工程文件，生成带来源证据的 Markdown 材料包。"
description_en: "Ingest documents, webpages, email, images, media, archives, and authorized engineering files into evidence-linked Markdown bundles."
version: "0.5.4"
author: "AstraClaw"
---

# MarkItDown

把 PDF、DOCX/PPTX/XLSX、网页、邮件、图片、音视频和压缩包，以及授权的 STEP/STP、STL、OBJ、GLB/glTF、DXF、DWG/DWT、SLDPRT/SLDASM/SLDDRW、IFC/IGES/Parasolid 等 2D/3D 工程文件，集中整理成可追溯的 Markdown 材料包。普通资料提取正文、标题、表格和媒体内容；工程文件按解析器真实证据生成固定八段 Markdown、`engineering-material-v3` JSON 侧车和可用 SVG/PNG 预览，并明确标注 `metadata`、`semantic-summary`、`explicit-primitives` 或 `reconstructable`。每个输入独立处理；单项失败会给出原因和处理建议，不会拖垮整批任务。

## 能完成的任务

- 把 PDF、DOCX、PPTX、XLSX/XLS、HTML、CSV、JSON、XML、RSS、TXT 和 IPYNB 内容转成 Markdown。
- TXT、CSV/TSV、HTML、DOCX、PPTX、RSS 和 IPYNB 使用轻量只读路径；CSV 的 BOM、多行单元格和竖线会安全处理。XLSX 保留公式及常见数字格式，默认不导出隐藏工作表、隐藏行和隐藏列；DOCX 排除隐藏文字并读取页眉页脚，PPTX 排除隐藏幻灯片和演示者备注。
- 递归整理文件夹；安全展开 ZIP，并把 EML 正文和受支持附件拆成独立材料。
- 对 JPG、PNG 等图片和扫描 PDF 做本地中英文 OCR，尝试方向校正并保留可移植图片资产；混合 PDF 会识别没有文字层的页面并补做 OCR，补取失败时不提交静默缺页的结果。
- 在本机分段转写常见音频与视频音轨，长任务中断后从已完成分段继续；无音轨、静音、纯音或没有可靠时间边界的结果不会被写成语音证据。
- 获取用户已经提供的公开网页 URL；保存内容快照与哈希，拒绝本机、私网和危险重定向。
- 批量预检文件数、总量、单文件大小、PDF 页数和音视频时长；支持页码选择、受控并发、进度回执与崩溃续跑，同一回执不会被并发进程互相覆盖。
- 每份 Markdown 都带有不可信来源边界；主动协议、`file://` 引用、远程图片和含敏感查询参数的活动引用会被去活或脱敏，源内容中的文字不会被误当作 Agent 授权。
- 通过只读 `industrial-corpus` Adapter 把 STEP/STP、STL、OBJ、GLB/glTF 和工程格式整理成固定八段 Markdown 与 `engineering-material-v3` 紧凑 JSON 侧车；内置 DWG/DXF/DWT 解析器会输出可直接落盘的公开 ASCII DXF 文本、显式图元、字体/线型/尺寸等资源、块/布局/附着属性关系，以及 OCS/WCS 正确、呈现尺寸值与附着属性、保留剖面边界的模型空间和逐布局纸空间 SVG 预览。文字实体同时提供可审计 `rawText` 和便于检索的 `plainText`。TCH/VLO/AutoCAD Mechanical 私有对象与 3DSOLID 除注册对象胶囊外，还写入隐藏标准 POINT/XDATA 冗余载体；第三方 DXF 编辑器只要保留标准 XDATA，兼容 Runtime 就能校验摘要并恢复原始句柄、属主和字节。交换 DXF 还在标准 Named Object Dictionary 中携带源语义基线；第三方另存后的公开实体、资源或关系发生变化时，兼容 Runtime 会撤销源等价交换并降级。Windows x64 遇到原生 CAD 时还会按公开稳定通道检查并自动安装最新兼容 `cad-cli`，以 `capabilities` 中的 `inspect` 命令协商兼容性；它不是内置 2D 往返成立的前提。

本 Skill 的主产物是 Markdown。工程文件还会输出可查询 JSON 侧车和解析器实际生成的 SVG/PNG 预览；这些证据不替代 B-Rep、装配或制造审批。它不转码图片、音频、视频或 PDF。用户要搜索、比较或研究网页时，改用搜索或研究工具。

## 执行

1. 从本次实际加载的 `SKILL.md` 路径取得 Skill 目录，不假设固定安装位置。
2. 普通任务调用同目录的简洁入口 `scripts/convert.py`；需要续跑、页码、并发、最低工程能力或模型参数时调用 `scripts/convert_batch.py`。Windows x64 首次处理工程文件且未发现 `industrial-corpus` 时，先说明下载大小，以安装器清单为准，取得用户明确同意后才运行包内 `scripts/install-runtime.ps1 -ConfirmedByUser`；不要另写临时转换脚本。处理 DWG/DXF/DWT 时，默认 `auto` 会按包内动态策略匿名检查并自动安装最新兼容 `cad-cli`，每批只解析一次；要禁止这次联网和专业 Adapter，显式使用 `--engineering-adapter builtin`。
3. 先处理版本更新状态，再报告批量摘要、每项结果和输出路径。

Windows 使用可用的 CPython 3.10–3.13：

```powershell
py -3.12 "<Skill目录>\scripts\convert.py" --output-dir "<输出目录>" "<输入1>" "<输入2>"
```

Linux/macOS：

```bash
python3 "<Skill目录>/scripts/convert.py" --output-dir "<输出目录>" "<输入1>" "<输入2>"
```

先读 [上手示例与常见问答](references/examples-and-faq.md) 选择合适场景。转换 2D/3D 工程文件时读 [工程文件材料包](references/engineering-files.md)；Windows 没有 `py`、需要批量加速、续跑、选择 PDF 页码、调整 OCR 或语音模型时，读 [高级用法](references/advanced-usage.md)；遇到安装、沙箱、格式或质量问题时，读 [故障处理](references/troubleshooting.md)。

## 批量任务与回执

控制器先预检再转换，默认最多同时执行 2 项，避免大文件把机器内存占满。最终标准输出是一份 JSON 回执；使用 `--progress jsonl` 时，进度事件写到标准错误，不破坏最终 JSON。

- `schemaVersion: 3` 与 `batchOutcome`：明确整批仍在运行、全部提交、部分提交或未应用。
- `documentRuntimeVersion`、`materialRuntimeVersion` 与 `cadSemanticAdapterVersion`：分别表示普通文档 Runtime、工程材料 Runtime 和本批协商到的可选 CAD 语义 Adapter，不再把三者混写成 `runtimeVersion`。
- `summary`：除总数、成功数、未处理数、待处理数和续跑复用数外，直接汇总 `engineeringTotal`、`engineeringPending`、`engineeringFailed`、`metadataOnly`、`semanticSummary`、`explicitPrimitives`、`reconstructable`、`previewsGenerated`、`itemsWithGeometry` 和 `itemsWithDimensions`；尺寸按源 handle 合并内置图元与 `cad-cli` 语义观察，不重复计数同一实体。
- `engineeringOutcome`：工程批次的主能力结论；任一工程项失败时为 `partial`，不会因其余项目可重建而把整批写成 `reconstructable`。
- `cadCliRuntime`：本批是否不需要、已是最新、刚安装/更新、离线沿用兼容旧版或不可用；记录协商后的版本和 `cad-cli-inspect-command-v1`，不暴露本地可执行路径。
- `outcome: "committed"`：结果已写入 `output`。
- `outcome: "not-applied"`：没有伪造结果；向用户说明 `error.code`、`error.message` 和 `error.suggestion`。
- `quality: "local-ocr"`：图片或扫描 PDF 已在本机 OCR，复杂版面仍需校对。
- `quality: "native-plus-local-ocr"`：PDF 原生文字与检测到的扫描页 OCR 已合并。
- `quality: "local-transcript"`：音频已在本机转写，专业术语和嘈杂片段仍需校对。
- `quality: "image-preserved"`：只保留图片引用和基础信息，不能宣称已识别正文。
- `quality: "engineering-reconstructable"`：JSON 含已验证的 ASCII DXF 文本；Runtime 已重新打开该文本，并证明实体、资源、关系及公开 XRECORD 数据的语义指纹一致。源指纹同时写入标准 Named Object Dictionary；独立工具另存时若保持公开语义，基线继续通过，若改变任何受覆盖的公开实体、资源或关系，重新解析会撤销源等价交换并降为 `explicit-primitives`。没有公开 DXF 表示的 TCH/VLO/AutoCAD Mechanical 私有对象和 3DSOLID 原始模型器载荷会同时写入摘要绑定的 `cad-opaque-capsule-v1` 注册记录与隐藏标准 POINT/XDATA 冗余载体，且只有源/重开胶囊数、载荷摘要和关系闭包全部一致时才通过。公开图元可独立编辑；第三方 DXF 编辑器保留标准 XDATA 时，兼容 Runtime 可校验摘要并按原始句柄、属主和字节恢复私有记录，私有语义的实际编辑仍需原厂应用。可用包内 `extract-cad-roundtrip.py` 独立提取为 `.dxf`。外部字体、绘图仪、图像、底图或外部参照路径会原样保留，但被引用文件不随材料包打包。
- `quality: "engineering-explicit-primitives"`：有非空显式几何，但存在未支持实体、胶囊恢复/摘要验证失败或文本交换重开指纹差异，不能声称完整还原；降级时仍保留几何、JSON 和预览。
- `quality: "engineering-semantic-summary"`：只有领域摘要，不包含足以重建的完整图元。
- `quality: "engineering-metadata"`：只得到明确标注的元数据或能力缺口，不能宣称已解析几何。
- `resumeState: "reused"`：来源内容和转换选项未变化，复用了已校验输出。
- `resumeState: "recovered"`：输出已原子提交、但上次回执未收尾，本次已校验恢复。

整批 `batchOutcome: "not-applied"` 时进程退出码为 2；至少有一项成功时退出码为 0，并由 `committed` 或 `partial` 区分结果。图片 OCR 异常会返回失败；OCR 正常完成但没有识别出文字时，只以 `image-preserved` 保留图片和基础信息，不声称提取出了正文。如只想保留图片，可显式使用 `--ocr-mode never`。

批量回执默认保存为输出目录中的 `markitdown-batch-receipt.json`。每项转换前先记为 `pending`，成功内容经同目录暂存文件原子提交并立即更新回执。同一回执同时只能由一个进程持有；第二个进程会以 `RECEIPT_IN_USE` 明确退出，不会覆盖审计链。任务中断后用同一输出目录加 `--resume`：本地文件按内容哈希、网页按保存快照、转换按选项指纹复用，不会把旧结果或半成品冒充成功；损坏回执会先按 SHA-256 命名备份并在新回执写明警告。

输出目录同时包含 `sources/` 网页/邮件/压缩包快照、`assets/` 可移植图片或工程预览、隐藏的分段转写检查点，以及每项的 `sourceSha256`、`outputSha256`、来源类型和提取结构摘要。工程项在回执中另外绑定 JSON 侧车及其 SHA-256。需要下游消费时，以回执为入口，不要扫描目录猜测材料关系。

## 首次使用

TXT、CSV/TSV、HTML、DOCX、PPTX、RSS 和 IPYNB 直接使用轻量本地路径，不准备基础 Runtime；XLSX、PDF、OCR、音频和其余文档能力才会按实际输入在标准用户缓存目录准备隔离环境，不修改 Agent 自身 Python。基础运行时只安装锁定版本；用户已有的 `PIP_INDEX_URL`、离线源、代理和证书配置优先；没有配置时先尝试清华大学 PyPI 镜像，再尝试 PyPI。无需账户或 API Key，也无需系统 FFmpeg。

基础文档支持 Windows x64/ARM64、macOS Intel/Apple Silicon、glibc 2.27+ Linux x64/ARM64；常规环境使用 CPython 3.10–3.13，Windows ARM 原生路径使用 3.11–3.13。Windows ARM 会自动寻找 x64 CPython 并通过系统仿真运行完整 OCR/转写；没有 x64 CPython 时仍可处理基础文档，但对应媒体能力会明确降级。macOS 本地 OCR 要求 13 或更高版本。不支持的能力会在安装前返回明确错误。

官方 `industrial-corpus` 工程 Runtime 0.3.4 当前提供 Windows x64 制品。包内安装器要求用户明确同意，固定版本、大小、SHA-256、下载主机和 ZIP 入口，并安装到 Skill 目录外的用户 Runtime 缓存；MarkItDown 会自动发现它。此版本把纸空间 VIEWPORT 的显示状态和编号纳入交换与语义指纹，修复无效 XRECORD 属主、空 GROUP 和私有载体对象图，并减少冗余几何与数值体积。可选 `cad-cli` 不固定产品版本：Windows x64 的 CAD 批次按 24 小时缓存的稳定解析器选择最新兼容制品，本机缺失时自动下载，逐包校验下载主机、大小、SHA-256、ZIP 路径、包身份、`--version` 与离线 `capabilities --json`；新版探测失败时保留并沿用已验证旧版。Windows ARM64、macOS 和 Linux 若要处理工程文件，需要用户自行提供兼容的 `industrial-corpus` 可执行文件到 `PATH` 或 `MARKITDOWN_ENGINEERING_INSPECTOR`，否则只返回能力缺口，不影响普通文档转换。

要在断网环境直接转写音频，联网时先运行一次：

```bash
python3 "<Skill目录>/scripts/convert_batch.py" --prepare-runtime audio
```

模型会校验后存入标准缓存；之后普通转换会自动发现它，无需再填模型路径。

## 先处理更新提醒

每次使用前都会通过 SkillHub 公共接口检查版本，无需登录或 API Key。

- `status: "update-available"`：暂停转换，告诉用户当前版本和可用版本，询问是否先更新。
- 用户同意后，通过当前 Agent 平台的 Skill 更新入口安装 `@org-qu1qhx78/markitdown`；没有入口时打开回执中的 `page`。
- 用户明确拒绝本次更新后，才加 `--allow-current-version` 继续；不要长期关闭检查。
- 更新查询暂时失败时，保留诊断并继续使用本地能力。

## 质量与安全

优先使用原生 Office 文件、数字版 PDF、方向正确的清晰原图和人声清楚的录音。默认只导出 Office 可见内容；如果用户确实需要隐藏文字、隐藏工作表/行列、隐藏幻灯片或演示者备注，必须单独取得明确授权并使用专门审查流程。OCR、本地转写和工程解析不会上传源文件；只处理用户授权的文件或 URL，不覆盖源文件，也不擅自启用第三方插件、云端内容理解或付费规划。固定的官方 `industrial-corpus` 仍只在用户确认后安装；CAD 输入启用 `auto` 时会访问公开 Runtime 解析器并自动安装最新兼容 `cad-cli`，使用 `builtin` 可明确禁止。Adapter 缺失时返回明确能力缺口，不把 metadata-only 冒充解析成功。
