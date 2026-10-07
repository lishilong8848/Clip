# 高级用法

## Python 选择

需要 CPython 3.10–3.13。Windows 优先依次尝试 `py -3.13`、`py -3.12`、`py -3.11`、`py -3.10`；没有 Python Launcher 时再使用不指向 `WindowsApps` 的 `python3` 或 `python`。Linux/macOS 通常使用 `python3`。

运行脚本前始终从实际加载的 `SKILL.md` 推导 Skill 目录，禁止硬编码用户目录。

## 常用示例

批量转换并持续输出进度：

```bash
python3 "<Skill目录>/scripts/convert_batch.py" --output-dir "./markdown" --progress jsonl -- "report.pdf" "meeting.mp3" "table.xlsx"
```

递归整理目录，只包含业务材料并排除临时文件：

```bash
python3 "<Skill目录>/scripts/convert_batch.py" --output-dir "./材料包" --include "**/*.pdf" --include "**/*.docx" --exclude "**/~$*" -- "./项目资料"
```

安全获取公开网页、展开 ZIP/EML，并复制图片资产到材料包：

```bash
python3 "<Skill目录>/scripts/convert_batch.py" --output-dir "./材料包" --asset-mode copy -- "https://example.com/report" "邮件.eml" "附件.zip"
```

只处理 PDF 第 1–5、8、10–12 页：

```bash
python3 "<Skill目录>/scripts/convert_batch.py" --output-dir "./markdown" --pdf-pages "1-5,8,10-12" -- "manual.pdf"
```

任务中断后续跑：

```bash
python3 "<Skill目录>/scripts/convert_batch.py" --output-dir "./markdown" --resume -- "report.pdf" "meeting.mp3"
```

把 STEP、DWG 和 GLB 整理成带 JSON 证据与预览的工程材料包：

```bash
python3 "<Skill目录>/scripts/convert_batch.py" --output-dir "./工程材料" --engineering-mode auto --engineering-detail standard -- "part.step" "layout.dwg" "scene.glb"
```

低配机器降低并发并使用更小语音模型：

```bash
python3 "<Skill目录>/scripts/convert_batch.py" --output-dir "./markdown" --jobs 1 --whisper-model tiny -- "meeting.mp3"
```

联网时一次性准备全部能力，供后续弱网或离线环境使用：

```bash
python3 "<Skill目录>/scripts/convert_batch.py" --prepare-runtime all
```

## 参数与取值建议

| 参数 | 默认值 | 何时调整 |
| --- | --- | --- |
| `--jobs` | `2`（单核为 `1`） | `1–4`。低内存或长音频用 `1`；大量小文档可用 `2–4`。 |
| `--timeout-seconds` | `180` | 普通文档单项超时；大型 Office 或 PDF 可提高。 |
| `--media-timeout-seconds` | `900` | OCR 和音频转写单项超时。 |
| `--max-file-mb` | `1024` | `1–4096`。只有确认磁盘和内存充足后才提高。 |
| `--max-files` / `--max-total-mb` | `1000` / `2048` | 限制目录展开后的文件数和输入总量；大目录优先配合包含/排除规则。 |
| `--include` / `--exclude` | 无 | 目录相对路径 glob，可重复；符号链接不会被跟随。 |
| `--max-url-mb` | `100` | 公开 URL 快照大小上限；每次跳转都重新检查目标地址。 |
| `--max-container-depth` | `2` | ZIP/EML 最大嵌套层数；另有条目数、展开总量和压缩比限制。 |
| `--asset-mode` | `copy` | `copy` 复制并哈希图片资产，材料包可迁移；`link` 仅保留原路径引用。 |
| `--max-pdf-pages` | `500` | `1–10000`。也可用 `--pdf-pages` 缩小本次范围。 |
| `--max-audio-minutes` | `180` | `1–1440`。超长录音优先拆分。 |
| `--pdf-pages` | 全部 | 一页起始，格式如 `1-5,8,10-12`；只适用于本地 PDF。 |
| `--ocr-mode` | `auto` | `auto` 仅在需要时 OCR；`always` 强制扫描 PDF OCR；`never` 禁用 OCR。 |
| `--whisper-model` | `base` | `tiny` 更快、`base` 均衡、`small` 更准但更慢；也可给本地模型目录。 |
| `--whisper-language` | `auto` | 已知语言可用 `zh`、`en` 等短代码，减少误判。 |
| `--whisper-beam-size` | `5` | `1–10`。较大值可能更准，但更慢、更耗内存。 |
| `--transcript-chunk-minutes` | `30` | `5–120`。音视频按块转写并保存检查点；低内存或不稳定环境可缩小。 |
| `--progress` | `off` | `jsonl` 把阶段进度写到标准错误，最终 JSON 仍在标准输出。 |
| `--receipt-file` | 输出目录内固定文件 | 需要集中保存回执时指定路径。 |
| `--resume` | 关闭 | 复用回执中输入指纹一致、输出存在且非空的成功项。 |
| `--prepare-runtime` | 关闭 | `documents`、`ocr`、`audio` 或 `all`；只准备依赖/模型，不要求输入和输出目录。 |
| `--engineering-mode` | `auto` | `auto` 接受诚实能力状态；`required` 默认至少要求 `explicit-primitives`；`off` 关闭工程处理。 |
| `--engineering-min-capability` | 按 mode 推导 | 可选 `metadata`、`semantic-summary`、`explicit-primitives`、`reconstructable`；未达到时该项 `not-applied`。 |
| `--engineering-detail` | `standard` | 控制 Markdown 展示预算，不把完整 STEP 实体或大网格写入 Markdown。 |
| `--engineering-adapter` | `auto` | CAD 批次每次进程只解析一次最新兼容 `cad-cli`；缺失时自动校验下载，失败时回退内置解析。`builtin` 禁止查询和调用；`cad-cli` 强制深度检查。 |

`auto` 的更新元数据缓存 24 小时，不会为批次中的每张图重复联网。若要完全离线且不允许更新查询，使用 `--engineering-adapter builtin`。`--cad-cli <路径>` 或 `MARKITDOWN_CAD_CLI` 表示该二进制由用户管理：控制器只验证 `--version` 与 `capabilities --json` 中的 `inspect` 能力，不自动替换。企业沙箱或全新环境验收可用 `MARKITDOWN_CAD_RUNTIME_ROOT` 指定独立的受管缓存根，普通用户不要设置。最终回执的 `cadCliRuntime` 会报告 `current`、`installed`、`updated`、`current-offline`、`current-update-failed` 或 `unavailable`。

## OCR 与 PDF 策略

- `auto`：图片先 OCR；PDF 先检查每页文字层，数字页保留原生提取，缺少文字层的扫描页单独 OCR 后合并。扫描页补取失败时整项不提交，避免混合 PDF 静默缺页。
- `always`：扫描 PDF 或明确没有文本层的 PDF 直接逐页 OCR。
- `never`：不安装或调用 OCR；适合只处理数字版 PDF，或禁止本机 OCR 依赖的环境。
- `--pdf-pages` 同时约束数字提取与 OCR，避免无意处理整本大文档。
- `never` 遇到没有文字层的页面时会在回执列出未读取页码，不能把结果当作完整 PDF 内容。

## 音视频模型

默认模型从 ModelScope 魔搭社区匿名下载固定文件，并逐文件校验大小和 SHA-256。`tiny` 下载快、资源占用低；`base` 适合日常中英文录音；`small` 更耗时与磁盘，适合更看重准确率的场景。

需要离线使用时，联网阶段先运行 `--prepare-runtime audio`。控制器会在标准缓存中准备并校验模型，之后普通音频或视频音轨转换会自动发现 MarkItDown 自有缓存、ModelScope 缓存和 Hugging Face 缓存，无需再填路径。只有管理自有 CTranslate2 模型时，才需要用 `--whisper-model` 或 `MARKITDOWN_SKILL_WHISPER_MODEL` 指向目录；环境变量只提供默认值，显式命令参数优先。

Windows ARM64 会先寻找已安装的 x64 CPython 3.10–3.13，再通过 Windows 系统仿真自动重启控制器，以覆盖 CTranslate2 和 OpenCV 尚未提供原生 Windows ARM64 轮子的能力。也可在高级环境中用 `MARKITDOWN_SKILL_X64_PYTHON` 指定解释器；普通用户不需要设置它。

## 材料包与安全边界

- `sources/web/` 保存公开 URL 的内容寻址快照；回执记录原始 URL（敏感查询参数脱敏）、最终 URL、内容类型、大小和 SHA-256。
- `sources/containers/` 保存 ZIP 条目、EML 正文和附件；拒绝路径穿越、符号链接、加密条目、异常压缩比和超限展开。
- `assets/` 保存图片内容寻址副本；Markdown 使用相对引用，移动整份输出目录后仍可使用。
- 工程项的 `*.engineering.json` 是 `engineering-material-v3` 内容侧车；回执绑定其 SHA-256，`assets/<输出名>/` 保存解析器实际生成的 SVG/PNG 预览。读取器仍接受旧 v1/v2 侧车，但没有经重开和语义指纹验证的旧侧车不会被接受为 `reconstructable`。
- `.markitdown-state/transcripts/` 是长音视频的分块检查点。它不代表最终成功，最终状态只看批量回执。
- URL 只允许公开 `http/https` 地址；本机、私网、链路本地、多播和内嵌凭据会在访问前拒绝。该能力是读取用户明确提供的地址，不是网页搜索器。
