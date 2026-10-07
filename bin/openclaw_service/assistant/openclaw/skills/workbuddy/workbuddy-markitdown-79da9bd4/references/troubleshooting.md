# 故障处理

先看最终回执中的 `error.code`、`error.message` 和 `error.suggestion`。不要把空文件、旧输出或部分结果说成成功。

## 常见错误

| 错误码 | 含义 | 处理方法 |
| --- | --- | --- |
| `INPUT_NOT_FOUND` | 文件不存在或不可读 | 确认路径、权限和文件是否被移动。 |
| `INVALID_URL` | URL 缺少协议或主机名 | 提供完整的 `http://` 或 `https://` 地址。 |
| `UNSUPPORTED_FORMAT` | 扩展名未知或不在支持范围 | 确认真正格式；不要把未知二进制改名冒充支持格式。 |
| `INPUT_TOO_LARGE` | 超过文件大小安全限制 | 优先拆分；资源充足时再提高 `--max-file-mb`。 |
| `INPUT_FILE_LIMIT` / `INPUT_TOTAL_SIZE_LIMIT` | 目录展开后的文件数或总量超限 | 缩小目录，使用 `--include`/`--exclude`，或分批处理。 |
| `NO_SUPPORTED_INPUTS` | 目录中没有匹配规则的受支持文件 | 检查目录、包含/排除规则与真实扩展名。 |
| `INPUT_LIMIT_EXCEEDED` | PDF 页数或音频时长超过限制 | 使用 `--pdf-pages`、拆分音频，或显式提高限制。 |
| `INVALID_PDF_PAGE_SELECTION` | 页码格式或范围错误 | 使用 `1-5,8,10-12`，且不得超出原 PDF 页数。 |
| `DEPENDENCY_UNAVAILABLE` | 对应格式依赖未能准备 | 检查网络、PyPI 镜像、代理、证书与磁盘空间后重试。 |
| `PREFLIGHT_UNAVAILABLE` | 无法读取 PDF 页数或音频时长 | 检查文件是否损坏、加密或编码不受支持。 |
| `PDF_MIXED_OCR_INCOMPLETE` | 混合 PDF 的扫描页未能可靠 OCR | 提高清晰度、使用 `--ocr-mode always`，或提供带完整文字层的 PDF。 |
| `NO_AUDIO_TRACK` | 视频没有可读取音轨 | 提供字幕或单独音轨；不要把画面时长当作音轨时长。 |
| `NO_RELIABLE_SPEECH` | 静音、纯音或模型没有返回可靠语音 | 提供包含清晰人声的音轨；不会生成猜测性文字。 |
| `RECEIPT_IN_USE` | 另一个进程正在使用同一回执 | 等待其完成并用 `--resume`，或为并发批次指定不同回执。 |
| `URL_PRIVATE_ADDRESS` | URL 指向本机、私网或非公开地址 | 提供公开 URL，或将用户授权内容保存为本地文件。 |
| `URL_TOO_LARGE` / `URL_REDIRECT_LIMIT` | 网页快照过大或跳转过多 | 核实站点后下载较小副本，或在资源可控时显式提高限制。 |
| `ARCHIVE_*` / `CONTAINER_UNSAFE_OR_INVALID` | ZIP/EML 损坏、越界、加密或存在危险条目 | 重新打包可信材料；不要关闭路径、压缩比和展开量保护。 |
| `CONVERSION_TIMEOUT` | 单项普通转换超时 | 提高 `--timeout-seconds`，缩小 PDF 范围或拆分文件。 |
| `NO_MARKDOWN_CONTENT` | 没有提取到可靠内容 | 检查损坏、密码保护、清晰度和是否确有正文。 |
| `ENGINEERING_ADAPTER_UNAVAILABLE` | 找不到 `industrial-corpus` 只读工程解析器 | Windows x64 先征得用户同意，再运行包内 `scripts/install-runtime.ps1 -ConfirmedByUser`；其他平台显式提供兼容 Runtime。不要把元数据说成已解析几何。 |
| `CAD_ADAPTER_UNAVAILABLE` | 强制 `cad-cli`，但稳定解析器、下载、完整性校验或 `capabilities/inspect` 协商未得到兼容 Runtime | 查看 `cadCliRuntime.diagnostic`，检查网络、磁盘和 HTTPS 代理；也可改用 `auto` 让内置解析器安全回退，或用 `builtin` 明确离线。 |
| `ENGINEERING_CAPABILITY_REQUIRED` | 实际能力低于 `required` 默认门槛或 `--engineering-min-capability` | 提供匹配 Runtime/Adapter、降低明确门槛，或在接受摘要边界时使用 `auto`。 |
| `ENGINEERING_CONTRACT_INVALID` | JSON侧车身份、源哈希或权威矩阵不匹配 | 更新匹配版本的 MarkItDown 与工业 Runtime 后重试。 |
| `ENGINEERING_EXTRACTION_FAILED` | 文件损坏、不完整，或解析器没有产生可验证工程证据 | 检查真实格式和文件完整性；不要把失败报告当作已转换材料。 |
| `ENGINEERING_TIMEOUT` | 工程文件只读解析超时 | 使用 `summary`、拆分装配或在资源充足时提高普通超时。 |
| `WORKER_FAILED` | 转换任务异常退出 | 用 `--resume --jobs 1` 重试，保留回执供诊断。 |

## 首次安装慢或超时

Skill 不再安装 `markitdown[all]`。基础运行时和 PDF、Office、OCR、音频依赖按输入格式分开安装，但 OCR 与音频仍包含原生轮子，首次使用需要更多时间和磁盘。

1. 优先保留系统或 Agent 已配置的 `PIP_INDEX_URL`、代理和证书。
2. 没有配置时，控制器先尝试清华大学 PyPI 镜像，再尝试 PyPI。
3. 安装被中断后直接重试；共享安装使用操作系统文件锁，进程退出即释放，不依赖沙箱删除锁目录。
4. 不要把 Agent 自身 Python 环境当作 Skill 运行时手工塞入依赖。

首次处理 DWG/DXF/DWT 时，`auto` 还可能下载公开 `cad-cli` Runtime；当前制品约百兆，后续大小以解析器声明为准。下载使用跨进程文件锁并安装到新的版本目录；另一进程会等待同一安装完成，不会重复覆盖。下载中断、SHA-256 不符、ZIP 越界、包身份错误或能力协商失败时不会切换到新版本，已有兼容版本仍会保留。完全不希望发生该网络访问时使用 `--engineering-adapter builtin`。

## Windows 找不到 Python

Microsoft Store 的 `WindowsApps\python.exe` 可能只是占位入口。优先安装 CPython 3.10–3.13 和 Python Launcher，然后使用 `py -0p` 查看解释器，运行时选择 `py -3.12` 等已存在版本。

Windows ARM64 上，基础文档可使用原生 ARM64 Python 3.11–3.13。OCR 与音频依赖的部分上游轮子仍只提供 Windows x64；安装任一 CPython x64 3.10–3.13 后重新运行原命令，控制器会自动发现、使用独立运行时并通过系统仿真调用，不需要切换参数。若仍提示缺少 x64 Python，先用 `py -0p` 确认列表中存在 x64 解释器。

## 图片或扫描 PDF 没识别出正文

- 使用方向正确、分辨率更高、对比度更好的原图。
- 扫描 PDF 可用 `--ocr-mode always`；数字版 PDF 保持 `auto`。
- 表格、双栏、手写、印章、倾斜或低清文字仍可能需要人工校对。
- `quality: image-preserved` 只表示保留了图片引用，不表示 OCR 成功。

## 音视频没有可靠文字稿

- 已知语言时指定 `--whisper-language zh` 或 `en`。
- 嘈杂、多人重叠、远距离收音先做分段或降噪。
- 低配机器用 `--jobs 1 --whisper-model tiny`；更看重准确率且资源足够时使用 `small`。
- 首次使用会从 ModelScope 下载并校验模型。要离线使用，联网时先运行 `--prepare-runtime audio`；完成后控制器自动发现缓存，不需要手工填写模型目录。
- 长音视频默认每 30 分钟写入检查点；中断后用同一输出目录和参数加 `--resume`。检查点不是最终结果，以批量回执中的 `outcome` 为准。

## 回执停在 running 或 pending

这通常表示进程、机器或沙箱在转换中断。保留输出目录，使用完全相同的输入和转换选项加 `--resume`：已校验的成功项直接复用；已原子提交但未记账的输出会恢复；未完成项沿用原目标路径重跑。不要手工把 `pending` 改成 `committed`。

工程项还会校验 JSON侧车和全部预览 SHA-256。Markdown存在但侧车或预览缺失时不会复用；重新解析会覆盖同名受控证据，不扫描目录猜测材料关系。

## 沙箱不允许删除文件

控制器对清理失败做了保护：单项转换失败仍会返回结构化回执，不会因为回收站或 `unlink` 受限而让整批任务崩溃。若空临时文件仍被沙箱保留，按回执中的路径由用户授权清理，不要扩大删除范围。

## 隐私边界

图片 OCR 和音频转写默认在本机完成。普通 URL 转换会访问用户明确提供的网址；版本检查访问 SkillHub 公共接口；首次依赖安装访问配置的软件源；语音模型下载访问 ModelScope；CAD `auto` 访问公开 Runtime 解析器并可能从允许的 OSS 主机下载 `cad-cli`。图纸本身始终只交给本地只读进程，不上传。不要上传本地媒体，也不要擅自启用 Microsoft 云端内容理解或第三方付费插件。
