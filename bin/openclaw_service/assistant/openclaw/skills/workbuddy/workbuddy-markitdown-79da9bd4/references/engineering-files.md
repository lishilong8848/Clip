# 工程文件材料包

MarkItDown 只负责材料编排和 Markdown 呈现。工程事实由只读 `industrial-corpus` 及可选专业 Adapter 提供；转换不调用云端规划、不修改源文件，也不从文件名猜测结构、材料、mate、feature 或位姿。

## 快速使用

Windows x64 首次使用时，Agent 先说明安装器清单中的下载大小，并在用户明确同意后运行：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File "<Skill目录>\scripts\install-runtime.ps1" -ConfirmedByUser
```

安装器从公开 Hub 解析固定的 `industrial-corpus` 0.3.4 制品，校验版本、字节数、SHA-256、下载主机和 ZIP 入口，再原子安装到用户目录的 `.skillhub/runtimes`；不会写入 Skill 包或源文件。默认自动发现该目录、`PATH` 或显式提供的 `industrial-corpus`。DWG/DXF/DWT 的可编辑 2D 文本交换、显式图元以及模型空间/纸空间 SVG 预览由此 Runtime 内置提供；纸空间 VIEWPORT 的开关状态和编号属于受保护语义，状态改变会影响指纹及预览。交换 DXF 在标准 Named Object Dictionary 中保存源语义基线，第三方另存若改变公开实体、资源或关系，重新解析会撤销源等价交换并诚实降级。TCH/VLO/AutoCAD Mechanical 私有对象及 3DSOLID 同时写入注册胶囊与隐藏标准 POINT/XDATA 载体，第三方编辑器保留标准 XDATA 时可由兼容 Runtime 完整复原。默认 `auto` 还会为 CAD 输入检查公开稳定通道，并在本机缺失时自动安装最新兼容 `cad-cli`：

```bash
python3 "<Skill目录>/scripts/convert.py" --output-dir "./工程材料" "part.step" "layout.dwg" "scene.glb"
```

必须取得完整可重建 2D 参数而不接受静默降级：

```bash
python3 "<Skill目录>/scripts/convert_batch.py" --output-dir "./工程材料" --engineering-mode required --engineering-min-capability reconstructable -- "layout.dwg"
```

输出包括：

```text
工程材料/
├─ part.step.md
├─ part.step.engineering.json
├─ assets/part.step/
└─ markitdown-batch-receipt.json
```

Markdown 固定包含 LLM 快速摘要、表示权威矩阵、对象层级、工程语义、几何摘要、多视图预览、不确定性和证据定位。JSON 侧车保留可查询细节；Markdown 不展开完整 STEP 实体或大规模网格。

对 `reconstructable` 的 CAD 侧车，可把公开文本交换独立提取为标准 DXF：

```bash
python3 "<Skill目录>/scripts/extract-cad-roundtrip.py" "./工程材料/layout.dwg.engineering.json" "./layout-restored.dxf"
```

脚本验证侧车版本、ASCII 字节数、内容 SHA-256、源/重开语义指纹、实体/资源/关系计数，以及不透明扩展与模型器胶囊的数量和源/重开摘要后才写盘。`drawing2d.textExchange.text` 本身就是普通 ASCII DXF，不是源路径或 Base64 侧载文件；中文以 DXF 标准 `\\U+XXXX` MIF 转义表示。没有公开 DXF 表示的厂商记录会使用 DXF 注册类代理与 XRECORD 二进制组保存，独立 CAD 可读取公开图元，但必须保留这些代理记录才能维持私有载荷。每个非空纸空间布局另有 `paper-space:<布局名>` 预览；它呈现布局显式图元和视口边框，并按视口中心、比例与裁剪区域投影视口中的模型内容，但不冒充完整打印引擎、打印样式或 PDF 出图结果。

## 参数

| 参数 | 默认值 | 含义 |
|---|---|---|
| `--engineering-mode` | `auto` | `auto` 接受诚实能力状态；`required` 默认至少要求 `explicit-primitives`；`off` 关闭工程处理。 |
| `--engineering-min-capability` | 按 mode 推导 | `metadata`、`semantic-summary`、`explicit-primitives` 或 `reconstructable`；低于门槛时 `not-applied` 并返回非零批次退出码（当全部失败时）。 |
| `--engineering-detail` | `standard` | `summary`、`standard`、`full` 只控制 Markdown 展示预算；JSON 仍保存解析器返回的完整有界结构。 |
| `--engineering-adapter` | `auto` | `auto` 每批一次检查/安装最新兼容 `cad-cli`，不兼容、断网或安装失败时保留诊断并使用内置解析；`builtin` 完全不查询或调用 `cad-cli`；`cad-cli` 强制专业 Adapter，无法协商时该项失败。 |

普通 Windows x64 用户不需要提供实现路径。固定的 `industrial-corpus` 安装前必须由用户明确同意；CAD 输入使用 `auto` 或 `cad-cli` 时，转换控制器会依据 [动态 CAD Runtime 策略](cad-runtime-policy.json) 自动解析、下载和更新 `cad-cli`，安装到 `.skillhub/runtimes/cad-cli/<版本>/win-x64`。它不安装 AutoCAD Bridge、不修改系统 PATH，也不上传图纸。`MARKITDOWN_CAD_CLI` / `--cad-cli` 是用户管理二进制的显式覆盖：仍做协议探测，但不替换或更新该路径。Windows ARM64、macOS 和 Linux 当前没有官方 `industrial-corpus` 或自动安装的 `cad-cli` 制品，需要用户自行提供兼容可执行文件。高级诊断可使用 `MARKITDOWN_ENGINEERING_INSPECTOR` 或命令行 `--engineering-inspector` 指向用户明确提供的解析器。

`cad-cli` 不按产品版本锁定。控制器匿名查询稳定解析器，读取本机 `cad-cli --version`，再要求离线 `capabilities --json` 的 `cad-cli` 记录版本一致且命令集合包含 `inspect`。更新元数据缓存 24 小时；批量转换只解析一次。下载后依次校验允许的 HTTPS 主机、声明字节数、SHA-256、安全 ZIP 路径、根 `runtime-package.json` 和实际能力，再原子放入新版本目录。旧版不会被删除；远端不可用或新版验证失败时，`auto` 沿用最高的已验证兼容版并在 `cadCliRuntime` 写明原因。

当一批输入全部是工程文件时，控制器不会准备无关的 Microsoft MarkItDown 基础 Runtime。已预装 `industrial-corpus` 时，断网仍能以内置解析完成；若已有经过协议验证的 `cad-cli`，`auto` 也会在更新查询失败时继续使用它。混合批次里基础 Runtime 准备失败时，普通文档逐项报告失败，工程项仍可继续。

## P0 真实能力矩阵

| 格式 | 当前输出 | 权威限制 |
|---|---|---|
| STEP/STP | 产品、实例、已解析父子关系与位姿、部分几何统计 | B-Rep 为 `partial`；不声称精确拓扑、面积或体积 |
| STL | 三角形数与包围盒跨度 | Mesh 可验证；不是 B-Rep，无装配语义 |
| OBJ | 顶点、三角化后面数、对象/组、`mtllib`/`usemtl` 引用与包围盒跨度 | 材质只作为文件内引用；不证明工程特征或制造几何 |
| GLB/glTF | 稳定节点/Mesh ID、父子关系、实例、TRS/矩阵变换、材质引用、声明三角形数和 POSITION 访问器包围盒 | Mesh 为 `partial`：场景 JSON 可验证，但本阶段不完整校验全部二进制 Buffer；不证明 B-Rep 或制造语义 |
| DWG/DXF/DWT | 内置输出 ASCII DXF 文本；结构化保留实体参数、INSERT 附着属性、字体/文字样式、线型 pattern、尺寸样式、专业样式资源、块/布局/匿名尺寸块关系、公开 XRECORD，以及 3DSOLID 的 ACIS 载荷摘要和线框；文字同时给出 `rawText`/`plainText`；预览递归展开块、把 OCS 正确变换到 WCS，并用 SVG 原生文字及剖面边界呈现可见内容 | 生成文本必须可重开，公开语义指纹和计数必须一致；TCH/VLO/Mechanical 私有对象与 3DSOLID 模型器载荷若存在，还必须经 `cad-opaque-capsule-v1` 逐条恢复并证明源/重开摘要一致，才为 `reconstructable`。公开图元可编辑；胶囊内私有语义不独立可编辑，但兼容 Runtime 可按原始载荷恢复。任何实体、胶囊或指纹失败都降为 `explicit-primitives`。外部依赖路径原样保留，被引用文件不自动打包 |
| SLDPRT/SLDASM/SLDDRW | CFB 文档元数据、配置和真实文件引用候选 | 不声称 mate、feature、B-Rep 或组件位姿 |
| IFC、IGES、Parasolid、SAT、3DM及厂商格式 | 文件身份、大小、哈希和格式元数据 | 明确标为 `metadata-only` |

能力层级按 `metadata < semantic-summary < explicit-primitives < reconstructable` 排序；表示权威状态仍为 `verified`、`partial`、`metadata-only` 和 `unavailable`。`reconstructable` 要求 ASCII DXF 文本经重新打开后，与源文档的公开实体、资源、关系及 XRECORD 数据语义指纹一致；首次交换还写入源语义基线，后续独立另存只有在该基线与当前公开语义继续一致时才能保持这一层级。任何无公开表示的厂商对象和 3DSOLID 模型器载荷还必须由摘要绑定胶囊逐条恢复。它表示“公开内容可编辑、私有载荷可逆保留”，不表示私有语义能被第三方编辑，也不等同于原始 DWG 容器字节相同，更不替代工程审批或标准符合性。

## 与专业 Skill 联动

- CAD 设计与制图：公开 `cad-cli` Runtime 可提供额外只读检查；MarkItDown 通过稳定解析器跟踪最新兼容版本，不跟随 CAD Skill 包版本发布，基础 DWG/DXF/DWT 往返仍不依赖专业 Adapter 成功。
- SOLIDWORKS 工业设计：现阶段只消费可验证的文档封装/引用及其既有证据；原生特征树和 mate 仍属于后续只读 Adapter。
- Scene3D 世界设计：GLB/glTF 只按 Mesh/场景表示读取；现有 Scene Evidence 可以作为额外材料，但不自动变成 B-Rep。
- Office 全能助手：可以把 Markdown、JSON侧车和预览导入报告或PPT，但不能把材料包当作工程审批。

## 失败与续跑

找不到 `industrial-corpus` 时，当前文件返回 `ENGINEERING_ADAPTER_UNAVAILABLE`，其他文件继续处理。强制 `cad-cli` 且解析/下载/协议协商失败时返回 `CAD_ADAPTER_UNAVAILABLE`；`auto` 则记录 `cadCliRuntime.diagnostic` 并继续内置解析。解析器明确报告 `failed` 时返回 `ENGINEERING_EXTRACTION_FAILED`，不会把失败报告提交为材料。`--resume` 的转换指纹包含本批实际协商到的 `cad-cli` 版本与协议；版本改变会重新解析，且只有 Markdown、JSON侧车及全部预览的 SHA-256 均匹配时才复用。
