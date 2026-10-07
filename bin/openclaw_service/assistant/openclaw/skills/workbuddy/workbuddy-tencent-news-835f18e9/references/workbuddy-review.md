# WorkBuddy 提交与环境准备

技能需要能执行 Shell 或 PowerShell，并访问腾讯新闻 CLI 下载服务与业务服务。帮助与环境诊断可在未安装 CLI、未配置 Key 的环境运行；实时业务结果仍需要可用 CLI 和有效凭证。

## 打包

从仓库根目录执行：

```sh
pnpm run zip tencent-news --workbuddy
```

该模式仅在暂存目录清理 frontmatter，保留技能描述、版本等展示字段，以及 `examples_zh` / `examples_en` 中的对话示例；排除标签字段、`_skillhub_meta.json`、`_user_meta.json` 等本地元数据和隐藏文件。zip 根目录直接包含 `SKILL.md`、`scripts/`、`references/`，正文也保留三个对话用例。源文件不会因打包而被修改。

需要测试环境时使用 `pnpm run zip:test tencent-news --workbuddy`。该包名称带 `-test`，安装地址与 Key 获取地址一起切换为测试环境。生产包与测试包不可混用凭证。

## 提交前自检

解压 zip，在包含 `SKILL.md` 的目录执行：

```sh
sh scripts/run-cli.sh
sh scripts/run-cli.sh --help
sh scripts/cli-state.sh --help
sh scripts/cli-state.sh status
```

Windows 使用 `powershell` 和对应 `.ps1` 文件。前三条显示帮助并退出 0；最后一条输出真实环境诊断。未就绪时必须保留 `success: false`、失败原因和恢复步骤，不能把诊断退出 0 当作业务完成。

## 评测环境需要提供的条件

1. 按 [安装指南](installation-guide.md) 在评测环境预装 CLI，或允许按指南下载安装；确认下载服务 `mat1.gtimg.com` 可达。
2. 由凭证持有人在评测环境本地配置对应环境的 Key，按 [配置指南](env-setup-guide.md) 操作。禁止把真实 Key 放入 zip、脚本、示例、回复或评测报告。
3. 确认执行智能体的账号与 shell 能读取该配置，运行 `cli-state status` 验证，再读取 `run-cli help`，按实际命令完成一次授权的业务查询。
4. 如果平台不能配置上述依赖，请向平台运营确认其依赖安装、凭证注入和联网方式。这些条件未确认前，只能验证帮助、诊断和打包，不能宣称实时业务评测已通过。

官方依据：[WorkBuddy 技能规范](https://open.workbuddy.cn/docs/skill)。该页面说明包结构与 frontmatter；未公开沙箱评分规则。上述整改不保证平台审核通过，最终以重新提交后的结果为准。
