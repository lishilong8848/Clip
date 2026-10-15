# 灯塔公网访问

局域网 `18766` 先启动；只有正式部署机 `192.168.224.130` 自动在后台准备公网组件。
其它电脑（包括开发机 `192.168.224.122`）不下载凭据、不建立正式隧道，即使复制了启用配置也不启动。
启动仍使用 `启动程序.bat` 或 `bin/refactored_main.py`。无需额外常驻 Python 或 BAT。

## 文件与更新

- `bin/public_access/`：随更新发布的小型控制代码、FRPC 分发清单及不含密钥的 `deployment.json`。
- `bin/data/public_access/deployment-credential.json`：正式机下载后的 Windows DPAPI 加密缓存，禁止打包、分享。
- `bin/data/public_access/config.json`：旧的管理员导入配置，仅供准备/发布工具使用，不再控制正式机自动部署。
- `bin/data/public_access/autostart-disabled.json`：管理员明确关闭公网时的本机标记，不随更新覆盖。
- `bin/data/public_access/frpc/`：自动下载、校验的版本、缓存和已成功连接版本的回退指针。
- `bin/data/public_access/status.json`：状态与更新时间，非实时连通性保证。
- `bin/data/public_access/frpc.log`：最多四份、每份约 1MiB 的脱敏日志。

FRPC 组件包仅含 `frpc.exe` 与 `LICENSE`，上传既有依赖分发表 `tblUECEItaUpOWvd`。
经授权另存一条“灯塔公网部署凭据（受限）”附件，不放入 FRPC 包、更新补丁或日志。
此附件含认证令牌，必须限制表格及附件读取权限；IP 绑定是防误启，不是云端访问控制。
下载由原飞书组件镜像完成，不启动 OpenClaw；不上传服务器 SSH 私钥、证书私钥或业务数据。
补丁不包含上述运行目录；每次更新保留缓存。正式机按清单核对凭据版本，命中缓存不请求飞书。
首次或版本变化才后台下载、校验大小和 SHA256，核对楼机 IP、域名及端口后加密保存。
凭据临时文件下载后清理；FRPC 必需的明文运行配置仅在受限目录保存，正常关闭后删除。
完整补丁携带公网控制代码或清单时，均标记必须重启，不用本机基线推断远端客户版本；
原程序退出时结束其所属 FRPC，新程序启动后按正式机清单及保留的缓存后台启动。
无需额外安装系统服务；用户正常更新、启动和关闭灯塔即可。
正式机 IP 必须固定为 `192.168.224.130`；IP 改变时停止自动启用，不能误连另一台机器。

## dengta.sjhl.online 部署记录（2026-10-15）

- 域名解析已核对，独立 HTTPS 站点已启用；HTTP 跳转 HTTPS，证书验证路径保留。
- 已签发并安装独立 Let's Encrypt 证书，有效期至 2027-01-13；
  证书与私钥保留在服务器 `/opt/clipflow-public/tls/`，不进入项目或更新包。
- 飞书应用 `cli_a9d1c9fe9d381cee` 已新增
  `https://dengta.sjhl.online/api/auth/feishu/callback`，原回调地址保留。
- 正式机部署清单绑定 `192.168.224.130` 和上述域名；本机已关闭自动启用，测试不会抢占正式隧道。
- 经用户明确授权后，服务器激活脚本已执行成功：TCP 6100 的公网 IPv4/IPv6 直连已隔离，
  HTTPS 转发和 `clipflow-cert-renew.timer` 已启用；每日按需续期并校验、平滑重载 Nginx。
- 手动重做或核对部署时，可在服务器终端运行
  `bash /opt/clipflow-public/deployment/activate-https.sh`。
  脚本与三个配置文件已预放到该准备目录。
  脚本先核验域名证书与防火墙区域，再隔离 6100、验证并平滑加载灯塔独立站点、启用续期。
  站点激活失败时恢复原灯塔配置，不移除已添加的端口隔离，不修改其它站点或重启 FRPS。
- 已核对原有 Nginx 站点配置摘要无变化，未重启共享 FRPS；按用户要求停止 `mealie.service`，
  食谱数据与配置保留，未修改其网站配置。
- 已用隔离业务数据和真实隧道完成两轮启动/关闭/重启验证：HTTPS 健康检查成功，
  公网 Qt 内部接口被拒绝，匿名业务访问需登录，授权 URL 使用 HTTPS 回调，6100 直连 HTTP 被拒绝。
  关闭 FRP 后其子进程与入口退出，局域网服务仍可访问；验证后已关闭全部测试进程。
- 已从空缓存实际下载并校验飞书镜像中的 FRPC 0.71.0，组件不必进入完整更新补丁。
- 未代替用户实际扫码登录，也未执行真实通告/多维业务写入或发布正式更新补丁。

## 正式机首次启用与后续更新

更新包携带 `deployment.json`，正式机运行原 `启动程序.bat` 或 `refactored_main.py` 即自动准备。
局域网启动路径不进行凭据下载。首次联网失败时后台按 5/15/30/60 秒退避，不影响局域网使用。
已有同版本加密缓存时可离线复用；加密缓存绑定 Windows 账号，换账号运行时重新下载。
不要把开发机的 `bin/data/` 整个复制到正式机，也不要手工启动原直通 `18766` 的隧道。

管理员需要更新云端凭据时，先准备本机受限配置，再显式发布：

```powershell
bin\.venv\Scripts\python.exe bin\tools\publish_public_access_profile.py --deployment-host 192.168.224.130 --publish
```

发布工具复用依赖表附件上传及幂等记录，回读下载校验成功后才写新版无密钥清单。
普通打包只核验并携带该清单，不重传凭据；下次更新后正式机才采用新版本。

## 管理员导入配置（非用户安装步骤）

使用项目 Python 执行以下工具，或者在 PyCharm 运行对应 Python 文件并配置参数：

```powershell
bin\.venv\Scripts\python.exe bin\tools\configure_public_access.py --import-frpc "已有的frpc.toml路径"
bin\.venv\Scripts\python.exe bin\tools\configure_public_access.py --public-url https://实际公网域名
bin\.venv\Scripts\python.exe bin\tools\configure_public_access.py --prepare
```

导入只提取原配置中的服务器、令牌和远端端口。会强制关闭启用状态；本机目标固定改为安全入口 `127.0.0.1:18767`。
不要手工运行原来的直通 `18766` 配置。
`--prepare` 只下载并验证组件，不启动隧道；首次缺少组件时主程序也会在后台完成此步骤。

## 服务器上线前置条件

1. 公网服务器运行匹配版本 FRPS，允许本机访问控制端口 `7000`。
2. 远端转发端口 `6100` 仅允许服务器自身的 HTTPS 反向代理访问，禁止互联网直连。
   FRPS 独享实例可配置 `proxyBindAddr = "127.0.0.1"`；共享实例不能擅改该项，应单独隔离端口。
3. 配置实际域名、有效 HTTPS 证书、连接与请求限流。反向代理保留配置的 Host，
   关闭响应及上传缓冲，支持长连接 HTTP 流，读取超时不少于 300 秒。
4. 飞书应用增加 `https://实际公网域名/api/auth/feishu/callback` 回调地址。
5. 按实际用途核对现有系统的公网链接配置，不能把 `192.168.*` 地址发给公网用户。

正式机默认自动启用；仅在此前主动关闭过公网时，管理员恢复启用并正常重启灯塔：

```powershell
bin\.venv\Scripts\python.exe bin\tools\configure_public_access.py --enable --confirm-server-ready
bin\.venv\Scripts\python.exe bin\tools\configure_public_access.py --status
```

关闭公网使用 `--disable`，写入本机停用标记，下次启动生效。
`--enable` 清除该标记，但仍不能绕过正式机 IP 限制。

## 安全与故障边界

- 公网入口仅绑定回环地址；公网请求进入原服务时强制标记为非本机。
- Qt 控制、关闭后端、调试接口和匿名机柜访客不可通过公网访问。
- 业务 API 仍验证原登录、模块、楼栋及操作权限；有签名令牌的工单/签名链接继续由原接口核验。
- 修改请求必须同源；登录额外限流；文件流式传输，上限 300MiB，同时最多两路大文件上传。
- 当前页面使用 HTTP/SSE。公网入口不开放 WebSocket；新增该协议时需单独审查后实现。
- 公网连接中断不自动重放提交。结果不明时查询原任务，避免重复写入。
- 下载或安装失败保留已校验旧版；新版本校验失败或启动即退出尝试回退。
- FRPC 失联自动重连，异常退出按 5/15/30/60 秒退避；本机绑定失败不会抢占其他进程。
- 公网开启仍会使用本机带宽和资源，不是物理资源隔离。服务器侧限流不可省略。

## 发布组件

```powershell
bin\.venv\Scripts\python.exe bin\tools\publish_frpc.py --source "FRP解压目录" --output build_output\frpc --publish
```

只有明确使用 `--publish` 才上传依赖表。打包发布检查验证清单，不自动上传或启动公网。
官方配置参考：https://gofrp.org/en/docs/reference/client-configures/
