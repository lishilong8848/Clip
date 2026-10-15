# 灯塔公网访问

局域网 `18766` 先启动；公网组件随后在后台准备。默认关闭，不建立隧道、不下载、不新增轮询。
启动仍使用 `启动程序.bat` 或 `bin/refactored_main.py`。无需额外常驻 Python 或 BAT。

## 文件与更新

- `bin/public_access/`：随更新发布的小型控制代码和 FRPC 分发清单。
- `bin/data/public_access/config.json`：仅本机保存的部署配置和认证令牌，禁止打包、分享。
- `bin/data/public_access/frpc/`：自动下载、校验的版本、缓存和已成功连接版本的回退指针。
- `bin/data/public_access/status.json`：状态与更新时间，非实时连通性保证。
- `bin/data/public_access/frpc.log`：最多四份、每份约 1MiB 的脱敏日志。

FRPC 仅打包 `frpc.exe` 与 `LICENSE`，上传既有依赖分发表 `tblUECEItaUpOWvd`。
不会上传 `frpc.toml`、`frps.exe`、业务数据或凭证。下载由原飞书组件镜像完成，不启动 OpenClaw。
补丁不包含上述运行目录；每次更新保留配置，只有启用公网的部署电脑按清单下载新版本。

## 首次配置

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

完成后只在指定部署电脑启用，并正常重启灯塔：

```powershell
bin\.venv\Scripts\python.exe bin\tools\configure_public_access.py --enable --confirm-server-ready
bin\.venv\Scripts\python.exe bin\tools\configure_public_access.py --status
```

关闭公网使用 `--disable`，下次启动生效。不安装系统服务、不开机自启、不占用普通用户机器公网端口。

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
