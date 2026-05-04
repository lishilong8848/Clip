# 局域网多维表模板生成门户

## 功能
- 默认监听 `18766`
- 默认绑定 `0.0.0.0`
- 局域网内可通过浏览器访问
- 按 `计划维护月份 / 专业类别 / 楼栋` 过滤记录
- 动态多选 `维护总项`
- 为每条选中记录单独填写时间、位置、内容、原因、影响
- 生成维保通告模板文本

## 启动方式
该门户会随主程序一起启动：

```powershell
cd D:\桌面\ShiJian_Code\pythonProject\上传维保变更设备调整
启动程序.bat
```

或：

```powershell
cd D:\桌面\ShiJian_Code\pythonProject\上传维保变更设备调整\bin
D:\Python313\python.exe -m lan_bitable_template_portal.server
```

主程序启动后，可通过：

- 本机：`http://127.0.0.1:18766/`
- 局域网：`http://本机IP:18766/`

如果 `18766` 已被占用，服务会自动顺延寻找可用端口。
