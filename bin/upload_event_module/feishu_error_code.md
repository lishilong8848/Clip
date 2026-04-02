| HTTP状态码 | 错误码  | 描述                                                         | 排查建议                                                     |
| ---------- | ------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 200        | 1254000 | WrongRequestJson                                             | 请求体错误                                                   |
| 200        | 1254001 | WrongRequestBody                                             | 请求体错误                                                   |
| 200        | 1254002 | Fail                                                         | 内部错误，请联系[技术支持](https://applink.feishu.cn/TLJpeNdW) |
| 200        | 1254003 | WrongBaseToken                                               | app_token 错误                                               |
| 200        | 1254004 | WrongTableId                                                 | table_id 错误                                                |
| 200        | 1254005 | WrongViewId                                                  | view_id 错误                                                 |
| 200        | 1254006 | WrongRecordId                                                | 检查 record_id                                               |
| 200        | 1254007 | EmptyValue                                                   | 空值                                                         |
| 200        | 1254008 | EmptyView                                                    | 空视图                                                       |
| 200        | 1254009 | WrongFieldId                                                 | 字段 id 错误                                                 |
| 200        | 1254010 | ReqConvError                                                 | 请求错误                                                     |
| 400        | 1254015 | Field types do not match.                                    | 字段类型和值不匹配                                           |
| 403        | 1254027 | UploadAttachNotAllowed                                       | 附件未挂载, 禁止上传                                         |
| 200        | 1254030 | TooLargeResponse                                             | 响应体过大                                                   |
| 400        | 1254036 | Base is copying, please try again later.                     | 复制多维表格为异步操作，该错误码表示当前多维表格仍在复制中，在复制期间无法操作当前多维表格。需要等待复制完成后再操作。 |
| 400        | 1254037 | Invalid client token, make sure that it complies with the specification. | 幂等键格式错误，需要传入 uuidv4 格式                         |
| 200        | 1254040 | BaseTokenNotFound                                            | app_token 不存在                                             |
| 200        | 1254041 | TableIdNotFound                                              | table_id 不存在                                              |
| 200        | 1254042 | ViewIdNotFound                                               | view_id 不存在                                               |
| 200        | 1254043 | RecordIdNotFound                                             | record_id 不存在                                             |
| 200        | 1254044 | FieldIdNotFound                                              | field_id 不存在                                              |
| 200        | 1254045 | FieldNameNotFound                                            | 字段名称不存在。请检查接口中字段名称和多维表格中的字段名称是否完全匹配。如果难以排查，建议你调用[列出字段](https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/reference/bitable-v1/app-table-field/list)接口获取字段名称，因为根据表格页面的 UI 名称可能会忽略空格、换行或特殊符号等差异。 |
| 200        | 1254060 | TextFieldConvFail                                            | 多行文本字段错误                                             |
| 200        | 1254061 | NumberFieldConvFail                                          | 数字字段错误                                                 |
| 200        | 1254062 | SingleSelectFieldConvFail                                    | 单选字段错误                                                 |
| 200        | 1254063 | MultiSelectFieldConvFail                                     | 多选字段错误                                                 |
| 200        | 1254064 | DatetimeFieldConvFail                                        | 日期字段错误                                                 |
| 200        | 1254065 | CheckboxFieldConvFail                                        | 复选框字段错误                                               |
| 200        | 1254066 | UserFieldConvFail                                            | 人员字段有误。原因可能是：`user_id_type` 参数指定的 ID 类型与传入的 ID 类型不匹配传入了不识别的类型或结构，目前只支持填写 `id` 参数，且需要传入数组跨应用传入了 `open_id`。如果跨应用传入 ID，建议使用 `user_id`。不同应用获取的 `open_id` 不能交叉使用若想对人员字段传空，可传 null |
| 200        | 1254067 | LinkFieldConvFail                                            | 关联字段错误                                                 |
| 200        | 1254068 | URLFieldConvFail                                             | 超链接字段错误                                               |
| 200        | 1254069 | AttachFieldConvFail                                          | 附件字段错误                                                 |
| 200        | 1254072 | Failed to convert phone field, please make sure it is correct. | 电话字段错误                                                 |
| 400        | 1254074 | The parameters of Duplex Link field are invalid and need to be filled with an array of string. | 双向关联字段格式非法                                         |
| 200        | 1254100 | TableExceedLimit                                             | 数据表或仪表盘数量超限。每个多维表格中，数据表加仪表盘的数量最多为 100 个 |
| 200        | 1254101 | ViewExceedLimit                                              | 视图数量超限, 限制200个                                      |
| 200        | 1254102 | FileExceedLimit                                              | 文件数量超限                                                 |
| 200        | 1254103 | RecordExceedLimit                                            | 记录数量超限, 限制20,000条                                   |
| 200        | 1254104 | RecordAddOnceExceedLimit                                     | 单次添加记录数量超限, 限制500条                              |
| 200        | 1254105 | ColumnExceedLimit                                            | 字段数量超限                                                 |
| 200        | 1254106 | AttachExceedLimit                                            | 附件过多                                                     |
| 200        | 1254130 | TooLargeCell                                                 | 格子内容过大                                                 |
| 200        | 1254290 | TooManyRequest                                               | 请求过快，稍后重试                                           |
| 200        | 1254291 | Write conflict                                               | 同一个数据表(table) 不支持并发调用写接口，请检查是否存在并发调用写接口。写接口包括：新增、修改、删除记录；新增、修改、删除字段；修改表单；修改视图等。 |
| 200        | 1254301 | OperationTypeError                                           | 多维表格未开启高级权限或不支持开启高级权限                   |
| 200        | 1254303 | The attachment does not belong to this bitable.              | 没有写入附件至多维表格的权限。要在多维表格中写入附件，你需先调用[上传素材](https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/reference/drive-v1/media/upload_all)接口，将附件上传到当前多维表格中，再新增记录 |
| 200        | 1255001 | InternalError                                                | 内部错误，请联系[技术支持](https://applink.feishu.cn/TLJpeNdW) |
| 200        | 1255002 | RpcError                                                     | 内部错误，请联系[技术支持](https://applink.feishu.cn/TLJpeNdW) |
| 200        | 1255003 | MarshalError                                                 | 序列化错误，请联系[技术支持](https://applink.feishu.cn/TLJpeNdW) |
| 200        | 1255004 | UmMarshalError                                               | 反序列化错误                                                 |
| 200        | 1255005 | ConvError                                                    | 内部错误，请联系[技术支持](https://applink.feishu.cn/TLJpeNdW) |
| 400        | 1255006 | Client token conflict, please generate a new client token and try again. | 幂等键冲突，需要重新随机生成一个幂等键                       |
| 504        | 1255040 | 请求超时                                                     | 进行重试                                                     |
| 400        | 1254607 | Data not ready, please try again later                       | 该报错一般是由于前置操作未执行完成，或本次操作数据太大，服务器计算超时导致。遇到该错误码时，建议等待一段时间后重试。通常有以下几种原因：**编辑操作频繁**：开发者对多维表格的编辑操作非常频繁。可能会导致由于等待前置操作处理完成耗时过长而超时的情况。多维表格底层对数据表的处理基于版本维度的串行方式，不支持并发。因此，并发请求时容易出现此类错误，不建议开发者对单个数据表进行并发请求。**批量操作负载重**：开发者在多维表格中进行批量新增、删除等操作时，如果数据表的数据量非常大，可能会导致单次请求耗时过长，最终导致请求超时。建议开发者适当降低批量请求的 page_size 以减少请求耗时。**资源分配与计算开销**：资源分配是基于单文档维度的，如果读接口涉及公式计算、排序等计算逻辑，会占用较多资源。例如，并发读取一个文档下的多个数据表也可能导致该文档阻塞。 |
| 403        | 1254302 | Permission denied.                                           | 调用身份缺少多维表格的高级权限。你需要为调用身份授予高级权限：对用户授予高级权限，你需要在多维表格页面右上方 **分享** 入口为当前用户添加可管理权限。![image.png](https://sf3-cn.feishucdn.com/obj/open-platform-opendoc/df3911b4f747d75914f35a46962d667d_dAsfLjv3QC.png?height=546&lazyload=true&maxWidth=550)对应用授予高级权限，你需通过多维表格页面右上方 **「...」** -> **「...更多」** ->**「添加文档应用」** 入口为应用添加可管理权限。![img](https://sf3-cn.feishucdn.com/obj/open-platform-opendoc/22c027f63c540592d3ca8f41d48bb107_CSas7OYJBR.png?height=1994&maxWidth=550&width=3278)![image.png](https://sf3-cn.feishucdn.com/obj/open-platform-opendoc/9f3353931fafeea16a39f0eb887db175_0tjzC9P3zU.png?maxWidth=550)**注意**：在 **添加文档应用** 前，你需确保目标应用至少开通了一个多维表格的 [API 权限](https://open.feishu.cn/document/ukTMukTMukTM/uYTM5UjL2ETO14iNxkTN/scope-list)。否则你将无法在文档应用窗口搜索到目标应用。你也可以在 **多维表格高级权限设置** 中添加用户或一个包含应用的群组, 给予这个群自定义的读写等权限。 |
| 403        | 1254304 | Permission denied.                                           | 调用身份缺少高级权限。调用身份需拥有多维表格的可管理权限。了解更多，参考[如何为应用或用户开通文档权限](https://open.feishu.cn/document/ukTMukTMukTM/uczNzUjL3czM14yN3MTN#16c6475a)。 |
| 403        | 1254306 | The tenant or base owner is subject to base plan limits.     | 联系租户管理员申请权益                                       |
| 403        | 1254608 | Same API requests are submitted repeatedly.                  | 基于同一个多维表格版本重复提交了更新请求，常见于并发或时间间隔极短的请求，例如并发将一个视图的信息更新为相同的内容。建议稍后重试 |