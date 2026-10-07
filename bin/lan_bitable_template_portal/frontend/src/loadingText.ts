export function isLoadingText(text: string): boolean {
  return !/失败|错误|中断|超时/.test(text)
    && /(?:正在.*(?:加载|读取|进入|查询|同步|保存|提交|连接|检查|核验|处理|导出|上传|下载|搜索|准备|生成))|(?:加载|读取|进入|查询|同步|保存|提交|连接|检查|核验|处理)中/.test(text);
}
