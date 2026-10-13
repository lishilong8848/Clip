"""Offline Mermaid flowcharts from bounded structured data, never user scripts."""
import hashlib
import math
from pathlib import Path

from .lighthouse_ai import AssistantError
from .lighthouse_artifacts import check_content

MERMAID_VERSION = '12.1.0'
MERMAID_SHA256 = '6484afc32872a3aa16cac9a76ba1816a1ed4cc870a6593cc2e17757750f518b2'
MERMAID_ASSET = Path(__file__).resolve().parents[2] / 'resources/assistant_tools/mermaid.min.js'


def flowchart_source(title, nodes, edges, direction='TD'):
    check_content({'title': title, 'nodes': nodes, 'edges': edges})
    if not isinstance(title, str) or not 1 <= len(title) <= 60 or direction not in {'TD', 'LR'}:
        raise AssistantError('标题为1至60字，方向为TD（纵向）或LR（横向）。')
    if not isinstance(nodes, list) or not 1 <= len(nodes) <= 40 or not isinstance(edges, list) or len(edges) > 80:
        raise AssistantError('流程图最多40个节点、80条连线，请将复杂流程拆图。')

    def label(value, limit=80):
        if not isinstance(value, str) or not 1 <= len(value) <= limit:
            raise AssistantError('节点文字为1至80字，连线文字最多30字。')
        return ''.join(f'#{ord(char)};' if char in '#&"<>|`\\\r\n' else char for char in value)

    identities, lines = {}, ['flowchart ' + direction]
    shapes = {'process': ('[', ']'), 'decision': ('{', '}'), 'terminal': ('([', '])')}
    for index, node in enumerate(nodes):
        if not isinstance(node, dict) or set(node) - {'id', 'label', 'kind'} or not isinstance(node.get('id'), str) or not 1 <= len(node['id']) <= 40 or node['id'] in identities:
            raise AssistantError('节点须有唯一id与label，可选kind为process、decision或terminal。')
        kind = node.get('kind', 'process')
        if kind not in shapes:
            raise AssistantError('不支持此节点类型。')
        identifier = 'n' + str(index)
        identities[node['id']] = identifier
        left, right = shapes[kind]
        lines.append(f'{identifier}{left}"{label(node.get("label"))}"{right}')
    for edge in edges:
        if not isinstance(edge, dict) or set(edge) - {'from', 'to', 'label'} or any(not isinstance(edge.get(key), str) or edge[key] not in identities for key in ('from', 'to')):
            raise AssistantError('每条连线的from、to必须对应已有节点。')
        text = f'|"{label(edge["label"], 30)}"|' if edge.get('label') else ''
        lines.append(f'{identities[edge["from"]]} -->{text} {identities[edge["to"]]}')
    return '\n'.join(lines)


def render_flowchart(title, nodes, edges, direction='TD'):
    source = flowchart_source(title, nodes, edges, direction)
    if not MERMAID_ASSET.is_file() or hashlib.sha256(MERMAID_ASSET.read_bytes()).hexdigest() != MERMAID_SHA256:
        raise AssistantError('本地流程图库缺失或校验失败，请更新程序后重试。', 503)
    from playwright.sync_api import sync_playwright, Error as BrowserError
    with sync_playwright() as driver:
        try:
            browser = driver.chromium.launch(channel='msedge', headless=True, chromium_sandbox=True,
                args=['--disable-background-networking', '--disable-component-update', '--disable-sync', '--no-first-run'])
        except BrowserError:
            raise AssistantError('未能启动本机Edge进行离线绘图，请确认已安装Edge且未被系统策略禁止。', 503) from None
        try:
            context = browser.new_context(viewport={'width': 1200, 'height': 900}, device_scale_factor=1,
                offline=True, service_workers='block', accept_downloads=False, permissions=[])
            context.route('**/*', lambda route: route.abort())
            context.route_web_socket('**/*', lambda route: route.close())
            page = context.new_page()
            page.set_default_timeout(20000)
            page.set_content('<!doctype html><html lang="zh-CN"><meta charset="utf-8"><style>body{margin:0;background:white;color:#25343e;font-family:"Microsoft YaHei",sans-serif}main{display:inline-block;padding:28px;box-sizing:border-box}h1{font-size:20px;font-weight:600;margin:0 0 24px;overflow-wrap:anywhere}svg{display:block;max-width:none!important}</style><main><h1></h1><div id="diagram"></div></main></html>')
            page.locator('h1').evaluate('(element,title) => element.textContent=title', title)
            page.add_script_tag(path=str(MERMAID_ASSET))
            page.evaluate('''async source => {
                mermaid.initialize({startOnLoad:false,securityLevel:'strict',htmlLabels:false,
                    maxTextSize:16000,maxEdges:80,fontFamily:'Microsoft YaHei, sans-serif',
                    theme:'base',themeVariables:{primaryColor:'#eef5fb',primaryTextColor:'#25343e',
                        primaryBorderColor:'#287ab8',lineColor:'#6a8192',secondaryColor:'#e7f6f1'},
                    flowchart:{htmlLabels:false,useMaxWidth:false,curve:'basis',nodeSpacing:35,rankSpacing:45}});
                const result = await mermaid.render('lighthouseFlow',source);
                document.querySelector('#diagram').innerHTML=result.svg;
                const svg=document.querySelector('svg'), box=svg.viewBox.baseVal;
                svg.setAttribute('width',String(box.width));svg.setAttribute('height',String(box.height));
                document.querySelector('main').style.width=Math.max(520,box.width+56)+'px';
            }''', source)
            rectangle = page.locator('main').bounding_box()
            if not rectangle or any(not math.isfinite(rectangle[key]) for key in ('width', 'height')) or rectangle['width'] > 2200 or rectangle['height'] > 4200:
                raise AssistantError('流程图过宽或过长，请拆分节点或调整横向/纵向后生成。', 413)
            content = page.locator('main').screenshot(type='png', animations='disabled')
            return {'files': [{'name': '流程图.png', 'content': content}],
                'summary': {'nodes': len(nodes), 'edges': len(edges), 'direction': direction,
                            'note': '本地离线绘制，不核验或代替现场操作流程审批。'}}
        finally:
            browser.close()
