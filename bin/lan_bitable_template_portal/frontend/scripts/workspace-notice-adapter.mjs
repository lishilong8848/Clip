// Only the independent V2 build consumes the original parser's HTML response as data.
export function adaptNoticeParserClient(code) {
  const marker='const parsed: unknown = await response.json();';
  if(code.split(marker).length!==2)throw new Error('Notice parser transport contract changed');
  return code.replace(marker, `const parsed: unknown = response.ok && path.split('?')[0] === '/workbench-lite/parse' && response.headers.get('Content-Type')?.includes('text/html')
          ? {fragments:{detail:await response.text()}} : await response.json();`);
}
