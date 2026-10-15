import postcss from 'postcss';

// Build-time presentation only: shared/legacy source and runtime data stay untouched.
const colorToken = /#[\da-f]{8}\b|#[\da-f]{6}\b|#[\da-f]{4}\b|#[\da-f]{3}\b|\brgba?\([^()]*\)|\b(?:white|black|whitesmoke|snow|ivory)\b/gi;
function parseColor(token) {
  const named={white:'#ffffff',black:'#000000',whitesmoke:'#f5f5f5',snow:'#fffafa',ivory:'#fffff0'};
  token=named[token.toLowerCase()]||token;
  if(token[0]==='#') {
    let hex=token.slice(1);
    if(hex.length<=4)hex=[...hex].map(c=>c+c).join('');
    return [...[0,2,4].map(i=>parseInt(hex.slice(i,i+2),16)),hex.length===8?parseInt(hex.slice(6),16)/255:1];
  }
  const values=token.slice(token.indexOf('(')+1,-1).trim().split(/[\s,/]+/);
  if(values.length<3||values.length>4||values.some(value=>!/^\d*\.?\d+%?$/.test(value)))return null;
  return values.slice(0,3).map(v=>parseFloat(v)*(v.endsWith('%')?2.55:1)).concat(values[3]===undefined?1:parseFloat(values[3])/(values[3].endsWith('%')?100:1));
}
export function nightColor(token,role) {
  const rgba=parseColor(token);if(!rgba)return token;
  const [r,g,b,a]=rgba,highest=Math.max(r,g,b),lowest=Math.min(r,g,b),light=(highest+lowest)/510;
  let rgb=rgba.slice(0,3);
  if(role==='text') {
    if(light<.74) {
      if(highest-lowest<32){const level=Math.max(160,Math.round(242-light*116));rgb=[level,level+4,level+9];}
      else rgb=rgb.map(c=>c+(255-c)*.54);
    }
  }else if(role==='border') {
    if(light>.48)rgb=rgb.map(c=>Math.round(43+c*.13));
  }else if(role==='shadow')rgb=[0,0,0];
  else if(role==='surface'&&light>.65)rgb=rgb.map(c=>Math.round(27+c*.04+(c-(r+g+b)/3)*.18));
  if(rgb.every((c,i)=>c===rgba[i]))return token;
  return a===1?'#'+rgb.map(c=>Math.round(c).toString(16).padStart(2,'0')).join(''):`rgba(${rgb.map(Math.round).join(',')},${Number(a.toFixed(3))})`;
}
function roleFor(prop) {
  if(/^(?:color|caret-color|text-decoration-color)$/.test(prop))return 'text';
  if(/^background(?:-color|-image)?$/.test(prop))return 'surface';
  if(/^(?:border|outline)(?:-|$)/.test(prop))return 'border';
  if(/^(?:box|text)-shadow$/.test(prop))return 'shadow';
  if(prop.startsWith('--')) {
    if(/(?:surface|canvas|bg|background)(?:-|$)/.test(prop))return 'surface';
    if(/border|divider/.test(prop))return 'border';
    if(/shadow/.test(prop))return 'shadow';
    if(/(?:text|ink|muted|blue|danger|warning|success)(?:-|$)/.test(prop))return 'text';
  }
  return '';
}
const rootSelector='html.workspace-v2[data-v2-theme="dark"]';
function scope(selector) {
  if(/^(?::root|html|\.workspace-v2)(?![\w-])/.test(selector))return selector.replace(/^(?::root|html|\.workspace-v2)/,rootSelector);
  return rootSelector+' '+selector;
}
export default function workspaceNightTheme() {
  return {postcssPlugin:'workspace-night-theme',OnceExit(root) {
    const rules=[];root.walkRules(rule=>rules.push(rule));
    for(const rule of rules) {
      if(/data-v2-theme|weather-lighthouse|\.wl-|\.lighthouse|\.assistant-|\.bot-|signature-pad|signature-canvas|print-sheet/.test(rule.selector))continue;
      let excluded=false;
      for(let parent=rule.parent;parent;parent=parent.parent)if(parent.type==='atrule'&&(/keyframes/.test(parent.name)||/print|forced-colors/.test(parent.params)))excluded=true;
      if(excluded)continue;
      const declarations=[];
      rule.each(decl=>{
        if(decl.type!=='decl')return;
        const role=decl.prop==='fill'&&/(?:\btext\b|label)/.test(rule.selector)?'text':roleFor(decl.prop);
        if(!role||/url\(|light-dark\(|var\(--(?:lh-|bot-)/.test(decl.value))return;
        const value=decl.value.replace(colorToken,color=>nightColor(color,role));
        if(value!==decl.value)declarations.push(decl.clone({value}));
      });
      if(!declarations.length)continue;
      const themed=rule.clone({selector:postcss.list.comma(rule.selector).map(scope).join(',')});
      themed.removeAll();themed.append(declarations);
      const screen=postcss.atRule({name:'media',params:'screen and (forced-colors: none)'});
      screen.append(themed);rule.after(screen);
    }
  }};
}
