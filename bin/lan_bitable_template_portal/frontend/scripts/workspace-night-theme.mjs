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
    if(/(?:surface|canvas|panel|soft|bg|background|selected)(?:-|$)/.test(prop))return 'surface';
    if(/^--n-color(?:-|$)/.test(prop))return 'surface';
    if(/border|divider|(?:^|-)line(?:-|$)|^--filter-hover$/.test(prop))return 'border';
    if(/shadow/.test(prop))return 'shadow';
    if(/(?:text|ink|muted|blue|danger|warning|success|placeholder|caret)(?:-|$)/.test(prop))return 'text';
  }
  return '';
}
function brandBlue(rgba) {
  return rgba&&rgba[2]>rgba[0]+45&&rgba[2]>rgba[1]*1.08&&rgba[1]>rgba[0]+15;
}
function paletteColor(token,role,dark,variant) {
  const rgba=parseColor(token);if(!rgba)return token;
  const [r,g,b,a]=rgba,light=(Math.max(r,g,b)+Math.min(r,g,b))/510,neutral=Math.max(r,g,b)-Math.min(r,g,b)<32;
  let name='';
  if(brandBlue(rgba)&&['text','surface','border'].includes(role))name=role==='surface'?variant:'accent-text';
  else if(neutral&&role==='surface'&&light>.65)name=light>.97?'surface':'soft';
  else if(neutral&&role==='border'&&light>.48)name='border';
  else if(neutral&&role==='text'&&light<.74)name=light<.35?'text':'muted';
  if(!name)return dark?nightColor(token,role):token;
  const variable='var(--v2-palette-'+name+','+(dark?nightColor(token,role):token)+')';
  return a===1?variable:'color-mix(in srgb,'+variable+' '+Number((a*100).toFixed(2))+'%,transparent)';
}
const rootSelector='html.workspace-v2[data-v2-theme="dark"]';
function scope(selector,dark=true) {
  const prefix=dark?rootSelector:'html.workspace-v2[data-v2-theme="light"][data-v2-palette]';
  if(/^(?::root|html|\.workspace-v2)(?![\w-])/.test(selector))return selector.replace(/^(?::root|html|\.workspace-v2)/,prefix);
  return prefix+' '+selector;
}
export default function workspaceNightTheme() {
  return {postcssPlugin:'workspace-night-theme',OnceExit(root) {
    const rules=[];root.walkRules(rule=>rules.push(rule));
    for(const rule of rules) {
      const selectors=postcss.list.comma(rule.selector).filter(selector=>!/data-v2-theme|weather-lighthouse|\.wl-|\.lighthouse|\.assistant-|\.bot-|signature-pad|signature-canvas|print-sheet|\.sheet-(?:table-wrap|preview-table|signers|image-warning)\b/.test(selector));
      if(!selectors.length)continue;
      let excluded=false;
      for(let parent=rule.parent;parent;parent=parent.parent)if(parent.type==='atrule'&&(/keyframes/.test(parent.name)||/print|forced-colors/.test(parent.params)))excluded=true;
      if(excluded)continue;
      for(const dark of [false,true]) {
      const declarations=[];
      const accentFill=rule.nodes.some(decl=>decl.type==='decl'&&/^background(?:-color)?$/.test(decl.prop)&&/^var\(--(?:v2-blue|mb-accent|cf-brand-blue)(?:,|\))/.test(decl.value));
      const literalAccent=rule.nodes.some(decl=>decl.type==='decl'&&/^background(?:-color)?$/.test(decl.prop)&&brandBlue(parseColor(decl.value)));
      const variant=/:active/.test(rule.selector)?'pressed':/:hover|:focus/.test(rule.selector)?'hover':'accent';
      rule.each(decl=>{
        if(decl.type!=='decl')return;
        const role=decl.prop==='fill'&&/(?:\btext\b|label)/.test(rule.selector)?'text':roleFor(decl.prop);
        if(!role)return;
        const value=(accentFill||literalAccent)&&decl.prop==='color'&&/^(?:white|#fff|#ffffff)$/i.test(decl.value)?literalAccent?'var(--v2-palette-on-accent)':'var(--v2-on-accent, #14202a)':/url\(|light-dark\(|var\(--(?:lh-|bot-)/.test(decl.value)?decl.value:decl.value.replace(colorToken,color=>paletteColor(color,role,dark,variant));
        // Keep later transparent/none/var() overrides at the same specificity as converted colors.
        declarations.push(decl.clone({value}));
      });
      if(!declarations.length)continue;
      const themed=rule.clone({selector:selectors.map(selector=>scope(selector,dark)).join(',')});
      themed.removeAll();themed.append(declarations);
      const screen=postcss.atRule({name:'media',params:'screen and (forced-colors: none)'});
      screen.append(themed);rule.after(screen);
      }
    }
  }};
}
