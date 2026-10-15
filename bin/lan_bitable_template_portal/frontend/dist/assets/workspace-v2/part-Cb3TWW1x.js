import{b9 as Se,aB as de,as as O,au as D,at as S,aq as E,av as ie,bD as ze,dz as se,dF as he,cS as I,dM as fe,dK as ue,aR as Be,dm as le,aw as Z,b7 as oe,dR as ae,b0 as $e,cD as y,aP as R,aM as w,cq as H,ck as P,d8 as ge,cp as ee,dN as pe,dS as me,aI as U,aS as te,aN as Q,bo as Fe,cM as _e,dZ as ce,cA as Ie,cw as Ae,dl as V,aO as N,cY as De,du as _,e0 as q,a$ as X,aX as J,cj as Pe,bT as ne,E as Te,bL as be,t as Ve,cX as Ee,g as Me,aD as ve,j as Le,ah as Ue,aC as Ne,aj as Oe}from"./app-DXg8PPQD.js";import{S as je}from"./part-UR02RnXY.js";var He={radioSizeSmall:"14px",radioSizeMedium:"16px",radioSizeLarge:"18px",labelPadding:"0 8px",labelFontWeight:"400"};function Ye(e){const{borderColor:t,primaryColor:o,baseColor:l,textColorDisabled:r,inputColorDisabled:u,textColor2:f,opacityDisabled:v,borderRadius:s,fontSizeSmall:m,fontSizeMedium:x,fontSizeLarge:p,heightSmall:h,heightMedium:z,heightLarge:k,lineHeight:n}=e;return{...He,labelLineHeight:n,buttonHeightSmall:h,buttonHeightMedium:z,buttonHeightLarge:k,fontSizeSmall:m,fontSizeMedium:x,fontSizeLarge:p,boxShadow:`inset 0 0 0 1px ${t}`,boxShadowActive:`inset 0 0 0 1px ${o}`,boxShadowFocus:`inset 0 0 0 1px ${o}, 0 0 0 2px ${de(o,{alpha:.2})}`,boxShadowHover:`inset 0 0 0 1px ${o}`,boxShadowDisabled:`inset 0 0 0 1px ${t}`,color:l,colorDisabled:u,colorActive:"#0000",textColor:f,textColorDisabled:r,dotColorActive:o,dotColorDisabled:t,buttonBorderColor:t,buttonBorderColorActive:o,buttonBorderColorHover:t,buttonColor:l,buttonColorActive:l,buttonTextColor:f,buttonTextColorActive:o,buttonTextColorHover:o,opacityDisabled:v,buttonBoxShadowFocus:`inset 0 0 0 1px ${o}, 0 0 0 2px ${de(o,{alpha:.3})}`,buttonBoxShadowHover:"inset 0 0 0 1px #0000",buttonBoxShadow:"inset 0 0 0 1px #0000",buttonBorderRadius:s}}const xe={name:"Radio",common:Se,self:Ye};var Ge=O("radio",`
 line-height: var(--n-label-line-height);
 outline: none;
 position: relative;
 user-select: none;
 -webkit-user-select: none;
 display: inline-flex;
 align-items: flex-start;
 flex-wrap: nowrap;
 font-size: var(--n-font-size);
 word-break: break-word;
`,[D("checked",[S("dot",`
 background-color: var(--n-color-active);
 `)]),S("dot-wrapper",`
 position: relative;
 flex-shrink: 0;
 flex-grow: 0;
 width: var(--n-radio-size);
 `),O("radio-input",`
 position: absolute;
 border: 0;
 width: 0;
 height: 0;
 opacity: 0;
 margin: 0;
 `),S("dot",`
 position: absolute;
 top: 50%;
 left: 0;
 transform: translateY(-50%);
 height: var(--n-radio-size);
 width: var(--n-radio-size);
 background: var(--n-color);
 box-shadow: var(--n-box-shadow);
 border-radius: 50%;
 transition:
 background-color .3s var(--n-bezier),
 box-shadow .3s var(--n-bezier);
 `,[E("&::before",`
 content: "";
 opacity: 0;
 position: absolute;
 left: 4px;
 top: 4px;
 height: calc(100% - 8px);
 width: calc(100% - 8px);
 border-radius: 50%;
 transform: scale(.8);
 background: var(--n-dot-color-active);
 transition: 
 opacity .3s var(--n-bezier),
 background-color .3s var(--n-bezier),
 transform .3s var(--n-bezier);
 `),D("checked",{boxShadow:"var(--n-box-shadow-active)"},[E("&::before",`
 opacity: 1;
 transform: scale(1);
 `)])]),S("label",`
 color: var(--n-text-color);
 padding: var(--n-label-padding);
 font-weight: var(--n-label-font-weight);
 display: inline-block;
 transition: color .3s var(--n-bezier);
 `),ie("disabled",`
 cursor: pointer;
 `,[E("&:hover",[S("dot",{boxShadow:"var(--n-box-shadow-hover)"})]),D("focus",[E("&:not(:active)",[S("dot",{boxShadow:"var(--n-box-shadow-focus)"})])])]),D("disabled",`
 cursor: not-allowed;
 `,[S("dot",{boxShadow:"var(--n-box-shadow-disabled)",backgroundColor:"var(--n-color-disabled)"},[E("&::before",{backgroundColor:"var(--n-dot-color-disabled)"}),D("checked",`
 opacity: 1;
 `)]),S("label",{color:"var(--n-text-color-disabled)"}),O("radio-input",`
 cursor: not-allowed;
 `)])]);const ye={name:String,value:{type:[String,Number,Boolean],default:"on"},checked:{type:Boolean,default:void 0},defaultChecked:Boolean,disabled:{type:Boolean,default:void 0},label:String,size:String,onUpdateChecked:[Function,Array],"onUpdate:checked":[Function,Array],checkedValue:{type:Boolean,default:void 0}},Ce=Be("n-radio-group");function we(e){const t=ze(Ce,null),{mergedClsPrefixRef:o,mergedComponentPropsRef:l}=se(e),r=he(e,{mergedSize(c){var C,T;const{size:b}=e;if(b!==void 0)return b;if(t){const{mergedSizeRef:{value:M}}=t;if(M!==void 0)return M}if(c)return c.mergedSize.value;const g=(T=(C=l==null?void 0:l.value)==null?void 0:C.Radio)==null?void 0:T.size;return g||"medium"},mergedDisabled(c){return!!(e.disabled||t!=null&&t.disabledRef.value||c!=null&&c.disabled.value)}}),{mergedSizeRef:u,mergedDisabledRef:f}=r,v=I(null),s=I(null),m=I(e.defaultChecked),x=le(e,"checked"),p=fe(x,m),h=ue(()=>t?t.valueRef.value===e.value:p.value),z=ue(()=>{const{name:c}=e;if(c!==void 0)return c;if(t)return t.nameRef.value}),k=I(!1);function n(){if(t){const{doUpdateValue:c}=t,{value:b}=e;Z(c,b)}else{const{onUpdateChecked:c,"onUpdate:checked":b}=e,{nTriggerFormInput:g,nTriggerFormChange:C}=r;c&&Z(c,!0),b&&Z(b,!0),g(),C(),m.value=!0}}function B(){f.value||h.value||n()}function $(){B(),v.value&&(v.value.checked=h.value)}function F(){k.value=!1}function A(){k.value=!0}return{mergedClsPrefix:t?t.mergedClsPrefixRef:o,inputRef:v,labelRef:s,mergedName:z,mergedDisabled:f,renderSafeChecked:h,focus:k,mergedSize:u,handleRadioInputChange:$,handleRadioInputBlur:F,handleRadioInputFocus:A}}const We=["value","name","checked","disabled","onChange","onFocus","onBlur"],Ke={...ae.props,...ye};var qe=oe({name:"Radio",props:Ke,setup(e){const t=we(e),o=ae("Radio","-radio",Ge,xe,e,t.mergedClsPrefix),l=U(()=>{const{mergedSize:{value:m}}=t,{common:{cubicBezierEaseInOut:x},self:{boxShadow:p,boxShadowActive:h,boxShadowDisabled:z,boxShadowFocus:k,boxShadowHover:n,color:B,colorDisabled:$,colorActive:F,textColor:A,textColorDisabled:c,dotColorActive:b,dotColorDisabled:g,labelPadding:C,labelLineHeight:T,labelFontWeight:M,[te("fontSize",m)]:Y,[te("radioSize",m)]:j}}=o.value;return{"--n-bezier":x,"--n-label-line-height":T,"--n-label-font-weight":M,"--n-box-shadow":p,"--n-box-shadow-active":h,"--n-box-shadow-disabled":z,"--n-box-shadow-focus":k,"--n-box-shadow-hover":n,"--n-color":B,"--n-color-active":F,"--n-color-disabled":$,"--n-dot-color-active":b,"--n-dot-color-disabled":g,"--n-font-size":Y,"--n-radio-size":j,"--n-text-color":A,"--n-text-color-disabled":c,"--n-label-padding":C}}),{inlineThemeDisabled:r,mergedClsPrefixRef:u,mergedRtlRef:f}=se(e),v=pe("Radio",f,u),s=r?me("radio",U(()=>t.mergedSize.value[0]),l,e):void 0;return Object.assign(t,{rtlEnabled:v,cssVars:r?void 0:l,themeClass:s==null?void 0:s.themeClass,onRender:s==null?void 0:s.onRender})},render(){const{$slots:e,mergedClsPrefix:t,onRender:o,label:l}=this;return o==null||o(),(()=>{const r=$e("f8c6901d8cd45c02");return y(),R("label",{class:P([`${t}-radio`,this.themeClass,this.rtlEnabled&&`${t}-radio--rtl`,this.mergedDisabled&&`${t}-radio--disabled`,this.renderSafeChecked&&`${t}-radio--checked`,this.focus&&`${t}-radio--focus`]),style:ee(this.cssVars)},[w("div",{class:P(`${t}-radio__dot-wrapper`)},[r[0]||(r[0]=H(" ",-1)),w("div",{class:P([`${t}-radio__dot`,this.renderSafeChecked&&`${t}-radio__dot--checked`])},null,2),w("input",{ref:"inputRef",type:"radio",class:P(`${t}-radio-input`),value:this.value,name:this.mergedName,checked:this.renderSafeChecked,disabled:this.mergedDisabled,onChange:this.handleRadioInputChange,onFocus:this.handleRadioInputFocus,onBlur:this.handleRadioInputBlur},null,42,We)],2),H(()=>ge(e.default,u=>!u&&!l?null:(y(),R("div",{ref:"labelRef",class:P(`${t}-radio__label`)},[H(()=>u||l)],2))))],6)})()}});const Xe=["value","name","checked","disabled","onChange","onFocus","onBlur"];var re=oe({name:"RadioButton",props:ye,setup:we,render(){const{mergedClsPrefix:e}=this;return y(),R("label",{class:P([`${e}-radio-button`,this.mergedDisabled&&`${e}-radio-button--disabled`,this.renderSafeChecked&&`${e}-radio-button--checked`,this.focus&&[`${e}-radio-button--focus`]])},[w("input",{ref:"inputRef",type:"radio",class:P(`${e}-radio-input`),value:this.value,name:this.mergedName,checked:this.renderSafeChecked,disabled:this.mergedDisabled,onChange:this.handleRadioInputChange,onFocus:this.handleRadioInputFocus,onBlur:this.handleRadioInputBlur},null,42,Xe),w("div",{class:P(`${e}-radio-button__state-border`)},null,2),H(()=>ge(this.$slots.default,t=>!t&&!this.label?null:(y(),R("div",{ref:"labelRef",class:P(`${e}-radio__label`)},[H(()=>t||this.label)],2))))],2)}});function Ze(e,t="default",o=[]){const l=e.$slots[t];return l===void 0?o:l()}var Je=O("radio-group",`
 display: inline-block;
 font-size: var(--n-font-size);
`,[S("splitor",`
 display: inline-block;
 vertical-align: bottom;
 width: 1px;
 transition:
 background-color .3s var(--n-bezier),
 opacity .3s var(--n-bezier);
 background: var(--n-button-border-color);
 `,[D("checked",{backgroundColor:"var(--n-button-border-color-active)"}),D("disabled",{opacity:"var(--n-opacity-disabled)"})]),D("button-group",`
 white-space: nowrap;
 height: var(--n-height);
 line-height: var(--n-height);
 `,[O("radio-button",{height:"var(--n-height)",lineHeight:"var(--n-height)"}),S("splitor",{height:"var(--n-height)"})]),O("radio-button",`
 vertical-align: bottom;
 outline: none;
 position: relative;
 user-select: none;
 -webkit-user-select: none;
 display: inline-block;
 box-sizing: border-box;
 padding-left: 14px;
 padding-right: 14px;
 white-space: nowrap;
 transition:
 background-color .3s var(--n-bezier),
 opacity .3s var(--n-bezier),
 border-color .3s var(--n-bezier),
 color .3s var(--n-bezier);
 background: var(--n-button-color);
 color: var(--n-button-text-color);
 border-top: 1px solid var(--n-button-border-color);
 border-bottom: 1px solid var(--n-button-border-color);
 `,[O("radio-input",`
 pointer-events: none;
 position: absolute;
 border: 0;
 border-radius: inherit;
 left: 0;
 right: 0;
 top: 0;
 bottom: 0;
 opacity: 0;
 z-index: 1;
 `),S("state-border",`
 z-index: 1;
 pointer-events: none;
 position: absolute;
 box-shadow: var(--n-button-box-shadow);
 transition: box-shadow .3s var(--n-bezier);
 left: -1px;
 bottom: -1px;
 right: -1px;
 top: -1px;
 `),E("&:first-child",`
 border-top-left-radius: var(--n-button-border-radius);
 border-bottom-left-radius: var(--n-button-border-radius);
 border-left: 1px solid var(--n-button-border-color);
 `,[S("state-border",`
 border-top-left-radius: var(--n-button-border-radius);
 border-bottom-left-radius: var(--n-button-border-radius);
 `)]),E("&:last-child",`
 border-top-right-radius: var(--n-button-border-radius);
 border-bottom-right-radius: var(--n-button-border-radius);
 border-right: 1px solid var(--n-button-border-color);
 `,[S("state-border",`
 border-top-right-radius: var(--n-button-border-radius);
 border-bottom-right-radius: var(--n-button-border-radius);
 `)]),ie("disabled",`
 cursor: pointer;
 `,[E("&:hover",[S("state-border",`
 transition: box-shadow .3s var(--n-bezier);
 box-shadow: var(--n-button-box-shadow-hover);
 `),ie("checked",{color:"var(--n-button-text-color-hover)"})]),D("focus",[E("&:not(:active)",[S("state-border",{boxShadow:"var(--n-button-box-shadow-focus)"})])])]),D("checked",`
 background: var(--n-button-color-active);
 color: var(--n-button-text-color-active);
 border-color: var(--n-button-border-color-active);
 `),D("disabled",`
 cursor: not-allowed;
 opacity: var(--n-opacity-disabled);
 `)])]);const Qe=["onFocusin","onFocusout"];function et(e,t,o){var u;const l=[];let r=!1;for(let f=0;f<e.length;++f){const v=e[f],s=(u=v.type)==null?void 0:u.name;s==="RadioButton"&&(r=!0);const m=v.props;if(s!=="RadioButton"){l.push(v);continue}if(f===0)l.push(v);else{const x=l[l.length-1].props,p=t===x.value,h=x.disabled,z=t===m.value,k=m.disabled,n=(p?2:0)+(h?0:1),B=(z?2:0)+(k?0:1),$={[`${o}-radio-group__splitor--disabled`]:h,[`${o}-radio-group__splitor--checked`]:p},F={[`${o}-radio-group__splitor--disabled`]:k,[`${o}-radio-group__splitor--checked`]:z},A=n<B?F:$;l.push((y(),R("div",{key:1,class:P([`${o}-radio-group__splitor`,A])},null,2)),v)}}return{children:l,isButtonGroup:r}}const tt={...ae.props,name:String,options:Array,labelField:{type:String,default:"label"},valueField:{type:String,default:"value"},value:[String,Number,Boolean],defaultValue:{type:[String,Number,Boolean],default:null},size:String,disabled:{type:Boolean,default:void 0},"onUpdate:value":[Function,Array],onUpdateValue:[Function,Array]};var ot=oe({name:"RadioGroup",props:tt,setup(e){const t=I(null),{mergedSizeRef:o,mergedDisabledRef:l,nTriggerFormChange:r,nTriggerFormInput:u,nTriggerFormBlur:f,nTriggerFormFocus:v}=he(e),{mergedClsPrefixRef:s,inlineThemeDisabled:m,mergedRtlRef:x}=se(e),p=ae("Radio","-radio-group",Je,xe,e,s),h=I(e.defaultValue),z=le(e,"value"),k=fe(z,h);function n(b){const{onUpdateValue:g,"onUpdate:value":C}=e;g&&Z(g,b),C&&Z(C,b),h.value=b,r(),u()}function B(b){const{value:g}=t;g&&(g.contains(b.relatedTarget)||v())}function $(b){const{value:g}=t;g&&(g.contains(b.relatedTarget)||f())}_e(Ce,{mergedClsPrefixRef:s,nameRef:le(e,"name"),valueRef:k,disabledRef:l,mergedSizeRef:o,doUpdateValue:n});const F=pe("Radio",x,s),A=U(()=>{const{value:b}=o,{common:{cubicBezierEaseInOut:g},self:{buttonBorderColor:C,buttonBorderColorActive:T,buttonBorderRadius:M,buttonBoxShadow:Y,buttonBoxShadowFocus:j,buttonBoxShadowHover:G,buttonColor:W,buttonColorActive:d,buttonTextColor:a,buttonTextColorActive:i,buttonTextColorHover:L,opacityDisabled:K,[te("buttonHeight",b)]:ke,[te("fontSize",b)]:Re}}=p.value;return{"--n-font-size":Re,"--n-bezier":g,"--n-button-border-color":C,"--n-button-border-color-active":T,"--n-button-border-radius":M,"--n-button-box-shadow":Y,"--n-button-box-shadow-focus":j,"--n-button-box-shadow-hover":G,"--n-button-color":W,"--n-button-color-active":d,"--n-button-text-color":a,"--n-button-text-color-hover":L,"--n-button-text-color-active":i,"--n-height":ke,"--n-opacity-disabled":K}}),c=m?me("radio-group",U(()=>o.value[0]),A,e):void 0;return{selfElRef:t,rtlEnabled:F,mergedClsPrefix:s,mergedValue:k,handleFocusout:$,handleFocusin:B,cssVars:m?void 0:A,themeClass:c==null?void 0:c.themeClass,onRender:c==null?void 0:c.onRender}},render(){var m;const{mergedValue:e,mergedClsPrefix:t,handleFocusin:o,handleFocusout:l}=this,{options:r,labelField:u,valueField:f}=this.$props,{children:v,isButtonGroup:s}=et(r?r.map(x=>{const p=x[f];return y(),Q(qe,{key:typeof p=="boolean"?`__n_${p}`:p,value:p,disabled:x.disabled,label:x[u]},null,8,["value","disabled","label"])}):Fe(Ze(this)),e,t);return(m=this.onRender)==null||m.call(this),y(),R("div",{onFocusin:o,onFocusout:l,ref:"selfElRef",class:P([`${t}-radio-group`,this.rtlEnabled&&`${t}-radio-group--rtl`,this.themeClass,s&&`${t}-radio-group--button-group`]),style:ee(this.cssVars)},[H(()=>v)],46,Qe)}});const at=["aria-label","aria-busy"],nt={key:0},rt={key:0},it={class:"v2-chart-surface"},lt=["aria-hidden","aria-label"],st={key:1,class:"v2-chart-total","aria-hidden":"true"},dt={key:2,class:"v2-chart-overlay"},ut={key:4,class:"v2-chart-overlay v2-chart-zero"},ct=["open"],bt={class:"v2-chart-details","aria-label":"图表明细"},vt=oe({__name:"MotionChart",props:{title:{},caption:{},items:{},loading:{type:Boolean},failed:{type:Boolean},trend:{type:Boolean},view:{},unit:{default:"条"}},emits:["select"],setup(e,{emit:t}){const o=e,l=t,r=I(),u=I(o.view||"bar"),f=I(!1),v=I(null),s=I(!1),m=I(!1),x=I(!1),p=U(()=>o.items.some(d=>!Number.isFinite(d.value)||d.value<0)),h=U(()=>!!(o.loading||o.failed||p.value||!o.items.length)),z=U(()=>p.value?0:o.items.reduce((d,a)=>d+a.value,0)),k=U(()=>o.items.map(d=>`${d.label} ${d.value} ${o.unit}`).join("，"));let n,B,$,F=!1,A=!1,c=0,b=!1;const g=window.matchMedia("(prefers-reduced-motion: reduce)");function C(){if(v.value=null,!(!n||h.value))try{n.setOption(Ne(o.items,u.value,o.unit,x.value,!g.matches&&m.value),{replaceMerge:["series","xAxis","yAxis","dataZoom"],lazyUpdate:!0}),s.value=!1}catch{s.value=!0}}function T(){if(!n||!o.items.length||!ne(u.value)||h.value)return;const d=o.items.length-1,a=n.convertToPixel({seriesIndex:0},[d,o.items[d].value]);v.value=Array.isArray(a)&&a.every(Number.isFinite)?a:null}async function M(){if(!(A||n||F)){A=!0;try{const{init:d}=await Ue(async()=>{const{init:a}=await import("./part-DXdJ9xix.js");return{init:a}},[]);if(F||!r.value)return;x.value=r.value.clientWidth<440,n=d(r.value,void 0,{renderer:"svg"}),n.on("finished",T),n.on("mousedown",a=>{var K;const i=(K=a.event)==null?void 0:K.target;if(g.matches||!["bar","horizontal"].includes(u.value)||(i==null?void 0:i.type)!=="rect")return;const L=i.getBoundingRect();i.originY=L.y+L.height,i.animateTo({scaleY:.96},{duration:90,done:()=>{F||i.animateTo({scaleY:1},{duration:220,easing:"cubicOut"})}})}),n.on("click",a=>{if(h.value)return;const i=o.items[a.dataIndex];i&&l("select",i,a.dataIndex)}),B=new ResizeObserver(()=>{cancelAnimationFrame(c),c=requestAnimationFrame(()=>{var i;if(F)return;n==null||n.resize();const a=(((i=r.value)==null?void 0:i.clientWidth)||0)<440;x.value!==a&&(x.value=a,C()),T()})}),B.observe(r.value),C()}catch{s.value=!0}finally{A=!1}}}function Y(d){var a,i;!ne(u.value)||!n||h.value||(f.value=!0,(i=(a=r.value)==null?void 0:a.setPointerCapture)==null||i.call(a,d.pointerId),j(d))}function j(d){if(!f.value||!n||!r.value||h.value)return;const a=r.value.getBoundingClientRect(),i=n.convertFromPixel({seriesIndex:0},[d.clientX-a.left,d.clientY-a.top]);!Array.isArray(i)||!Number.isFinite(i[0])||n.dispatchAction({type:"showTip",seriesIndex:0,dataIndex:Math.max(0,Math.min(o.items.length-1,Math.round(i[0])))})}function G(){f.value&&(f.value=!1,n==null||n.dispatchAction({type:"hideTip"}))}function W(){m.value=b&&!document.hidden,C()}return ce(()=>[o.items,o.loading,o.failed,o.unit,u.value],C,{deep:!0}),ce(()=>o.view,d=>{d&&(u.value=d)}),Ie(()=>{$=new IntersectionObserver(d=>{b=d.some(a=>a.isIntersecting),W(),b&&M()},{rootMargin:"80px"}),r.value&&$.observe(r.value),g.addEventListener("change",C),document.addEventListener("visibilitychange",W)}),Ae(()=>{F=!0,$==null||$.disconnect(),B==null||B.disconnect(),cancelAnimationFrame(c),g.removeEventListener("change",C),document.removeEventListener("visibilitychange",W),n==null||n.dispose()}),(d,a)=>(y(),R("section",{class:"v2-motion-chart","aria-label":e.title||"业务图表","aria-busy":e.loading},[e.title||e.caption||e.trend||d.$slots.actions?(y(),R("header",nt,[e.title?(y(),R("h3",rt,V(e.title),1)):N("",!0),w("span",null,V(e.caption),1),De(d.$slots,"actions",{},void 0,!0),e.trend?(y(),Q(_(ot),{key:1,value:u.value,"onUpdate:value":a[0]||(a[0]=i=>u.value=i),size:"small","aria-label":"图表类型"},{default:q(()=>[X(_(re),{value:"bar"},{default:q(()=>[...a[1]||(a[1]=[J("柱状图",-1)])]),_:1}),X(_(re),{value:"line"},{default:q(()=>[...a[2]||(a[2]=[J("折线图",-1)])]),_:1}),X(_(re),{value:"area"},{default:q(()=>[...a[3]||(a[3]=[J("面积图",-1)])]),_:1})]),_:1},8,["value"])):N("",!0)])):N("",!0),w("div",it,[w("div",{ref_key:"host",ref:r,class:Pe(["v2-chart-canvas",{muted:h.value}]),role:"img","aria-hidden":h.value,"aria-label":k.value,onPointerdown:Y,onPointermove:j,onPointerup:G,onPointercancel:G,onPointerleave:G},null,42,lt),m.value&&_(ne)(u.value)&&v.value&&!f.value&&!h.value&&!s.value?(y(),R("span",{key:0,class:"v2-chart-end",style:ee({left:v.value[0]+"px",top:v.value[1]+"px"}),"aria-hidden":"true"},null,4)):N("",!0),!h.value&&!s.value&&u.value==="donut"?(y(),R("div",st,[w("strong",null,V(z.value.toLocaleString("zh-CN")),1),w("span",null,"合计 · "+V(e.unit),1)])):N("",!0),e.loading?(y(),R("div",dt,[X(_(je),{height:"100%",sharp:!1})])):e.failed||p.value||s.value||!e.items.length?(y(),Q(_(Te),{key:3,class:"v2-chart-overlay",description:e.failed||p.value?"数据暂不可用":s.value?"图表暂不可用，请查看数据明细":"暂无数据",size:"small"},null,8,["description"])):_(be)(u.value)&&z.value===0?(y(),R("p",ut,"当前合计 0 "+V(e.unit),1)):N("",!0)]),!e.loading&&!e.failed&&!p.value&&e.items.length?(y(),R("details",{key:1,class:"v2-chart-data",open:s.value||x.value&&_(be)(u.value)},[w("summary",null,[a[4]||(a[4]=J("数据明细 ",-1)),w("span",null,V(e.items.length)+" 项",1)]),w("div",bt,[(y(!0),R(Ve,null,Ee(e.items,(i,L)=>(y(),Q(_(Me),{key:String(i.key??i.label)+L,"data-v2-navigation":"",text:"",onClick:K=>d.$emit("select",i,L)},{default:q(()=>[w("i",{style:ee({background:_(ve)[L%_(ve).length]}),"aria-hidden":"true"},null,4),w("span",null,V(i.label),1),w("strong",null,V(i.value.toLocaleString("zh-CN"))+" "+V(e.unit),1),X(_(Le),{size:14})]),_:2},1032,["onClick"]))),128))])],8,ct)):N("",!0)],8,at))}}),ht=Oe(vt,[["__scopeId","data-v-05763c9a"]]),pt=Object.freeze(Object.defineProperty({__proto__:null,default:ht},Symbol.toStringTag,{value:"Module"}));export{ht as D,pt as M,re as R,ot as a,qe as b,xe as r};
