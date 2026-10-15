import{be as Se,aG as de,ax as O,az as P,ay as S,av as E,aA as ie,bI as ze,dE as se,dK as he,cX as I,dR as fe,dP as ue,aW as Be,ds as le,aB as Q,bc as oe,dW as ae,b5 as $e,cI as y,aU as R,aR as k,cv as j,cp as T,dd as ge,cu as ee,dS as pe,dX as me,aN as L,aX as te,aS as J,bt as Fe,cR as _e,e2 as ce,cF as Ie,cB as Ae,dr as V,aT as N,d1 as Pe,dz as _,e5 as X,b4 as q,b0 as Z,co as Te,bY as ne,E as De,bQ as be,w as Ve,d0 as Ee,h as Me,aI as ve,k as Ue,am as Le,aH as Ne,ao as Oe}from"./app-v2-BtC_jOn5.js";import{S as He}from"./part-v2-Cr5-k1f_.js";var je={radioSizeSmall:"14px",radioSizeMedium:"16px",radioSizeLarge:"18px",labelPadding:"0 8px",labelFontWeight:"400"};function We(e){const{borderColor:t,primaryColor:o,baseColor:l,textColorDisabled:r,inputColorDisabled:u,textColor2:f,opacityDisabled:v,borderRadius:s,fontSizeSmall:m,fontSizeMedium:x,fontSizeLarge:p,heightSmall:h,heightMedium:z,heightLarge:w,lineHeight:n}=e;return{...je,labelLineHeight:n,buttonHeightSmall:h,buttonHeightMedium:z,buttonHeightLarge:w,fontSizeSmall:m,fontSizeMedium:x,fontSizeLarge:p,boxShadow:`inset 0 0 0 1px ${t}`,boxShadowActive:`inset 0 0 0 1px ${o}`,boxShadowFocus:`inset 0 0 0 1px ${o}, 0 0 0 2px ${de(o,{alpha:.2})}`,boxShadowHover:`inset 0 0 0 1px ${o}`,boxShadowDisabled:`inset 0 0 0 1px ${t}`,color:l,colorDisabled:u,colorActive:"#0000",textColor:f,textColorDisabled:r,dotColorActive:o,dotColorDisabled:t,buttonBorderColor:t,buttonBorderColorActive:o,buttonBorderColorHover:t,buttonColor:l,buttonColorActive:l,buttonTextColor:f,buttonTextColorActive:o,buttonTextColorHover:o,opacityDisabled:v,buttonBoxShadowFocus:`inset 0 0 0 1px ${o}, 0 0 0 2px ${de(o,{alpha:.3})}`,buttonBoxShadowHover:"inset 0 0 0 1px #0000",buttonBoxShadow:"inset 0 0 0 1px #0000",buttonBorderRadius:s}}const xe={name:"Radio",common:Se,self:We};var Ge=O("radio",`
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
`,[P("checked",[S("dot",`
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
 `),P("checked",{boxShadow:"var(--n-box-shadow-active)"},[E("&::before",`
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
 `,[E("&:hover",[S("dot",{boxShadow:"var(--n-box-shadow-hover)"})]),P("focus",[E("&:not(:active)",[S("dot",{boxShadow:"var(--n-box-shadow-focus)"})])])]),P("disabled",`
 cursor: not-allowed;
 `,[S("dot",{boxShadow:"var(--n-box-shadow-disabled)",backgroundColor:"var(--n-color-disabled)"},[E("&::before",{backgroundColor:"var(--n-dot-color-disabled)"}),P("checked",`
 opacity: 1;
 `)]),S("label",{color:"var(--n-text-color-disabled)"}),O("radio-input",`
 cursor: not-allowed;
 `)])]);const ye={name:String,value:{type:[String,Number,Boolean],default:"on"},checked:{type:Boolean,default:void 0},defaultChecked:Boolean,disabled:{type:Boolean,default:void 0},label:String,size:String,onUpdateChecked:[Function,Array],"onUpdate:checked":[Function,Array],checkedValue:{type:Boolean,default:void 0}},Ce=Be("n-radio-group");function ke(e){const t=ze(Ce,null),{mergedClsPrefixRef:o,mergedComponentPropsRef:l}=se(e),r=he(e,{mergedSize(c){var C,D;const{size:b}=e;if(b!==void 0)return b;if(t){const{mergedSizeRef:{value:M}}=t;if(M!==void 0)return M}if(c)return c.mergedSize.value;const g=(D=(C=l==null?void 0:l.value)==null?void 0:C.Radio)==null?void 0:D.size;return g||"medium"},mergedDisabled(c){return!!(e.disabled||t!=null&&t.disabledRef.value||c!=null&&c.disabled.value)}}),{mergedSizeRef:u,mergedDisabledRef:f}=r,v=I(null),s=I(null),m=I(e.defaultChecked),x=le(e,"checked"),p=fe(x,m),h=ue(()=>t?t.valueRef.value===e.value:p.value),z=ue(()=>{const{name:c}=e;if(c!==void 0)return c;if(t)return t.nameRef.value}),w=I(!1);function n(){if(t){const{doUpdateValue:c}=t,{value:b}=e;Q(c,b)}else{const{onUpdateChecked:c,"onUpdate:checked":b}=e,{nTriggerFormInput:g,nTriggerFormChange:C}=r;c&&Q(c,!0),b&&Q(b,!0),g(),C(),m.value=!0}}function B(){f.value||h.value||n()}function $(){B(),v.value&&(v.value.checked=h.value)}function F(){w.value=!1}function A(){w.value=!0}return{mergedClsPrefix:t?t.mergedClsPrefixRef:o,inputRef:v,labelRef:s,mergedName:z,mergedDisabled:f,renderSafeChecked:h,focus:w,mergedSize:u,handleRadioInputChange:$,handleRadioInputBlur:F,handleRadioInputFocus:A}}const Ye=["value","name","checked","disabled","onChange","onFocus","onBlur"],Ke={...ae.props,...ye};var Xe=oe({name:"Radio",props:Ke,setup(e){const t=ke(e),o=ae("Radio","-radio",Ge,xe,e,t.mergedClsPrefix),l=L(()=>{const{mergedSize:{value:m}}=t,{common:{cubicBezierEaseInOut:x},self:{boxShadow:p,boxShadowActive:h,boxShadowDisabled:z,boxShadowFocus:w,boxShadowHover:n,color:B,colorDisabled:$,colorActive:F,textColor:A,textColorDisabled:c,dotColorActive:b,dotColorDisabled:g,labelPadding:C,labelLineHeight:D,labelFontWeight:M,[te("fontSize",m)]:W,[te("radioSize",m)]:H}}=o.value;return{"--n-bezier":x,"--n-label-line-height":D,"--n-label-font-weight":M,"--n-box-shadow":p,"--n-box-shadow-active":h,"--n-box-shadow-disabled":z,"--n-box-shadow-focus":w,"--n-box-shadow-hover":n,"--n-color":B,"--n-color-active":F,"--n-color-disabled":$,"--n-dot-color-active":b,"--n-dot-color-disabled":g,"--n-font-size":W,"--n-radio-size":H,"--n-text-color":A,"--n-text-color-disabled":c,"--n-label-padding":C}}),{inlineThemeDisabled:r,mergedClsPrefixRef:u,mergedRtlRef:f}=se(e),v=pe("Radio",f,u),s=r?me("radio",L(()=>t.mergedSize.value[0]),l,e):void 0;return Object.assign(t,{rtlEnabled:v,cssVars:r?void 0:l,themeClass:s==null?void 0:s.themeClass,onRender:s==null?void 0:s.onRender})},render(){const{$slots:e,mergedClsPrefix:t,onRender:o,label:l}=this;return o==null||o(),(()=>{const r=$e("f8c6901d8cd45c02");return y(),R("label",{class:T([`${t}-radio`,this.themeClass,this.rtlEnabled&&`${t}-radio--rtl`,this.mergedDisabled&&`${t}-radio--disabled`,this.renderSafeChecked&&`${t}-radio--checked`,this.focus&&`${t}-radio--focus`]),style:ee(this.cssVars)},[k("div",{class:T(`${t}-radio__dot-wrapper`)},[r[0]||(r[0]=j(" ",-1)),k("div",{class:T([`${t}-radio__dot`,this.renderSafeChecked&&`${t}-radio__dot--checked`])},null,2),k("input",{ref:"inputRef",type:"radio",class:T(`${t}-radio-input`),value:this.value,name:this.mergedName,checked:this.renderSafeChecked,disabled:this.mergedDisabled,onChange:this.handleRadioInputChange,onFocus:this.handleRadioInputFocus,onBlur:this.handleRadioInputBlur},null,42,Ye)],2),j(()=>ge(e.default,u=>!u&&!l?null:(y(),R("div",{ref:"labelRef",class:T(`${t}-radio__label`)},[j(()=>u||l)],2))))],6)})()}});const qe=["value","name","checked","disabled","onChange","onFocus","onBlur"];var re=oe({name:"RadioButton",props:ye,setup:ke,render(){const{mergedClsPrefix:e}=this;return y(),R("label",{class:T([`${e}-radio-button`,this.mergedDisabled&&`${e}-radio-button--disabled`,this.renderSafeChecked&&`${e}-radio-button--checked`,this.focus&&[`${e}-radio-button--focus`]])},[k("input",{ref:"inputRef",type:"radio",class:T(`${e}-radio-input`),value:this.value,name:this.mergedName,checked:this.renderSafeChecked,disabled:this.mergedDisabled,onChange:this.handleRadioInputChange,onFocus:this.handleRadioInputFocus,onBlur:this.handleRadioInputBlur},null,42,qe),k("div",{class:T(`${e}-radio-button__state-border`)},null,2),j(()=>ge(this.$slots.default,t=>!t&&!this.label?null:(y(),R("div",{ref:"labelRef",class:T(`${e}-radio__label`)},[j(()=>t||this.label)],2))))],2)}});function Qe(e,t="default",o=[]){const l=e.$slots[t];return l===void 0?o:l()}var Ze=O("radio-group",`
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
 `,[P("checked",{backgroundColor:"var(--n-button-border-color-active)"}),P("disabled",{opacity:"var(--n-opacity-disabled)"})]),P("button-group",`
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
 `),ie("checked",{color:"var(--n-button-text-color-hover)"})]),P("focus",[E("&:not(:active)",[S("state-border",{boxShadow:"var(--n-button-box-shadow-focus)"})])])]),P("checked",`
 background: var(--n-button-color-active);
 color: var(--n-button-text-color-active);
 border-color: var(--n-button-border-color-active);
 `),P("disabled",`
 cursor: not-allowed;
 opacity: var(--n-opacity-disabled);
 `)])]);const Je=["onFocusin","onFocusout"];function et(e,t,o){var u;const l=[];let r=!1;for(let f=0;f<e.length;++f){const v=e[f],s=(u=v.type)==null?void 0:u.name;s==="RadioButton"&&(r=!0);const m=v.props;if(s!=="RadioButton"){l.push(v);continue}if(f===0)l.push(v);else{const x=l[l.length-1].props,p=t===x.value,h=x.disabled,z=t===m.value,w=m.disabled,n=(p?2:0)+(h?0:1),B=(z?2:0)+(w?0:1),$={[`${o}-radio-group__splitor--disabled`]:h,[`${o}-radio-group__splitor--checked`]:p},F={[`${o}-radio-group__splitor--disabled`]:w,[`${o}-radio-group__splitor--checked`]:z},A=n<B?F:$;l.push((y(),R("div",{key:1,class:T([`${o}-radio-group__splitor`,A])},null,2)),v)}}return{children:l,isButtonGroup:r}}const tt={...ae.props,name:String,options:Array,labelField:{type:String,default:"label"},valueField:{type:String,default:"value"},value:[String,Number,Boolean],defaultValue:{type:[String,Number,Boolean],default:null},size:String,disabled:{type:Boolean,default:void 0},"onUpdate:value":[Function,Array],onUpdateValue:[Function,Array]};var ot=oe({name:"RadioGroup",props:tt,setup(e){const t=I(null),{mergedSizeRef:o,mergedDisabledRef:l,nTriggerFormChange:r,nTriggerFormInput:u,nTriggerFormBlur:f,nTriggerFormFocus:v}=he(e),{mergedClsPrefixRef:s,inlineThemeDisabled:m,mergedRtlRef:x}=se(e),p=ae("Radio","-radio-group",Ze,xe,e,s),h=I(e.defaultValue),z=le(e,"value"),w=fe(z,h);function n(b){const{onUpdateValue:g,"onUpdate:value":C}=e;g&&Q(g,b),C&&Q(C,b),h.value=b,r(),u()}function B(b){const{value:g}=t;g&&(g.contains(b.relatedTarget)||v())}function $(b){const{value:g}=t;g&&(g.contains(b.relatedTarget)||f())}_e(Ce,{mergedClsPrefixRef:s,nameRef:le(e,"name"),valueRef:w,disabledRef:l,mergedSizeRef:o,doUpdateValue:n});const F=pe("Radio",x,s),A=L(()=>{const{value:b}=o,{common:{cubicBezierEaseInOut:g},self:{buttonBorderColor:C,buttonBorderColorActive:D,buttonBorderRadius:M,buttonBoxShadow:W,buttonBoxShadowFocus:H,buttonBoxShadowHover:G,buttonColor:Y,buttonColorActive:d,buttonTextColor:a,buttonTextColorActive:i,buttonTextColorHover:U,opacityDisabled:K,[te("buttonHeight",b)]:we,[te("fontSize",b)]:Re}}=p.value;return{"--n-font-size":Re,"--n-bezier":g,"--n-button-border-color":C,"--n-button-border-color-active":D,"--n-button-border-radius":M,"--n-button-box-shadow":W,"--n-button-box-shadow-focus":H,"--n-button-box-shadow-hover":G,"--n-button-color":Y,"--n-button-color-active":d,"--n-button-text-color":a,"--n-button-text-color-hover":U,"--n-button-text-color-active":i,"--n-height":we,"--n-opacity-disabled":K}}),c=m?me("radio-group",L(()=>o.value[0]),A,e):void 0;return{selfElRef:t,rtlEnabled:F,mergedClsPrefix:s,mergedValue:w,handleFocusout:$,handleFocusin:B,cssVars:m?void 0:A,themeClass:c==null?void 0:c.themeClass,onRender:c==null?void 0:c.onRender}},render(){var m;const{mergedValue:e,mergedClsPrefix:t,handleFocusin:o,handleFocusout:l}=this,{options:r,labelField:u,valueField:f}=this.$props,{children:v,isButtonGroup:s}=et(r?r.map(x=>{const p=x[f];return y(),J(Xe,{key:typeof p=="boolean"?`__n_${p}`:p,value:p,disabled:x.disabled,label:x[u]},null,8,["value","disabled","label"])}):Fe(Qe(this)),e,t);return(m=this.onRender)==null||m.call(this),y(),R("div",{onFocusin:o,onFocusout:l,ref:"selfElRef",class:T([`${t}-radio-group`,this.rtlEnabled&&`${t}-radio-group--rtl`,this.themeClass,s&&`${t}-radio-group--button-group`]),style:ee(this.cssVars)},[j(()=>v)],46,Je)}});const at=["aria-label","aria-busy"],nt={key:0},rt={key:0},it={class:"v2-chart-surface"},lt=["aria-hidden","aria-label"],st={key:1,class:"v2-chart-total","aria-hidden":"true"},dt={key:2,class:"v2-chart-overlay"},ut={key:4,class:"v2-chart-overlay v2-chart-zero"},ct=["open"],bt={class:"v2-chart-details","aria-label":"图表明细"},vt=oe({__name:"MotionChart",props:{title:{},caption:{},items:{},loading:{type:Boolean},failed:{type:Boolean},trend:{type:Boolean},view:{},unit:{default:"条"}},emits:["select"],setup(e,{emit:t}){const o=e,l=t,r=I(),u=I(o.view||"bar"),f=I(!1),v=I(null),s=I(!1),m=I(!1),x=I(!1),p=L(()=>o.items.some(d=>!Number.isFinite(d.value)||d.value<0)),h=L(()=>!!(o.loading||o.failed||p.value||!o.items.length)),z=L(()=>p.value?0:o.items.reduce((d,a)=>d+a.value,0)),w=L(()=>o.items.map(d=>`${d.label} ${d.value} ${o.unit}`).join("，"));let n,B,$,F=!1,A=!1,c=0,b=!1;const g=window.matchMedia("(prefers-reduced-motion: reduce)");function C(){if(v.value=null,!(!n||h.value))try{n.setOption(Ne(o.items,u.value,o.unit,x.value,!g.matches&&m.value),{replaceMerge:["series","xAxis","yAxis","dataZoom"],lazyUpdate:!0}),s.value=!1}catch{s.value=!0}}function D(){if(!n||!o.items.length||!ne(u.value)||h.value)return;const d=o.items.length-1,a=n.convertToPixel({seriesIndex:0},[d,o.items[d].value]);v.value=Array.isArray(a)&&a.every(Number.isFinite)?a:null}async function M(){if(!(A||n||F)){A=!0;try{const{init:d}=await Le(async()=>{const{init:a}=await import("./part-v2-DXdJ9xix.js");return{init:a}},[]);if(F||!r.value)return;x.value=r.value.clientWidth<440,n=d(r.value,void 0,{renderer:"svg"}),n.on("finished",D),n.on("mousedown",a=>{var K;const i=(K=a.event)==null?void 0:K.target;if(g.matches||!["bar","horizontal"].includes(u.value)||(i==null?void 0:i.type)!=="rect")return;const U=i.getBoundingRect();i.originY=U.y+U.height,i.animateTo({scaleY:.96},{duration:90,done:()=>{F||i.animateTo({scaleY:1},{duration:220,easing:"cubicOut"})}})}),n.on("click",a=>{if(h.value)return;const i=o.items[a.dataIndex];i&&l("select",i,a.dataIndex)}),B=new ResizeObserver(()=>{cancelAnimationFrame(c),c=requestAnimationFrame(()=>{var i;if(F)return;n==null||n.resize();const a=(((i=r.value)==null?void 0:i.clientWidth)||0)<440;x.value!==a&&(x.value=a,C()),D()})}),B.observe(r.value),C()}catch{s.value=!0}finally{A=!1}}}function W(d){var a,i;!ne(u.value)||!n||h.value||(f.value=!0,(i=(a=r.value)==null?void 0:a.setPointerCapture)==null||i.call(a,d.pointerId),H(d))}function H(d){if(!f.value||!n||!r.value||h.value)return;const a=r.value.getBoundingClientRect(),i=n.convertFromPixel({seriesIndex:0},[d.clientX-a.left,d.clientY-a.top]);!Array.isArray(i)||!Number.isFinite(i[0])||n.dispatchAction({type:"showTip",seriesIndex:0,dataIndex:Math.max(0,Math.min(o.items.length-1,Math.round(i[0])))})}function G(){f.value&&(f.value=!1,n==null||n.dispatchAction({type:"hideTip"}))}function Y(){m.value=b&&!document.hidden,C()}return ce(()=>[o.items,o.loading,o.failed,o.unit,u.value],C,{deep:!0}),ce(()=>o.view,d=>{d&&(u.value=d)}),Ie(()=>{$=new IntersectionObserver(d=>{b=d.some(a=>a.isIntersecting),Y(),b&&M()},{rootMargin:"80px"}),r.value&&$.observe(r.value),g.addEventListener("change",C),document.addEventListener("visibilitychange",Y)}),Ae(()=>{F=!0,$==null||$.disconnect(),B==null||B.disconnect(),cancelAnimationFrame(c),g.removeEventListener("change",C),document.removeEventListener("visibilitychange",Y),n==null||n.dispose()}),(d,a)=>(y(),R("section",{class:"v2-motion-chart","aria-label":e.title||"业务图表","aria-busy":e.loading},[e.title||e.caption||e.trend||d.$slots.actions?(y(),R("header",nt,[e.title?(y(),R("h3",rt,V(e.title),1)):N("",!0),k("span",null,V(e.caption),1),Pe(d.$slots,"actions",{},void 0,!0),e.trend?(y(),J(_(ot),{key:1,value:u.value,"onUpdate:value":a[0]||(a[0]=i=>u.value=i),size:"small","aria-label":"图表类型"},{default:X(()=>[q(_(re),{value:"bar"},{default:X(()=>[...a[1]||(a[1]=[Z("柱状图",-1)])]),_:1}),q(_(re),{value:"line"},{default:X(()=>[...a[2]||(a[2]=[Z("折线图",-1)])]),_:1}),q(_(re),{value:"area"},{default:X(()=>[...a[3]||(a[3]=[Z("面积图",-1)])]),_:1})]),_:1},8,["value"])):N("",!0)])):N("",!0),k("div",it,[k("div",{ref_key:"host",ref:r,class:Te(["v2-chart-canvas",{muted:h.value}]),role:"img","aria-hidden":h.value,"aria-label":w.value,onPointerdown:W,onPointermove:H,onPointerup:G,onPointercancel:G,onPointerleave:G},null,42,lt),m.value&&_(ne)(u.value)&&v.value&&!f.value&&!h.value&&!s.value?(y(),R("span",{key:0,class:"v2-chart-end",style:ee({left:v.value[0]+"px",top:v.value[1]+"px"}),"aria-hidden":"true"},null,4)):N("",!0),!h.value&&!s.value&&u.value==="donut"?(y(),R("div",st,[k("strong",null,V(z.value.toLocaleString("zh-CN")),1),k("span",null,"合计 · "+V(e.unit),1)])):N("",!0),e.loading?(y(),R("div",dt,[q(_(He),{height:"100%",sharp:!1})])):e.failed||p.value||s.value||!e.items.length?(y(),J(_(De),{key:3,class:"v2-chart-overlay",description:e.failed||p.value?"数据暂不可用":s.value?"图表暂不可用，请查看数据明细":"暂无数据",size:"small"},null,8,["description"])):_(be)(u.value)&&z.value===0?(y(),R("p",ut,"当前合计 0 "+V(e.unit),1)):N("",!0)]),!e.loading&&!e.failed&&!p.value&&e.items.length?(y(),R("details",{key:1,class:"v2-chart-data",open:s.value||x.value&&_(be)(u.value)},[k("summary",null,[a[4]||(a[4]=Z("数据明细 ",-1)),k("span",null,V(e.items.length)+" 项",1)]),k("div",bt,[(y(!0),R(Ve,null,Ee(e.items,(i,U)=>(y(),J(_(Me),{key:String(i.key??i.label)+U,"data-v2-navigation":"",text:"",onClick:K=>d.$emit("select",i,U)},{default:X(()=>[k("i",{style:ee({background:_(ve)[U%_(ve).length]}),"aria-hidden":"true"},null,4),k("span",null,V(i.label),1),k("strong",null,V(i.value.toLocaleString("zh-CN"))+" "+V(e.unit),1),q(_(Ue),{size:14})]),_:2},1032,["onClick"]))),128))])],8,ct)):N("",!0)],8,at))}}),ht=Oe(vt,[["__scopeId","data-v-05763c9a"]]),pt=Object.freeze(Object.defineProperty({__proto__:null,default:ht},Symbol.toStringTag,{value:"Module"}));export{ht as D,pt as M,re as R,ot as a,Xe as b,xe as r};
