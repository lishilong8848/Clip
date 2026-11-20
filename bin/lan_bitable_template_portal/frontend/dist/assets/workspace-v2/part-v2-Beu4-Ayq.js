import{bv as he,bl as De,b5 as C,bi as Je,cm as Qe,aF as Ze,bx as Oe,aG as ce,aI as fe,f8 as be,dB as u,b8 as $,b6 as P,ed as z,da as b,dh as k,eR as Ae,eX as Ue,dU as U,f2 as Ee,f0 as Y,fi as et,f4 as tt,b0 as se,ef as nt,eb as Ce,z as Ie,ak as _e,ez as Le,aN as D,d7 as rt,dr as Fe,aq as it,aU as at,aJ as o,ck as Me,aK as T,aL as Te,cD as Be,P as ot,cQ as lt,I as st,dg as Ne,f9 as ut,bc as q,dO as Ve,bw as N}from"./app-v2-CHOg2lmz.js";import{u as dt}from"./part-v2-tGfUUa5p.js";import{I as ct}from"./part-v2-BtQ8p3Yt.js";var ft=he({name:"Add",render(){return(()=>{const e=De("b30130fbba5c5b23");return e[0]||(e[0]=C("svg",{width:"512",height:"512",viewBox:"0 0 512 512",fill:"none",xmlns:"http://www.w3.org/2000/svg"},[C("path",{d:"M256 112V400M400 256H112",stroke:"currentColor","stroke-width":"32","stroke-linecap":"round","stroke-linejoin":"round"})],-1))})()}}),ht=he({name:"Remove",render(){return(()=>{const e=De("a77472467b8adb0a");return e[0]||(e[0]=C("svg",{xmlns:"http://www.w3.org/2000/svg",viewBox:"0 0 512 512"},[C("line",{x1:"400",y1:"256",x2:"112",y2:"256",style:`
        fill: none;
        stroke: currentColor;
        stroke-linecap: round;
        stroke-linejoin: round;
        stroke-width: 32px;
      `})],-1))})()}});function bt(e){const{textColorDisabled:a}=e;return{iconColorDisabled:a}}const mt=Je({name:"InputNumber",common:Oe,peers:{Button:Ze,Input:Qe},self:bt});var vt=ce([fe("input-number-suffix",`
 display: inline-block;
 margin-right: 10px;
 `),fe("input-number-prefix",`
 display: inline-block;
 margin-left: 10px;
 `)]);function gt(e){return e==null||typeof e=="string"&&e.trim()===""?null:Number(e)}function pt(e){return e.includes(".")&&(/^(-)?\d+.*(\.|0)$/.test(e)||/^-?\d*$/.test(e))||e==="-"||e==="-0"}function Se(e){return e==null?!0:!Number.isNaN(e)}function Pe(e,a){return typeof e!="number"?"":a===void 0?String(e):e.toFixed(a)}function Re(e){if(e===null)return null;if(typeof e=="number")return e;{const a=Number(e);return Number.isNaN(a)?null:a}}const ze=800,$e=100,wt={...be.props,autofocus:Boolean,loading:{type:Boolean,default:void 0},placeholder:String,defaultValue:{type:Number,default:null},value:Number,step:{type:[Number,String],default:1},min:[Number,String],max:[Number,String],size:String,disabled:{type:Boolean,default:void 0},validator:Function,bordered:{type:Boolean,default:void 0},showButton:{type:Boolean,default:!0},buttonPlacement:{type:String,default:"right"},inputProps:Object,readonly:Boolean,clearable:Boolean,keyboard:{type:Object,default:{}},updateValueOnInput:{type:Boolean,default:!0},round:{type:Boolean,default:void 0},parse:Function,format:Function,precision:Number,status:String,"onUpdate:value":[Function,Array],onUpdateValue:[Function,Array],onFocus:[Function,Array],onBlur:[Function,Array],onClear:[Function,Array],onChange:[Function,Array]};var It=he({name:"InputNumber",props:wt,slots:Object,setup(e){const{mergedBorderedRef:a,mergedClsPrefixRef:m,mergedRtlRef:c,mergedComponentPropsRef:d}=Ae(e),f=be("InputNumber","-input-number",vt,mt,e,m),{localeRef:E}=dt("InputNumber"),y=Ue(e,{mergedSize:t=>{var w,x;const{size:n}=e;if(n)return n;const{mergedSize:i}=t||{};if(i!=null&&i.value)return i.value;const h=(x=(w=d==null?void 0:d.value)==null?void 0:w.InputNumber)==null?void 0:x.size;return h||"medium"}}),{mergedSizeRef:L,mergedDisabledRef:J,mergedStatusRef:I}=y,v=U(null),l=U(null),g=U(null),_=U(e.defaultValue),Q=Le(e,"value"),B=Ee(Q,_),F=U(""),ne=t=>{const n=String(t).split(".")[1];return n?n.length:0},me=t=>{const n=[e.min,e.max,e.step,t].map(i=>i===void 0?0:ne(i));return Math.max(...n)},ve=Y(()=>{const{placeholder:t}=e;return t!==void 0?t:E.value.placeholder}),Z=Y(()=>{const t=Re(e.step);return t!==null?t===0?1:Math.abs(t):1}),ue=Y(()=>{const t=Re(e.min);return t!==null?t:null}),re=Y(()=>{const t=Re(e.max);return t!==null?t:null}),p=()=>{const{value:t}=B;if(Se(t)){const{format:n,precision:i}=e;n?F.value=n(t):t===null||i===void 0||ne(t)>i?F.value=Pe(t,void 0):F.value=Pe(t,i)}else F.value=String(t)};p();const r=t=>{const{value:n}=B;if(t===n){p();return}const{"onUpdate:value":i,onUpdateValue:h,onChange:w}=e,{nTriggerFormInput:x,nTriggerFormChange:te}=y;w&&D(w,t),h&&D(h,t),i&&D(i,t),_.value=t,x(),te()},s=({offset:t,doUpdateIfValid:n,fixPrecision:i,isInputing:h})=>{const{value:w}=F;if(h&&pt(w))return!1;const x=(e.parse||gt)(w);if(x===null)return n&&r(null),null;if(Se(x)){const te=ne(x),{precision:xe}=e;if(xe!==void 0&&xe<te&&!i)return!1;let A=Number.parseFloat((x+t).toFixed(xe??me(x)));if(Se(A)){const{value:ye}=re,{value:ke}=ue;if(ye!==null&&A>ye){if(!n||h)return!1;A=ye}if(ke!==null&&A<ke){if(!n||h)return!1;A=ke}return e.validator&&!e.validator(A)?!1:(n&&r(A),A)}}return!1},M=Y(()=>s({offset:0,doUpdateIfValid:!1,isInputing:!1,fixPrecision:!1})===!1),V=Y(()=>{const{value:t}=B;if(e.validator&&t===null)return!1;const{value:n}=Z;return s({offset:-n,doUpdateIfValid:!1,isInputing:!1,fixPrecision:!1})!==!1}),K=Y(()=>{const{value:t}=B;if(e.validator&&t===null)return!1;const{value:n}=Z;return s({offset:+n,doUpdateIfValid:!1,isInputing:!1,fixPrecision:!1})!==!1});function ie(t){const{onFocus:n}=e,{nTriggerFormFocus:i}=y;n&&D(n,t),i()}function ge(t){var w,x;if(t.target===((w=v.value)==null?void 0:w.wrapperElRef))return;const n=s({offset:0,doUpdateIfValid:!0,isInputing:!1,fixPrecision:!0});if(n!==!1){const te=(x=v.value)==null?void 0:x.inputElRef;te&&(te.value=String(n||"")),B.value===n&&p()}else p();const{onBlur:i}=e,{nTriggerFormBlur:h}=y;i&&D(i,t),h(),rt(()=>{p()})}function pe(t){const{onClear:n}=e;n&&D(n,t)}function ae(){const{value:t}=K;if(!t){X();return}const{value:n}=B;if(n===null)e.validator||r(de());else{const{value:i}=Z;s({offset:i,doUpdateIfValid:!0,isInputing:!1,fixPrecision:!0})}}function oe(){const{value:t}=V;if(!t){G();return}const{value:n}=B;if(n===null)e.validator||r(de());else{const{value:i}=Z;s({offset:-i,doUpdateIfValid:!0,isInputing:!1,fixPrecision:!0})}}const S=ie,we=ge;function de(){if(e.validator)return null;const{value:t}=ue,{value:n}=re;return t!==null?Math.max(0,t):n!==null?Math.min(0,n):0}function R(t){pe(t),r(null)}function H(t){var n,i,h;(n=g.value)!=null&&n.$el.contains(t.target)&&t.preventDefault(),(i=l.value)!=null&&i.$el.contains(t.target)&&t.preventDefault(),(h=v.value)==null||h.activate()}let j=null,W=null,ee=null;function G(){ee&&(window.clearTimeout(ee),ee=null),j&&(window.clearInterval(j),j=null)}let O=null;function X(){O&&(window.clearTimeout(O),O=null),W&&(window.clearInterval(W),W=null)}function Ke(){G(),ee=window.setTimeout(()=>{j=window.setInterval(()=>{oe()},$e)},ze),Fe("mouseup",document,G,{once:!0})}function He(){X(),O=window.setTimeout(()=>{W=window.setInterval(()=>{ae()},$e)},ze),Fe("mouseup",document,X,{once:!0})}const je=()=>{W||ae()},We=()=>{j||oe()};function Ge(t){var n,i;if(t.key==="Enter"){if(t.target===((n=v.value)==null?void 0:n.wrapperElRef))return;s({offset:0,doUpdateIfValid:!0,isInputing:!1,fixPrecision:!0})!==!1&&((i=v.value)==null||i.deactivate())}else if(t.key==="ArrowUp"){if(!K.value||e.keyboard.ArrowUp===!1)return;t.preventDefault(),s({offset:0,doUpdateIfValid:!0,isInputing:!1,fixPrecision:!0})!==!1&&ae()}else if(t.key==="ArrowDown"){if(!V.value||e.keyboard.ArrowDown===!1)return;t.preventDefault(),s({offset:0,doUpdateIfValid:!0,isInputing:!1,fixPrecision:!0})!==!1&&oe()}}function Xe(t){F.value=t,e.updateValueOnInput&&!e.format&&!e.parse&&e.precision===void 0&&s({offset:0,doUpdateIfValid:!0,isInputing:!0,fixPrecision:!1})}et(B,()=>{p()});const Ye={focus:()=>{var t;return(t=v.value)==null?void 0:t.focus()},blur:()=>{var t;return(t=v.value)==null?void 0:t.blur()},select:()=>{var t;return(t=v.value)==null?void 0:t.select()}},qe=tt("InputNumber",c,m);return{...Ye,rtlEnabled:qe,inputInstRef:v,minusButtonInstRef:l,addButtonInstRef:g,mergedClsPrefix:m,mergedBordered:a,uncontrolledValue:_,mergedValue:B,mergedPlaceholder:ve,displayedValueInvalid:M,mergedSize:L,mergedDisabled:J,displayedValue:F,addable:K,minusable:V,mergedStatus:I,handleFocus:S,handleBlur:we,handleClear:R,handleMouseDown:H,handleAddClick:je,handleMinusClick:We,handleAddMousedown:He,handleMinusMousedown:Ke,handleKeyDown:Ge,handleUpdateDisplayedValue:Xe,mergedTheme:f,inputThemeOverrides:{paddingSmall:"0 8px 0 10px",paddingMedium:"0 8px 0 12px",paddingLarge:"0 8px 0 14px"},buttonThemeOverrides:se(()=>{const{self:{iconColorDisabled:t}}=f.value,[n,i,h,w]=nt(t);return{textColorTextDisabled:`rgb(${n}, ${i}, ${h})`,opacityDisabled:`${w}`}})}},render(){const{mergedClsPrefix:e,$slots:a}=this,m=()=>(u(),P(_e,{text:!0,disabled:!this.minusable||this.mergedDisabled||this.readonly,focusable:!1,theme:this.mergedTheme.peers.Button,themeOverrides:this.mergedTheme.peerOverrides.Button,builtinThemeOverrides:this.buttonThemeOverrides,onClick:this.handleMinusClick,onMousedown:this.handleMinusMousedown,ref:"minusButtonInstRef"},{icon:()=>Ce(a["minus-icon"],()=>[(u(),P(Ie,{clsPrefix:e},{default:()=>(u(),P(ht))},1032,["clsPrefix"]))])},1032,["disabled","theme","themeOverrides","builtinThemeOverrides","onClick","onMousedown"])),c=()=>(u(),P(_e,{text:!0,disabled:!this.addable||this.mergedDisabled||this.readonly,focusable:!1,theme:this.mergedTheme.peers.Button,themeOverrides:this.mergedTheme.peerOverrides.Button,builtinThemeOverrides:this.buttonThemeOverrides,onClick:this.handleAddClick,onMousedown:this.handleAddMousedown,ref:"addButtonInstRef"},{icon:()=>Ce(a["add-icon"],()=>[(u(),P(Ie,{clsPrefix:e},{default:()=>(u(),P(ft))},1032,["clsPrefix"]))])},1032,["disabled","theme","themeOverrides","builtinThemeOverrides","onClick","onMousedown"]));return u(),$("div",{class:b([`${e}-input-number`,this.rtlEnabled&&`${e}-input-number--rtl`])},[(u(),P(ct,{ref:"inputInstRef",autofocus:this.autofocus,status:this.mergedStatus,bordered:this.mergedBordered,loading:this.loading,value:this.displayedValue,onUpdateValue:this.handleUpdateDisplayedValue,theme:this.mergedTheme.peers.Input,themeOverrides:this.mergedTheme.peerOverrides.Input,builtinThemeOverrides:this.inputThemeOverrides,size:this.mergedSize,placeholder:this.mergedPlaceholder,disabled:this.mergedDisabled,readonly:this.readonly,round:this.round,textDecoration:this.displayedValueInvalid?"line-through":void 0,onFocus:this.handleFocus,onBlur:this.handleBlur,onKeydown:this.handleKeyDown,onMousedown:this.handleMouseDown,onClear:this.handleClear,clearable:this.clearable,inputProps:this.inputProps,internalLoadingBeforeSuffix:!0},{prefix:()=>{var d;return this.showButton&&this.buttonPlacement==="both"?[m(),z(a.prefix,f=>f?(u(),$("span",{key:1,class:b(`${e}-input-number-prefix`)},[k(()=>f)],2)):null)]:(d=a.prefix)==null?void 0:d.call(a)},suffix:()=>{var d;return this.showButton?[z(a.suffix,f=>f?(u(),$("span",{key:2,class:b(`${e}-input-number-suffix`)},[k(()=>f)],2)):null),this.buttonPlacement==="right"?m():null,c()]:(d=a.suffix)==null?void 0:d.call(a)}},1032,["autofocus","status","bordered","loading","value","onUpdateValue","theme","themeOverrides","builtinThemeOverrides","size","placeholder","disabled","readonly","round","textDecoration","onFocus","onBlur","onKeydown","onMousedown","onClear","clearable","inputProps"]))],2)}});function xt(e){const{primaryColor:a,opacityDisabled:m,borderRadius:c,textColor3:d}=e;return{...it,iconColor:d,textColor:"white",loadingColor:a,opacityDisabled:m,railColor:"rgba(0, 0, 0, .14)",railColorActive:a,buttonBoxShadow:"0 1px 4px 0 rgba(0, 0, 0, 0.3), inset 0 0 1px 0 rgba(0, 0, 0, 0.05)",buttonColor:"#FFF",railBorderRadiusSmall:c,railBorderRadiusMedium:c,railBorderRadiusLarge:c,buttonBorderRadiusSmall:c,buttonBorderRadiusMedium:c,buttonBorderRadiusLarge:c,boxShadowFocus:`0 0 0 2px ${at(a,{alpha:.2})}`}}const yt={common:Oe,self:xt};var kt=fe("switch",`
 height: var(--n-height);
 min-width: var(--n-width);
 vertical-align: middle;
 user-select: none;
 -webkit-user-select: none;
 display: inline-flex;
 outline: none;
 justify-content: center;
 align-items: center;
`,[o("children-placeholder",`
 height: var(--n-rail-height);
 display: flex;
 flex-direction: column;
 overflow: hidden;
 pointer-events: none;
 visibility: hidden;
 `),o("rail-placeholder",`
 display: flex;
 flex-wrap: none;
 `),o("button-placeholder",`
 width: calc(1.75 * var(--n-rail-height));
 height: var(--n-rail-height);
 `),fe("base-loading",`
 position: absolute;
 top: 50%;
 left: 50%;
 transform: translateX(-50%) translateY(-50%);
 font-size: calc(var(--n-button-width) - 4px);
 color: var(--n-loading-color);
 transition: color .3s var(--n-bezier);
 `,[Me({left:"50%",top:"50%",originalTransform:"translateX(-50%) translateY(-50%)"})]),o("checked, unchecked",`
 transition: color .3s var(--n-bezier);
 color: var(--n-text-color);
 box-sizing: border-box;
 position: absolute;
 white-space: nowrap;
 top: 0;
 bottom: 0;
 display: flex;
 align-items: center;
 line-height: 1;
 `),o("checked",`
 right: 0;
 padding-right: calc(1.25 * var(--n-rail-height) - var(--n-offset));
 `),o("unchecked",`
 left: 0;
 justify-content: flex-end;
 padding-left: calc(1.25 * var(--n-rail-height) - var(--n-offset));
 `),ce("&:focus",[o("rail",`
 box-shadow: var(--n-box-shadow-focus);
 `)]),T("round",[o("rail","border-radius: calc(var(--n-rail-height) / 2);",[o("button","border-radius: calc(var(--n-button-height) / 2);")])]),Te("disabled",[Te("icon",[T("rubber-band",[T("pressed",[o("rail",[o("button","max-width: var(--n-button-width-pressed);")])]),o("rail",[ce("&:active",[o("button","max-width: var(--n-button-width-pressed);")])]),T("active",[T("pressed",[o("rail",[o("button","left: calc(100% - var(--n-offset) - var(--n-button-width-pressed));")])]),o("rail",[ce("&:active",[o("button","left: calc(100% - var(--n-offset) - var(--n-button-width-pressed));")])])])])])]),T("active",[o("rail",[o("button","left: calc(100% - var(--n-button-width) - var(--n-offset))")])]),o("rail",`
 overflow: hidden;
 height: var(--n-rail-height);
 min-width: var(--n-rail-width);
 border-radius: var(--n-rail-border-radius);
 cursor: pointer;
 position: relative;
 transition:
 opacity .3s var(--n-bezier),
 background .3s var(--n-bezier),
 box-shadow .3s var(--n-bezier);
 background-color: var(--n-rail-color);
 `,[o("button-icon",`
 color: var(--n-icon-color);
 transition: color .3s var(--n-bezier);
 font-size: calc(var(--n-button-height) - 4px);
 position: absolute;
 left: 0;
 right: 0;
 top: 0;
 bottom: 0;
 display: flex;
 justify-content: center;
 align-items: center;
 line-height: 1;
 `,[Me()]),o("button",`
 align-items: center; 
 top: var(--n-offset);
 left: var(--n-offset);
 height: var(--n-button-height);
 width: var(--n-button-width-pressed);
 max-width: var(--n-button-width);
 border-radius: var(--n-button-border-radius);
 background-color: var(--n-button-color);
 box-shadow: var(--n-button-box-shadow);
 box-sizing: border-box;
 cursor: inherit;
 content: "";
 position: absolute;
 transition:
 background-color .3s var(--n-bezier),
 left .3s var(--n-bezier),
 opacity .3s var(--n-bezier),
 max-width .3s var(--n-bezier),
 box-shadow .3s var(--n-bezier);
 `)]),T("active",[o("rail","background-color: var(--n-rail-color-active);")]),T("loading",[o("rail",`
 cursor: wait;
 `)]),T("disabled",[o("rail",`
 cursor: not-allowed;
 opacity: .5;
 `)])]);const Bt=["aria-checked","tabindex","onClick","onFocus","onBlur","onKeyup","onKeydown"],Vt={...be.props,size:String,value:{type:[String,Number,Boolean],default:void 0},loading:Boolean,defaultValue:{type:[String,Number,Boolean],default:!1},disabled:{type:Boolean,default:void 0},round:{type:Boolean,default:!0},"onUpdate:value":[Function,Array],onUpdateValue:[Function,Array],checkedValue:{type:[String,Number,Boolean],default:!0},uncheckedValue:{type:[String,Number,Boolean],default:!1},railStyle:Function,rubberBand:{type:Boolean,default:!0},spinProps:Object,onChange:[Function,Array]};let le;var _t=he({name:"Switch",props:Vt,slots:Object,setup(e){le===void 0&&(typeof CSS<"u"?typeof CSS.supports<"u"?le=CSS.supports("width","max(1px)"):le=!1:le=!0);const{mergedClsPrefixRef:a,inlineThemeDisabled:m,mergedComponentPropsRef:c}=Ae(e),d=be("Switch","-switch",kt,yt,e,a),f=Ue(e,{mergedSize(r){var M,V;if(e.size!==void 0)return e.size;if(r)return r.mergedSize.value;const s=(V=(M=c==null?void 0:c.value)==null?void 0:M.Switch)==null?void 0:V.size;return s||"medium"}}),{mergedSizeRef:E,mergedDisabledRef:y}=f,L=U(e.defaultValue),J=Le(e,"value"),I=Ee(J,L),v=se(()=>I.value===e.checkedValue),l=U(!1),g=U(!1),_=se(()=>{const{railStyle:r}=e;if(r)return r({focused:g.value,checked:v.value})});function Q(r){const{"onUpdate:value":s,onChange:M,onUpdateValue:V}=e,{nTriggerFormInput:K,nTriggerFormChange:ie}=f;s&&D(s,r),V&&D(V,r),M&&D(M,r),L.value=r,K(),ie()}function B(){const{nTriggerFormFocus:r}=f;r()}function F(){const{nTriggerFormBlur:r}=f;r()}function ne(){e.loading||y.value||(I.value!==e.checkedValue?Q(e.checkedValue):Q(e.uncheckedValue))}function me(){g.value=!0,B()}function ve(){g.value=!1,F(),l.value=!1}function Z(r){e.loading||y.value||r.key===" "&&(I.value!==e.checkedValue?Q(e.checkedValue):Q(e.uncheckedValue),l.value=!1)}function ue(r){e.loading||y.value||r.key===" "&&(r.preventDefault(),l.value=!0)}const re=se(()=>{const{value:r}=E,{self:{opacityDisabled:s,railColor:M,railColorActive:V,buttonBoxShadow:K,buttonColor:ie,boxShadowFocus:ge,loadingColor:pe,textColor:ae,iconColor:oe,[q("buttonHeight",r)]:S,[q("buttonWidth",r)]:we,[q("buttonWidthPressed",r)]:de,[q("railHeight",r)]:R,[q("railWidth",r)]:H,[q("railBorderRadius",r)]:j,[q("buttonBorderRadius",r)]:W},common:{cubicBezierEaseInOut:ee}}=d.value;let G,O,X;return le?(G=`calc((${R} - ${S}) / 2)`,O=`max(${R}, ${S})`,X=`max(${H}, calc(${H} + ${S} - ${R}))`):(G=Ve((N(R)-N(S))/2),O=Ve(Math.max(N(R),N(S))),X=N(R)>N(S)?H:Ve(N(H)+N(S)-N(R))),{"--n-bezier":ee,"--n-button-border-radius":W,"--n-button-box-shadow":K,"--n-button-color":ie,"--n-button-width":we,"--n-button-width-pressed":de,"--n-button-height":S,"--n-height":O,"--n-offset":G,"--n-opacity-disabled":s,"--n-rail-border-radius":j,"--n-rail-color":M,"--n-rail-color-active":V,"--n-rail-height":R,"--n-rail-width":H,"--n-width":X,"--n-box-shadow-focus":ge,"--n-loading-color":pe,"--n-text-color":ae,"--n-icon-color":oe}}),p=m?ut("switch",se(()=>E.value[0]),re,e):void 0;return{handleClick:ne,handleBlur:ve,handleFocus:me,handleKeyup:Z,handleKeydown:ue,mergedRailStyle:_,pressed:l,mergedClsPrefix:a,mergedValue:I,checked:v,mergedDisabled:y,cssVars:m?void 0:re,themeClass:p==null?void 0:p.themeClass,onRender:p==null?void 0:p.onRender}},render(){const{mergedClsPrefix:e,mergedDisabled:a,checked:m,mergedRailStyle:c,onRender:d,$slots:f}=this;d==null||d();const{checked:E,unchecked:y,icon:L,"checked-icon":J,"unchecked-icon":I}=f,v=!(Be(L)&&Be(J)&&Be(I));return u(),$("div",{role:"switch","aria-checked":m,class:b([`${e}-switch`,this.themeClass,v&&`${e}-switch--icon`,m&&`${e}-switch--active`,a&&`${e}-switch--disabled`,this.round&&`${e}-switch--round`,this.loading&&`${e}-switch--loading`,this.pressed&&`${e}-switch--pressed`,this.rubberBand&&`${e}-switch--rubber-band`]),tabindex:this.mergedDisabled?void 0:0,style:Ne(this.cssVars),onClick:this.handleClick,onFocus:this.handleFocus,onBlur:this.handleBlur,onKeyup:this.handleKeyup,onKeydown:this.handleKeydown},[C("div",{class:b(`${e}-switch__rail`),"aria-hidden":"true",style:Ne(c)},[k(()=>z(E,l=>z(y,g=>l||g?(u(),$("div",{key:4,"aria-hidden":!0,class:b(`${e}-switch__children-placeholder`)},[C("div",{class:b(`${e}-switch__rail-placeholder`)},[C("div",{class:b(`${e}-switch__button-placeholder`)},null,2),k(()=>l)],2),C("div",{class:b(`${e}-switch__rail-placeholder`)},[C("div",{class:b(`${e}-switch__button-placeholder`)},null,2),k(()=>g)],2)],2)):null))),C("div",{class:b(`${e}-switch__button`)},[k(()=>z(L,l=>z(J,g=>z(I,_=>(u(),P(st,null,{default:()=>this.loading?(u(),P(ot,lt({key:"loading",clsPrefix:e,strokeWidth:20},this.spinProps),null,16,["clsPrefix"])):this.checked&&(g||l)?(u(),$("div",{class:b(`${e}-switch__button-icon`),key:g?"checked-icon":"icon"},[k(()=>g||l)],2)):!this.checked&&(_||l)?(u(),$("div",{class:b(`${e}-switch__button-icon`),key:_?"unchecked-icon":"icon"},[k(()=>_||l)],2)):null},1024)))))),k(()=>z(E,l=>l&&(u(),$("div",{key:"checked",class:b(`${e}-switch__checked`)},[k(()=>l)],2)))),k(()=>z(y,l=>l&&(u(),$("div",{key:"unchecked",class:b(`${e}-switch__unchecked`)},[k(()=>l)],2))))],2)],6)],46,Bt)}});export{ft as A,It as I,_t as S};
