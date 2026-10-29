import{bj as E,b4 as M,aJ as k,aL as c,aN as F,aM as S,cf as xe,ci as ge,cj as me,bt as H,eY as J,e3 as pe,ds as D,b7 as B,dc as $,d5 as _,bi as ye,K as Ce,dh as we,db as ze,cg as Re,eF as O,dK as V,eT as W,eR as Se,eL as Q,aY as Te,eU as _e,eZ as De,b0 as N,b8 as Fe,eo as j,bb as G,aQ as i,ba as $e,J as Y,b5 as Be,dD as Me}from"./app-v2-BB1giTOZ.js";var Ae=()=>(()=>{const e=E("75be776d8875fa17");return e[0]||(e[0]=M("svg",{viewBox:"0 0 64 64",class:"check-icon"},[M("path",{d:"M50.42,16.76L22.34,39.45l-8.1-11.46c-1.12-1.58-3.3-1.96-4.88-0.84c-1.58,1.12-1.95,3.3-0.84,4.88l10.26,14.51  c0.56,0.79,1.42,1.31,2.38,1.45c0.16,0.02,0.32,0.03,0.48,0.03c0.8,0,1.57-0.27,2.2-0.78l30.99-25.03c1.5-1.21,1.74-3.42,0.52-4.92  C54.13,15.78,51.93,15.55,50.42,16.76z"})],-1))})(),Ie=()=>(()=>{const e=E("c6eed899356c8404");return e[0]||(e[0]=M("svg",{viewBox:"0 0 100 100",class:"line-icon"},[M("path",{d:"M80.2,55.5H21.4c-2.8,0-5.1-2.5-5.1-5.5l0,0c0-3,2.3-5.5,5.1-5.5h58.7c2.8,0,5.1,2.5,5.1,5.5l0,0C85.2,53.1,82.9,55.5,80.2,55.5z"})],-1))})(),Ke=k([c("checkbox",`
 font-size: var(--n-font-size);
 outline: none;
 cursor: pointer;
 display: inline-flex;
 flex-wrap: nowrap;
 align-items: flex-start;
 word-break: break-word;
 line-height: var(--n-size);
 --n-merged-color-table: var(--n-color-table);
 `,[F("show-label","line-height: var(--n-label-line-height);"),k("&:hover",[c("checkbox-box",[S("border","border: var(--n-border-checked);")])]),k("&:focus:not(:active)",[c("checkbox-box",[S("border",`
 border: var(--n-border-focus);
 box-shadow: var(--n-box-shadow-focus);
 `)])]),F("inside-table",[c("checkbox-box",`
 background-color: var(--n-merged-color-table);
 `)]),F("checked",[c("checkbox-box",`
 background-color: var(--n-color-checked);
 `,[c("checkbox-icon",[k(".check-icon",`
 opacity: 1;
 transform: scale(1);
 `)])])]),F("indeterminate",[c("checkbox-box",[c("checkbox-icon",[k(".check-icon",`
 opacity: 0;
 transform: scale(.5);
 `),k(".line-icon",`
 opacity: 1;
 transform: scale(1);
 `)])])]),F("checked, indeterminate",[k("&:focus:not(:active)",[c("checkbox-box",[S("border",`
 border: var(--n-border-checked);
 box-shadow: var(--n-box-shadow-focus);
 `)])]),c("checkbox-box",`
 background-color: var(--n-color-checked);
 border-left: 0;
 border-top: 0;
 `,[S("border",{border:"var(--n-border-checked)"})])]),F("disabled",{cursor:"not-allowed"},[F("checked",[c("checkbox-box",`
 background-color: var(--n-color-disabled-checked);
 `,[S("border",{border:"var(--n-border-disabled-checked)"}),c("checkbox-icon",[k(".check-icon, .line-icon",{fill:"var(--n-check-mark-color-disabled-checked)"})])])]),c("checkbox-box",`
 background-color: var(--n-color-disabled);
 `,[S("border",`
 border: var(--n-border-disabled);
 `),c("checkbox-icon",[k(".check-icon, .line-icon",`
 fill: var(--n-check-mark-color-disabled);
 `)])]),S("label",`
 color: var(--n-text-color-disabled);
 `)]),c("checkbox-box-wrapper",`
 position: relative;
 width: var(--n-size);
 flex-shrink: 0;
 flex-grow: 0;
 user-select: none;
 -webkit-user-select: none;
 `),c("checkbox-box",`
 position: absolute;
 left: 0;
 top: 50%;
 transform: translateY(-50%);
 height: var(--n-size);
 width: var(--n-size);
 display: inline-block;
 box-sizing: border-box;
 border-radius: var(--n-border-radius);
 background-color: var(--n-color);
 transition: background-color 0.3s var(--n-bezier);
 `,[S("border",`
 transition:
 border-color .3s var(--n-bezier),
 box-shadow .3s var(--n-bezier);
 border-radius: inherit;
 position: absolute;
 left: 0;
 right: 0;
 top: 0;
 bottom: 0;
 border: var(--n-border);
 `),c("checkbox-icon",`
 display: flex;
 align-items: center;
 justify-content: center;
 position: absolute;
 left: 1px;
 right: 1px;
 top: 1px;
 bottom: 1px;
 `,[k(".check-icon, .line-icon",`
 width: 100%;
 fill: var(--n-check-mark-color);
 opacity: 0;
 transform: scale(0.5);
 transform-origin: center;
 transition:
 fill 0.3s var(--n-bezier),
 transform 0.3s var(--n-bezier),
 opacity 0.3s var(--n-bezier),
 border-color 0.3s var(--n-bezier);
 `),xe({left:"1px",top:"1px"})])]),S("label",`
 color: var(--n-text-color);
 transition: color .3s var(--n-bezier);
 user-select: none;
 -webkit-user-select: none;
 padding: var(--n-label-padding);
 font-weight: var(--n-label-font-weight);
 `,[k("&:empty",{display:"none"})])]),ge(c("checkbox",`
 --n-merged-color-table: var(--n-color-table-modal);
 `)),me(c("checkbox",`
 --n-merged-color-table: var(--n-color-table-popover);
 `))]);const Ue=["id"],Ne=["tabindex","aria-checked","aria-labelledby","onKeyup","onKeydown","onClick"],Pe={...J.props,size:String,checked:{type:[Boolean,String,Number],default:void 0},defaultChecked:{type:[Boolean,String,Number],default:!1},value:[String,Number],disabled:{type:Boolean,default:void 0},indeterminate:Boolean,label:String,focusable:{type:Boolean,default:!0},checkedValue:{type:[Boolean,String,Number],default:!0},uncheckedValue:{type:[Boolean,String,Number],default:!1},"onUpdate:checked":[Function,Array],onUpdateChecked:[Function,Array],privateInsideTable:Boolean,onChange:[Function,Array]};var Ve=H({name:"Checkbox",props:Pe,setup(e){const n=Re(Z,null),u=V(null),{mergedClsPrefixRef:t,inlineThemeDisabled:s,mergedRtlRef:C,mergedComponentPropsRef:T}=O(e),h=V(e.defaultChecked),l=j(e,"checked"),K=W(l,h),w=Se(()=>{if(n){const o=n.valueSetRef.value;return o&&e.value!==void 0?o.has(e.value):!1}else return K.value===e.checkedValue}),f=Q(e,{mergedSize(o){var y,z;const{size:m}=e;if(m!==void 0)return m;if(n){const{value:R}=n.mergedSizeRef;if(R!==void 0)return R}if(o){const{mergedSize:R}=o;if(R!==void 0)return R.value}const p=(z=(y=T==null?void 0:T.value)==null?void 0:y.Checkbox)==null?void 0:z.size;return p||"medium"},mergedDisabled(o){const{disabled:m}=e;if(m!==void 0)return m;if(n){if(n.disabledRef.value)return!0;const{maxRef:{value:p},checkedCountRef:y}=n;if(p!==void 0&&y.value>=p&&!w.value)return!0;const{minRef:{value:z}}=n;if(z!==void 0&&y.value<=z&&w.value)return!0}return o?o.disabled.value:!1}}),{mergedDisabledRef:r,mergedSizeRef:v}=f,x=J("Checkbox","-checkbox",Ke,Te,e,t);function a(o){if(n&&e.value!==void 0)n.toggleCheckbox(!w.value,e.value);else{const{onChange:m,"onUpdate:checked":p,onUpdateChecked:y}=e,{nTriggerFormInput:z,nTriggerFormChange:R}=f,I=w.value?e.uncheckedValue:e.checkedValue;p&&i(p,I,o),y&&i(y,I,o),m&&i(m,I,o),z(),R(),h.value=I}}function b(o){r.value||a(o)}function g(o){if(!r.value)switch(o.key){case" ":case"Enter":a(o)}}function d(o){switch(o.key){case" ":o.preventDefault()}}const U={focus:()=>{var o;(o=u.value)==null||o.focus()},blur:()=>{var o;(o=u.value)==null||o.blur()}},P=_e("Checkbox",C,t),L=N(()=>{const{value:o}=v,{common:{cubicBezierEaseInOut:m},self:{borderRadius:p,color:y,colorChecked:z,colorDisabled:R,colorTableHeader:I,colorTableHeaderModal:q,colorTableHeaderPopover:X,checkMarkColor:ee,checkMarkColorDisabled:oe,border:re,borderFocus:ae,borderDisabled:ce,borderChecked:ne,boxShadowFocus:le,textColor:te,textColorDisabled:ie,checkMarkColorDisabledChecked:de,colorDisabledChecked:se,borderDisabledChecked:be,labelPadding:ue,labelLineHeight:he,labelFontWeight:fe,[G("fontSize",o)]:ke,[G("size",o)]:ve}}=x.value;return{"--n-label-line-height":he,"--n-label-font-weight":fe,"--n-size":ve,"--n-bezier":m,"--n-border-radius":p,"--n-border":re,"--n-border-checked":ne,"--n-border-focus":ae,"--n-border-disabled":ce,"--n-border-disabled-checked":be,"--n-box-shadow-focus":le,"--n-color":y,"--n-color-checked":z,"--n-color-table":I,"--n-color-table-modal":q,"--n-color-table-popover":X,"--n-color-disabled":R,"--n-color-disabled-checked":se,"--n-text-color":te,"--n-text-color-disabled":ie,"--n-check-mark-color":ee,"--n-check-mark-color-disabled":oe,"--n-check-mark-color-disabled-checked":de,"--n-font-size":ke,"--n-label-padding":ue}}),A=s?De("checkbox",N(()=>v.value[0]),L,e):void 0;return Object.assign(f,U,{rtlEnabled:P,selfRef:u,mergedClsPrefix:t,mergedDisabled:r,renderedChecked:w,mergedTheme:x,labelId:Fe(),handleClick:b,handleKeyUp:g,handleKeyDown:d,cssVars:s?void 0:L,themeClass:A==null?void 0:A.themeClass,onRender:A==null?void 0:A.onRender})},render(){var x;const{$slots:e,renderedChecked:n,mergedDisabled:u,indeterminate:t,privateInsideTable:s,cssVars:C,labelId:T,label:h,mergedClsPrefix:l,focusable:K,handleKeyUp:w,handleKeyDown:f,handleClick:r}=this;(x=this.onRender)==null||x.call(this);const v=pe(e.default,a=>h||a?(D(),B("span",{key:1,class:_(`${l}-checkbox__label`),id:T},[$(()=>h||a)],10,Ue)):null);return(()=>{const a=E("70be6e74cd27cb50");return D(),B("div",{ref:"selfRef",class:_([`${l}-checkbox`,this.themeClass,this.rtlEnabled&&`${l}-checkbox--rtl`,n&&`${l}-checkbox--checked`,u&&`${l}-checkbox--disabled`,t&&`${l}-checkbox--indeterminate`,s&&`${l}-checkbox--inside-table`,v&&`${l}-checkbox--show-label`]),tabindex:u||!K?void 0:0,role:"checkbox","aria-checked":t?"mixed":n,"aria-labelledby":T,style:ze(C),onKeyup:w,onKeydown:f,onClick:r,onMousedown:a[0]||(a[0]=()=>{we("selectstart",window,b=>{b.preventDefault()},{once:!0})})},[M("div",{class:_(`${l}-checkbox-box-wrapper`)},[a[1]||(a[1]=$(" ",-1)),M("div",{class:_(`${l}-checkbox-box`)},[ye(Ce,null,{default:()=>this.indeterminate?(D(),B("div",{key:"indeterminate",class:_(`${l}-checkbox-icon`)},[$(()=>Ie())],2)):(D(),B("div",{key:"check",class:_(`${l}-checkbox-icon`)},[$(()=>Ae())],2))},1024),M("div",{class:_(`${l}-checkbox-box__border`)},null,2)],2)],2),$(()=>v)],46,Ne)})()}});const Z=$e("n-checkbox-group"),je={min:Number,max:Number,size:String,options:Array,labelField:{type:String,default:"label"},valueField:{type:String,default:"value"},value:Array,defaultValue:{type:Array,default:null},disabled:{type:Boolean,default:void 0},"onUpdate:value":[Function,Array],onUpdateValue:[Function,Array],onChange:[Function,Array]};var Le=H({name:"CheckboxGroup",props:je,setup(e){const{mergedClsPrefixRef:n}=O(e),u=Q(e),{mergedSizeRef:t,mergedDisabledRef:s}=u,C=V(e.defaultValue),T=N(()=>e.value),h=W(T,C),l=N(()=>{var f;return((f=h.value)==null?void 0:f.length)||0}),K=N(()=>Array.isArray(h.value)?new Set(h.value):new Set);function w(f,r){const{nTriggerFormInput:v,nTriggerFormChange:x}=u,{onChange:a,"onUpdate:value":b,onUpdateValue:g}=e;if(Array.isArray(h.value)){const d=Array.from(h.value),U=d.findIndex(P=>P===r);f?~U||(d.push(r),g&&i(g,d,{actionType:"check",value:r}),b&&i(b,d,{actionType:"check",value:r}),v(),x(),C.value=d,a&&i(a,d)):~U&&(d.splice(U,1),g&&i(g,d,{actionType:"uncheck",value:r}),b&&i(b,d,{actionType:"uncheck",value:r}),a&&i(a,d),C.value=d,v(),x())}else f?(g&&i(g,[r],{actionType:"check",value:r}),b&&i(b,[r],{actionType:"check",value:r}),a&&i(a,[r]),C.value=[r],v(),x()):(g&&i(g,[],{actionType:"uncheck",value:r}),b&&i(b,[],{actionType:"uncheck",value:r}),a&&i(a,[]),C.value=[],v(),x())}return Me(Z,{checkedCountRef:l,maxRef:j(e,"max"),minRef:j(e,"min"),valueSetRef:K,disabledRef:s,mergedSizeRef:t,toggleCheckbox:w}),{mergedClsPrefix:n}},render(){const{options:e,labelField:n,valueField:u}=this.$props;return D(),B("div",{class:_(`${this.mergedClsPrefix}-checkbox-group`),role:"group"},[e?(D(),B(Y,{key:0},[$(()=>e.map(t=>{const s=t[u];return D(),Be(Ve,{key:s,value:s,disabled:t.disabled,label:t[n]},null,8,["value","disabled","label"])}))],64)):(D(),B(Y,{key:1},[$(()=>{var t,s;return(s=(t=this.$slots).default)==null?void 0:s.call(t)})],64))],2)}});export{Le as C,Ve as a};
