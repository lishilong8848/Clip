import{bd as G,a_ as A,aD as k,aF as c,aH as $,aG as S,c5 as xe,c8 as me,c9 as ge,bm as H,eC as O,dS as pe,dg as D,b1 as B,d1 as F,cW as _,bc as ye,I as Ce,d6 as we,d0 as ze,c6 as Re,ek as q,dy as V,ex as Y,ev as Se,eq as J,aS as Te,ey as _e,eD as De,aW as N,b2 as $e,e7 as E,b5 as L,aK as i,b4 as Fe,G as W,a$ as Be,ds as Ae}from"./app-v2-Bm-TazMs.js";var Ie=()=>(()=>{const e=G("75be776d8875fa17");return e[0]||(e[0]=A("svg",{viewBox:"0 0 64 64",class:"check-icon"},[A("path",{d:"M50.42,16.76L22.34,39.45l-8.1-11.46c-1.12-1.58-3.3-1.96-4.88-0.84c-1.58,1.12-1.95,3.3-0.84,4.88l10.26,14.51  c0.56,0.79,1.42,1.31,2.38,1.45c0.16,0.02,0.32,0.03,0.48,0.03c0.8,0,1.57-0.27,2.2-0.78l30.99-25.03c1.5-1.21,1.74-3.42,0.52-4.92  C54.13,15.78,51.93,15.55,50.42,16.76z"})],-1))})(),Me=()=>(()=>{const e=G("c6eed899356c8404");return e[0]||(e[0]=A("svg",{viewBox:"0 0 100 100",class:"line-icon"},[A("path",{d:"M80.2,55.5H21.4c-2.8,0-5.1-2.5-5.1-5.5l0,0c0-3,2.3-5.5,5.1-5.5h58.7c2.8,0,5.1,2.5,5.1,5.5l0,0C85.2,53.1,82.9,55.5,80.2,55.5z"})],-1))})(),Ke=k([c("checkbox",`
 font-size: var(--n-font-size);
 outline: none;
 cursor: pointer;
 display: inline-flex;
 flex-wrap: nowrap;
 align-items: flex-start;
 word-break: break-word;
 line-height: var(--n-size);
 --n-merged-color-table: var(--n-color-table);
 `,[$("show-label","line-height: var(--n-label-line-height);"),k("&:hover",[c("checkbox-box",[S("border","border: var(--n-border-checked);")])]),k("&:focus:not(:active)",[c("checkbox-box",[S("border",`
 border: var(--n-border-focus);
 box-shadow: var(--n-box-shadow-focus);
 `)])]),$("inside-table",[c("checkbox-box",`
 background-color: var(--n-merged-color-table);
 `)]),$("checked",[c("checkbox-box",`
 background-color: var(--n-color-checked);
 `,[c("checkbox-icon",[k(".check-icon",`
 opacity: 1;
 transform: scale(1);
 `)])])]),$("indeterminate",[c("checkbox-box",[c("checkbox-icon",[k(".check-icon",`
 opacity: 0;
 transform: scale(.5);
 `),k(".line-icon",`
 opacity: 1;
 transform: scale(1);
 `)])])]),$("checked, indeterminate",[k("&:focus:not(:active)",[c("checkbox-box",[S("border",`
 border: var(--n-border-checked);
 box-shadow: var(--n-box-shadow-focus);
 `)])]),c("checkbox-box",`
 background-color: var(--n-color-checked);
 border-left: 0;
 border-top: 0;
 `,[S("border",{border:"var(--n-border-checked)"})])]),$("disabled",{cursor:"not-allowed"},[$("checked",[c("checkbox-box",`
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
 `,[k("&:empty",{display:"none"})])]),me(c("checkbox",`
 --n-merged-color-table: var(--n-color-table-modal);
 `)),ge(c("checkbox",`
 --n-merged-color-table: var(--n-color-table-popover);
 `))]);const Ue=["id"],Ne=["tabindex","aria-checked","aria-labelledby","onKeyup","onKeydown","onClick"],Pe={...O.props,size:String,checked:{type:[Boolean,String,Number],default:void 0},defaultChecked:{type:[Boolean,String,Number],default:!1},value:[String,Number],disabled:{type:Boolean,default:void 0},indeterminate:Boolean,label:String,focusable:{type:Boolean,default:!0},checkedValue:{type:[Boolean,String,Number],default:!0},uncheckedValue:{type:[Boolean,String,Number],default:!1},"onUpdate:checked":[Function,Array],onUpdateChecked:[Function,Array],privateInsideTable:Boolean,onChange:[Function,Array]};var Ve=H({name:"Checkbox",props:Pe,setup(e){const n=Re(Q,null),u=V(null),{mergedClsPrefixRef:t,inlineThemeDisabled:s,mergedRtlRef:C,mergedComponentPropsRef:T}=q(e),h=V(e.defaultChecked),l=E(e,"checked"),K=Y(l,h),w=Se(()=>{if(n){const o=n.valueSetRef.value;return o&&e.value!==void 0?o.has(e.value):!1}else return K.value===e.checkedValue}),f=J(e,{mergedSize(o){var y,z;const{size:g}=e;if(g!==void 0)return g;if(n){const{value:R}=n.mergedSizeRef;if(R!==void 0)return R}if(o){const{mergedSize:R}=o;if(R!==void 0)return R.value}const p=(z=(y=T==null?void 0:T.value)==null?void 0:y.Checkbox)==null?void 0:z.size;return p||"medium"},mergedDisabled(o){const{disabled:g}=e;if(g!==void 0)return g;if(n){if(n.disabledRef.value)return!0;const{maxRef:{value:p},checkedCountRef:y}=n;if(p!==void 0&&y.value>=p&&!w.value)return!0;const{minRef:{value:z}}=n;if(z!==void 0&&y.value<=z&&w.value)return!0}return o?o.disabled.value:!1}}),{mergedDisabledRef:r,mergedSizeRef:v}=f,x=O("Checkbox","-checkbox",Ke,Te,e,t);function a(o){if(n&&e.value!==void 0)n.toggleCheckbox(!w.value,e.value);else{const{onChange:g,"onUpdate:checked":p,onUpdateChecked:y}=e,{nTriggerFormInput:z,nTriggerFormChange:R}=f,M=w.value?e.uncheckedValue:e.checkedValue;p&&i(p,M,o),y&&i(y,M,o),g&&i(g,M,o),z(),R(),h.value=M}}function b(o){r.value||a(o)}function m(o){if(!r.value)switch(o.key){case" ":case"Enter":a(o)}}function d(o){switch(o.key){case" ":o.preventDefault()}}const U={focus:()=>{var o;(o=u.value)==null||o.focus()},blur:()=>{var o;(o=u.value)==null||o.blur()}},P=_e("Checkbox",C,t),j=N(()=>{const{value:o}=v,{common:{cubicBezierEaseInOut:g},self:{borderRadius:p,color:y,colorChecked:z,colorDisabled:R,colorTableHeader:M,colorTableHeaderModal:X,colorTableHeaderPopover:Z,checkMarkColor:ee,checkMarkColorDisabled:oe,border:re,borderFocus:ae,borderDisabled:ce,borderChecked:ne,boxShadowFocus:le,textColor:te,textColorDisabled:ie,checkMarkColorDisabledChecked:de,colorDisabledChecked:se,borderDisabledChecked:be,labelPadding:ue,labelLineHeight:he,labelFontWeight:fe,[L("fontSize",o)]:ke,[L("size",o)]:ve}}=x.value;return{"--n-label-line-height":he,"--n-label-font-weight":fe,"--n-size":ve,"--n-bezier":g,"--n-border-radius":p,"--n-border":re,"--n-border-checked":ne,"--n-border-focus":ae,"--n-border-disabled":ce,"--n-border-disabled-checked":be,"--n-box-shadow-focus":le,"--n-color":y,"--n-color-checked":z,"--n-color-table":M,"--n-color-table-modal":X,"--n-color-table-popover":Z,"--n-color-disabled":R,"--n-color-disabled-checked":se,"--n-text-color":te,"--n-text-color-disabled":ie,"--n-check-mark-color":ee,"--n-check-mark-color-disabled":oe,"--n-check-mark-color-disabled-checked":de,"--n-font-size":ke,"--n-label-padding":ue}}),I=s?De("checkbox",N(()=>v.value[0]),j,e):void 0;return Object.assign(f,U,{rtlEnabled:P,selfRef:u,mergedClsPrefix:t,mergedDisabled:r,renderedChecked:w,mergedTheme:x,labelId:$e(),handleClick:b,handleKeyUp:m,handleKeyDown:d,cssVars:s?void 0:j,themeClass:I==null?void 0:I.themeClass,onRender:I==null?void 0:I.onRender})},render(){var x;const{$slots:e,renderedChecked:n,mergedDisabled:u,indeterminate:t,privateInsideTable:s,cssVars:C,labelId:T,label:h,mergedClsPrefix:l,focusable:K,handleKeyUp:w,handleKeyDown:f,handleClick:r}=this;(x=this.onRender)==null||x.call(this);const v=pe(e.default,a=>h||a?(D(),B("span",{key:1,class:_(`${l}-checkbox__label`),id:T},[F(()=>h||a)],10,Ue)):null);return(()=>{const a=G("70be6e74cd27cb50");return D(),B("div",{ref:"selfRef",class:_([`${l}-checkbox`,this.themeClass,this.rtlEnabled&&`${l}-checkbox--rtl`,n&&`${l}-checkbox--checked`,u&&`${l}-checkbox--disabled`,t&&`${l}-checkbox--indeterminate`,s&&`${l}-checkbox--inside-table`,v&&`${l}-checkbox--show-label`]),tabindex:u||!K?void 0:0,role:"checkbox","aria-checked":t?"mixed":n,"aria-labelledby":T,style:ze(C),onKeyup:w,onKeydown:f,onClick:r,onMousedown:a[0]||(a[0]=()=>{we("selectstart",window,b=>{b.preventDefault()},{once:!0})})},[A("div",{class:_(`${l}-checkbox-box-wrapper`)},[a[1]||(a[1]=F(" ",-1)),A("div",{class:_(`${l}-checkbox-box`)},[ye(Ce,null,{default:()=>this.indeterminate?(D(),B("div",{key:"indeterminate",class:_(`${l}-checkbox-icon`)},[F(()=>Me())],2)):(D(),B("div",{key:"check",class:_(`${l}-checkbox-icon`)},[F(()=>Ie())],2))},1024),A("div",{class:_(`${l}-checkbox-box__border`)},null,2)],2)],2),F(()=>v)],46,Ne)})()}});const Q=Fe("n-checkbox-group"),Ee={min:Number,max:Number,size:String,options:Array,labelField:{type:String,default:"label"},valueField:{type:String,default:"value"},value:Array,defaultValue:{type:Array,default:null},disabled:{type:Boolean,default:void 0},"onUpdate:value":[Function,Array],onUpdateValue:[Function,Array],onChange:[Function,Array]};var je=H({name:"CheckboxGroup",props:Ee,setup(e){const{mergedClsPrefixRef:n}=q(e),u=J(e),{mergedSizeRef:t,mergedDisabledRef:s}=u,C=V(e.defaultValue),T=N(()=>e.value),h=Y(T,C),l=N(()=>{var f;return((f=h.value)==null?void 0:f.length)||0}),K=N(()=>Array.isArray(h.value)?new Set(h.value):new Set);function w(f,r){const{nTriggerFormInput:v,nTriggerFormChange:x}=u,{onChange:a,"onUpdate:value":b,onUpdateValue:m}=e;if(Array.isArray(h.value)){const d=Array.from(h.value),U=d.findIndex(P=>P===r);f?~U||(d.push(r),m&&i(m,d,{actionType:"check",value:r}),b&&i(b,d,{actionType:"check",value:r}),v(),x(),C.value=d,a&&i(a,d)):~U&&(d.splice(U,1),m&&i(m,d,{actionType:"uncheck",value:r}),b&&i(b,d,{actionType:"uncheck",value:r}),a&&i(a,d),C.value=d,v(),x())}else f?(m&&i(m,[r],{actionType:"check",value:r}),b&&i(b,[r],{actionType:"check",value:r}),a&&i(a,[r]),C.value=[r],v(),x()):(m&&i(m,[],{actionType:"uncheck",value:r}),b&&i(b,[],{actionType:"uncheck",value:r}),a&&i(a,[]),C.value=[],v(),x())}return Ae(Q,{checkedCountRef:l,maxRef:E(e,"max"),minRef:E(e,"min"),valueSetRef:K,disabledRef:s,mergedSizeRef:t,toggleCheckbox:w}),{mergedClsPrefix:n}},render(){const{options:e,labelField:n,valueField:u}=this.$props;return D(),B("div",{class:_(`${this.mergedClsPrefix}-checkbox-group`),role:"group"},[e?(D(),B(W,{key:0},[F(()=>e.map(t=>{const s=t[u];return D(),Be(Ve,{key:s,value:s,disabled:t.disabled,label:t[n]},null,8,["value","disabled","label"])}))],64)):(D(),B(W,{key:1},[F(()=>{var t,s;return(s=(t=this.$slots).default)==null?void 0:s.call(t)})],64))],2)}});export{je as C,Ve as a};
