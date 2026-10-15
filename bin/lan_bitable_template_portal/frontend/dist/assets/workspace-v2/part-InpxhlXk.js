import{b9 as xe,aB as me,b0 as H,aM as I,aq as p,as as n,au as F,at as D,bC as ge,bF as pe,bG as Ce,b7 as W,dR as q,d8 as ye,cD as $,aP as B,cq as _,ck as T,a$ as ze,I as we,cu as Se,cp as Re,bD as De,dz as O,cS as V,dM as Q,dK as Te,dF as Y,dN as $e,dS as Me,aI as K,aQ as Fe,dm as E,aS as G,aw as t,aR as _e,t as j,aN as Be,cM as Ie}from"./app-DXg8PPQD.js";var Ae={sizeSmall:"14px",sizeMedium:"16px",sizeLarge:"18px",labelPadding:"0 8px",labelFontWeight:"400"};function Ne(e){const{baseColor:c,inputColorDisabled:s,cardColor:i,modalColor:b,popoverColor:m,textColorDisabled:f,borderColor:d,primaryColor:r,textColor2:M,fontSizeSmall:g,fontSizeMedium:u,fontSizeLarge:a,borderRadiusSmall:k,lineHeight:v}=e;return{...Ae,labelLineHeight:v,fontSizeSmall:g,fontSizeMedium:u,fontSizeLarge:a,borderRadius:k,color:c,colorChecked:r,colorDisabled:s,colorDisabledChecked:s,colorTableHeader:i,colorTableHeaderModal:b,colorTableHeaderPopover:m,checkMarkColor:c,checkMarkColorDisabled:f,checkMarkColorDisabledChecked:f,border:`1px solid ${d}`,borderDisabled:`1px solid ${d}`,borderDisabledChecked:`1px solid ${d}`,borderChecked:`1px solid ${r}`,borderFocus:`1px solid ${r}`,boxShadowFocus:`0 0 0 2px ${me(r,{alpha:.3})}`,textColor:M,textColorDisabled:f}}const Pe={name:"Checkbox",common:xe,self:Ne};var Ke=()=>(()=>{const e=H("75be776d8875fa17");return e[0]||(e[0]=I("svg",{viewBox:"0 0 64 64",class:"check-icon"},[I("path",{d:"M50.42,16.76L22.34,39.45l-8.1-11.46c-1.12-1.58-3.3-1.96-4.88-0.84c-1.58,1.12-1.95,3.3-0.84,4.88l10.26,14.51  c0.56,0.79,1.42,1.31,2.38,1.45c0.16,0.02,0.32,0.03,0.48,0.03c0.8,0,1.57-0.27,2.2-0.78l30.99-25.03c1.5-1.21,1.74-3.42,0.52-4.92  C54.13,15.78,51.93,15.55,50.42,16.76z"})],-1))})(),Ue=()=>(()=>{const e=H("c6eed899356c8404");return e[0]||(e[0]=I("svg",{viewBox:"0 0 100 100",class:"line-icon"},[I("path",{d:"M80.2,55.5H21.4c-2.8,0-5.1-2.5-5.1-5.5l0,0c0-3,2.3-5.5,5.1-5.5h58.7c2.8,0,5.1,2.5,5.1,5.5l0,0C85.2,53.1,82.9,55.5,80.2,55.5z"})],-1))})(),Ve=p([n("checkbox",`
 font-size: var(--n-font-size);
 outline: none;
 cursor: pointer;
 display: inline-flex;
 flex-wrap: nowrap;
 align-items: flex-start;
 word-break: break-word;
 line-height: var(--n-size);
 --n-merged-color-table: var(--n-color-table);
 `,[F("show-label","line-height: var(--n-label-line-height);"),p("&:hover",[n("checkbox-box",[D("border","border: var(--n-border-checked);")])]),p("&:focus:not(:active)",[n("checkbox-box",[D("border",`
 border: var(--n-border-focus);
 box-shadow: var(--n-box-shadow-focus);
 `)])]),F("inside-table",[n("checkbox-box",`
 background-color: var(--n-merged-color-table);
 `)]),F("checked",[n("checkbox-box",`
 background-color: var(--n-color-checked);
 `,[n("checkbox-icon",[p(".check-icon",`
 opacity: 1;
 transform: scale(1);
 `)])])]),F("indeterminate",[n("checkbox-box",[n("checkbox-icon",[p(".check-icon",`
 opacity: 0;
 transform: scale(.5);
 `),p(".line-icon",`
 opacity: 1;
 transform: scale(1);
 `)])])]),F("checked, indeterminate",[p("&:focus:not(:active)",[n("checkbox-box",[D("border",`
 border: var(--n-border-checked);
 box-shadow: var(--n-box-shadow-focus);
 `)])]),n("checkbox-box",`
 background-color: var(--n-color-checked);
 border-left: 0;
 border-top: 0;
 `,[D("border",{border:"var(--n-border-checked)"})])]),F("disabled",{cursor:"not-allowed"},[F("checked",[n("checkbox-box",`
 background-color: var(--n-color-disabled-checked);
 `,[D("border",{border:"var(--n-border-disabled-checked)"}),n("checkbox-icon",[p(".check-icon, .line-icon",{fill:"var(--n-check-mark-color-disabled-checked)"})])])]),n("checkbox-box",`
 background-color: var(--n-color-disabled);
 `,[D("border",`
 border: var(--n-border-disabled);
 `),n("checkbox-icon",[p(".check-icon, .line-icon",`
 fill: var(--n-check-mark-color-disabled);
 `)])]),D("label",`
 color: var(--n-text-color-disabled);
 `)]),n("checkbox-box-wrapper",`
 position: relative;
 width: var(--n-size);
 flex-shrink: 0;
 flex-grow: 0;
 user-select: none;
 -webkit-user-select: none;
 `),n("checkbox-box",`
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
 `,[D("border",`
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
 `),n("checkbox-icon",`
 display: flex;
 align-items: center;
 justify-content: center;
 position: absolute;
 left: 1px;
 right: 1px;
 top: 1px;
 bottom: 1px;
 `,[p(".check-icon, .line-icon",`
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
 `),ge({left:"1px",top:"1px"})])]),D("label",`
 color: var(--n-text-color);
 transition: color .3s var(--n-bezier);
 user-select: none;
 -webkit-user-select: none;
 padding: var(--n-label-padding);
 font-weight: var(--n-label-font-weight);
 `,[p("&:empty",{display:"none"})])]),pe(n("checkbox",`
 --n-merged-color-table: var(--n-color-table-modal);
 `)),Ce(n("checkbox",`
 --n-merged-color-table: var(--n-color-table-popover);
 `))]);const Ee=["id"],He=["tabindex","aria-checked","aria-labelledby","onKeyup","onKeydown","onClick"],Le={...q.props,size:String,checked:{type:[Boolean,String,Number],default:void 0},defaultChecked:{type:[Boolean,String,Number],default:!1},value:[String,Number],disabled:{type:Boolean,default:void 0},indeterminate:Boolean,label:String,focusable:{type:Boolean,default:!0},checkedValue:{type:[Boolean,String,Number],default:!0},uncheckedValue:{type:[Boolean,String,Number],default:!1},"onUpdate:checked":[Function,Array],onUpdateChecked:[Function,Array],privateInsideTable:Boolean,onChange:[Function,Array]};var Ge=W({name:"Checkbox",props:Le,setup(e){const c=De(J,null),s=V(null),{mergedClsPrefixRef:i,inlineThemeDisabled:b,mergedRtlRef:m,mergedComponentPropsRef:f}=O(e),d=V(e.defaultChecked),r=E(e,"checked"),M=Q(r,d),g=Te(()=>{if(c){const o=c.valueSetRef.value;return o&&e.value!==void 0?o.has(e.value):!1}else return M.value===e.checkedValue}),u=Y(e,{mergedSize(o){var w,S;const{size:y}=e;if(y!==void 0)return y;if(c){const{value:R}=c.mergedSizeRef;if(R!==void 0)return R}if(o){const{mergedSize:R}=o;if(R!==void 0)return R.value}const z=(S=(w=f==null?void 0:f.value)==null?void 0:w.Checkbox)==null?void 0:S.size;return z||"medium"},mergedDisabled(o){const{disabled:y}=e;if(y!==void 0)return y;if(c){if(c.disabledRef.value)return!0;const{maxRef:{value:z},checkedCountRef:w}=c;if(z!==void 0&&w.value>=z&&!g.value)return!0;const{minRef:{value:S}}=c;if(S!==void 0&&w.value<=S&&g.value)return!0}return o?o.disabled.value:!1}}),{mergedDisabledRef:a,mergedSizeRef:k}=u,v=q("Checkbox","-checkbox",Ve,Pe,e,i);function l(o){if(c&&e.value!==void 0)c.toggleCheckbox(!g.value,e.value);else{const{onChange:y,"onUpdate:checked":z,onUpdateChecked:w}=e,{nTriggerFormInput:S,nTriggerFormChange:R}=u,N=g.value?e.uncheckedValue:e.checkedValue;z&&t(z,N,o),w&&t(w,N,o),y&&t(y,N,o),S(),R(),d.value=N}}function x(o){a.value||l(o)}function C(o){if(!a.value)switch(o.key){case" ":case"Enter":l(o)}}function h(o){switch(o.key){case" ":o.preventDefault()}}const P={focus:()=>{var o;(o=s.value)==null||o.focus()},blur:()=>{var o;(o=s.value)==null||o.blur()}},U=$e("Checkbox",m,i),L=K(()=>{const{value:o}=k,{common:{cubicBezierEaseInOut:y},self:{borderRadius:z,color:w,colorChecked:S,colorDisabled:R,colorTableHeader:N,colorTableHeaderModal:X,colorTableHeaderPopover:Z,checkMarkColor:ee,checkMarkColorDisabled:oe,border:re,borderFocus:ae,borderDisabled:ce,borderChecked:le,boxShadowFocus:ne,textColor:ie,textColorDisabled:de,checkMarkColorDisabledChecked:te,colorDisabledChecked:se,borderDisabledChecked:be,labelPadding:ue,labelLineHeight:he,labelFontWeight:fe,[G("fontSize",o)]:ke,[G("size",o)]:ve}}=v.value;return{"--n-label-line-height":he,"--n-label-font-weight":fe,"--n-size":ve,"--n-bezier":y,"--n-border-radius":z,"--n-border":re,"--n-border-checked":le,"--n-border-focus":ae,"--n-border-disabled":ce,"--n-border-disabled-checked":be,"--n-box-shadow-focus":ne,"--n-color":w,"--n-color-checked":S,"--n-color-table":N,"--n-color-table-modal":X,"--n-color-table-popover":Z,"--n-color-disabled":R,"--n-color-disabled-checked":se,"--n-text-color":ie,"--n-text-color-disabled":de,"--n-check-mark-color":ee,"--n-check-mark-color-disabled":oe,"--n-check-mark-color-disabled-checked":te,"--n-font-size":ke,"--n-label-padding":ue}}),A=b?Me("checkbox",K(()=>k.value[0]),L,e):void 0;return Object.assign(u,P,{rtlEnabled:U,selfRef:s,mergedClsPrefix:i,mergedDisabled:a,renderedChecked:g,mergedTheme:v,labelId:Fe(),handleClick:x,handleKeyUp:C,handleKeyDown:h,cssVars:b?void 0:L,themeClass:A==null?void 0:A.themeClass,onRender:A==null?void 0:A.onRender})},render(){var v;const{$slots:e,renderedChecked:c,mergedDisabled:s,indeterminate:i,privateInsideTable:b,cssVars:m,labelId:f,label:d,mergedClsPrefix:r,focusable:M,handleKeyUp:g,handleKeyDown:u,handleClick:a}=this;(v=this.onRender)==null||v.call(this);const k=ye(e.default,l=>d||l?($(),B("span",{key:1,class:T(`${r}-checkbox__label`),id:f},[_(()=>d||l)],10,Ee)):null);return(()=>{const l=H("70be6e74cd27cb50");return $(),B("div",{ref:"selfRef",class:T([`${r}-checkbox`,this.themeClass,this.rtlEnabled&&`${r}-checkbox--rtl`,c&&`${r}-checkbox--checked`,s&&`${r}-checkbox--disabled`,i&&`${r}-checkbox--indeterminate`,b&&`${r}-checkbox--inside-table`,k&&`${r}-checkbox--show-label`]),tabindex:s||!M?void 0:0,role:"checkbox","aria-checked":i?"mixed":c,"aria-labelledby":f,style:Re(m),onKeyup:g,onKeydown:u,onClick:a,onMousedown:l[0]||(l[0]=()=>{Se("selectstart",window,x=>{x.preventDefault()},{once:!0})})},[I("div",{class:T(`${r}-checkbox-box-wrapper`)},[l[1]||(l[1]=_(" ",-1)),I("div",{class:T(`${r}-checkbox-box`)},[ze(we,null,{default:()=>this.indeterminate?($(),B("div",{key:"indeterminate",class:T(`${r}-checkbox-icon`)},[_(()=>Ue())],2)):($(),B("div",{key:"check",class:T(`${r}-checkbox-icon`)},[_(()=>Ke())],2))},1024),I("div",{class:T(`${r}-checkbox-box__border`)},null,2)],2)],2),_(()=>k)],46,He)})()}});const J=_e("n-checkbox-group"),je={min:Number,max:Number,size:String,options:Array,labelField:{type:String,default:"label"},valueField:{type:String,default:"value"},value:Array,defaultValue:{type:Array,default:null},disabled:{type:Boolean,default:void 0},"onUpdate:value":[Function,Array],onUpdateValue:[Function,Array],onChange:[Function,Array]};var qe=W({name:"CheckboxGroup",props:je,setup(e){const{mergedClsPrefixRef:c}=O(e),s=Y(e),{mergedSizeRef:i,mergedDisabledRef:b}=s,m=V(e.defaultValue),f=K(()=>e.value),d=Q(f,m),r=K(()=>{var u;return((u=d.value)==null?void 0:u.length)||0}),M=K(()=>Array.isArray(d.value)?new Set(d.value):new Set);function g(u,a){const{nTriggerFormInput:k,nTriggerFormChange:v}=s,{onChange:l,"onUpdate:value":x,onUpdateValue:C}=e;if(Array.isArray(d.value)){const h=Array.from(d.value),P=h.findIndex(U=>U===a);u?~P||(h.push(a),C&&t(C,h,{actionType:"check",value:a}),x&&t(x,h,{actionType:"check",value:a}),k(),v(),m.value=h,l&&t(l,h)):~P&&(h.splice(P,1),C&&t(C,h,{actionType:"uncheck",value:a}),x&&t(x,h,{actionType:"uncheck",value:a}),l&&t(l,h),m.value=h,k(),v())}else u?(C&&t(C,[a],{actionType:"check",value:a}),x&&t(x,[a],{actionType:"check",value:a}),l&&t(l,[a]),m.value=[a],k(),v()):(C&&t(C,[],{actionType:"uncheck",value:a}),x&&t(x,[],{actionType:"uncheck",value:a}),l&&t(l,[]),m.value=[],k(),v())}return Ie(J,{checkedCountRef:r,maxRef:E(e,"max"),minRef:E(e,"min"),valueSetRef:M,disabledRef:b,mergedSizeRef:i,toggleCheckbox:g}),{mergedClsPrefix:c}},render(){const{options:e,labelField:c,valueField:s}=this.$props;return $(),B("div",{class:T(`${this.mergedClsPrefix}-checkbox-group`),role:"group"},[e?($(),B(j,{key:0},[_(()=>e.map(i=>{const b=i[s];return $(),Be(Ge,{key:b,value:b,disabled:i.disabled,label:i[c]},null,8,["value","disabled","label"])}))],64)):($(),B(j,{key:1},[_(()=>{var i,b;return(b=(i=this.$slots).default)==null?void 0:b.call(i)})],64))],2)}});export{qe as C,Ge as a,Pe as c};
