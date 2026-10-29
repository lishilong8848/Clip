import{a0 as me,D as xe,Y as H,t as p,u as n,x as F,w as D,am as ge,ao as pe,ap as Ce,bl as W,b0 as ye,aQ as M,aN as T,I as ze,aS as we,ba as O,bf as Re,bc as Q,bi as Se,bm as De,P as Te,R as j,z as d,Q as $e}from"./c-CGYYLFaA.js";import{J as Y}from"./c-CvzbGdhy.js";import{j as A,t as J,W as $,m as B,r as _e,M as Fe,z as Me,$ as V,h as U,a8 as E,F as G,k as Be,X as Ae}from"./c-xWaq51p2.js";var Ie={sizeSmall:"14px",sizeMedium:"16px",sizeLarge:"18px",labelPadding:"0 8px",labelFontWeight:"400"};function Pe(e){const{baseColor:c,inputColorDisabled:s,cardColor:i,modalColor:b,popoverColor:x,textColorDisabled:f,borderColor:t,primaryColor:r,textColor2:_,fontSizeSmall:g,fontSizeMedium:u,fontSizeLarge:a,borderRadiusSmall:k,lineHeight:v}=e;return{...Ie,labelLineHeight:v,fontSizeSmall:g,fontSizeMedium:u,fontSizeLarge:a,borderRadius:k,color:c,colorChecked:r,colorDisabled:s,colorDisabledChecked:s,colorTableHeader:i,colorTableHeaderModal:b,colorTableHeaderPopover:x,checkMarkColor:c,checkMarkColorDisabled:f,checkMarkColorDisabledChecked:f,border:`1px solid ${t}`,borderDisabled:`1px solid ${t}`,borderDisabledChecked:`1px solid ${t}`,borderChecked:`1px solid ${r}`,borderFocus:`1px solid ${r}`,boxShadowFocus:`0 0 0 2px ${xe(r,{alpha:.3})}`,textColor:_,textColorDisabled:f}}const Ne={name:"Checkbox",common:me,self:Pe};var Ue=()=>(()=>{const e=H("75be776d8875fa17");return e[0]||(e[0]=A("svg",{viewBox:"0 0 64 64",class:"check-icon"},[A("path",{d:"M50.42,16.76L22.34,39.45l-8.1-11.46c-1.12-1.58-3.3-1.96-4.88-0.84c-1.58,1.12-1.95,3.3-0.84,4.88l10.26,14.51  c0.56,0.79,1.42,1.31,2.38,1.45c0.16,0.02,0.32,0.03,0.48,0.03c0.8,0,1.57-0.27,2.2-0.78l30.99-25.03c1.5-1.21,1.74-3.42,0.52-4.92  C54.13,15.78,51.93,15.55,50.42,16.76z"})],-1))})(),Ke=()=>(()=>{const e=H("c6eed899356c8404");return e[0]||(e[0]=A("svg",{viewBox:"0 0 100 100",class:"line-icon"},[A("path",{d:"M80.2,55.5H21.4c-2.8,0-5.1-2.5-5.1-5.5l0,0c0-3,2.3-5.5,5.1-5.5h58.7c2.8,0,5.1,2.5,5.1,5.5l0,0C85.2,53.1,82.9,55.5,80.2,55.5z"})],-1))})(),Ve=p([n("checkbox",`
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
 `))]);const Ee=["id"],He=["tabindex","aria-checked","aria-labelledby","onKeyup","onKeydown","onClick"],Le={...W.props,size:String,checked:{type:[Boolean,String,Number],default:void 0},defaultChecked:{type:[Boolean,String,Number],default:!1},value:[String,Number],disabled:{type:Boolean,default:void 0},indeterminate:Boolean,label:String,focusable:{type:Boolean,default:!0},checkedValue:{type:[Boolean,String,Number],default:!0},uncheckedValue:{type:[Boolean,String,Number],default:!1},"onUpdate:checked":[Function,Array],onUpdateChecked:[Function,Array],privateInsideTable:Boolean,onChange:[Function,Array]};var je=J({name:"Checkbox",props:Le,setup(e){const c=Me(X,null),s=V(null),{mergedClsPrefixRef:i,inlineThemeDisabled:b,mergedRtlRef:x,mergedComponentPropsRef:f}=O(e),t=V(e.defaultChecked),r=E(e,"checked"),_=Y(r,t),g=Re(()=>{if(c){const o=c.valueSetRef.value;return o&&e.value!==void 0?o.has(e.value):!1}else return _.value===e.checkedValue}),u=Q(e,{mergedSize(o){var w,R;const{size:y}=e;if(y!==void 0)return y;if(c){const{value:S}=c.mergedSizeRef;if(S!==void 0)return S}if(o){const{mergedSize:S}=o;if(S!==void 0)return S.value}const z=(R=(w=f==null?void 0:f.value)==null?void 0:w.Checkbox)==null?void 0:R.size;return z||"medium"},mergedDisabled(o){const{disabled:y}=e;if(y!==void 0)return y;if(c){if(c.disabledRef.value)return!0;const{maxRef:{value:z},checkedCountRef:w}=c;if(z!==void 0&&w.value>=z&&!g.value)return!0;const{minRef:{value:R}}=c;if(R!==void 0&&w.value<=R&&g.value)return!0}return o?o.disabled.value:!1}}),{mergedDisabledRef:a,mergedSizeRef:k}=u,v=W("Checkbox","-checkbox",Ve,Ne,e,i);function l(o){if(c&&e.value!==void 0)c.toggleCheckbox(!g.value,e.value);else{const{onChange:y,"onUpdate:checked":z,onUpdateChecked:w}=e,{nTriggerFormInput:R,nTriggerFormChange:S}=u,P=g.value?e.uncheckedValue:e.checkedValue;z&&d(z,P,o),w&&d(w,P,o),y&&d(y,P,o),R(),S(),t.value=P}}function m(o){a.value||l(o)}function C(o){if(!a.value)switch(o.key){case" ":case"Enter":l(o)}}function h(o){switch(o.key){case" ":o.preventDefault()}}const N={focus:()=>{var o;(o=s.value)==null||o.focus()},blur:()=>{var o;(o=s.value)==null||o.blur()}},K=Se("Checkbox",x,i),L=U(()=>{const{value:o}=k,{common:{cubicBezierEaseInOut:y},self:{borderRadius:z,color:w,colorChecked:R,colorDisabled:S,colorTableHeader:P,colorTableHeaderModal:q,colorTableHeaderPopover:Z,checkMarkColor:ee,checkMarkColorDisabled:oe,border:re,borderFocus:ae,borderDisabled:ce,borderChecked:le,boxShadowFocus:ne,textColor:ie,textColorDisabled:te,checkMarkColorDisabledChecked:de,colorDisabledChecked:se,borderDisabledChecked:be,labelPadding:ue,labelLineHeight:he,labelFontWeight:fe,[j("fontSize",o)]:ke,[j("size",o)]:ve}}=v.value;return{"--n-label-line-height":he,"--n-label-font-weight":fe,"--n-size":ve,"--n-bezier":y,"--n-border-radius":z,"--n-border":re,"--n-border-checked":le,"--n-border-focus":ae,"--n-border-disabled":ce,"--n-border-disabled-checked":be,"--n-box-shadow-focus":ne,"--n-color":w,"--n-color-checked":R,"--n-color-table":P,"--n-color-table-modal":q,"--n-color-table-popover":Z,"--n-color-disabled":S,"--n-color-disabled-checked":se,"--n-text-color":ie,"--n-text-color-disabled":te,"--n-check-mark-color":ee,"--n-check-mark-color-disabled":oe,"--n-check-mark-color-disabled-checked":de,"--n-font-size":ke,"--n-label-padding":ue}}),I=b?De("checkbox",U(()=>k.value[0]),L,e):void 0;return Object.assign(u,N,{rtlEnabled:K,selfRef:s,mergedClsPrefix:i,mergedDisabled:a,renderedChecked:g,mergedTheme:v,labelId:Te(),handleClick:m,handleKeyUp:C,handleKeyDown:h,cssVars:b?void 0:L,themeClass:I==null?void 0:I.themeClass,onRender:I==null?void 0:I.onRender})},render(){var v;const{$slots:e,renderedChecked:c,mergedDisabled:s,indeterminate:i,privateInsideTable:b,cssVars:x,labelId:f,label:t,mergedClsPrefix:r,focusable:_,handleKeyUp:g,handleKeyDown:u,handleClick:a}=this;(v=this.onRender)==null||v.call(this);const k=ye(e.default,l=>t||l?($(),B("span",{key:1,class:T(`${r}-checkbox__label`),id:f},[M(()=>t||l)],10,Ee)):null);return(()=>{const l=H("70be6e74cd27cb50");return $(),B("div",{ref:"selfRef",class:T([`${r}-checkbox`,this.themeClass,this.rtlEnabled&&`${r}-checkbox--rtl`,c&&`${r}-checkbox--checked`,s&&`${r}-checkbox--disabled`,i&&`${r}-checkbox--indeterminate`,b&&`${r}-checkbox--inside-table`,k&&`${r}-checkbox--show-label`]),tabindex:s||!_?void 0:0,role:"checkbox","aria-checked":i?"mixed":c,"aria-labelledby":f,style:Fe(x),onKeyup:g,onKeydown:u,onClick:a,onMousedown:l[0]||(l[0]=()=>{we("selectstart",window,m=>{m.preventDefault()},{once:!0})})},[A("div",{class:T(`${r}-checkbox-box-wrapper`)},[l[1]||(l[1]=M(" ",-1)),A("div",{class:T(`${r}-checkbox-box`)},[_e(ze,null,{default:()=>this.indeterminate?($(),B("div",{key:"indeterminate",class:T(`${r}-checkbox-icon`)},[M(()=>Ke())],2)):($(),B("div",{key:"check",class:T(`${r}-checkbox-icon`)},[M(()=>Ue())],2))},1024),A("div",{class:T(`${r}-checkbox-box__border`)},null,2)],2)],2),M(()=>k)],46,He)})()}});const X=$e("n-checkbox-group"),Ge={min:Number,max:Number,size:String,options:Array,labelField:{type:String,default:"label"},valueField:{type:String,default:"value"},value:Array,defaultValue:{type:Array,default:null},disabled:{type:Boolean,default:void 0},"onUpdate:value":[Function,Array],onUpdateValue:[Function,Array],onChange:[Function,Array]};var Ye=J({name:"CheckboxGroup",props:Ge,setup(e){const{mergedClsPrefixRef:c}=O(e),s=Q(e),{mergedSizeRef:i,mergedDisabledRef:b}=s,x=V(e.defaultValue),f=U(()=>e.value),t=Y(f,x),r=U(()=>{var u;return((u=t.value)==null?void 0:u.length)||0}),_=U(()=>Array.isArray(t.value)?new Set(t.value):new Set);function g(u,a){const{nTriggerFormInput:k,nTriggerFormChange:v}=s,{onChange:l,"onUpdate:value":m,onUpdateValue:C}=e;if(Array.isArray(t.value)){const h=Array.from(t.value),N=h.findIndex(K=>K===a);u?~N||(h.push(a),C&&d(C,h,{actionType:"check",value:a}),m&&d(m,h,{actionType:"check",value:a}),k(),v(),x.value=h,l&&d(l,h)):~N&&(h.splice(N,1),C&&d(C,h,{actionType:"uncheck",value:a}),m&&d(m,h,{actionType:"uncheck",value:a}),l&&d(l,h),x.value=h,k(),v())}else u?(C&&d(C,[a],{actionType:"check",value:a}),m&&d(m,[a],{actionType:"check",value:a}),l&&d(l,[a]),x.value=[a],k(),v()):(C&&d(C,[],{actionType:"uncheck",value:a}),m&&d(m,[],{actionType:"uncheck",value:a}),l&&d(l,[]),x.value=[],k(),v())}return Ae(X,{checkedCountRef:r,maxRef:E(e,"max"),minRef:E(e,"min"),valueSetRef:_,disabledRef:b,mergedSizeRef:i,toggleCheckbox:g}),{mergedClsPrefix:c}},render(){const{options:e,labelField:c,valueField:s}=this.$props;return $(),B("div",{class:T(`${this.mergedClsPrefix}-checkbox-group`),role:"group"},[e?($(),B(G,{key:0},[M(()=>e.map(i=>{const b=i[s];return $(),Be(je,{key:b,value:b,disabled:i.disabled,label:i[c]},null,8,["value","disabled","label"])}))],64)):($(),B(G,{key:1},[M(()=>{var i,b;return(b=(i=this.$slots).default)==null?void 0:b.call(i)})],64))],2)}});export{Ye as C,je as a,Ne as c};
