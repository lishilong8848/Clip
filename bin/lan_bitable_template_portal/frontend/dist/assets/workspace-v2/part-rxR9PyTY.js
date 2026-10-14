import{b5 as xe,aA as me,a$ as L,aL as A,as as p,au as n,aw as M,av as T,bv as ge,by as pe,bz as Ce,b3 as O,du as W,cT as ye,cs as $,aO as B,ce as F,c7 as D,a_ as ze,I as we,cj as Re,cd as Se,bw as Te,df as q,cD as V,dp as Q,dm as De,dh as Y,dq as $e,dv as _e,aH as K,aP as Me,d5 as H,aR as j,ay as t,aQ as Fe,t as G,aM as Be,cy as Ae}from"./app-CYHGN6cH.js";var Ie={sizeSmall:"14px",sizeMedium:"16px",sizeLarge:"18px",labelPadding:"0 8px",labelFontWeight:"400"};function Pe(e){const{baseColor:c,inputColorDisabled:s,cardColor:i,modalColor:b,popoverColor:m,textColorDisabled:f,borderColor:d,primaryColor:r,textColor2:_,fontSizeSmall:g,fontSizeMedium:u,fontSizeLarge:a,borderRadiusSmall:k,lineHeight:v}=e;return{...Ie,labelLineHeight:v,fontSizeSmall:g,fontSizeMedium:u,fontSizeLarge:a,borderRadius:k,color:c,colorChecked:r,colorDisabled:s,colorDisabledChecked:s,colorTableHeader:i,colorTableHeaderModal:b,colorTableHeaderPopover:m,checkMarkColor:c,checkMarkColorDisabled:f,checkMarkColorDisabledChecked:f,border:`1px solid ${d}`,borderDisabled:`1px solid ${d}`,borderDisabledChecked:`1px solid ${d}`,borderChecked:`1px solid ${r}`,borderFocus:`1px solid ${r}`,boxShadowFocus:`0 0 0 2px ${me(r,{alpha:.3})}`,textColor:_,textColorDisabled:f}}const Ue={name:"Checkbox",common:xe,self:Pe};var Ke=()=>(()=>{const e=L("75be776d8875fa17");return e[0]||(e[0]=A("svg",{viewBox:"0 0 64 64",class:"check-icon"},[A("path",{d:"M50.42,16.76L22.34,39.45l-8.1-11.46c-1.12-1.58-3.3-1.96-4.88-0.84c-1.58,1.12-1.95,3.3-0.84,4.88l10.26,14.51  c0.56,0.79,1.42,1.31,2.38,1.45c0.16,0.02,0.32,0.03,0.48,0.03c0.8,0,1.57-0.27,2.2-0.78l30.99-25.03c1.5-1.21,1.74-3.42,0.52-4.92  C54.13,15.78,51.93,15.55,50.42,16.76z"})],-1))})(),Ne=()=>(()=>{const e=L("c6eed899356c8404");return e[0]||(e[0]=A("svg",{viewBox:"0 0 100 100",class:"line-icon"},[A("path",{d:"M80.2,55.5H21.4c-2.8,0-5.1-2.5-5.1-5.5l0,0c0-3,2.3-5.5,5.1-5.5h58.7c2.8,0,5.1,2.5,5.1,5.5l0,0C85.2,53.1,82.9,55.5,80.2,55.5z"})],-1))})(),Ve=p([n("checkbox",`
 font-size: var(--n-font-size);
 outline: none;
 cursor: pointer;
 display: inline-flex;
 flex-wrap: nowrap;
 align-items: flex-start;
 word-break: break-word;
 line-height: var(--n-size);
 --n-merged-color-table: var(--n-color-table);
 `,[M("show-label","line-height: var(--n-label-line-height);"),p("&:hover",[n("checkbox-box",[T("border","border: var(--n-border-checked);")])]),p("&:focus:not(:active)",[n("checkbox-box",[T("border",`
 border: var(--n-border-focus);
 box-shadow: var(--n-box-shadow-focus);
 `)])]),M("inside-table",[n("checkbox-box",`
 background-color: var(--n-merged-color-table);
 `)]),M("checked",[n("checkbox-box",`
 background-color: var(--n-color-checked);
 `,[n("checkbox-icon",[p(".check-icon",`
 opacity: 1;
 transform: scale(1);
 `)])])]),M("indeterminate",[n("checkbox-box",[n("checkbox-icon",[p(".check-icon",`
 opacity: 0;
 transform: scale(.5);
 `),p(".line-icon",`
 opacity: 1;
 transform: scale(1);
 `)])])]),M("checked, indeterminate",[p("&:focus:not(:active)",[n("checkbox-box",[T("border",`
 border: var(--n-border-checked);
 box-shadow: var(--n-box-shadow-focus);
 `)])]),n("checkbox-box",`
 background-color: var(--n-color-checked);
 border-left: 0;
 border-top: 0;
 `,[T("border",{border:"var(--n-border-checked)"})])]),M("disabled",{cursor:"not-allowed"},[M("checked",[n("checkbox-box",`
 background-color: var(--n-color-disabled-checked);
 `,[T("border",{border:"var(--n-border-disabled-checked)"}),n("checkbox-icon",[p(".check-icon, .line-icon",{fill:"var(--n-check-mark-color-disabled-checked)"})])])]),n("checkbox-box",`
 background-color: var(--n-color-disabled);
 `,[T("border",`
 border: var(--n-border-disabled);
 `),n("checkbox-icon",[p(".check-icon, .line-icon",`
 fill: var(--n-check-mark-color-disabled);
 `)])]),T("label",`
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
 `,[T("border",`
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
 `),ge({left:"1px",top:"1px"})])]),T("label",`
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
 `))]);const He=["id"],Le=["tabindex","aria-checked","aria-labelledby","onKeyup","onKeydown","onClick"],Ee={...W.props,size:String,checked:{type:[Boolean,String,Number],default:void 0},defaultChecked:{type:[Boolean,String,Number],default:!1},value:[String,Number],disabled:{type:Boolean,default:void 0},indeterminate:Boolean,label:String,focusable:{type:Boolean,default:!0},checkedValue:{type:[Boolean,String,Number],default:!0},uncheckedValue:{type:[Boolean,String,Number],default:!1},"onUpdate:checked":[Function,Array],onUpdateChecked:[Function,Array],privateInsideTable:Boolean,onChange:[Function,Array]};var je=O({name:"Checkbox",props:Ee,setup(e){const c=Te(J,null),s=V(null),{mergedClsPrefixRef:i,inlineThemeDisabled:b,mergedRtlRef:m,mergedComponentPropsRef:f}=q(e),d=V(e.defaultChecked),r=H(e,"checked"),_=Q(r,d),g=De(()=>{if(c){const o=c.valueSetRef.value;return o&&e.value!==void 0?o.has(e.value):!1}else return _.value===e.checkedValue}),u=Y(e,{mergedSize(o){var w,R;const{size:y}=e;if(y!==void 0)return y;if(c){const{value:S}=c.mergedSizeRef;if(S!==void 0)return S}if(o){const{mergedSize:S}=o;if(S!==void 0)return S.value}const z=(R=(w=f==null?void 0:f.value)==null?void 0:w.Checkbox)==null?void 0:R.size;return z||"medium"},mergedDisabled(o){const{disabled:y}=e;if(y!==void 0)return y;if(c){if(c.disabledRef.value)return!0;const{maxRef:{value:z},checkedCountRef:w}=c;if(z!==void 0&&w.value>=z&&!g.value)return!0;const{minRef:{value:R}}=c;if(R!==void 0&&w.value<=R&&g.value)return!0}return o?o.disabled.value:!1}}),{mergedDisabledRef:a,mergedSizeRef:k}=u,v=W("Checkbox","-checkbox",Ve,Ue,e,i);function l(o){if(c&&e.value!==void 0)c.toggleCheckbox(!g.value,e.value);else{const{onChange:y,"onUpdate:checked":z,onUpdateChecked:w}=e,{nTriggerFormInput:R,nTriggerFormChange:S}=u,P=g.value?e.uncheckedValue:e.checkedValue;z&&t(z,P,o),w&&t(w,P,o),y&&t(y,P,o),R(),S(),d.value=P}}function x(o){a.value||l(o)}function C(o){if(!a.value)switch(o.key){case" ":case"Enter":l(o)}}function h(o){switch(o.key){case" ":o.preventDefault()}}const U={focus:()=>{var o;(o=s.value)==null||o.focus()},blur:()=>{var o;(o=s.value)==null||o.blur()}},N=$e("Checkbox",m,i),E=K(()=>{const{value:o}=k,{common:{cubicBezierEaseInOut:y},self:{borderRadius:z,color:w,colorChecked:R,colorDisabled:S,colorTableHeader:P,colorTableHeaderModal:X,colorTableHeaderPopover:Z,checkMarkColor:ee,checkMarkColorDisabled:oe,border:re,borderFocus:ae,borderDisabled:ce,borderChecked:le,boxShadowFocus:ne,textColor:ie,textColorDisabled:de,checkMarkColorDisabledChecked:te,colorDisabledChecked:se,borderDisabledChecked:be,labelPadding:ue,labelLineHeight:he,labelFontWeight:fe,[j("fontSize",o)]:ke,[j("size",o)]:ve}}=v.value;return{"--n-label-line-height":he,"--n-label-font-weight":fe,"--n-size":ve,"--n-bezier":y,"--n-border-radius":z,"--n-border":re,"--n-border-checked":le,"--n-border-focus":ae,"--n-border-disabled":ce,"--n-border-disabled-checked":be,"--n-box-shadow-focus":ne,"--n-color":w,"--n-color-checked":R,"--n-color-table":P,"--n-color-table-modal":X,"--n-color-table-popover":Z,"--n-color-disabled":S,"--n-color-disabled-checked":se,"--n-text-color":ie,"--n-text-color-disabled":de,"--n-check-mark-color":ee,"--n-check-mark-color-disabled":oe,"--n-check-mark-color-disabled-checked":te,"--n-font-size":ke,"--n-label-padding":ue}}),I=b?_e("checkbox",K(()=>k.value[0]),E,e):void 0;return Object.assign(u,U,{rtlEnabled:N,selfRef:s,mergedClsPrefix:i,mergedDisabled:a,renderedChecked:g,mergedTheme:v,labelId:Me(),handleClick:x,handleKeyUp:C,handleKeyDown:h,cssVars:b?void 0:E,themeClass:I==null?void 0:I.themeClass,onRender:I==null?void 0:I.onRender})},render(){var v;const{$slots:e,renderedChecked:c,mergedDisabled:s,indeterminate:i,privateInsideTable:b,cssVars:m,labelId:f,label:d,mergedClsPrefix:r,focusable:_,handleKeyUp:g,handleKeyDown:u,handleClick:a}=this;(v=this.onRender)==null||v.call(this);const k=ye(e.default,l=>d||l?($(),B("span",{key:1,class:D(`${r}-checkbox__label`),id:f},[F(()=>d||l)],10,He)):null);return(()=>{const l=L("70be6e74cd27cb50");return $(),B("div",{ref:"selfRef",class:D([`${r}-checkbox`,this.themeClass,this.rtlEnabled&&`${r}-checkbox--rtl`,c&&`${r}-checkbox--checked`,s&&`${r}-checkbox--disabled`,i&&`${r}-checkbox--indeterminate`,b&&`${r}-checkbox--inside-table`,k&&`${r}-checkbox--show-label`]),tabindex:s||!_?void 0:0,role:"checkbox","aria-checked":i?"mixed":c,"aria-labelledby":f,style:Se(m),onKeyup:g,onKeydown:u,onClick:a,onMousedown:l[0]||(l[0]=()=>{Re("selectstart",window,x=>{x.preventDefault()},{once:!0})})},[A("div",{class:D(`${r}-checkbox-box-wrapper`)},[l[1]||(l[1]=F(" ",-1)),A("div",{class:D(`${r}-checkbox-box`)},[ze(we,null,{default:()=>this.indeterminate?($(),B("div",{key:"indeterminate",class:D(`${r}-checkbox-icon`)},[F(()=>Ne())],2)):($(),B("div",{key:"check",class:D(`${r}-checkbox-icon`)},[F(()=>Ke())],2))},1024),A("div",{class:D(`${r}-checkbox-box__border`)},null,2)],2)],2),F(()=>k)],46,Le)})()}});const J=Fe("n-checkbox-group"),Ge={min:Number,max:Number,size:String,options:Array,labelField:{type:String,default:"label"},valueField:{type:String,default:"value"},value:Array,defaultValue:{type:Array,default:null},disabled:{type:Boolean,default:void 0},"onUpdate:value":[Function,Array],onUpdateValue:[Function,Array],onChange:[Function,Array]};var We=O({name:"CheckboxGroup",props:Ge,setup(e){const{mergedClsPrefixRef:c}=q(e),s=Y(e),{mergedSizeRef:i,mergedDisabledRef:b}=s,m=V(e.defaultValue),f=K(()=>e.value),d=Q(f,m),r=K(()=>{var u;return((u=d.value)==null?void 0:u.length)||0}),_=K(()=>Array.isArray(d.value)?new Set(d.value):new Set);function g(u,a){const{nTriggerFormInput:k,nTriggerFormChange:v}=s,{onChange:l,"onUpdate:value":x,onUpdateValue:C}=e;if(Array.isArray(d.value)){const h=Array.from(d.value),U=h.findIndex(N=>N===a);u?~U||(h.push(a),C&&t(C,h,{actionType:"check",value:a}),x&&t(x,h,{actionType:"check",value:a}),k(),v(),m.value=h,l&&t(l,h)):~U&&(h.splice(U,1),C&&t(C,h,{actionType:"uncheck",value:a}),x&&t(x,h,{actionType:"uncheck",value:a}),l&&t(l,h),m.value=h,k(),v())}else u?(C&&t(C,[a],{actionType:"check",value:a}),x&&t(x,[a],{actionType:"check",value:a}),l&&t(l,[a]),m.value=[a],k(),v()):(C&&t(C,[],{actionType:"uncheck",value:a}),x&&t(x,[],{actionType:"uncheck",value:a}),l&&t(l,[]),m.value=[],k(),v())}return Ae(J,{checkedCountRef:r,maxRef:H(e,"max"),minRef:H(e,"min"),valueSetRef:_,disabledRef:b,mergedSizeRef:i,toggleCheckbox:g}),{mergedClsPrefix:c}},render(){const{options:e,labelField:c,valueField:s}=this.$props;return $(),B("div",{class:D(`${this.mergedClsPrefix}-checkbox-group`),role:"group"},[e?($(),B(G,{key:0},[F(()=>e.map(i=>{const b=i[s];return $(),Be(je,{key:b,value:b,disabled:i.disabled,label:i[c]},null,8,["value","disabled","label"])}))],64)):($(),B(G,{key:1},[F(()=>{var i,b;return(b=(i=this.$slots).default)==null?void 0:b.call(i)})],64))],2)}});export{We as C,je as a,Ue as c};
