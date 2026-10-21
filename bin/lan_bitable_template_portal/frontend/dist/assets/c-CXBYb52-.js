import{a0 as ce,D as G,v as _,y as x,x as f,u as R,z as L,bb as j,bd as W,bg as O,Q as ue,A as V,bm as U,Y as be,aR as I,aO as B,b1 as he,bj as Y,bn as q,R as T,ae as fe}from"./index-rMV3914F.js";import{J}from"./c-CwDoK4JE.js";import{M as ve,ac as D,au as N,z as Q,a6 as A,s as E,p as K,Y as X,n as P,q as ge,a8 as pe}from"./c-sNzMfqfR.js";var me={radioSizeSmall:"14px",radioSizeMedium:"16px",radioSizeLarge:"18px",labelPadding:"0 8px",labelFontWeight:"400"};function xe(o){const{borderColor:e,primaryColor:t,baseColor:r,textColorDisabled:d,inputColorDisabled:b,textColor2:c,opacityDisabled:l,borderRadius:n,fontSizeSmall:u,fontSizeMedium:v,fontSizeLarge:h,heightSmall:g,heightMedium:C,heightLarge:p,lineHeight:w}=o;return{...me,labelLineHeight:w,buttonHeightSmall:g,buttonHeightMedium:C,buttonHeightLarge:p,fontSizeSmall:u,fontSizeMedium:v,fontSizeLarge:h,boxShadow:`inset 0 0 0 1px ${e}`,boxShadowActive:`inset 0 0 0 1px ${t}`,boxShadowFocus:`inset 0 0 0 1px ${t}, 0 0 0 2px ${G(t,{alpha:.2})}`,boxShadowHover:`inset 0 0 0 1px ${t}`,boxShadowDisabled:`inset 0 0 0 1px ${e}`,color:r,colorDisabled:b,colorActive:"#0000",textColor:c,textColorDisabled:d,dotColorActive:t,dotColorDisabled:e,buttonBorderColor:e,buttonBorderColorActive:t,buttonBorderColorHover:e,buttonColor:r,buttonColorActive:r,buttonTextColor:c,buttonTextColorActive:t,buttonTextColorHover:t,opacityDisabled:l,buttonBoxShadowFocus:`inset 0 0 0 1px ${t}, 0 0 0 2px ${G(t,{alpha:.3})}`,buttonBoxShadowHover:"inset 0 0 0 1px #0000",buttonBoxShadow:"inset 0 0 0 1px #0000",buttonBorderRadius:n}}const Z={name:"Radio",common:ce,self:xe};var Ce=_("radio",`
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
`,[x("checked",[f("dot",`
 background-color: var(--n-color-active);
 `)]),f("dot-wrapper",`
 position: relative;
 flex-shrink: 0;
 flex-grow: 0;
 width: var(--n-radio-size);
 `),_("radio-input",`
 position: absolute;
 border: 0;
 width: 0;
 height: 0;
 opacity: 0;
 margin: 0;
 `),f("dot",`
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
 `,[R("&::before",`
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
 `),x("checked",{boxShadow:"var(--n-box-shadow-active)"},[R("&::before",`
 opacity: 1;
 transform: scale(1);
 `)])]),f("label",`
 color: var(--n-text-color);
 padding: var(--n-label-padding);
 font-weight: var(--n-label-font-weight);
 display: inline-block;
 transition: color .3s var(--n-bezier);
 `),L("disabled",`
 cursor: pointer;
 `,[R("&:hover",[f("dot",{boxShadow:"var(--n-box-shadow-hover)"})]),x("focus",[R("&:not(:active)",[f("dot",{boxShadow:"var(--n-box-shadow-focus)"})])])]),x("disabled",`
 cursor: not-allowed;
 `,[f("dot",{boxShadow:"var(--n-box-shadow-disabled)",backgroundColor:"var(--n-color-disabled)"},[R("&::before",{backgroundColor:"var(--n-dot-color-disabled)"}),x("checked",`
 opacity: 1;
 `)]),f("label",{color:"var(--n-text-color-disabled)"}),_("radio-input",`
 cursor: not-allowed;
 `)])]);const we={name:String,value:{type:[String,Number,Boolean],default:"on"},checked:{type:Boolean,default:void 0},defaultChecked:Boolean,disabled:{type:Boolean,default:void 0},label:String,size:String,onUpdateChecked:[Function,Array],"onUpdate:checked":[Function,Array],checkedValue:{type:Boolean,default:void 0}},ee=ue("n-radio-group");function Re(o){const e=ve(ee,null),{mergedClsPrefixRef:t,mergedComponentPropsRef:r}=j(o),d=W(o,{mergedSize(a){var m,$;const{size:i}=o;if(i!==void 0)return i;if(e){const{mergedSizeRef:{value:F}}=e;if(F!==void 0)return F}if(a)return a.mergedSize.value;const s=($=(m=r==null?void 0:r.value)==null?void 0:m.Radio)==null?void 0:$.size;return s||"medium"},mergedDisabled(a){return!!(o.disabled||e!=null&&e.disabledRef.value||a!=null&&a.disabled.value)}}),{mergedSizeRef:b,mergedDisabledRef:c}=d,l=D(null),n=D(null),u=D(o.defaultChecked),v=N(o,"checked"),h=J(v,u),g=O(()=>e?e.valueRef.value===o.value:h.value),C=O(()=>{const{name:a}=o;if(a!==void 0)return a;if(e)return e.nameRef.value}),p=D(!1);function w(){if(e){const{doUpdateValue:a}=e,{value:i}=o;V(a,i)}else{const{onUpdateChecked:a,"onUpdate:checked":i}=o,{nTriggerFormInput:s,nTriggerFormChange:m}=d;a&&V(a,!0),i&&V(i,!0),s(),m(),u.value=!0}}function z(){c.value||g.value||w()}function k(){z(),l.value&&(l.value.checked=g.value)}function y(){p.value=!1}function S(){p.value=!0}return{mergedClsPrefix:e?e.mergedClsPrefixRef:t,inputRef:l,labelRef:n,mergedName:C,mergedDisabled:c,renderSafeChecked:g,focus:p,mergedSize:b,handleRadioInputChange:k,handleRadioInputBlur:y,handleRadioInputFocus:S}}const Se=["value","name","checked","disabled","onChange","onFocus","onBlur"],ze={...U.props,...we};var ke=Q({name:"Radio",props:ze,setup(o){const e=Re(o),t=U("Radio","-radio",Ce,Z,o,e.mergedClsPrefix),r=P(()=>{const{mergedSize:{value:u}}=e,{common:{cubicBezierEaseInOut:v},self:{boxShadow:h,boxShadowActive:g,boxShadowDisabled:C,boxShadowFocus:p,boxShadowHover:w,color:z,colorDisabled:k,colorActive:y,textColor:S,textColorDisabled:a,dotColorActive:i,dotColorDisabled:s,labelPadding:m,labelLineHeight:$,labelFontWeight:F,[T("fontSize",u)]:H,[T("radioSize",u)]:M}}=t.value;return{"--n-bezier":v,"--n-label-line-height":$,"--n-label-font-weight":F,"--n-box-shadow":h,"--n-box-shadow-active":g,"--n-box-shadow-disabled":C,"--n-box-shadow-focus":p,"--n-box-shadow-hover":w,"--n-color":z,"--n-color-active":y,"--n-color-disabled":k,"--n-dot-color-active":i,"--n-dot-color-disabled":s,"--n-font-size":H,"--n-radio-size":M,"--n-text-color":S,"--n-text-color-disabled":a,"--n-label-padding":m}}),{inlineThemeDisabled:d,mergedClsPrefixRef:b,mergedRtlRef:c}=j(o),l=Y("Radio",c,b),n=d?q("radio",P(()=>e.mergedSize.value[0]),r,o):void 0;return Object.assign(e,{rtlEnabled:l,cssVars:d?void 0:r,themeClass:n==null?void 0:n.themeClass,onRender:n==null?void 0:n.onRender})},render(){const{$slots:o,mergedClsPrefix:e,onRender:t,label:r}=this;return t==null||t(),(()=>{const d=be("f8c6901d8cd45c02");return A(),E("label",{class:B([`${e}-radio`,this.themeClass,this.rtlEnabled&&`${e}-radio--rtl`,this.mergedDisabled&&`${e}-radio--disabled`,this.renderSafeChecked&&`${e}-radio--checked`,this.focus&&`${e}-radio--focus`]),style:X(this.cssVars)},[K("div",{class:B(`${e}-radio__dot-wrapper`)},[d[0]||(d[0]=I(" ",-1)),K("div",{class:B([`${e}-radio__dot`,this.renderSafeChecked&&`${e}-radio__dot--checked`])},null,2),K("input",{ref:"inputRef",type:"radio",class:B(`${e}-radio-input`),value:this.value,name:this.mergedName,checked:this.renderSafeChecked,disabled:this.mergedDisabled,onChange:this.handleRadioInputChange,onFocus:this.handleRadioInputFocus,onBlur:this.handleRadioInputBlur},null,42,Se)],2),I(()=>he(o.default,b=>!b&&!r?null:(A(),E("div",{ref:"labelRef",class:B(`${e}-radio__label`)},[I(()=>b||r)],2))))],6)})()}});function ye(o,e="default",t=[]){const r=o.$slots[e];return r===void 0?t:r()}var Be=_("radio-group",`
 display: inline-block;
 font-size: var(--n-font-size);
`,[f("splitor",`
 display: inline-block;
 vertical-align: bottom;
 width: 1px;
 transition:
 background-color .3s var(--n-bezier),
 opacity .3s var(--n-bezier);
 background: var(--n-button-border-color);
 `,[x("checked",{backgroundColor:"var(--n-button-border-color-active)"}),x("disabled",{opacity:"var(--n-opacity-disabled)"})]),x("button-group",`
 white-space: nowrap;
 height: var(--n-height);
 line-height: var(--n-height);
 `,[_("radio-button",{height:"var(--n-height)",lineHeight:"var(--n-height)"}),f("splitor",{height:"var(--n-height)"})]),_("radio-button",`
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
 `,[_("radio-input",`
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
 `),f("state-border",`
 z-index: 1;
 pointer-events: none;
 position: absolute;
 box-shadow: var(--n-button-box-shadow);
 transition: box-shadow .3s var(--n-bezier);
 left: -1px;
 bottom: -1px;
 right: -1px;
 top: -1px;
 `),R("&:first-child",`
 border-top-left-radius: var(--n-button-border-radius);
 border-bottom-left-radius: var(--n-button-border-radius);
 border-left: 1px solid var(--n-button-border-color);
 `,[f("state-border",`
 border-top-left-radius: var(--n-button-border-radius);
 border-bottom-left-radius: var(--n-button-border-radius);
 `)]),R("&:last-child",`
 border-top-right-radius: var(--n-button-border-radius);
 border-bottom-right-radius: var(--n-button-border-radius);
 border-right: 1px solid var(--n-button-border-color);
 `,[f("state-border",`
 border-top-right-radius: var(--n-button-border-radius);
 border-bottom-right-radius: var(--n-button-border-radius);
 `)]),L("disabled",`
 cursor: pointer;
 `,[R("&:hover",[f("state-border",`
 transition: box-shadow .3s var(--n-bezier);
 box-shadow: var(--n-button-box-shadow-hover);
 `),L("checked",{color:"var(--n-button-text-color-hover)"})]),x("focus",[R("&:not(:active)",[f("state-border",{boxShadow:"var(--n-button-box-shadow-focus)"})])])]),x("checked",`
 background: var(--n-button-color-active);
 color: var(--n-button-text-color-active);
 border-color: var(--n-button-border-color-active);
 `),x("disabled",`
 cursor: not-allowed;
 opacity: var(--n-opacity-disabled);
 `)])]);const _e=["onFocusin","onFocusout"];function $e(o,e,t){var b;const r=[];let d=!1;for(let c=0;c<o.length;++c){const l=o[c],n=(b=l.type)==null?void 0:b.name;n==="RadioButton"&&(d=!0);const u=l.props;if(n!=="RadioButton"){r.push(l);continue}if(c===0)r.push(l);else{const v=r[r.length-1].props,h=e===v.value,g=v.disabled,C=e===u.value,p=u.disabled,w=(h?2:0)+(g?0:1),z=(C?2:0)+(p?0:1),k={[`${t}-radio-group__splitor--disabled`]:g,[`${t}-radio-group__splitor--checked`]:h},y={[`${t}-radio-group__splitor--disabled`]:p,[`${t}-radio-group__splitor--checked`]:C},S=w<z?y:k;r.push((A(),E("div",{key:1,class:B([`${t}-radio-group__splitor`,S])},null,2)),l)}}return{children:r,isButtonGroup:d}}const Fe={...U.props,name:String,options:Array,labelField:{type:String,default:"label"},valueField:{type:String,default:"value"},value:[String,Number,Boolean],defaultValue:{type:[String,Number,Boolean],default:null},size:String,disabled:{type:Boolean,default:void 0},"onUpdate:value":[Function,Array],onUpdateValue:[Function,Array]};var Ie=Q({name:"RadioGroup",props:Fe,setup(o){const e=D(null),{mergedSizeRef:t,mergedDisabledRef:r,nTriggerFormChange:d,nTriggerFormInput:b,nTriggerFormBlur:c,nTriggerFormFocus:l}=W(o),{mergedClsPrefixRef:n,inlineThemeDisabled:u,mergedRtlRef:v}=j(o),h=U("Radio","-radio-group",Be,Z,o,n),g=D(o.defaultValue),C=N(o,"value"),p=J(C,g);function w(i){const{onUpdateValue:s,"onUpdate:value":m}=o;s&&V(s,i),m&&V(m,i),g.value=i,d(),b()}function z(i){const{value:s}=e;s&&(s.contains(i.relatedTarget)||l())}function k(i){const{value:s}=e;s&&(s.contains(i.relatedTarget)||c())}pe(ee,{mergedClsPrefixRef:n,nameRef:N(o,"name"),valueRef:p,disabledRef:r,mergedSizeRef:t,doUpdateValue:w});const y=Y("Radio",v,n),S=P(()=>{const{value:i}=t,{common:{cubicBezierEaseInOut:s},self:{buttonBorderColor:m,buttonBorderColorActive:$,buttonBorderRadius:F,buttonBoxShadow:H,buttonBoxShadowFocus:M,buttonBoxShadowHover:oe,buttonColor:te,buttonColorActive:re,buttonTextColor:ae,buttonTextColorActive:ne,buttonTextColorHover:ie,opacityDisabled:de,[T("buttonHeight",i)]:le,[T("fontSize",i)]:se}}=h.value;return{"--n-font-size":se,"--n-bezier":s,"--n-button-border-color":m,"--n-button-border-color-active":$,"--n-button-border-radius":F,"--n-button-box-shadow":H,"--n-button-box-shadow-focus":M,"--n-button-box-shadow-hover":oe,"--n-button-color":te,"--n-button-color-active":re,"--n-button-text-color":ae,"--n-button-text-color-hover":ie,"--n-button-text-color-active":ne,"--n-height":le,"--n-opacity-disabled":de}}),a=u?q("radio-group",P(()=>t.value[0]),S,o):void 0;return{selfElRef:e,rtlEnabled:y,mergedClsPrefix:n,mergedValue:p,handleFocusout:k,handleFocusin:z,cssVars:u?void 0:S,themeClass:a==null?void 0:a.themeClass,onRender:a==null?void 0:a.onRender}},render(){var u;const{mergedValue:o,mergedClsPrefix:e,handleFocusin:t,handleFocusout:r}=this,{options:d,labelField:b,valueField:c}=this.$props,{children:l,isButtonGroup:n}=$e(d?d.map(v=>{const h=v[c];return A(),ge(ke,{key:typeof h=="boolean"?`__n_${h}`:h,value:h,disabled:v.disabled,label:v[b]},null,8,["value","disabled","label"])}):fe(ye(this)),o,e);return(u=this.onRender)==null||u.call(this),A(),E("div",{onFocusin:t,onFocusout:r,ref:"selfElRef",class:B([`${e}-radio-group`,this.rtlEnabled&&`${e}-radio-group--rtl`,this.themeClass,n&&`${e}-radio-group--button-group`]),style:X(this.cssVars)},[I(()=>l)],46,_e)}});function Te(o){if(o.defaultPrevented||o.button!==0||o.ctrlKey||o.metaKey||o.altKey||o.shiftKey)return!1;const e=o.target,t=o.currentTarget;if(!(e instanceof Element)||!(t instanceof Element)||e.closest('button,a,input,select,textarea,label,summary,[role="button"],[role="combobox"],[contenteditable="true"]'))return!1;const r=window.getSelection();return!(r!=null&&r.toString())||!t.contains(r.anchorNode)&&!t.contains(r.focusNode)}export{Ie as R,ke as a,Te as b,Z as c,we as r,Re as s};
