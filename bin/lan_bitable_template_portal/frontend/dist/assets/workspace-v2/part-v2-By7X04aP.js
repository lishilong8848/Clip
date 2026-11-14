import{aN as f,aO as u,bx as w,f1 as d,dw as a,bb as i,dg as s,e8 as v,d9 as l,b8 as N,L as T,df as m,eK as B,ek as E,eZ as V,f2 as k,b4 as y}from"./app-v2-aGtQKw93.js";var F=f("statistic",[u("label",`
 font-weight: var(--n-label-font-weight);
 transition: .3s color var(--n-bezier);
 font-size: var(--n-label-font-size);
 color: var(--n-label-text-color);
 `),f("statistic-value",`
 margin-top: 4px;
 font-weight: var(--n-value-font-weight);
 `,[u("prefix",`
 margin: 0 4px 0 0;
 font-size: var(--n-value-font-size);
 transition: .3s color var(--n-bezier);
 color: var(--n-value-prefix-text-color);
 `,[f("icon",{verticalAlign:"-0.125em"})]),u("content",`
 font-size: var(--n-value-font-size);
 transition: .3s color var(--n-bezier);
 color: var(--n-value-text-color);
 `),u("suffix",`
 margin: 0 0 0 4px;
 font-size: var(--n-value-font-size);
 transition: .3s color var(--n-bezier);
 color: var(--n-value-suffix-text-color);
 `,[f("icon",{verticalAlign:"-0.125em"})])])]);const P={...d.props,tabularNums:Boolean,label:String,value:[String,Number]};var W=w({name:"Statistic",props:P,slots:Object,setup(t){const{mergedClsPrefixRef:n,inlineThemeDisabled:r,mergedRtlRef:b}=B(t),x=d("Statistic","-statistic",F,E,t,n),c=V("Statistic",b,n),e=y(()=>{const{self:{labelFontWeight:g,valueFontSize:p,valueFontWeight:z,valuePrefixTextColor:h,labelTextColor:S,valueSuffixTextColor:_,valueTextColor:C,labelFontSize:R},common:{cubicBezierEaseInOut:$}}=x.value;return{"--n-bezier":$,"--n-label-font-size":R,"--n-label-font-weight":g,"--n-label-text-color":S,"--n-value-font-weight":z,"--n-value-font-size":p,"--n-value-prefix-text-color":h,"--n-value-suffix-text-color":_,"--n-value-text-color":C}}),o=r?k("statistic",void 0,e,t):void 0;return{rtlEnabled:c,mergedClsPrefix:n,cssVars:r?void 0:e,themeClass:o==null?void 0:o.themeClass,onRender:o==null?void 0:o.onRender}},render(){var c;const{mergedClsPrefix:t,$slots:{default:n,label:r,prefix:b,suffix:x}}=this;return(c=this.onRender)==null||c.call(this),a(),i("div",{class:l([`${t}-statistic`,this.themeClass,this.rtlEnabled&&`${t}-statistic--rtl`]),style:m(this.cssVars)},[s(()=>v(r,e=>(a(),i("div",{class:l(`${t}-statistic__label`)},[s(()=>this.label||e)],2)))),N("div",{class:l(`${t}-statistic-value`),style:m({fontVariantNumeric:this.tabularNums?"tabular-nums":""})},[s(()=>v(b,e=>e&&(a(),i("span",{class:l(`${t}-statistic-value__prefix`)},[s(()=>e)],2)))),this.value!==void 0?(a(),i("span",{key:0,class:l(`${t}-statistic-value__content`)},[s(()=>this.value)],2)):(a(),i(T,{key:1},[s(()=>v(n,e=>e&&(a(),i("span",{class:l(`${t}-statistic-value__content`)},[s(()=>e)],2))))],64)),s(()=>v(x,e=>e&&(a(),i("span",{class:l(`${t}-statistic-value__suffix`)},[s(()=>e)],2))))],6)],6)}});export{W as S};
