import{aL as u,aM as f,bt as T,eY as d,ds as a,b7 as i,dc as s,e3 as v,d5 as l,b4 as w,J as B,db as m,eF as E,ef as F,eU as N,eZ as V,b0 as y}from"./app-v2-CmDVOMek.js";var P=u("statistic",[f("label",`
 font-weight: var(--n-label-font-weight);
 transition: .3s color var(--n-bezier);
 font-size: var(--n-label-font-size);
 color: var(--n-label-text-color);
 `),u("statistic-value",`
 margin-top: 4px;
 font-weight: var(--n-value-font-weight);
 `,[f("prefix",`
 margin: 0 4px 0 0;
 font-size: var(--n-value-font-size);
 transition: .3s color var(--n-bezier);
 color: var(--n-value-prefix-text-color);
 `,[u("icon",{verticalAlign:"-0.125em"})]),f("content",`
 font-size: var(--n-value-font-size);
 transition: .3s color var(--n-bezier);
 color: var(--n-value-text-color);
 `),f("suffix",`
 margin: 0 0 0 4px;
 font-size: var(--n-value-font-size);
 transition: .3s color var(--n-bezier);
 color: var(--n-value-suffix-text-color);
 `,[u("icon",{verticalAlign:"-0.125em"})])])]);const k={...d.props,tabularNums:Boolean,label:String,value:[String,Number]};var A=T({name:"Statistic",props:k,slots:Object,setup(t){const{mergedClsPrefixRef:n,inlineThemeDisabled:r,mergedRtlRef:b}=E(t),x=d("Statistic","-statistic",P,F,t,n),c=N("Statistic",b,n),e=y(()=>{const{self:{labelFontWeight:p,valueFontSize:g,valueFontWeight:z,valuePrefixTextColor:h,labelTextColor:S,valueSuffixTextColor:_,valueTextColor:C,labelFontSize:R},common:{cubicBezierEaseInOut:$}}=x.value;return{"--n-bezier":$,"--n-label-font-size":R,"--n-label-font-weight":p,"--n-label-text-color":S,"--n-value-font-weight":z,"--n-value-font-size":g,"--n-value-prefix-text-color":h,"--n-value-suffix-text-color":_,"--n-value-text-color":C}}),o=r?V("statistic",void 0,e,t):void 0;return{rtlEnabled:c,mergedClsPrefix:n,cssVars:r?void 0:e,themeClass:o==null?void 0:o.themeClass,onRender:o==null?void 0:o.onRender}},render(){var c;const{mergedClsPrefix:t,$slots:{default:n,label:r,prefix:b,suffix:x}}=this;return(c=this.onRender)==null||c.call(this),a(),i("div",{class:l([`${t}-statistic`,this.themeClass,this.rtlEnabled&&`${t}-statistic--rtl`]),style:m(this.cssVars)},[s(()=>v(r,e=>(a(),i("div",{class:l(`${t}-statistic__label`)},[s(()=>this.label||e)],2)))),w("div",{class:l(`${t}-statistic-value`),style:m({fontVariantNumeric:this.tabularNums?"tabular-nums":""})},[s(()=>v(b,e=>e&&(a(),i("span",{class:l(`${t}-statistic-value__prefix`)},[s(()=>e)],2)))),this.value!==void 0?(a(),i("span",{key:0,class:l(`${t}-statistic-value__content`)},[s(()=>this.value)],2)):(a(),i(B,{key:1},[s(()=>v(n,e=>e&&(a(),i("span",{class:l(`${t}-statistic-value__content`)},[s(()=>e)],2))))],64)),s(()=>v(x,e=>e&&(a(),i("span",{class:l(`${t}-statistic-value__suffix`)},[s(()=>e)],2))))],6)],6)}});export{A as S};
