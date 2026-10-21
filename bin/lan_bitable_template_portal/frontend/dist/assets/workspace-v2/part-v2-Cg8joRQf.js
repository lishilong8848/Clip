import{aJ as u,aK as f,br as T,eV as d,dq as s,b5 as i,da as a,e0 as v,d3 as l,b2 as V,H as w,d9 as m,eC as B,ec as E,eR as N,eW as y,a_ as F}from"./app-v2-pwH5Vm7e.js";var P=u("statistic",[f("label",`
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
 `,[u("icon",{verticalAlign:"-0.125em"})])])]);const k={...d.props,tabularNums:Boolean,label:String,value:[String,Number]};var A=T({name:"Statistic",props:k,slots:Object,setup(t){const{mergedClsPrefixRef:n,inlineThemeDisabled:r,mergedRtlRef:x}=B(t),b=d("Statistic","-statistic",P,E,t,n),c=N("Statistic",x,n),e=F(()=>{const{self:{labelFontWeight:p,valueFontSize:g,valueFontWeight:z,valuePrefixTextColor:h,labelTextColor:S,valueSuffixTextColor:_,valueTextColor:C,labelFontSize:R},common:{cubicBezierEaseInOut:$}}=b.value;return{"--n-bezier":$,"--n-label-font-size":R,"--n-label-font-weight":p,"--n-label-text-color":S,"--n-value-font-weight":z,"--n-value-font-size":g,"--n-value-prefix-text-color":h,"--n-value-suffix-text-color":_,"--n-value-text-color":C}}),o=r?y("statistic",void 0,e,t):void 0;return{rtlEnabled:c,mergedClsPrefix:n,cssVars:r?void 0:e,themeClass:o==null?void 0:o.themeClass,onRender:o==null?void 0:o.onRender}},render(){var c;const{mergedClsPrefix:t,$slots:{default:n,label:r,prefix:x,suffix:b}}=this;return(c=this.onRender)==null||c.call(this),s(),i("div",{class:l([`${t}-statistic`,this.themeClass,this.rtlEnabled&&`${t}-statistic--rtl`]),style:m(this.cssVars)},[a(()=>v(r,e=>(s(),i("div",{class:l(`${t}-statistic__label`)},[a(()=>this.label||e)],2)))),V("div",{class:l(`${t}-statistic-value`),style:m({fontVariantNumeric:this.tabularNums?"tabular-nums":""})},[a(()=>v(x,e=>e&&(s(),i("span",{class:l(`${t}-statistic-value__prefix`)},[a(()=>e)],2)))),this.value!==void 0?(s(),i("span",{key:0,class:l(`${t}-statistic-value__content`)},[a(()=>this.value)],2)):(s(),i(w,{key:1},[a(()=>v(n,e=>e&&(s(),i("span",{class:l(`${t}-statistic-value__content`)},[a(()=>e)],2))))],64)),a(()=>v(b,e=>e&&(s(),i("span",{class:l(`${t}-statistic-value__suffix`)},[a(()=>e)],2))))],6)],6)}});export{A as S};
