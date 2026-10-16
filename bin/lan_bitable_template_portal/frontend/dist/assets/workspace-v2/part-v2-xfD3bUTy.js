import{aD as u,aE as f,bj as T,ez as d,dd as s,a$ as i,c_ as a,dP as v,cT as l,aY as E,z as w,cZ as m,eh as B,dY as N,ev as P,eA as V,aU as y}from"./app-v2-BHCbgX4Y.js";var F=u("statistic",[f("label",`
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
 `,[u("icon",{verticalAlign:"-0.125em"})])])]);const k={...d.props,tabularNums:Boolean,label:String,value:[String,Number]};var W=T({name:"Statistic",props:k,slots:Object,setup(t){const{mergedClsPrefixRef:n,inlineThemeDisabled:r,mergedRtlRef:x}=B(t),b=d("Statistic","-statistic",F,N,t,n),c=P("Statistic",x,n),e=y(()=>{const{self:{labelFontWeight:p,valueFontSize:z,valueFontWeight:g,valuePrefixTextColor:h,labelTextColor:S,valueSuffixTextColor:_,valueTextColor:C,labelFontSize:$},common:{cubicBezierEaseInOut:R}}=b.value;return{"--n-bezier":R,"--n-label-font-size":$,"--n-label-font-weight":p,"--n-label-text-color":S,"--n-value-font-weight":g,"--n-value-font-size":z,"--n-value-prefix-text-color":h,"--n-value-suffix-text-color":_,"--n-value-text-color":C}}),o=r?V("statistic",void 0,e,t):void 0;return{rtlEnabled:c,mergedClsPrefix:n,cssVars:r?void 0:e,themeClass:o==null?void 0:o.themeClass,onRender:o==null?void 0:o.onRender}},render(){var c;const{mergedClsPrefix:t,$slots:{default:n,label:r,prefix:x,suffix:b}}=this;return(c=this.onRender)==null||c.call(this),s(),i("div",{class:l([`${t}-statistic`,this.themeClass,this.rtlEnabled&&`${t}-statistic--rtl`]),style:m(this.cssVars)},[a(()=>v(r,e=>(s(),i("div",{class:l(`${t}-statistic__label`)},[a(()=>this.label||e)],2)))),E("div",{class:l(`${t}-statistic-value`),style:m({fontVariantNumeric:this.tabularNums?"tabular-nums":""})},[a(()=>v(x,e=>e&&(s(),i("span",{class:l(`${t}-statistic-value__prefix`)},[a(()=>e)],2)))),this.value!==void 0?(s(),i("span",{key:0,class:l(`${t}-statistic-value__content`)},[a(()=>this.value)],2)):(s(),i(w,{key:1},[a(()=>v(n,e=>e&&(s(),i("span",{class:l(`${t}-statistic-value__content`)},[a(()=>e)],2))))],64)),a(()=>v(b,e=>e&&(s(),i("span",{class:l(`${t}-statistic-value__suffix`)},[a(()=>e)],2))))],6)],6)}});export{W as S};
