import{az as u,aA as f,bg as T,e2 as d,cN as s,aW as i,cA as a,dl as v,cu as l,aT as w,w as N,cz as m,dM as B,du as E,d_ as P,e3 as V,aP as y}from"./app-v2-dLiaPnr4.js";var F=u("statistic",[f("label",`
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
 `,[u("icon",{verticalAlign:"-0.125em"})])])]);const k={...d.props,tabularNums:Boolean,label:String,value:[String,Number]};var W=T({name:"Statistic",props:k,slots:Object,setup(t){const{mergedClsPrefixRef:n,inlineThemeDisabled:r,mergedRtlRef:x}=B(t),b=d("Statistic","-statistic",F,E,t,n),c=P("Statistic",x,n),e=y(()=>{const{self:{labelFontWeight:g,valueFontSize:p,valueFontWeight:z,valuePrefixTextColor:h,labelTextColor:S,valueSuffixTextColor:_,valueTextColor:C,labelFontSize:R},common:{cubicBezierEaseInOut:$}}=b.value;return{"--n-bezier":$,"--n-label-font-size":R,"--n-label-font-weight":g,"--n-label-text-color":S,"--n-value-font-weight":z,"--n-value-font-size":p,"--n-value-prefix-text-color":h,"--n-value-suffix-text-color":_,"--n-value-text-color":C}}),o=r?V("statistic",void 0,e,t):void 0;return{rtlEnabled:c,mergedClsPrefix:n,cssVars:r?void 0:e,themeClass:o==null?void 0:o.themeClass,onRender:o==null?void 0:o.onRender}},render(){var c;const{mergedClsPrefix:t,$slots:{default:n,label:r,prefix:x,suffix:b}}=this;return(c=this.onRender)==null||c.call(this),s(),i("div",{class:l([`${t}-statistic`,this.themeClass,this.rtlEnabled&&`${t}-statistic--rtl`]),style:m(this.cssVars)},[a(()=>v(r,e=>(s(),i("div",{class:l(`${t}-statistic__label`)},[a(()=>this.label||e)],2)))),w("div",{class:l(`${t}-statistic-value`),style:m({fontVariantNumeric:this.tabularNums?"tabular-nums":""})},[a(()=>v(x,e=>e&&(s(),i("span",{class:l(`${t}-statistic-value__prefix`)},[a(()=>e)],2)))),this.value!==void 0?(s(),i("span",{key:0,class:l(`${t}-statistic-value__content`)},[a(()=>this.value)],2)):(s(),i(N,{key:1},[a(()=>v(n,e=>e&&(s(),i("span",{class:l(`${t}-statistic-value__content`)},[a(()=>e)],2))))],64)),a(()=>v(b,e=>e&&(s(),i("span",{class:l(`${t}-statistic-value__suffix`)},[a(()=>e)],2))))],6)],6)}});export{W as S};
