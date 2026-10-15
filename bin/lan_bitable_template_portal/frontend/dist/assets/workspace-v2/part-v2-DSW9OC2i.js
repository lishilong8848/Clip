import{be as $,ax as v,ay as x,bc as F,dW as d,cI as l,aU as o,cv as s,dd as b,cp as i,aR as w,w as E,cu as m,dE as N,dS as W,dX as y,aN as B}from"./app-v2-BtC_jOn5.js";function P(e){const{textColor2:a,textColor3:n,fontSize:u,fontWeight:r}=e;return{labelFontSize:u,labelFontWeight:r,valueFontWeight:r,valueFontSize:"24px",labelTextColor:n,valuePrefixTextColor:a,valueSuffixTextColor:a,valueTextColor:a}}const V={common:$,self:P};var k=v("statistic",[x("label",`
 font-weight: var(--n-label-font-weight);
 transition: .3s color var(--n-bezier);
 font-size: var(--n-label-font-size);
 color: var(--n-label-text-color);
 `),v("statistic-value",`
 margin-top: 4px;
 font-weight: var(--n-value-font-weight);
 `,[x("prefix",`
 margin: 0 4px 0 0;
 font-size: var(--n-value-font-size);
 transition: .3s color var(--n-bezier);
 color: var(--n-value-prefix-text-color);
 `,[v("icon",{verticalAlign:"-0.125em"})]),x("content",`
 font-size: var(--n-value-font-size);
 transition: .3s color var(--n-bezier);
 color: var(--n-value-text-color);
 `),x("suffix",`
 margin: 0 0 0 4px;
 font-size: var(--n-value-font-size);
 transition: .3s color var(--n-bezier);
 color: var(--n-value-suffix-text-color);
 `,[v("icon",{verticalAlign:"-0.125em"})])])]);const A={...d.props,tabularNums:Boolean,label:String,value:[String,Number]};var O=F({name:"Statistic",props:A,slots:Object,setup(e){const{mergedClsPrefixRef:a,inlineThemeDisabled:n,mergedRtlRef:u}=N(e),r=d("Statistic","-statistic",k,V,e,a),f=W("Statistic",u,a),t=B(()=>{const{self:{labelFontWeight:g,valueFontSize:p,valueFontWeight:z,valuePrefixTextColor:h,labelTextColor:S,valueSuffixTextColor:C,valueTextColor:_,labelFontSize:R},common:{cubicBezierEaseInOut:T}}=r.value;return{"--n-bezier":T,"--n-label-font-size":R,"--n-label-font-weight":g,"--n-label-text-color":S,"--n-value-font-weight":z,"--n-value-font-size":p,"--n-value-prefix-text-color":h,"--n-value-suffix-text-color":C,"--n-value-text-color":_}}),c=n?y("statistic",void 0,t,e):void 0;return{rtlEnabled:f,mergedClsPrefix:a,cssVars:n?void 0:t,themeClass:c==null?void 0:c.themeClass,onRender:c==null?void 0:c.onRender}},render(){var f;const{mergedClsPrefix:e,$slots:{default:a,label:n,prefix:u,suffix:r}}=this;return(f=this.onRender)==null||f.call(this),l(),o("div",{class:i([`${e}-statistic`,this.themeClass,this.rtlEnabled&&`${e}-statistic--rtl`]),style:m(this.cssVars)},[s(()=>b(n,t=>(l(),o("div",{class:i(`${e}-statistic__label`)},[s(()=>this.label||t)],2)))),w("div",{class:i(`${e}-statistic-value`),style:m({fontVariantNumeric:this.tabularNums?"tabular-nums":""})},[s(()=>b(u,t=>t&&(l(),o("span",{class:i(`${e}-statistic-value__prefix`)},[s(()=>t)],2)))),this.value!==void 0?(l(),o("span",{key:0,class:i(`${e}-statistic-value__content`)},[s(()=>this.value)],2)):(l(),o(E,{key:1},[s(()=>b(a,t=>t&&(l(),o("span",{class:i(`${e}-statistic-value__content`)},[s(()=>t)],2))))],64)),s(()=>b(r,t=>t&&(l(),o("span",{class:i(`${e}-statistic-value__suffix`)},[s(()=>t)],2))))],6)],6)}});export{O as S};
