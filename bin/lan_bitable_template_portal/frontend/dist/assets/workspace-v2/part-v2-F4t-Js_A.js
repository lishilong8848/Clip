import{aQ as f,aR as u,bz as T,f7 as d,dD as a,bd as i,di as s,ee as v,db as l,ba as w,M as B,dh as m,eQ as E,eq as N,f3 as V,f8 as y,b6 as F}from"./app-v2-B9s2VYf-.js";var P=f("statistic",[u("label",`
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
 `,[f("icon",{verticalAlign:"-0.125em"})])])]);const k={...d.props,tabularNums:Boolean,label:String,value:[String,Number]};var A=T({name:"Statistic",props:k,slots:Object,setup(t){const{mergedClsPrefixRef:n,inlineThemeDisabled:r,mergedRtlRef:b}=E(t),x=d("Statistic","-statistic",P,N,t,n),c=V("Statistic",b,n),e=F(()=>{const{self:{labelFontWeight:p,valueFontSize:g,valueFontWeight:z,valuePrefixTextColor:h,labelTextColor:S,valueSuffixTextColor:_,valueTextColor:C,labelFontSize:R},common:{cubicBezierEaseInOut:$}}=x.value;return{"--n-bezier":$,"--n-label-font-size":R,"--n-label-font-weight":p,"--n-label-text-color":S,"--n-value-font-weight":z,"--n-value-font-size":g,"--n-value-prefix-text-color":h,"--n-value-suffix-text-color":_,"--n-value-text-color":C}}),o=r?y("statistic",void 0,e,t):void 0;return{rtlEnabled:c,mergedClsPrefix:n,cssVars:r?void 0:e,themeClass:o==null?void 0:o.themeClass,onRender:o==null?void 0:o.onRender}},render(){var c;const{mergedClsPrefix:t,$slots:{default:n,label:r,prefix:b,suffix:x}}=this;return(c=this.onRender)==null||c.call(this),a(),i("div",{class:l([`${t}-statistic`,this.themeClass,this.rtlEnabled&&`${t}-statistic--rtl`]),style:m(this.cssVars)},[s(()=>v(r,e=>(a(),i("div",{class:l(`${t}-statistic__label`)},[s(()=>this.label||e)],2)))),w("div",{class:l(`${t}-statistic-value`),style:m({fontVariantNumeric:this.tabularNums?"tabular-nums":""})},[s(()=>v(b,e=>e&&(a(),i("span",{class:l(`${t}-statistic-value__prefix`)},[s(()=>e)],2)))),this.value!==void 0?(a(),i("span",{key:0,class:l(`${t}-statistic-value__content`)},[s(()=>this.value)],2)):(a(),i(B,{key:1},[s(()=>v(n,e=>e&&(a(),i("span",{class:l(`${t}-statistic-value__content`)},[s(()=>e)],2))))],64)),s(()=>v(x,e=>e&&(a(),i("span",{class:l(`${t}-statistic-value__suffix`)},[s(()=>e)],2))))],6)],6)}});export{A as S};
