import{b9 as $,as as v,at as x,b7 as F,dR as d,cD as l,aP as o,cq as s,d8 as b,ck as i,aM as N,t as P,cp as m,dz as w,dN as B,dS as E,aI as V}from"./app-DXg8PPQD.js";function W(e){const{textColor2:a,textColor3:n,fontSize:u,fontWeight:r}=e;return{labelFontSize:u,labelFontWeight:r,valueFontWeight:r,valueFontSize:"24px",labelTextColor:n,valuePrefixTextColor:a,valueSuffixTextColor:a,valueTextColor:a}}const k={common:$,self:W};var y=v("statistic",[x("label",`
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
 `,[v("icon",{verticalAlign:"-0.125em"})])])]);const A={...d.props,tabularNums:Boolean,label:String,value:[String,Number]};var I=F({name:"Statistic",props:A,slots:Object,setup(e){const{mergedClsPrefixRef:a,inlineThemeDisabled:n,mergedRtlRef:u}=w(e),r=d("Statistic","-statistic",y,k,e,a),f=B("Statistic",u,a),t=V(()=>{const{self:{labelFontWeight:g,valueFontSize:p,valueFontWeight:z,valuePrefixTextColor:h,labelTextColor:S,valueSuffixTextColor:C,valueTextColor:_,labelFontSize:R},common:{cubicBezierEaseInOut:T}}=r.value;return{"--n-bezier":T,"--n-label-font-size":R,"--n-label-font-weight":g,"--n-label-text-color":S,"--n-value-font-weight":z,"--n-value-font-size":p,"--n-value-prefix-text-color":h,"--n-value-suffix-text-color":C,"--n-value-text-color":_}}),c=n?E("statistic",void 0,t,e):void 0;return{rtlEnabled:f,mergedClsPrefix:a,cssVars:n?void 0:t,themeClass:c==null?void 0:c.themeClass,onRender:c==null?void 0:c.onRender}},render(){var f;const{mergedClsPrefix:e,$slots:{default:a,label:n,prefix:u,suffix:r}}=this;return(f=this.onRender)==null||f.call(this),l(),o("div",{class:i([`${e}-statistic`,this.themeClass,this.rtlEnabled&&`${e}-statistic--rtl`]),style:m(this.cssVars)},[s(()=>b(n,t=>(l(),o("div",{class:i(`${e}-statistic__label`)},[s(()=>this.label||t)],2)))),N("div",{class:i(`${e}-statistic-value`),style:m({fontVariantNumeric:this.tabularNums?"tabular-nums":""})},[s(()=>b(u,t=>t&&(l(),o("span",{class:i(`${e}-statistic-value__prefix`)},[s(()=>t)],2)))),this.value!==void 0?(l(),o("span",{key:0,class:i(`${e}-statistic-value__content`)},[s(()=>this.value)],2)):(l(),o(P,{key:1},[s(()=>b(a,t=>t&&(l(),o("span",{class:i(`${e}-statistic-value__content`)},[s(()=>t)],2))))],64)),s(()=>b(r,t=>t&&(l(),o("span",{class:i(`${e}-statistic-value__suffix`)},[s(()=>t)],2))))],6)],6)}});export{I as S};
