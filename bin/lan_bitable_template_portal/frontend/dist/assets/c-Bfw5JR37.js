import{a0 as Q,u as x,x as g,w as i,t as D,y as H,bl as M,aQ as b,aN as v,ba as U,bi as W,bm as J,Q as X,z as R,aO as G,F as Y,bb as Z,a$ as F,b1 as ee,aP as re,P as ae,bf as te,b5 as oe,d as le}from"./c-ABjiqBg8.js";import{C as ie,a as se}from"./c-Bq3PQMhU.js";import{f as ne}from"./c-un1qwOvr.js";import{J as de,v as B}from"./c-8cEq9ntv.js";import{t as A,W as h,m as _,M as ce,$ as pe,h as P,X as me,k as z,j as k,am as fe,ai as ue,a8 as V,z as xe}from"./c-xWaq51p2.js";function he(e){const{fontWeight:s,textColor1:o,textColor2:a,textColorDisabled:d,dividerColor:r,fontSize:n}=e;return{titleFontSize:n,titleFontWeight:s,dividerColor:r,titleTextColor:o,titleTextColorDisabled:d,fontSize:n,textColor:a,arrowColor:a,arrowColorDisabled:d,itemMargin:"16px 0 0 0",titlePadding:"16px 0 0 0"}}const ge={common:Q,self:he};var ve=x("collapse","width: 100%;",[x("collapse-item",`
 font-size: var(--n-font-size);
 color: var(--n-text-color);
 transition:
 color .3s var(--n-bezier),
 border-color .3s var(--n-bezier);
 margin: var(--n-item-margin);
 `,[g("disabled",[i("header","cursor: not-allowed;",[i("header-main",`
 color: var(--n-title-text-color-disabled);
 `),x("collapse-item-arrow",`
 color: var(--n-arrow-color-disabled);
 `)])]),x("collapse-item","margin-left: 32px;"),D("&:first-child","margin-top: 0;"),D("&:first-child >",[i("header","padding-top: 0;")]),g("left-arrow-placement",[i("header",[x("collapse-item-arrow","margin-right: 4px;")])]),g("right-arrow-placement",[i("header",[x("collapse-item-arrow","margin-left: 4px;")])]),i("content-wrapper",[i("content-inner","padding-top: 16px;"),ne({duration:"0.15s"})]),g("active",[i("header",[g("active",[x("collapse-item-arrow","transform: rotate(90deg);")])])]),D("&:not(:first-child)","border-top: 1px solid var(--n-divider-color);"),H("disabled",[g("trigger-area-main",[i("header",[i("header-main","cursor: pointer;"),x("collapse-item-arrow","cursor: default;")])]),g("trigger-area-arrow",[i("header",[x("collapse-item-arrow","cursor: pointer;")])]),g("trigger-area-extra",[i("header",[i("header-extra","cursor: pointer;")])])]),i("header",`
 font-size: var(--n-title-font-size);
 display: flex;
 flex-wrap: nowrap;
 align-items: center;
 transition: color .3s var(--n-bezier);
 position: relative;
 padding: var(--n-title-padding);
 color: var(--n-title-text-color);
 `,[i("header-main",`
 display: flex;
 flex-wrap: nowrap;
 align-items: center;
 font-weight: var(--n-title-font-weight);
 transition: color .3s var(--n-bezier);
 flex: 1;
 color: var(--n-title-text-color);
 `),i("header-extra",`
 display: flex;
 align-items: center;
 transition: color .3s var(--n-bezier);
 color: var(--n-text-color);
 `),x("collapse-item-arrow",`
 display: flex;
 transition:
 transform .15s var(--n-bezier),
 color .3s var(--n-bezier);
 font-size: 18px;
 color: var(--n-arrow-color);
 `)])])]);const Ce={...M.props,defaultExpandedNames:{type:[Array,String],default:null},expandedNames:[Array,String],arrowPlacement:{type:String,default:"left"},accordion:Boolean,displayDirective:{type:String,default:"if"},triggerAreas:{type:Array,default:()=>["main","extra","arrow"]},onItemHeaderClick:[Function,Array],"onUpdate:expandedNames":[Function,Array],onUpdateExpandedNames:[Function,Array],onExpandedNamesChange:{type:[Function,Array],validator:()=>!0,default:void 0}},j=X("n-collapse");var Re=A({name:"Collapse",props:Ce,slots:Object,setup(e,{slots:s}){const{mergedClsPrefixRef:o,inlineThemeDisabled:a,mergedRtlRef:d}=U(e),r=pe(e.defaultExpandedNames),n=P(()=>e.expandedNames),p=de(n,r),C=M("Collapse","-collapse",ve,ge,e,o);function m(f){const{"onUpdate:expandedNames":l,onUpdateExpandedNames:u,onExpandedNamesChange:N}=e;u&&R(u,f),l&&R(l,f),N&&R(N,f),r.value=f}function t(f){const{onItemHeaderClick:l}=e;l&&R(l,f)}function c(f,l,u){const{accordion:N}=e,{value:S}=p;if(N)f?(m([l]),t({name:l,expanded:!0,event:u})):(m([]),t({name:l,expanded:!1,event:u}));else if(!Array.isArray(S))m([l]),t({name:l,expanded:!0,event:u});else{const w=S.slice(),E=w.findIndex(I=>l===I);~E?(w.splice(E,1),m(w),t({name:l,expanded:!1,event:u})):(w.push(l),m(w),t({name:l,expanded:!0,event:u}))}}me(j,{props:e,mergedClsPrefixRef:o,expandedNamesRef:p,slots:s,toggleItem:c});const $=W("Collapse",d,o),T=P(()=>{const{common:{cubicBezierEaseInOut:f},self:{titleFontWeight:l,dividerColor:u,titlePadding:N,titleTextColor:S,titleTextColorDisabled:w,textColor:E,arrowColor:I,fontSize:O,titleFontSize:q,arrowColorDisabled:K,itemMargin:L}}=C.value;return{"--n-font-size":O,"--n-bezier":f,"--n-text-color":E,"--n-divider-color":u,"--n-title-padding":N,"--n-title-font-size":q,"--n-title-text-color":S,"--n-title-text-color-disabled":w,"--n-title-font-weight":l,"--n-arrow-color":I,"--n-arrow-color-disabled":K,"--n-item-margin":L}}),y=a?J("collapse",void 0,T,e):void 0;return{rtlEnabled:$,mergedTheme:C,mergedClsPrefix:o,cssVars:a?void 0:T,themeClass:y==null?void 0:y.themeClass,onRender:y==null?void 0:y.onRender}},render(){var e;return(e=this.onRender)==null||e.call(this),h(),_("div",{class:v([`${this.mergedClsPrefix}-collapse`,this.rtlEnabled&&`${this.mergedClsPrefix}-collapse--rtl`,this.themeClass]),style:ce(this.cssVars)},[b(()=>{var s,o;return(o=(s=this.$slots).default)==null?void 0:o.call(s)})],6)}}),we=A({name:"CollapseItemContent",props:{displayDirective:{type:String,required:!0},show:Boolean,clsPrefix:{type:String,required:!0}},setup(e){return{onceTrue:Z(V(e,"show"))}},render(){return h(),z(Y,null,{_:1,default:G(()=>{const{show:e,displayDirective:s,onceTrue:o,clsPrefix:a}=this,d=s==="show"&&o,r=(h(),_("div",{class:v(`${a}-collapse-item__content-wrapper`)},[k("div",{class:v(`${a}-collapse-item__content-inner`)},[b(()=>{var n,p;return(p=(n=this.$slots).default)==null?void 0:p.call(n)})],2)],2));return d?fe(r,[[ue,e]]):e?r:null})})}});const be=["onClick"],ye=["onClick"],Ne={title:String,name:[String,Number],disabled:Boolean,displayDirective:String};var $e=A({name:"CollapseItem",props:Ne,setup(e){const{mergedRtlRef:s}=U(e),o=ae(),a=te(()=>e.name??o),d=xe(j);d||oe("collapse-item","`n-collapse-item` must be placed inside `n-collapse`.");const{expandedNamesRef:r,props:n,mergedClsPrefixRef:p,slots:C}=d,m=P(()=>{const{value:t}=r;if(Array.isArray(t)){const{value:c}=a;return!~t.findIndex($=>$===c)}else if(t){const{value:c}=a;return c!==t}return!0});return{rtlEnabled:W("Collapse",s,p),collapseSlots:C,randomName:o,mergedClsPrefix:p,collapsed:m,triggerAreas:V(n,"triggerAreas"),mergedDisplayDirective:P(()=>{const{displayDirective:t}=e;return t||n.displayDirective}),arrowPlacement:P(()=>n.arrowPlacement),handleClick(t){let c="main";B(t,"arrow")&&(c="arrow"),B(t,"extra")&&(c="extra"),n.triggerAreas.includes(c)&&d&&!e.disabled&&d.toggleItem(m.value,a.value,t)}}},render(){const{collapseSlots:e,$slots:s,arrowPlacement:o,collapsed:a,mergedDisplayDirective:d,mergedClsPrefix:r,disabled:n,triggerAreas:p}=this,C=F(s.header,{collapsed:a},()=>[this.title]),m=s["header-extra"]||e["header-extra"],t=s.arrow||e.arrow;return h(),_("div",{class:v([`${r}-collapse-item`,`${r}-collapse-item--${o}-arrow-placement`,n&&`${r}-collapse-item--disabled`,!a&&`${r}-collapse-item--active`,p.map(c=>`${r}-collapse-item--trigger-area-${c}`)])},[k("div",{class:v([`${r}-collapse-item__header`,!a&&`${r}-collapse-item__header--active`])},[k("div",{class:v(`${r}-collapse-item__header-main`),onClick:this.handleClick},[b(()=>o==="right"&&C),(h(),_("div",{class:v(`${r}-collapse-item-arrow`),key:this.rtlEnabled?0:1,"data-arrow":!0},[b(()=>F(t,{collapsed:a},()=>[(h(),z(le,{clsPrefix:r},{default:()=>this.rtlEnabled?(h(),z(ie,{key:1})):(h(),z(se,{key:2}))},1032,["clsPrefix"]))]))],2)),b(()=>o==="left"&&C)],10,ye),b(()=>ee(m,{collapsed:a},c=>(h(),_("div",{class:v(`${r}-collapse-item__header-extra`),onClick:this.handleClick,"data-extra":!0},[b(()=>c)],10,be))))],2),(h(),z(we,{clsPrefix:r,displayDirective:d,show:!a},re(s),1032,["clsPrefix","displayDirective","show"]))],2)}});export{$e as C,Re as a};
