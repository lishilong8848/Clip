import{a0 as Q,v as x,y as g,x as i,u as D,z as G,bm as M,aR as b,aO as v,bb as U,bj as V,bn as H,Q as J,A as R,aP as Y,F as X,bc as Z,b0 as F,aQ as ee,P as re,bg as ae,b6 as te,d as oe,b2 as le}from"./index-CJti7frR.js";import{C as ie,a as se}from"./c-CgrSdP_U.js";import{f as ne}from"./c-BrnX_UiW.js";import{J as de,v as B}from"./c-DmWNkCbO.js";import{z as k,a6 as h,s as _,Y as ce,ad as pe,n as P,a8 as me,q as z,p as A,aL as fe,aG as ue,av as W,M as xe}from"./c-1SqeWcLE.js";function he(e){const{fontWeight:s,textColor1:o,textColor2:a,textColorDisabled:d,dividerColor:r,fontSize:n}=e;return{titleFontSize:n,titleFontWeight:s,dividerColor:r,titleTextColor:o,titleTextColorDisabled:d,fontSize:n,textColor:a,arrowColor:a,arrowColorDisabled:d,itemMargin:"16px 0 0 0",titlePadding:"16px 0 0 0"}}const ge={common:Q,self:he};var ve=x("collapse","width: 100%;",[x("collapse-item",`
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
 `)])]),x("collapse-item","margin-left: 32px;"),D("&:first-child","margin-top: 0;"),D("&:first-child >",[i("header","padding-top: 0;")]),g("left-arrow-placement",[i("header",[x("collapse-item-arrow","margin-right: 4px;")])]),g("right-arrow-placement",[i("header",[x("collapse-item-arrow","margin-left: 4px;")])]),i("content-wrapper",[i("content-inner","padding-top: 16px;"),ne({duration:"0.15s"})]),g("active",[i("header",[g("active",[x("collapse-item-arrow","transform: rotate(90deg);")])])]),D("&:not(:first-child)","border-top: 1px solid var(--n-divider-color);"),G("disabled",[g("trigger-area-main",[i("header",[i("header-main","cursor: pointer;"),x("collapse-item-arrow","cursor: default;")])]),g("trigger-area-arrow",[i("header",[x("collapse-item-arrow","cursor: pointer;")])]),g("trigger-area-extra",[i("header",[i("header-extra","cursor: pointer;")])])]),i("header",`
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
 `)])])]);const Ce={...M.props,defaultExpandedNames:{type:[Array,String],default:null},expandedNames:[Array,String],arrowPlacement:{type:String,default:"left"},accordion:Boolean,displayDirective:{type:String,default:"if"},triggerAreas:{type:Array,default:()=>["main","extra","arrow"]},onItemHeaderClick:[Function,Array],"onUpdate:expandedNames":[Function,Array],onUpdateExpandedNames:[Function,Array],onExpandedNamesChange:{type:[Function,Array],validator:()=>!0,default:void 0}},j=J("n-collapse");var Re=k({name:"Collapse",props:Ce,slots:Object,setup(e,{slots:s}){const{mergedClsPrefixRef:o,inlineThemeDisabled:a,mergedRtlRef:d}=U(e),r=pe(e.defaultExpandedNames),n=P(()=>e.expandedNames),p=de(n,r),C=M("Collapse","-collapse",ve,ge,e,o);function m(f){const{"onUpdate:expandedNames":l,onUpdateExpandedNames:u,onExpandedNamesChange:N}=e;u&&R(u,f),l&&R(l,f),N&&R(N,f),r.value=f}function t(f){const{onItemHeaderClick:l}=e;l&&R(l,f)}function c(f,l,u){const{accordion:N}=e,{value:S}=p;if(N)f?(m([l]),t({name:l,expanded:!0,event:u})):(m([]),t({name:l,expanded:!1,event:u}));else if(!Array.isArray(S))m([l]),t({name:l,expanded:!0,event:u});else{const w=S.slice(),E=w.findIndex($=>l===$);~E?(w.splice(E,1),m(w),t({name:l,expanded:!1,event:u})):(w.push(l),m(w),t({name:l,expanded:!0,event:u}))}}me(j,{props:e,mergedClsPrefixRef:o,expandedNamesRef:p,slots:s,toggleItem:c});const I=V("Collapse",d,o),T=P(()=>{const{common:{cubicBezierEaseInOut:f},self:{titleFontWeight:l,dividerColor:u,titlePadding:N,titleTextColor:S,titleTextColorDisabled:w,textColor:E,arrowColor:$,fontSize:q,titleFontSize:L,arrowColorDisabled:O,itemMargin:K}}=C.value;return{"--n-font-size":q,"--n-bezier":f,"--n-text-color":E,"--n-divider-color":u,"--n-title-padding":N,"--n-title-font-size":L,"--n-title-text-color":S,"--n-title-text-color-disabled":w,"--n-title-font-weight":l,"--n-arrow-color":$,"--n-arrow-color-disabled":O,"--n-item-margin":K}}),y=a?H("collapse",void 0,T,e):void 0;return{rtlEnabled:I,mergedTheme:C,mergedClsPrefix:o,cssVars:a?void 0:T,themeClass:y==null?void 0:y.themeClass,onRender:y==null?void 0:y.onRender}},render(){var e;return(e=this.onRender)==null||e.call(this),h(),_("div",{class:v([`${this.mergedClsPrefix}-collapse`,this.rtlEnabled&&`${this.mergedClsPrefix}-collapse--rtl`,this.themeClass]),style:ce(this.cssVars)},[b(()=>{var s,o;return(o=(s=this.$slots).default)==null?void 0:o.call(s)})],6)}}),we=k({name:"CollapseItemContent",props:{displayDirective:{type:String,required:!0},show:Boolean,clsPrefix:{type:String,required:!0}},setup(e){return{onceTrue:Z(W(e,"show"))}},render(){return h(),z(X,null,{_:1,default:Y(()=>{const{show:e,displayDirective:s,onceTrue:o,clsPrefix:a}=this,d=s==="show"&&o,r=(h(),_("div",{class:v(`${a}-collapse-item__content-wrapper`)},[A("div",{class:v(`${a}-collapse-item__content-inner`)},[b(()=>{var n,p;return(p=(n=this.$slots).default)==null?void 0:p.call(n)})],2)],2));return d?fe(r,[[ue,e]]):e?r:null})})}});const be=["onClick"],ye=["onClick"],Ne={title:String,name:[String,Number],disabled:Boolean,displayDirective:String};var Ie=k({name:"CollapseItem",props:Ne,setup(e){const{mergedRtlRef:s}=U(e),o=re(),a=ae(()=>e.name??o),d=xe(j);d||te("collapse-item","`n-collapse-item` must be placed inside `n-collapse`.");const{expandedNamesRef:r,props:n,mergedClsPrefixRef:p,slots:C}=d,m=P(()=>{const{value:t}=r;if(Array.isArray(t)){const{value:c}=a;return!~t.findIndex(I=>I===c)}else if(t){const{value:c}=a;return c!==t}return!0});return{rtlEnabled:V("Collapse",s,p),collapseSlots:C,randomName:o,mergedClsPrefix:p,collapsed:m,triggerAreas:W(n,"triggerAreas"),mergedDisplayDirective:P(()=>{const{displayDirective:t}=e;return t||n.displayDirective}),arrowPlacement:P(()=>n.arrowPlacement),handleClick(t){let c="main";B(t,"arrow")&&(c="arrow"),B(t,"extra")&&(c="extra"),n.triggerAreas.includes(c)&&d&&!e.disabled&&d.toggleItem(m.value,a.value,t)}}},render(){const{collapseSlots:e,$slots:s,arrowPlacement:o,collapsed:a,mergedDisplayDirective:d,mergedClsPrefix:r,disabled:n,triggerAreas:p}=this,C=F(s.header,{collapsed:a},()=>[this.title]),m=s["header-extra"]||e["header-extra"],t=s.arrow||e.arrow;return h(),_("div",{class:v([`${r}-collapse-item`,`${r}-collapse-item--${o}-arrow-placement`,n&&`${r}-collapse-item--disabled`,!a&&`${r}-collapse-item--active`,p.map(c=>`${r}-collapse-item--trigger-area-${c}`)])},[A("div",{class:v([`${r}-collapse-item__header`,!a&&`${r}-collapse-item__header--active`])},[A("div",{class:v(`${r}-collapse-item__header-main`),onClick:this.handleClick},[b(()=>o==="right"&&C),(h(),_("div",{class:v(`${r}-collapse-item-arrow`),key:this.rtlEnabled?0:1,"data-arrow":!0},[b(()=>F(t,{collapsed:a},()=>[(h(),z(oe,{clsPrefix:r},{default:()=>this.rtlEnabled?(h(),z(ie,{key:1})):(h(),z(se,{key:2}))},1032,["clsPrefix"]))]))],2)),b(()=>o==="left"&&C)],10,ye),b(()=>le(m,{collapsed:a},c=>(h(),_("div",{class:v(`${r}-collapse-item__header-extra`),onClick:this.handleClick,"data-extra":!0},[b(()=>c)],10,be))))],2),(h(),z(we,{clsPrefix:r,displayDirective:d,show:!a},ee(s),1032,["clsPrefix","displayDirective","show"]))],2)}});export{Ie as C,Re as a};
