import{aF as h,aH as g,aG as s,aD as D,bP as L,aI as G,bm as A,eC as U,dg as x,b1 as E,d1 as y,d0 as J,cW as v,ek as V,dy as Q,ex as X,aV as Y,ds as Z,ey as W,eD as ee,aW as P,b4 as ae,aK as R,a$ as _,c_ as re,a_ as k,eO as te,eI as le,F as oe,ep as se,e7 as M,dR as F,l as ie,H as ne,dT as de,c$ as ce,b2 as pe,ev as me,c6 as fe,e3 as ue,c3 as B}from"./app-v2-B7jB_4JY.js";import{C as he}from"./part-v2-Bj8P1bKc.js";var xe=h("collapse","width: 100%;",[h("collapse-item",`
 font-size: var(--n-font-size);
 color: var(--n-text-color);
 transition:
 color .3s var(--n-bezier),
 border-color .3s var(--n-bezier);
 margin: var(--n-item-margin);
 `,[g("disabled",[s("header","cursor: not-allowed;",[s("header-main",`
 color: var(--n-title-text-color-disabled);
 `),h("collapse-item-arrow",`
 color: var(--n-arrow-color-disabled);
 `)])]),h("collapse-item","margin-left: 32px;"),D("&:first-child","margin-top: 0;"),D("&:first-child >",[s("header","padding-top: 0;")]),g("left-arrow-placement",[s("header",[h("collapse-item-arrow","margin-right: 4px;")])]),g("right-arrow-placement",[s("header",[h("collapse-item-arrow","margin-left: 4px;")])]),s("content-wrapper",[s("content-inner","padding-top: 16px;"),L({duration:"0.15s"})]),g("active",[s("header",[g("active",[h("collapse-item-arrow","transform: rotate(90deg);")])])]),D("&:not(:first-child)","border-top: 1px solid var(--n-divider-color);"),G("disabled",[g("trigger-area-main",[s("header",[s("header-main","cursor: pointer;"),h("collapse-item-arrow","cursor: default;")])]),g("trigger-area-arrow",[s("header",[h("collapse-item-arrow","cursor: pointer;")])]),g("trigger-area-extra",[s("header",[s("header-extra","cursor: pointer;")])])]),s("header",`
 font-size: var(--n-title-font-size);
 display: flex;
 flex-wrap: nowrap;
 align-items: center;
 transition: color .3s var(--n-bezier);
 position: relative;
 padding: var(--n-title-padding);
 color: var(--n-title-text-color);
 `,[s("header-main",`
 display: flex;
 flex-wrap: nowrap;
 align-items: center;
 font-weight: var(--n-title-font-weight);
 transition: color .3s var(--n-bezier);
 flex: 1;
 color: var(--n-title-text-color);
 `),s("header-extra",`
 display: flex;
 align-items: center;
 transition: color .3s var(--n-bezier);
 color: var(--n-text-color);
 `),h("collapse-item-arrow",`
 display: flex;
 transition:
 transform .15s var(--n-bezier),
 color .3s var(--n-bezier);
 font-size: 18px;
 color: var(--n-arrow-color);
 `)])])]);const ge={...U.props,defaultExpandedNames:{type:[Array,String],default:null},expandedNames:[Array,String],arrowPlacement:{type:String,default:"left"},accordion:Boolean,displayDirective:{type:String,default:"if"},triggerAreas:{type:Array,default:()=>["main","extra","arrow"]},onItemHeaderClick:[Function,Array],"onUpdate:expandedNames":[Function,Array],onUpdateExpandedNames:[Function,Array],onExpandedNamesChange:{type:[Function,Array],validator:()=>!0,default:void 0}},j=ae("n-collapse");var _e=A({name:"Collapse",props:ge,slots:Object,setup(e,{slots:i}){const{mergedClsPrefixRef:l,inlineThemeDisabled:o,mergedRtlRef:c}=V(e),a=Q(e.defaultExpandedNames),d=P(()=>e.expandedNames),p=X(d,a),w=U("Collapse","-collapse",xe,Y,e,l);function m(f){const{"onUpdate:expandedNames":t,onUpdateExpandedNames:u,onExpandedNamesChange:N}=e;u&&R(u,f),t&&R(t,f),N&&R(N,f),a.value=f}function r(f){const{onItemHeaderClick:t}=e;t&&R(t,f)}function n(f,t,u){const{accordion:N}=e,{value:z}=p;if(N)f?(m([t]),r({name:t,expanded:!0,event:u})):(m([]),r({name:t,expanded:!1,event:u}));else if(!Array.isArray(z))m([t]),r({name:t,expanded:!0,event:u});else{const C=z.slice(),I=C.findIndex($=>t===$);~I?(C.splice(I,1),m(C),r({name:t,expanded:!1,event:u})):(C.push(t),m(C),r({name:t,expanded:!0,event:u}))}}Z(j,{props:e,mergedClsPrefixRef:l,expandedNamesRef:p,slots:i,toggleItem:n});const S=W("Collapse",c,l),T=P(()=>{const{common:{cubicBezierEaseInOut:f},self:{titleFontWeight:t,dividerColor:u,titlePadding:N,titleTextColor:z,titleTextColorDisabled:C,textColor:I,arrowColor:$,fontSize:H,titleFontSize:K,arrowColorDisabled:O,itemMargin:q}}=w.value;return{"--n-font-size":H,"--n-bezier":f,"--n-text-color":I,"--n-divider-color":u,"--n-title-padding":N,"--n-title-font-size":K,"--n-title-text-color":z,"--n-title-text-color-disabled":C,"--n-title-font-weight":t,"--n-arrow-color":$,"--n-arrow-color-disabled":O,"--n-item-margin":q}}),b=o?ee("collapse",void 0,T,e):void 0;return{rtlEnabled:S,mergedTheme:w,mergedClsPrefix:l,cssVars:o?void 0:T,themeClass:b==null?void 0:b.themeClass,onRender:b==null?void 0:b.onRender}},render(){var e;return(e=this.onRender)==null||e.call(this),x(),E("div",{class:v([`${this.mergedClsPrefix}-collapse`,this.rtlEnabled&&`${this.mergedClsPrefix}-collapse--rtl`,this.themeClass]),style:J(this.cssVars)},[y(()=>{var i,l;return(l=(i=this.$slots).default)==null?void 0:l.call(i)})],6)}}),ve=A({name:"CollapseItemContent",props:{displayDirective:{type:String,required:!0},show:Boolean,clsPrefix:{type:String,required:!0}},setup(e){return{onceTrue:se(M(e,"show"))}},render(){return x(),_(oe,null,{_:1,default:re(()=>{const{show:e,displayDirective:i,onceTrue:l,clsPrefix:o}=this,c=i==="show"&&l,a=(x(),E("div",{class:v(`${o}-collapse-item__content-wrapper`)},[k("div",{class:v(`${o}-collapse-item__content-inner`)},[y(()=>{var d,p;return(p=(d=this.$slots).default)==null?void 0:p.call(d)})],2)],2));return c?te(a,[[le,e]]):e?a:null})})}});const we=["onClick"],Ce=["onClick"],ye={title:String,name:[String,Number],disabled:Boolean,displayDirective:String};var Ee=A({name:"CollapseItem",props:ye,setup(e){const{mergedRtlRef:i}=V(e),l=pe(),o=me(()=>e.name??l),c=fe(j);c||ue("collapse-item","`n-collapse-item` must be placed inside `n-collapse`.");const{expandedNamesRef:a,props:d,mergedClsPrefixRef:p,slots:w}=c,m=P(()=>{const{value:r}=a;if(Array.isArray(r)){const{value:n}=o;return!~r.findIndex(S=>S===n)}else if(r){const{value:n}=o;return n!==r}return!0});return{rtlEnabled:W("Collapse",i,p),collapseSlots:w,randomName:l,mergedClsPrefix:p,collapsed:m,triggerAreas:M(d,"triggerAreas"),mergedDisplayDirective:P(()=>{const{displayDirective:r}=e;return r||d.displayDirective}),arrowPlacement:P(()=>d.arrowPlacement),handleClick(r){let n="main";B(r,"arrow")&&(n="arrow"),B(r,"extra")&&(n="extra"),d.triggerAreas.includes(n)&&c&&!e.disabled&&c.toggleItem(m.value,o.value,r)}}},render(){const{collapseSlots:e,$slots:i,arrowPlacement:l,collapsed:o,mergedDisplayDirective:c,mergedClsPrefix:a,disabled:d,triggerAreas:p}=this,w=F(i.header,{collapsed:o},()=>[this.title]),m=i["header-extra"]||e["header-extra"],r=i.arrow||e.arrow;return x(),E("div",{class:v([`${a}-collapse-item`,`${a}-collapse-item--${l}-arrow-placement`,d&&`${a}-collapse-item--disabled`,!o&&`${a}-collapse-item--active`,p.map(n=>`${a}-collapse-item--trigger-area-${n}`)])},[k("div",{class:v([`${a}-collapse-item__header`,!o&&`${a}-collapse-item__header--active`])},[k("div",{class:v(`${a}-collapse-item__header-main`),onClick:this.handleClick},[y(()=>l==="right"&&w),(x(),E("div",{class:v(`${a}-collapse-item-arrow`),key:this.rtlEnabled?0:1,"data-arrow":!0},[y(()=>F(r,{collapsed:o},()=>[(x(),_(ne,{clsPrefix:a},{default:()=>this.rtlEnabled?(x(),_(he,{key:1})):(x(),_(ie,{key:2}))},1032,["clsPrefix"]))]))],2)),y(()=>l==="left"&&w)],10,Ce),y(()=>de(m,{collapsed:o},n=>(x(),E("div",{class:v(`${a}-collapse-item__header-extra`),onClick:this.handleClick,"data-extra":!0},[y(()=>n)],10,we))))],2),(x(),_(ve,{clsPrefix:a,displayDirective:c,show:!o},ce(i),1032,["clsPrefix","displayDirective","show"]))],2)}});export{Ee as C,_e as a};
