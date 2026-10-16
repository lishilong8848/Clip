import{aD as h,aF as g,aE as s,aB as A,bM as K,aG as Y,bj as k,ez as U,dd as x,a$ as E,c_ as y,cZ as Z,cT as v,eh as M,dv as H,eu as Q,aT as X,dp as J,ev as V,eA as ee,aU as z,b2 as ae,aI as I,aZ as _,cX as re,aY as D,eL as te,eF as le,F as oe,em as se,e4 as j,dO as F,l as ie,G as ne,dQ as de,cY as ce,b0 as pe,es as me,c3 as fe,e0 as ue,c0 as B}from"./app-v2-DQDYIhE1.js";import{C as he}from"./part-v2-CMJA_DGS.js";var xe=h("collapse","width: 100%;",[h("collapse-item",`
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
 `)])]),h("collapse-item","margin-left: 32px;"),A("&:first-child","margin-top: 0;"),A("&:first-child >",[s("header","padding-top: 0;")]),g("left-arrow-placement",[s("header",[h("collapse-item-arrow","margin-right: 4px;")])]),g("right-arrow-placement",[s("header",[h("collapse-item-arrow","margin-left: 4px;")])]),s("content-wrapper",[s("content-inner","padding-top: 16px;"),K({duration:"0.15s"})]),g("active",[s("header",[g("active",[h("collapse-item-arrow","transform: rotate(90deg);")])])]),A("&:not(:first-child)","border-top: 1px solid var(--n-divider-color);"),Y("disabled",[g("trigger-area-main",[s("header",[s("header-main","cursor: pointer;"),h("collapse-item-arrow","cursor: default;")])]),g("trigger-area-arrow",[s("header",[h("collapse-item-arrow","cursor: pointer;")])]),g("trigger-area-extra",[s("header",[s("header-extra","cursor: pointer;")])])]),s("header",`
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
 `)])])]);const ge={...U.props,defaultExpandedNames:{type:[Array,String],default:null},expandedNames:[Array,String],arrowPlacement:{type:String,default:"left"},accordion:Boolean,displayDirective:{type:String,default:"if"},triggerAreas:{type:Array,default:()=>["main","extra","arrow"]},onItemHeaderClick:[Function,Array],"onUpdate:expandedNames":[Function,Array],onUpdateExpandedNames:[Function,Array],onExpandedNamesChange:{type:[Function,Array],validator:()=>!0,default:void 0}},W=ae("n-collapse");var _e=k({name:"Collapse",props:ge,slots:Object,setup(e,{slots:i}){const{mergedClsPrefixRef:l,inlineThemeDisabled:o,mergedRtlRef:c}=M(e),a=H(e.defaultExpandedNames),d=z(()=>e.expandedNames),p=Q(d,a),w=U("Collapse","-collapse",xe,X,e,l);function m(f){const{"onUpdate:expandedNames":t,onUpdateExpandedNames:u,onExpandedNamesChange:N}=e;u&&I(u,f),t&&I(t,f),N&&I(N,f),a.value=f}function r(f){const{onItemHeaderClick:t}=e;t&&I(t,f)}function n(f,t,u){const{accordion:N}=e,{value:P}=p;if(N)f?(m([t]),r({name:t,expanded:!0,event:u})):(m([]),r({name:t,expanded:!1,event:u}));else if(!Array.isArray(P))m([t]),r({name:t,expanded:!0,event:u});else{const C=P.slice(),S=C.findIndex($=>t===$);~S?(C.splice(S,1),m(C),r({name:t,expanded:!1,event:u})):(C.push(t),m(C),r({name:t,expanded:!0,event:u}))}}J(W,{props:e,mergedClsPrefixRef:l,expandedNamesRef:p,slots:i,toggleItem:n});const R=V("Collapse",c,l),T=z(()=>{const{common:{cubicBezierEaseInOut:f},self:{titleFontWeight:t,dividerColor:u,titlePadding:N,titleTextColor:P,titleTextColorDisabled:C,textColor:S,arrowColor:$,fontSize:L,titleFontSize:O,arrowColorDisabled:q,itemMargin:G}}=w.value;return{"--n-font-size":L,"--n-bezier":f,"--n-text-color":S,"--n-divider-color":u,"--n-title-padding":N,"--n-title-font-size":O,"--n-title-text-color":P,"--n-title-text-color-disabled":C,"--n-title-font-weight":t,"--n-arrow-color":$,"--n-arrow-color-disabled":q,"--n-item-margin":G}}),b=o?ee("collapse",void 0,T,e):void 0;return{rtlEnabled:R,mergedTheme:w,mergedClsPrefix:l,cssVars:o?void 0:T,themeClass:b==null?void 0:b.themeClass,onRender:b==null?void 0:b.onRender}},render(){var e;return(e=this.onRender)==null||e.call(this),x(),E("div",{class:v([`${this.mergedClsPrefix}-collapse`,this.rtlEnabled&&`${this.mergedClsPrefix}-collapse--rtl`,this.themeClass]),style:Z(this.cssVars)},[y(()=>{var i,l;return(l=(i=this.$slots).default)==null?void 0:l.call(i)})],6)}}),ve=k({name:"CollapseItemContent",props:{displayDirective:{type:String,required:!0},show:Boolean,clsPrefix:{type:String,required:!0}},setup(e){return{onceTrue:se(j(e,"show"))}},render(){return x(),_(oe,null,{_:1,default:re(()=>{const{show:e,displayDirective:i,onceTrue:l,clsPrefix:o}=this,c=i==="show"&&l,a=(x(),E("div",{class:v(`${o}-collapse-item__content-wrapper`)},[D("div",{class:v(`${o}-collapse-item__content-inner`)},[y(()=>{var d,p;return(p=(d=this.$slots).default)==null?void 0:p.call(d)})],2)],2));return c?te(a,[[le,e]]):e?a:null})})}});const we=["onClick"],Ce=["onClick"],ye={title:String,name:[String,Number],disabled:Boolean,displayDirective:String};var Ee=k({name:"CollapseItem",props:ye,setup(e){const{mergedRtlRef:i}=M(e),l=pe(),o=me(()=>e.name??l),c=fe(W);c||ue("collapse-item","`n-collapse-item` must be placed inside `n-collapse`.");const{expandedNamesRef:a,props:d,mergedClsPrefixRef:p,slots:w}=c,m=z(()=>{const{value:r}=a;if(Array.isArray(r)){const{value:n}=o;return!~r.findIndex(R=>R===n)}else if(r){const{value:n}=o;return n!==r}return!0});return{rtlEnabled:V("Collapse",i,p),collapseSlots:w,randomName:l,mergedClsPrefix:p,collapsed:m,triggerAreas:j(d,"triggerAreas"),mergedDisplayDirective:z(()=>{const{displayDirective:r}=e;return r||d.displayDirective}),arrowPlacement:z(()=>d.arrowPlacement),handleClick(r){let n="main";B(r,"arrow")&&(n="arrow"),B(r,"extra")&&(n="extra"),d.triggerAreas.includes(n)&&c&&!e.disabled&&c.toggleItem(m.value,o.value,r)}}},render(){const{collapseSlots:e,$slots:i,arrowPlacement:l,collapsed:o,mergedDisplayDirective:c,mergedClsPrefix:a,disabled:d,triggerAreas:p}=this,w=F(i.header,{collapsed:o},()=>[this.title]),m=i["header-extra"]||e["header-extra"],r=i.arrow||e.arrow;return x(),E("div",{class:v([`${a}-collapse-item`,`${a}-collapse-item--${l}-arrow-placement`,d&&`${a}-collapse-item--disabled`,!o&&`${a}-collapse-item--active`,p.map(n=>`${a}-collapse-item--trigger-area-${n}`)])},[D("div",{class:v([`${a}-collapse-item__header`,!o&&`${a}-collapse-item__header--active`])},[D("div",{class:v(`${a}-collapse-item__header-main`),onClick:this.handleClick},[y(()=>l==="right"&&w),(x(),E("div",{class:v(`${a}-collapse-item-arrow`),key:this.rtlEnabled?0:1,"data-arrow":!0},[y(()=>F(r,{collapsed:o},()=>[(x(),_(ne,{clsPrefix:a},{default:()=>this.rtlEnabled?(x(),_(he,{key:1})):(x(),_(ie,{key:2}))},1032,["clsPrefix"]))]))],2)),y(()=>l==="left"&&w)],10,Ce),y(()=>de(m,{collapsed:o},n=>(x(),E("div",{class:v(`${a}-collapse-item__header-extra`),onClick:this.handleClick,"data-extra":!0},[y(()=>n)],10,we))))],2),(x(),_(ve,{clsPrefix:a,displayDirective:c,show:!o},ce(i),1032,["clsPrefix","displayDirective","show"]))],2)}});export{Ee as C,_e as a};
