import{aK as h,aM as g,aL as s,aI as A,bX as X,aN as H,bs as k,eW as U,dr as x,b6 as P,db as b,da as G,d4 as v,eD as M,dI as J,eR as Q,a_ as Y,dC as Z,eS as V,eX as ee,a$ as E,b9 as ae,aP as z,b4 as _,d8 as re,b3 as D,fj as te,f1 as le,F as oe,eI as se,em as j,e0 as B,m as ie,K as ne,e2 as de,d9 as ce,b7 as pe,eP as me,cf as fe,eh as ue,cc as F}from"./app-v2-B_JvFXr_.js";import{C as he}from"./part-v2-DJMzM18m.js";var xe=h("collapse","width: 100%;",[h("collapse-item",`
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
 `)])]),h("collapse-item","margin-left: 32px;"),A("&:first-child","margin-top: 0;"),A("&:first-child >",[s("header","padding-top: 0;")]),g("left-arrow-placement",[s("header",[h("collapse-item-arrow","margin-right: 4px;")])]),g("right-arrow-placement",[s("header",[h("collapse-item-arrow","margin-left: 4px;")])]),s("content-wrapper",[s("content-inner","padding-top: 16px;"),X({duration:"0.15s"})]),g("active",[s("header",[g("active",[h("collapse-item-arrow","transform: rotate(90deg);")])])]),A("&:not(:first-child)","border-top: 1px solid var(--n-divider-color);"),H("disabled",[g("trigger-area-main",[s("header",[s("header-main","cursor: pointer;"),h("collapse-item-arrow","cursor: default;")])]),g("trigger-area-arrow",[s("header",[h("collapse-item-arrow","cursor: pointer;")])]),g("trigger-area-extra",[s("header",[s("header-extra","cursor: pointer;")])])]),s("header",`
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
 `)])])]);const ge={...U.props,defaultExpandedNames:{type:[Array,String],default:null},expandedNames:[Array,String],arrowPlacement:{type:String,default:"left"},accordion:Boolean,displayDirective:{type:String,default:"if"},triggerAreas:{type:Array,default:()=>["main","extra","arrow"]},onItemHeaderClick:[Function,Array],"onUpdate:expandedNames":[Function,Array],onUpdateExpandedNames:[Function,Array],onExpandedNamesChange:{type:[Function,Array],validator:()=>!0,default:void 0}},W=ae("n-collapse");var _e=k({name:"Collapse",props:ge,slots:Object,setup(e,{slots:i}){const{mergedClsPrefixRef:l,inlineThemeDisabled:o,mergedRtlRef:c}=M(e),a=J(e.defaultExpandedNames),d=E(()=>e.expandedNames),p=Q(d,a),w=U("Collapse","-collapse",xe,Y,e,l);function m(f){const{"onUpdate:expandedNames":t,onUpdateExpandedNames:u,onExpandedNamesChange:N}=e;u&&z(u,f),t&&z(t,f),N&&z(N,f),a.value=f}function r(f){const{onItemHeaderClick:t}=e;t&&z(t,f)}function n(f,t,u){const{accordion:N}=e,{value:I}=p;if(N)f?(m([t]),r({name:t,expanded:!0,event:u})):(m([]),r({name:t,expanded:!1,event:u}));else if(!Array.isArray(I))m([t]),r({name:t,expanded:!0,event:u});else{const C=I.slice(),S=C.findIndex($=>t===$);~S?(C.splice(S,1),m(C),r({name:t,expanded:!1,event:u})):(C.push(t),m(C),r({name:t,expanded:!0,event:u}))}}Z(W,{props:e,mergedClsPrefixRef:l,expandedNamesRef:p,slots:i,toggleItem:n});const R=V("Collapse",c,l),T=E(()=>{const{common:{cubicBezierEaseInOut:f},self:{titleFontWeight:t,dividerColor:u,titlePadding:N,titleTextColor:I,titleTextColorDisabled:C,textColor:S,arrowColor:$,fontSize:K,titleFontSize:L,arrowColorDisabled:q,itemMargin:O}}=w.value;return{"--n-font-size":K,"--n-bezier":f,"--n-text-color":S,"--n-divider-color":u,"--n-title-padding":N,"--n-title-font-size":L,"--n-title-text-color":I,"--n-title-text-color-disabled":C,"--n-title-font-weight":t,"--n-arrow-color":$,"--n-arrow-color-disabled":q,"--n-item-margin":O}}),y=o?ee("collapse",void 0,T,e):void 0;return{rtlEnabled:R,mergedTheme:w,mergedClsPrefix:l,cssVars:o?void 0:T,themeClass:y==null?void 0:y.themeClass,onRender:y==null?void 0:y.onRender}},render(){var e;return(e=this.onRender)==null||e.call(this),x(),P("div",{class:v([`${this.mergedClsPrefix}-collapse`,this.rtlEnabled&&`${this.mergedClsPrefix}-collapse--rtl`,this.themeClass]),style:G(this.cssVars)},[b(()=>{var i,l;return(l=(i=this.$slots).default)==null?void 0:l.call(i)})],6)}}),ve=k({name:"CollapseItemContent",props:{displayDirective:{type:String,required:!0},show:Boolean,clsPrefix:{type:String,required:!0}},setup(e){return{onceTrue:se(j(e,"show"))}},render(){return x(),_(oe,null,{_:1,default:re(()=>{const{show:e,displayDirective:i,onceTrue:l,clsPrefix:o}=this,c=i==="show"&&l,a=(x(),P("div",{class:v(`${o}-collapse-item__content-wrapper`)},[D("div",{class:v(`${o}-collapse-item__content-inner`)},[b(()=>{var d,p;return(p=(d=this.$slots).default)==null?void 0:p.call(d)})],2)],2));return c?te(a,[[le,e]]):e?a:null})})}});const we=["onClick"],Ce=["onClick"],be={title:String,name:[String,Number],disabled:Boolean,displayDirective:String};var Pe=k({name:"CollapseItem",props:be,setup(e){const{mergedRtlRef:i}=M(e),l=pe(),o=me(()=>e.name??l),c=fe(W);c||ue("collapse-item","`n-collapse-item` must be placed inside `n-collapse`.");const{expandedNamesRef:a,props:d,mergedClsPrefixRef:p,slots:w}=c,m=E(()=>{const{value:r}=a;if(Array.isArray(r)){const{value:n}=o;return!~r.findIndex(R=>R===n)}else if(r){const{value:n}=o;return n!==r}return!0});return{rtlEnabled:V("Collapse",i,p),collapseSlots:w,randomName:l,mergedClsPrefix:p,collapsed:m,triggerAreas:j(d,"triggerAreas"),mergedDisplayDirective:E(()=>{const{displayDirective:r}=e;return r||d.displayDirective}),arrowPlacement:E(()=>d.arrowPlacement),handleClick(r){let n="main";F(r,"arrow")&&(n="arrow"),F(r,"extra")&&(n="extra"),d.triggerAreas.includes(n)&&c&&!e.disabled&&c.toggleItem(m.value,o.value,r)}}},render(){const{collapseSlots:e,$slots:i,arrowPlacement:l,collapsed:o,mergedDisplayDirective:c,mergedClsPrefix:a,disabled:d,triggerAreas:p}=this,w=B(i.header,{collapsed:o},()=>[this.title]),m=i["header-extra"]||e["header-extra"],r=i.arrow||e.arrow;return x(),P("div",{class:v([`${a}-collapse-item`,`${a}-collapse-item--${l}-arrow-placement`,d&&`${a}-collapse-item--disabled`,!o&&`${a}-collapse-item--active`,p.map(n=>`${a}-collapse-item--trigger-area-${n}`)])},[D("div",{class:v([`${a}-collapse-item__header`,!o&&`${a}-collapse-item__header--active`])},[D("div",{class:v(`${a}-collapse-item__header-main`),onClick:this.handleClick},[b(()=>l==="right"&&w),(x(),P("div",{class:v(`${a}-collapse-item-arrow`),key:this.rtlEnabled?0:1,"data-arrow":!0},[b(()=>B(r,{collapsed:o},()=>[(x(),_(ne,{clsPrefix:a},{default:()=>this.rtlEnabled?(x(),_(he,{key:1})):(x(),_(ie,{key:2}))},1032,["clsPrefix"]))]))],2)),b(()=>l==="left"&&w)],10,Ce),b(()=>de(m,{collapsed:o},n=>(x(),P("div",{class:v(`${a}-collapse-item__header-extra`),onClick:this.handleClick,"data-extra":!0},[b(()=>n)],10,we))))],2),(x(),_(ve,{clsPrefix:a,displayDirective:c,show:!o},ce(i),1032,["clsPrefix","displayDirective","show"]))],2)}});export{Pe as C,_e as a};
