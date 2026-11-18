import{aQ as h,aS as x,aR as s,aO as A,c2 as Q,aT as H,bz as k,f7 as F,dD as g,bd as z,di as b,dh as G,db as v,eQ as U,dV as J,f2 as X,b5 as Y,dO as Z,f3 as j,f8 as ee,b6 as E,bg as ae,aV as R,bb as _,df as re,ba as D,fv as te,fe as le,I as oe,eV as se,ey as M,ed as B,p as ie,O as ne,ef as de,dg as ce,be as pe,f0 as me,cm as fe,et as ue,cj as V}from"./app-v2-HQva9qaW.js";import{C as he}from"./part-v2-B-h8rvpE.js";var ge=h("collapse","width: 100%;",[h("collapse-item",`
 font-size: var(--n-font-size);
 color: var(--n-text-color);
 transition:
 color .3s var(--n-bezier),
 border-color .3s var(--n-bezier);
 margin: var(--n-item-margin);
 `,[x("disabled",[s("header","cursor: not-allowed;",[s("header-main",`
 color: var(--n-title-text-color-disabled);
 `),h("collapse-item-arrow",`
 color: var(--n-arrow-color-disabled);
 `)])]),h("collapse-item","margin-left: 32px;"),A("&:first-child","margin-top: 0;"),A("&:first-child >",[s("header","padding-top: 0;")]),x("left-arrow-placement",[s("header",[h("collapse-item-arrow","margin-right: 4px;")])]),x("right-arrow-placement",[s("header",[h("collapse-item-arrow","margin-left: 4px;")])]),s("content-wrapper",[s("content-inner","padding-top: 16px;"),Q({duration:"0.15s"})]),x("active",[s("header",[x("active",[h("collapse-item-arrow","transform: rotate(90deg);")])])]),A("&:not(:first-child)","border-top: 1px solid var(--n-divider-color);"),H("disabled",[x("trigger-area-main",[s("header",[s("header-main","cursor: pointer;"),h("collapse-item-arrow","cursor: default;")])]),x("trigger-area-arrow",[s("header",[h("collapse-item-arrow","cursor: pointer;")])]),x("trigger-area-extra",[s("header",[s("header-extra","cursor: pointer;")])])]),s("header",`
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
 `)])])]);const xe={...F.props,defaultExpandedNames:{type:[Array,String],default:null},expandedNames:[Array,String],arrowPlacement:{type:String,default:"left"},accordion:Boolean,displayDirective:{type:String,default:"if"},triggerAreas:{type:Array,default:()=>["main","extra","arrow"]},onItemHeaderClick:[Function,Array],"onUpdate:expandedNames":[Function,Array],onUpdateExpandedNames:[Function,Array],onExpandedNamesChange:{type:[Function,Array],validator:()=>!0,default:void 0}},O=ae("n-collapse");var _e=k({name:"Collapse",props:xe,slots:Object,setup(e,{slots:i}){const{mergedClsPrefixRef:l,inlineThemeDisabled:o,mergedRtlRef:c}=U(e),a=J(e.defaultExpandedNames),d=E(()=>e.expandedNames),p=X(d,a),w=F("Collapse","-collapse",ge,Y,e,l);function m(f){const{"onUpdate:expandedNames":t,onUpdateExpandedNames:u,onExpandedNamesChange:N}=e;u&&R(u,f),t&&R(t,f),N&&R(N,f),a.value=f}function r(f){const{onItemHeaderClick:t}=e;t&&R(t,f)}function n(f,t,u){const{accordion:N}=e,{value:S}=p;if(N)f?(m([t]),r({name:t,expanded:!0,event:u})):(m([]),r({name:t,expanded:!1,event:u}));else if(!Array.isArray(S))m([t]),r({name:t,expanded:!0,event:u});else{const C=S.slice(),P=C.findIndex($=>t===$);~P?(C.splice(P,1),m(C),r({name:t,expanded:!1,event:u})):(C.push(t),m(C),r({name:t,expanded:!0,event:u}))}}Z(O,{props:e,mergedClsPrefixRef:l,expandedNamesRef:p,slots:i,toggleItem:n});const I=j("Collapse",c,l),T=E(()=>{const{common:{cubicBezierEaseInOut:f},self:{titleFontWeight:t,dividerColor:u,titlePadding:N,titleTextColor:S,titleTextColorDisabled:C,textColor:P,arrowColor:$,fontSize:W,titleFontSize:q,arrowColorDisabled:K,itemMargin:L}}=w.value;return{"--n-font-size":W,"--n-bezier":f,"--n-text-color":P,"--n-divider-color":u,"--n-title-padding":N,"--n-title-font-size":q,"--n-title-text-color":S,"--n-title-text-color-disabled":C,"--n-title-font-weight":t,"--n-arrow-color":$,"--n-arrow-color-disabled":K,"--n-item-margin":L}}),y=o?ee("collapse",void 0,T,e):void 0;return{rtlEnabled:I,mergedTheme:w,mergedClsPrefix:l,cssVars:o?void 0:T,themeClass:y==null?void 0:y.themeClass,onRender:y==null?void 0:y.onRender}},render(){var e;return(e=this.onRender)==null||e.call(this),g(),z("div",{class:v([`${this.mergedClsPrefix}-collapse`,this.rtlEnabled&&`${this.mergedClsPrefix}-collapse--rtl`,this.themeClass]),style:G(this.cssVars)},[b(()=>{var i,l;return(l=(i=this.$slots).default)==null?void 0:l.call(i)})],6)}}),ve=k({name:"CollapseItemContent",props:{displayDirective:{type:String,required:!0},show:Boolean,clsPrefix:{type:String,required:!0}},setup(e){return{onceTrue:se(M(e,"show"))}},render(){return g(),_(oe,null,{_:1,default:re(()=>{const{show:e,displayDirective:i,onceTrue:l,clsPrefix:o}=this,c=i==="show"&&l,a=(g(),z("div",{class:v(`${o}-collapse-item__content-wrapper`)},[D("div",{class:v(`${o}-collapse-item__content-inner`)},[b(()=>{var d,p;return(p=(d=this.$slots).default)==null?void 0:p.call(d)})],2)],2));return c?te(a,[[le,e]]):e?a:null})})}});const we=["onClick"],Ce=["onClick"],be={title:String,name:[String,Number],disabled:Boolean,displayDirective:String};var ze=k({name:"CollapseItem",props:be,setup(e){const{mergedRtlRef:i}=U(e),l=pe(),o=me(()=>e.name??l),c=fe(O);c||ue("collapse-item","`n-collapse-item` must be placed inside `n-collapse`.");const{expandedNamesRef:a,props:d,mergedClsPrefixRef:p,slots:w}=c,m=E(()=>{const{value:r}=a;if(Array.isArray(r)){const{value:n}=o;return!~r.findIndex(I=>I===n)}else if(r){const{value:n}=o;return n!==r}return!0});return{rtlEnabled:j("Collapse",i,p),collapseSlots:w,randomName:l,mergedClsPrefix:p,collapsed:m,triggerAreas:M(d,"triggerAreas"),mergedDisplayDirective:E(()=>{const{displayDirective:r}=e;return r||d.displayDirective}),arrowPlacement:E(()=>d.arrowPlacement),handleClick(r){let n="main";V(r,"arrow")&&(n="arrow"),V(r,"extra")&&(n="extra"),d.triggerAreas.includes(n)&&c&&!e.disabled&&c.toggleItem(m.value,o.value,r)}}},render(){const{collapseSlots:e,$slots:i,arrowPlacement:l,collapsed:o,mergedDisplayDirective:c,mergedClsPrefix:a,disabled:d,triggerAreas:p}=this,w=B(i.header,{collapsed:o},()=>[this.title]),m=i["header-extra"]||e["header-extra"],r=i.arrow||e.arrow;return g(),z("div",{class:v([`${a}-collapse-item`,`${a}-collapse-item--${l}-arrow-placement`,d&&`${a}-collapse-item--disabled`,!o&&`${a}-collapse-item--active`,p.map(n=>`${a}-collapse-item--trigger-area-${n}`)])},[D("div",{class:v([`${a}-collapse-item__header`,!o&&`${a}-collapse-item__header--active`])},[D("div",{class:v(`${a}-collapse-item__header-main`),onClick:this.handleClick},[b(()=>l==="right"&&w),(g(),z("div",{class:v(`${a}-collapse-item-arrow`),key:this.rtlEnabled?0:1,"data-arrow":!0},[b(()=>B(r,{collapsed:o},()=>[(g(),_(ne,{clsPrefix:a},{default:()=>this.rtlEnabled?(g(),_(he,{key:1})):(g(),_(ie,{key:2}))},1032,["clsPrefix"]))]))],2)),b(()=>l==="left"&&w)],10,Ce),b(()=>de(m,{collapsed:o},n=>(g(),z("div",{class:v(`${a}-collapse-item__header-extra`),onClick:this.handleClick,"data-extra":!0},[b(()=>n)],10,we))))],2),(g(),_(ve,{clsPrefix:a,displayDirective:c,show:!o},ce(i),1032,["clsPrefix","displayDirective","show"]))],2)}});export{ze as C,_e as a};
