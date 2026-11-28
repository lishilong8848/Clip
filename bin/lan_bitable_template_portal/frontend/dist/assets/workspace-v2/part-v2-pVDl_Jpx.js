import{bD as $,bt as H,bd as P,aO as h,aQ as x,aP as s,aM as k,c8 as Q,aR as Z,fS as M,dW as g,bg as z,dA as b,dz as G,dr as v,fy as U,ep as J,fM as X,b5 as Y,ec as ee,fO as L,fT as ae,b8 as E,bj as re,aU as I,be as _,dx as te,gi as le,f$ as oe,F as se,fD as ie,fe as V,eM as B,eO as ne,dy as de,bh as ce,fK as pe,cw as fe,f8 as me,ct as F,l as ue,K as he}from"./app-v2-DtKeHxdx.js";var ge=$({name:"ChevronLeft",render(){return(()=>{const e=H("dfe229c2639b2082");return e[0]||(e[0]=P("svg",{viewBox:"0 0 16 16",fill:"none",xmlns:"http://www.w3.org/2000/svg"},[P("path",{d:"M10.3536 3.14645C10.5488 3.34171 10.5488 3.65829 10.3536 3.85355L6.20711 8L10.3536 12.1464C10.5488 12.3417 10.5488 12.6583 10.3536 12.8536C10.1583 13.0488 9.84171 13.0488 9.64645 12.8536L5.14645 8.35355C4.95118 8.15829 4.95118 7.84171 5.14645 7.64645L9.64645 3.14645C9.84171 2.95118 10.1583 2.95118 10.3536 3.14645Z",fill:"currentColor"})],-1))})()}}),xe=h("collapse","width: 100%;",[h("collapse-item",`
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
 `)])]),h("collapse-item","margin-left: 32px;"),k("&:first-child","margin-top: 0;"),k("&:first-child >",[s("header","padding-top: 0;")]),x("left-arrow-placement",[s("header",[h("collapse-item-arrow","margin-right: 4px;")])]),x("right-arrow-placement",[s("header",[h("collapse-item-arrow","margin-left: 4px;")])]),s("content-wrapper",[s("content-inner","padding-top: 16px;"),Q({duration:"0.15s"})]),x("active",[s("header",[x("active",[h("collapse-item-arrow","transform: rotate(90deg);")])])]),k("&:not(:first-child)","border-top: 1px solid var(--n-divider-color);"),Z("disabled",[x("trigger-area-main",[s("header",[s("header-main","cursor: pointer;"),h("collapse-item-arrow","cursor: default;")])]),x("trigger-area-arrow",[s("header",[h("collapse-item-arrow","cursor: pointer;")])]),x("trigger-area-extra",[s("header",[s("header-extra","cursor: pointer;")])])]),s("header",`
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
 `)])])]);const ve={...M.props,defaultExpandedNames:{type:[Array,String],default:null},expandedNames:[Array,String],arrowPlacement:{type:String,default:"left"},accordion:Boolean,displayDirective:{type:String,default:"if"},triggerAreas:{type:Array,default:()=>["main","extra","arrow"]},onItemHeaderClick:[Function,Array],"onUpdate:expandedNames":[Function,Array],onUpdateExpandedNames:[Function,Array],onExpandedNamesChange:{type:[Function,Array],validator:()=>!0,default:void 0}},j=re("n-collapse");var _e=$({name:"Collapse",props:ve,slots:Object,setup(e,{slots:i}){const{mergedClsPrefixRef:l,inlineThemeDisabled:o,mergedRtlRef:c}=U(e),a=J(e.defaultExpandedNames),d=E(()=>e.expandedNames),p=X(d,a),w=M("Collapse","-collapse",xe,Y,e,l);function f(m){const{"onUpdate:expandedNames":t,onUpdateExpandedNames:u,onExpandedNamesChange:N}=e;u&&I(u,m),t&&I(t,m),N&&I(N,m),a.value=m}function r(m){const{onItemHeaderClick:t}=e;t&&I(t,m)}function n(m,t,u){const{accordion:N}=e,{value:S}=p;if(N)m?(f([t]),r({name:t,expanded:!0,event:u})):(f([]),r({name:t,expanded:!1,event:u}));else if(!Array.isArray(S))f([t]),r({name:t,expanded:!0,event:u});else{const C=S.slice(),R=C.findIndex(D=>t===D);~R?(C.splice(R,1),f(C),r({name:t,expanded:!1,event:u})):(C.push(t),f(C),r({name:t,expanded:!0,event:u}))}}ee(j,{props:e,mergedClsPrefixRef:l,expandedNamesRef:p,slots:i,toggleItem:n});const A=L("Collapse",c,l),T=E(()=>{const{common:{cubicBezierEaseInOut:m},self:{titleFontWeight:t,dividerColor:u,titlePadding:N,titleTextColor:S,titleTextColorDisabled:C,textColor:R,arrowColor:D,fontSize:O,titleFontSize:W,arrowColorDisabled:K,itemMargin:q}}=w.value;return{"--n-font-size":O,"--n-bezier":m,"--n-text-color":R,"--n-divider-color":u,"--n-title-padding":N,"--n-title-font-size":W,"--n-title-text-color":S,"--n-title-text-color-disabled":C,"--n-title-font-weight":t,"--n-arrow-color":D,"--n-arrow-color-disabled":K,"--n-item-margin":q}}),y=o?ae("collapse",void 0,T,e):void 0;return{rtlEnabled:A,mergedTheme:w,mergedClsPrefix:l,cssVars:o?void 0:T,themeClass:y==null?void 0:y.themeClass,onRender:y==null?void 0:y.onRender}},render(){var e;return(e=this.onRender)==null||e.call(this),g(),z("div",{class:v([`${this.mergedClsPrefix}-collapse`,this.rtlEnabled&&`${this.mergedClsPrefix}-collapse--rtl`,this.themeClass]),style:G(this.cssVars)},[b(()=>{var i,l;return(l=(i=this.$slots).default)==null?void 0:l.call(i)})],6)}}),we=$({name:"CollapseItemContent",props:{displayDirective:{type:String,required:!0},show:Boolean,clsPrefix:{type:String,required:!0}},setup(e){return{onceTrue:ie(V(e,"show"))}},render(){return g(),_(se,null,{_:1,default:te(()=>{const{show:e,displayDirective:i,onceTrue:l,clsPrefix:o}=this,c=i==="show"&&l,a=(g(),z("div",{class:v(`${o}-collapse-item__content-wrapper`)},[P("div",{class:v(`${o}-collapse-item__content-inner`)},[b(()=>{var d,p;return(p=(d=this.$slots).default)==null?void 0:p.call(d)})],2)],2));return c?le(a,[[oe,e]]):e?a:null})})}});const Ce=["onClick"],be=["onClick"],ye={title:String,name:[String,Number],disabled:Boolean,displayDirective:String};var ze=$({name:"CollapseItem",props:ye,setup(e){const{mergedRtlRef:i}=U(e),l=ce(),o=pe(()=>e.name??l),c=fe(j);c||me("collapse-item","`n-collapse-item` must be placed inside `n-collapse`.");const{expandedNamesRef:a,props:d,mergedClsPrefixRef:p,slots:w}=c,f=E(()=>{const{value:r}=a;if(Array.isArray(r)){const{value:n}=o;return!~r.findIndex(A=>A===n)}else if(r){const{value:n}=o;return n!==r}return!0});return{rtlEnabled:L("Collapse",i,p),collapseSlots:w,randomName:l,mergedClsPrefix:p,collapsed:f,triggerAreas:V(d,"triggerAreas"),mergedDisplayDirective:E(()=>{const{displayDirective:r}=e;return r||d.displayDirective}),arrowPlacement:E(()=>d.arrowPlacement),handleClick(r){let n="main";F(r,"arrow")&&(n="arrow"),F(r,"extra")&&(n="extra"),d.triggerAreas.includes(n)&&c&&!e.disabled&&c.toggleItem(f.value,o.value,r)}}},render(){const{collapseSlots:e,$slots:i,arrowPlacement:l,collapsed:o,mergedDisplayDirective:c,mergedClsPrefix:a,disabled:d,triggerAreas:p}=this,w=B(i.header,{collapsed:o},()=>[this.title]),f=i["header-extra"]||e["header-extra"],r=i.arrow||e.arrow;return g(),z("div",{class:v([`${a}-collapse-item`,`${a}-collapse-item--${l}-arrow-placement`,d&&`${a}-collapse-item--disabled`,!o&&`${a}-collapse-item--active`,p.map(n=>`${a}-collapse-item--trigger-area-${n}`)])},[P("div",{class:v([`${a}-collapse-item__header`,!o&&`${a}-collapse-item__header--active`])},[P("div",{class:v(`${a}-collapse-item__header-main`),onClick:this.handleClick},[b(()=>l==="right"&&w),(g(),z("div",{class:v(`${a}-collapse-item-arrow`),key:this.rtlEnabled?0:1,"data-arrow":!0},[b(()=>B(r,{collapsed:o},()=>[(g(),_(he,{clsPrefix:a},{default:()=>this.rtlEnabled?(g(),_(ge,{key:1})):(g(),_(ue,{key:2}))},1032,["clsPrefix"]))]))],2)),b(()=>l==="left"&&w)],10,be),b(()=>ne(f,{collapsed:o},n=>(g(),z("div",{class:v(`${a}-collapse-item__header-extra`),onClick:this.handleClick,"data-extra":!0},[b(()=>n)],10,Ce))))],2),(g(),_(we,{clsPrefix:a,displayDirective:c,show:!o},de(i),1032,["clsPrefix","displayDirective","show"]))],2)}});export{ge as C,ze as a,_e as b};
