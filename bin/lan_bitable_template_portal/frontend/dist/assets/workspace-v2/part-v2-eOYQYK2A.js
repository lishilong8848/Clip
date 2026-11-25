import{bE as $,bu as O,be as P,aP as h,aR as g,aQ as s,aN as A,c9 as Q,aS as Y,fD as V,dT as x,bh as E,dy as b,dx as Z,dp as v,fk as L,ei as G,fx as J,b6 as X,e9 as ee,fz as U,fE as ae,b9 as z,bk as re,aV as I,bf as _,dv as te,g1 as le,fK as oe,F as se,fp as ie,f2 as M,eD as B,eF as ne,dw as de,bi as ce,fv as pe,cu as fe,eY as me,cr as F,l as ue,H as he}from"./app-v2-DWPuzWwt.js";var xe=$({name:"ChevronLeft",render(){return(()=>{const e=O("dfe229c2639b2082");return e[0]||(e[0]=P("svg",{viewBox:"0 0 16 16",fill:"none",xmlns:"http://www.w3.org/2000/svg"},[P("path",{d:"M10.3536 3.14645C10.5488 3.34171 10.5488 3.65829 10.3536 3.85355L6.20711 8L10.3536 12.1464C10.5488 12.3417 10.5488 12.6583 10.3536 12.8536C10.1583 13.0488 9.84171 13.0488 9.64645 12.8536L5.14645 8.35355C4.95118 8.15829 4.95118 7.84171 5.14645 7.64645L9.64645 3.14645C9.84171 2.95118 10.1583 2.95118 10.3536 3.14645Z",fill:"currentColor"})],-1))})()}}),ge=h("collapse","width: 100%;",[h("collapse-item",`
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
 `)])]),h("collapse-item","margin-left: 32px;"),A("&:first-child","margin-top: 0;"),A("&:first-child >",[s("header","padding-top: 0;")]),g("left-arrow-placement",[s("header",[h("collapse-item-arrow","margin-right: 4px;")])]),g("right-arrow-placement",[s("header",[h("collapse-item-arrow","margin-left: 4px;")])]),s("content-wrapper",[s("content-inner","padding-top: 16px;"),Q({duration:"0.15s"})]),g("active",[s("header",[g("active",[h("collapse-item-arrow","transform: rotate(90deg);")])])]),A("&:not(:first-child)","border-top: 1px solid var(--n-divider-color);"),Y("disabled",[g("trigger-area-main",[s("header",[s("header-main","cursor: pointer;"),h("collapse-item-arrow","cursor: default;")])]),g("trigger-area-arrow",[s("header",[h("collapse-item-arrow","cursor: pointer;")])]),g("trigger-area-extra",[s("header",[s("header-extra","cursor: pointer;")])])]),s("header",`
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
 `)])])]);const ve={...V.props,defaultExpandedNames:{type:[Array,String],default:null},expandedNames:[Array,String],arrowPlacement:{type:String,default:"left"},accordion:Boolean,displayDirective:{type:String,default:"if"},triggerAreas:{type:Array,default:()=>["main","extra","arrow"]},onItemHeaderClick:[Function,Array],"onUpdate:expandedNames":[Function,Array],onUpdateExpandedNames:[Function,Array],onExpandedNamesChange:{type:[Function,Array],validator:()=>!0,default:void 0}},j=re("n-collapse");var _e=$({name:"Collapse",props:ve,slots:Object,setup(e,{slots:i}){const{mergedClsPrefixRef:l,inlineThemeDisabled:o,mergedRtlRef:c}=L(e),a=G(e.defaultExpandedNames),d=z(()=>e.expandedNames),p=J(d,a),w=V("Collapse","-collapse",ge,X,e,l);function f(m){const{"onUpdate:expandedNames":t,onUpdateExpandedNames:u,onExpandedNamesChange:N}=e;u&&I(u,m),t&&I(t,m),N&&I(N,m),a.value=m}function r(m){const{onItemHeaderClick:t}=e;t&&I(t,m)}function n(m,t,u){const{accordion:N}=e,{value:S}=p;if(N)m?(f([t]),r({name:t,expanded:!0,event:u})):(f([]),r({name:t,expanded:!1,event:u}));else if(!Array.isArray(S))f([t]),r({name:t,expanded:!0,event:u});else{const C=S.slice(),R=C.findIndex(D=>t===D);~R?(C.splice(R,1),f(C),r({name:t,expanded:!1,event:u})):(C.push(t),f(C),r({name:t,expanded:!0,event:u}))}}ee(j,{props:e,mergedClsPrefixRef:l,expandedNamesRef:p,slots:i,toggleItem:n});const k=U("Collapse",c,l),T=z(()=>{const{common:{cubicBezierEaseInOut:m},self:{titleFontWeight:t,dividerColor:u,titlePadding:N,titleTextColor:S,titleTextColorDisabled:C,textColor:R,arrowColor:D,fontSize:W,titleFontSize:K,arrowColorDisabled:q,itemMargin:H}}=w.value;return{"--n-font-size":W,"--n-bezier":m,"--n-text-color":R,"--n-divider-color":u,"--n-title-padding":N,"--n-title-font-size":K,"--n-title-text-color":S,"--n-title-text-color-disabled":C,"--n-title-font-weight":t,"--n-arrow-color":D,"--n-arrow-color-disabled":q,"--n-item-margin":H}}),y=o?ae("collapse",void 0,T,e):void 0;return{rtlEnabled:k,mergedTheme:w,mergedClsPrefix:l,cssVars:o?void 0:T,themeClass:y==null?void 0:y.themeClass,onRender:y==null?void 0:y.onRender}},render(){var e;return(e=this.onRender)==null||e.call(this),x(),E("div",{class:v([`${this.mergedClsPrefix}-collapse`,this.rtlEnabled&&`${this.mergedClsPrefix}-collapse--rtl`,this.themeClass]),style:Z(this.cssVars)},[b(()=>{var i,l;return(l=(i=this.$slots).default)==null?void 0:l.call(i)})],6)}}),we=$({name:"CollapseItemContent",props:{displayDirective:{type:String,required:!0},show:Boolean,clsPrefix:{type:String,required:!0}},setup(e){return{onceTrue:ie(M(e,"show"))}},render(){return x(),_(se,null,{_:1,default:te(()=>{const{show:e,displayDirective:i,onceTrue:l,clsPrefix:o}=this,c=i==="show"&&l,a=(x(),E("div",{class:v(`${o}-collapse-item__content-wrapper`)},[P("div",{class:v(`${o}-collapse-item__content-inner`)},[b(()=>{var d,p;return(p=(d=this.$slots).default)==null?void 0:p.call(d)})],2)],2));return c?le(a,[[oe,e]]):e?a:null})})}});const Ce=["onClick"],be=["onClick"],ye={title:String,name:[String,Number],disabled:Boolean,displayDirective:String};var Ee=$({name:"CollapseItem",props:ye,setup(e){const{mergedRtlRef:i}=L(e),l=ce(),o=pe(()=>e.name??l),c=fe(j);c||me("collapse-item","`n-collapse-item` must be placed inside `n-collapse`.");const{expandedNamesRef:a,props:d,mergedClsPrefixRef:p,slots:w}=c,f=z(()=>{const{value:r}=a;if(Array.isArray(r)){const{value:n}=o;return!~r.findIndex(k=>k===n)}else if(r){const{value:n}=o;return n!==r}return!0});return{rtlEnabled:U("Collapse",i,p),collapseSlots:w,randomName:l,mergedClsPrefix:p,collapsed:f,triggerAreas:M(d,"triggerAreas"),mergedDisplayDirective:z(()=>{const{displayDirective:r}=e;return r||d.displayDirective}),arrowPlacement:z(()=>d.arrowPlacement),handleClick(r){let n="main";F(r,"arrow")&&(n="arrow"),F(r,"extra")&&(n="extra"),d.triggerAreas.includes(n)&&c&&!e.disabled&&c.toggleItem(f.value,o.value,r)}}},render(){const{collapseSlots:e,$slots:i,arrowPlacement:l,collapsed:o,mergedDisplayDirective:c,mergedClsPrefix:a,disabled:d,triggerAreas:p}=this,w=B(i.header,{collapsed:o},()=>[this.title]),f=i["header-extra"]||e["header-extra"],r=i.arrow||e.arrow;return x(),E("div",{class:v([`${a}-collapse-item`,`${a}-collapse-item--${l}-arrow-placement`,d&&`${a}-collapse-item--disabled`,!o&&`${a}-collapse-item--active`,p.map(n=>`${a}-collapse-item--trigger-area-${n}`)])},[P("div",{class:v([`${a}-collapse-item__header`,!o&&`${a}-collapse-item__header--active`])},[P("div",{class:v(`${a}-collapse-item__header-main`),onClick:this.handleClick},[b(()=>l==="right"&&w),(x(),E("div",{class:v(`${a}-collapse-item-arrow`),key:this.rtlEnabled?0:1,"data-arrow":!0},[b(()=>B(r,{collapsed:o},()=>[(x(),_(he,{clsPrefix:a},{default:()=>this.rtlEnabled?(x(),_(xe,{key:1})):(x(),_(ue,{key:2}))},1032,["clsPrefix"]))]))],2)),b(()=>l==="left"&&w)],10,be),b(()=>ne(f,{collapsed:o},n=>(x(),E("div",{class:v(`${a}-collapse-item__header-extra`),onClick:this.handleClick,"data-extra":!0},[b(()=>n)],10,Ce))))],2),(x(),_(we,{clsPrefix:a,displayDirective:c,show:!o},de(i),1032,["clsPrefix","displayDirective","show"]))],2)}});export{xe as C,Ee as a,_e as b};
