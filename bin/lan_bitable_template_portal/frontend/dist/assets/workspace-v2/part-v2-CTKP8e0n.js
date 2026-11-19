import{aP as h,aR as g,aQ as s,aN as A,c1 as H,aS as Q,by as D,f6 as F,dC as x,bc as P,dh as y,dg as G,da as v,eP as V,dU as J,f1 as X,b4 as Y,dN as Z,f2 as M,f7 as ee,b5 as E,bf as ae,aU as R,ba as _,de as re,b9 as k,fu as te,fd as le,I as oe,eU as se,ex as j,ec as U,p as ie,O as ne,ee as de,df as ce,bd as pe,e$ as me,cl as fe,es as ue,ci as B}from"./app-v2-C-KrkZYP.js";import{C as he}from"./part-v2-z6KqP3kv.js";var xe=h("collapse","width: 100%;",[h("collapse-item",`
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
 `)])]),h("collapse-item","margin-left: 32px;"),A("&:first-child","margin-top: 0;"),A("&:first-child >",[s("header","padding-top: 0;")]),g("left-arrow-placement",[s("header",[h("collapse-item-arrow","margin-right: 4px;")])]),g("right-arrow-placement",[s("header",[h("collapse-item-arrow","margin-left: 4px;")])]),s("content-wrapper",[s("content-inner","padding-top: 16px;"),H({duration:"0.15s"})]),g("active",[s("header",[g("active",[h("collapse-item-arrow","transform: rotate(90deg);")])])]),A("&:not(:first-child)","border-top: 1px solid var(--n-divider-color);"),Q("disabled",[g("trigger-area-main",[s("header",[s("header-main","cursor: pointer;"),h("collapse-item-arrow","cursor: default;")])]),g("trigger-area-arrow",[s("header",[h("collapse-item-arrow","cursor: pointer;")])]),g("trigger-area-extra",[s("header",[s("header-extra","cursor: pointer;")])])]),s("header",`
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
 `)])])]);const ge={...F.props,defaultExpandedNames:{type:[Array,String],default:null},expandedNames:[Array,String],arrowPlacement:{type:String,default:"left"},accordion:Boolean,displayDirective:{type:String,default:"if"},triggerAreas:{type:Array,default:()=>["main","extra","arrow"]},onItemHeaderClick:[Function,Array],"onUpdate:expandedNames":[Function,Array],onUpdateExpandedNames:[Function,Array],onExpandedNamesChange:{type:[Function,Array],validator:()=>!0,default:void 0}},W=ae("n-collapse");var _e=D({name:"Collapse",props:ge,slots:Object,setup(e,{slots:i}){const{mergedClsPrefixRef:l,inlineThemeDisabled:o,mergedRtlRef:c}=V(e),a=J(e.defaultExpandedNames),d=E(()=>e.expandedNames),p=X(d,a),w=F("Collapse","-collapse",xe,Y,e,l);function m(f){const{"onUpdate:expandedNames":t,onUpdateExpandedNames:u,onExpandedNamesChange:N}=e;u&&R(u,f),t&&R(t,f),N&&R(N,f),a.value=f}function r(f){const{onItemHeaderClick:t}=e;t&&R(t,f)}function n(f,t,u){const{accordion:N}=e,{value:S}=p;if(N)f?(m([t]),r({name:t,expanded:!0,event:u})):(m([]),r({name:t,expanded:!1,event:u}));else if(!Array.isArray(S))m([t]),r({name:t,expanded:!0,event:u});else{const C=S.slice(),z=C.findIndex($=>t===$);~z?(C.splice(z,1),m(C),r({name:t,expanded:!1,event:u})):(C.push(t),m(C),r({name:t,expanded:!0,event:u}))}}Z(W,{props:e,mergedClsPrefixRef:l,expandedNamesRef:p,slots:i,toggleItem:n});const I=M("Collapse",c,l),T=E(()=>{const{common:{cubicBezierEaseInOut:f},self:{titleFontWeight:t,dividerColor:u,titlePadding:N,titleTextColor:S,titleTextColorDisabled:C,textColor:z,arrowColor:$,fontSize:O,titleFontSize:q,arrowColorDisabled:K,itemMargin:L}}=w.value;return{"--n-font-size":O,"--n-bezier":f,"--n-text-color":z,"--n-divider-color":u,"--n-title-padding":N,"--n-title-font-size":q,"--n-title-text-color":S,"--n-title-text-color-disabled":C,"--n-title-font-weight":t,"--n-arrow-color":$,"--n-arrow-color-disabled":K,"--n-item-margin":L}}),b=o?ee("collapse",void 0,T,e):void 0;return{rtlEnabled:I,mergedTheme:w,mergedClsPrefix:l,cssVars:o?void 0:T,themeClass:b==null?void 0:b.themeClass,onRender:b==null?void 0:b.onRender}},render(){var e;return(e=this.onRender)==null||e.call(this),x(),P("div",{class:v([`${this.mergedClsPrefix}-collapse`,this.rtlEnabled&&`${this.mergedClsPrefix}-collapse--rtl`,this.themeClass]),style:G(this.cssVars)},[y(()=>{var i,l;return(l=(i=this.$slots).default)==null?void 0:l.call(i)})],6)}}),ve=D({name:"CollapseItemContent",props:{displayDirective:{type:String,required:!0},show:Boolean,clsPrefix:{type:String,required:!0}},setup(e){return{onceTrue:se(j(e,"show"))}},render(){return x(),_(oe,null,{_:1,default:re(()=>{const{show:e,displayDirective:i,onceTrue:l,clsPrefix:o}=this,c=i==="show"&&l,a=(x(),P("div",{class:v(`${o}-collapse-item__content-wrapper`)},[k("div",{class:v(`${o}-collapse-item__content-inner`)},[y(()=>{var d,p;return(p=(d=this.$slots).default)==null?void 0:p.call(d)})],2)],2));return c?te(a,[[le,e]]):e?a:null})})}});const we=["onClick"],Ce=["onClick"],ye={title:String,name:[String,Number],disabled:Boolean,displayDirective:String};var Pe=D({name:"CollapseItem",props:ye,setup(e){const{mergedRtlRef:i}=V(e),l=pe(),o=me(()=>e.name??l),c=fe(W);c||ue("collapse-item","`n-collapse-item` must be placed inside `n-collapse`.");const{expandedNamesRef:a,props:d,mergedClsPrefixRef:p,slots:w}=c,m=E(()=>{const{value:r}=a;if(Array.isArray(r)){const{value:n}=o;return!~r.findIndex(I=>I===n)}else if(r){const{value:n}=o;return n!==r}return!0});return{rtlEnabled:M("Collapse",i,p),collapseSlots:w,randomName:l,mergedClsPrefix:p,collapsed:m,triggerAreas:j(d,"triggerAreas"),mergedDisplayDirective:E(()=>{const{displayDirective:r}=e;return r||d.displayDirective}),arrowPlacement:E(()=>d.arrowPlacement),handleClick(r){let n="main";B(r,"arrow")&&(n="arrow"),B(r,"extra")&&(n="extra"),d.triggerAreas.includes(n)&&c&&!e.disabled&&c.toggleItem(m.value,o.value,r)}}},render(){const{collapseSlots:e,$slots:i,arrowPlacement:l,collapsed:o,mergedDisplayDirective:c,mergedClsPrefix:a,disabled:d,triggerAreas:p}=this,w=U(i.header,{collapsed:o},()=>[this.title]),m=i["header-extra"]||e["header-extra"],r=i.arrow||e.arrow;return x(),P("div",{class:v([`${a}-collapse-item`,`${a}-collapse-item--${l}-arrow-placement`,d&&`${a}-collapse-item--disabled`,!o&&`${a}-collapse-item--active`,p.map(n=>`${a}-collapse-item--trigger-area-${n}`)])},[k("div",{class:v([`${a}-collapse-item__header`,!o&&`${a}-collapse-item__header--active`])},[k("div",{class:v(`${a}-collapse-item__header-main`),onClick:this.handleClick},[y(()=>l==="right"&&w),(x(),P("div",{class:v(`${a}-collapse-item-arrow`),key:this.rtlEnabled?0:1,"data-arrow":!0},[y(()=>U(r,{collapsed:o},()=>[(x(),_(ne,{clsPrefix:a},{default:()=>this.rtlEnabled?(x(),_(he,{key:1})):(x(),_(ie,{key:2}))},1032,["clsPrefix"]))]))],2)),y(()=>l==="left"&&w)],10,Ce),y(()=>de(m,{collapsed:o},n=>(x(),P("div",{class:v(`${a}-collapse-item__header-extra`),onClick:this.handleClick,"data-extra":!0},[y(()=>n)],10,we))))],2),(x(),_(ve,{clsPrefix:a,displayDirective:c,show:!o},ce(i),1032,["clsPrefix","displayDirective","show"]))],2)}});export{Pe as C,_e as a};
