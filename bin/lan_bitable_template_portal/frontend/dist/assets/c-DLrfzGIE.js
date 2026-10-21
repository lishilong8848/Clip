import{at as P,a0 as R,u as b,v as E,bm as v,aR as N,bb as V,R as _,aW as p,aY as H}from"./index-CoL_XHCO.js";import{z as A,I as F,U as I,a6 as L,s as T,F as W,n as k}from"./c-sNzMfqfR.js";let S=!1;function $(){if(P&&window.CSS&&!S&&(S=!0,"registerProperty"in(window==null?void 0:window.CSS)))try{CSS.registerProperty({name:"--n-color-start",syntax:"<color>",inherits:!1,initialValue:"#0000"}),CSS.registerProperty({name:"--n-color-end",syntax:"<color>",inherits:!1,initialValue:"#0000"})}catch{}}function K(e){const{heightSmall:t,heightMedium:r,heightLarge:a,borderRadius:n}=e;return{color:"#eee",colorEnd:"#ddd",borderRadius:n,heightSmall:t,heightMedium:r,heightLarge:a}}const M={common:R,self:K};var O=b([E("skeleton",`
 height: 1em;
 width: 100%;
 transition:
 --n-color-start .3s var(--n-bezier),
 --n-color-end .3s var(--n-bezier),
 background-color .3s var(--n-bezier);
 animation: 2s skeleton-loading infinite cubic-bezier(0.36, 0, 0.64, 1);
 background-color: var(--n-color-start);
 `),b("@keyframes skeleton-loading",`
 0% {
 background: var(--n-color-start);
 }
 40% {
 background: var(--n-color-end);
 }
 80% {
 background: var(--n-color-start);
 }
 100% {
 background: var(--n-color-start);
 }
 `)]);const U={...v.props,text:Boolean,round:Boolean,circle:Boolean,height:[String,Number],width:[String,Number],size:String,repeat:{type:Number,default:1},animated:{type:Boolean,default:!0},sharp:{type:Boolean,default:!0}};var q=A({name:"Skeleton",inheritAttrs:!1,props:U,setup(e){$();const{mergedClsPrefixRef:t,mergedComponentPropsRef:r}=V(e),a=k(()=>{var o,i;return e.size||((i=(o=r==null?void 0:r.value)==null?void 0:o.Skeleton)==null?void 0:i.size)}),n=v("Skeleton","-skeleton",O,M,e,t);return{mergedClsPrefix:t,style:k(()=>{const o=n.value,{common:{cubicBezierEaseInOut:i}}=o,h=o.self,{color:y,colorEnd:z,borderRadius:x}=h;let s;const{circle:l,sharp:B,round:w,width:c,height:d,text:g,animated:C}=e,f=a.value;f!==void 0&&(s=h[_("height",f)]);const u=l?c??d??s:c,m=(l?c??d:d)??s;return{display:g?"inline-block":"",verticalAlign:g?"-0.125em":"",borderRadius:l?"50%":w?"4096px":B?"":x,width:typeof u=="number"?p(u):u,height:typeof m=="number"?p(m):m,animation:C?"":"none","--n-bezier":i,"--n-color-start":y,"--n-color-end":z}})}},render(){const{repeat:e,style:t,mergedClsPrefix:r,$attrs:a}=this,n=F("div",I({class:`${r}-skeleton`,style:t},a));return e>1?(L(),T(W,{key:1},[N(()=>H(e,null).map(o=>[n,`
`]))],64)):n}});export{q as S};
