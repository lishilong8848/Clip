import{ax as P,a3 as R,v as b,w as V,bt as v,aV as E,b2 as N,bi as _,Z as H,b0 as p}from"./c-C45nw6FO.js";import{t as A,x as F,G as L,W as T,m as W,F as $,h as k}from"./c-xWaq51p2.js";let S=!1;function G(){if(P&&window.CSS&&!S&&(S=!0,"registerProperty"in(window==null?void 0:window.CSS)))try{CSS.registerProperty({name:"--n-color-start",syntax:"<color>",inherits:!1,initialValue:"#0000"}),CSS.registerProperty({name:"--n-color-end",syntax:"<color>",inherits:!1,initialValue:"#0000"})}catch{}}function I(e){const{heightSmall:t,heightMedium:r,heightLarge:a,borderRadius:n}=e;return{color:"#eee",colorEnd:"#ddd",borderRadius:n,heightSmall:t,heightMedium:r,heightLarge:a}}const K={common:R,self:I};var M=b([V("skeleton",`
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
 `)]);const O={...v.props,text:Boolean,round:Boolean,circle:Boolean,height:[String,Number],width:[String,Number],size:String,repeat:{type:Number,default:1},animated:{type:Boolean,default:!0},sharp:{type:Boolean,default:!0}};var q=A({name:"Skeleton",inheritAttrs:!1,props:O,setup(e){G();const{mergedClsPrefixRef:t,mergedComponentPropsRef:r}=_(e),a=k(()=>{var o,i;return e.size||((i=(o=r==null?void 0:r.value)==null?void 0:o.Skeleton)==null?void 0:i.size)}),n=v("Skeleton","-skeleton",M,K,e,t);return{mergedClsPrefix:t,style:k(()=>{const o=n.value,{common:{cubicBezierEaseInOut:i}}=o,h=o.self,{color:y,colorEnd:x,borderRadius:z}=h;let s;const{circle:l,sharp:w,round:B,width:c,height:d,text:g,animated:C}=e,f=a.value;f!==void 0&&(s=h[H("height",f)]);const u=l?c??d??s:c,m=(l?c??d:d)??s;return{display:g?"inline-block":"",verticalAlign:g?"-0.125em":"",borderRadius:l?"50%":B?"4096px":w?"":z,width:typeof u=="number"?p(u):u,height:typeof m=="number"?p(m):m,animation:C?"":"none","--n-bezier":i,"--n-color-start":y,"--n-color-end":x}})}},render(){const{repeat:e,style:t,mergedClsPrefix:r,$attrs:a}=this,n=F("div",L({class:`${r}-skeleton`,style:t},a));return e>1?(T(),W($,{key:1},[E(()=>N(e,null).map(o=>[n,`
`]))],64)):n}});export{q as S};
