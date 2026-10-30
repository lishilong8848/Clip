import{au as P,a1 as R,u as b,v as E,bm as v,aR as N,aY as V,bb as _,T as H,aW as p}from"./c-DE4GK1jV.js";import{t as T,x as W,G as A,W as F,m as L,F as $,h as k}from"./c-xWaq51p2.js";let S=!1;function G(){if(P&&window.CSS&&!S&&(S=!0,"registerProperty"in(window==null?void 0:window.CSS)))try{CSS.registerProperty({name:"--n-color-start",syntax:"<color>",inherits:!1,initialValue:"#0000"}),CSS.registerProperty({name:"--n-color-end",syntax:"<color>",inherits:!1,initialValue:"#0000"})}catch{}}function I(e){const{heightSmall:t,heightMedium:r,heightLarge:a,borderRadius:n}=e;return{color:"#eee",colorEnd:"#ddd",borderRadius:n,heightSmall:t,heightMedium:r,heightLarge:a}}const K={common:R,self:I};var M=b([E("skeleton",`
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
 `)]);const O={...v.props,text:Boolean,round:Boolean,circle:Boolean,height:[String,Number],width:[String,Number],size:String,repeat:{type:Number,default:1},animated:{type:Boolean,default:!0},sharp:{type:Boolean,default:!0}};var q=T({name:"Skeleton",inheritAttrs:!1,props:O,setup(e){G();const{mergedClsPrefixRef:t,mergedComponentPropsRef:r}=_(e),a=k(()=>{var o,i;return e.size||((i=(o=r==null?void 0:r.value)==null?void 0:o.Skeleton)==null?void 0:i.size)}),n=v("Skeleton","-skeleton",M,K,e,t);return{mergedClsPrefix:t,style:k(()=>{const o=n.value,{common:{cubicBezierEaseInOut:i}}=o,h=o.self,{color:y,colorEnd:z,borderRadius:x}=h;let s;const{circle:l,sharp:B,round:w,width:c,height:d,text:g,animated:C}=e,f=a.value;f!==void 0&&(s=h[H("height",f)]);const u=l?c??d??s:c,m=(l?c??d:d)??s;return{display:g?"inline-block":"",verticalAlign:g?"-0.125em":"",borderRadius:l?"50%":w?"4096px":B?"":x,width:typeof u=="number"?p(u):u,height:typeof m=="number"?p(m):m,animation:C?"":"none","--n-bezier":i,"--n-color-start":y,"--n-color-end":z}})}},render(){const{repeat:e,style:t,mergedClsPrefix:r,$attrs:a}=this,n=W("div",A({class:`${r}-skeleton`,style:t},a));return e>1?(F(),L($,{key:1},[N(()=>V(e,null).map(o=>[n,`
`]))],64)):n}});export{q as S};
