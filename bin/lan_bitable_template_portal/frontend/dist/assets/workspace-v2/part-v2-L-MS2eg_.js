import{cg as P,bo as R,aD as b,aF as E,bm as N,eC as y,c2 as V,cz as _,dg as H,b1 as A,d1 as F,dG as G,G as L,ek as T,aW as k,b5 as W,du as p}from"./app-v2-B7jB_4JY.js";let S=!1;function $(){if(P&&window.CSS&&!S&&(S=!0,"registerProperty"in(window==null?void 0:window.CSS)))try{CSS.registerProperty({name:"--n-color-start",syntax:"<color>",inherits:!1,initialValue:"#0000"}),CSS.registerProperty({name:"--n-color-end",syntax:"<color>",inherits:!1,initialValue:"#0000"})}catch{}}function D(e){const{heightSmall:t,heightMedium:r,heightLarge:a,borderRadius:n}=e;return{color:"#eee",colorEnd:"#ddd",borderRadius:n,heightSmall:t,heightMedium:r,heightLarge:a}}const I={common:R,self:D};var K=b([E("skeleton",`
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
 `)]);const M={...y.props,text:Boolean,round:Boolean,circle:Boolean,height:[String,Number],width:[String,Number],size:String,repeat:{type:Number,default:1},animated:{type:Boolean,default:!0},sharp:{type:Boolean,default:!0}};var j=N({name:"Skeleton",inheritAttrs:!1,props:M,setup(e){$();const{mergedClsPrefixRef:t,mergedComponentPropsRef:r}=T(e),a=k(()=>{var o,i;return e.size||((i=(o=r==null?void 0:r.value)==null?void 0:o.Skeleton)==null?void 0:i.size)}),n=y("Skeleton","-skeleton",K,I,e,t);return{mergedClsPrefix:t,style:k(()=>{const o=n.value,{common:{cubicBezierEaseInOut:i}}=o,g=o.self,{color:v,colorEnd:z,borderRadius:x}=g;let s;const{circle:l,sharp:B,round:w,width:c,height:d,text:h,animated:C}=e,f=a.value;f!==void 0&&(s=g[W("height",f)]);const u=l?c??d??s:c,m=(l?c??d:d)??s;return{display:h?"inline-block":"",verticalAlign:h?"-0.125em":"",borderRadius:l?"50%":w?"4096px":B?"":x,width:typeof u=="number"?p(u):u,height:typeof m=="number"?p(m):m,animation:C?"":"none","--n-bezier":i,"--n-color-start":v,"--n-color-end":z}})}},render(){const{repeat:e,style:t,mergedClsPrefix:r,$attrs:a}=this,n=V("div",_({class:`${r}-skeleton`,style:t},a));return e>1?(H(),A(L,{key:1},[F(()=>G(e,null).map(o=>[n,`
`]))],64)):n}});export{j as S};
