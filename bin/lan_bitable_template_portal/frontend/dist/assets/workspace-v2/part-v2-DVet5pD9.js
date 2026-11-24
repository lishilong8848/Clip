import{cD as P,bF as R,aM as b,aO as E,bD as N,fC as y,cp as V,cY as _,dS as H,bg as A,dx as D,ep as F,G as L,fj as M,b8 as p,bk as O,ea as k}from"./app-v2-BFk3s9yc.js";let S=!1;function T(){if(P&&window.CSS&&!S&&(S=!0,"registerProperty"in(window==null?void 0:window.CSS)))try{CSS.registerProperty({name:"--n-color-start",syntax:"<color>",inherits:!1,initialValue:"#0000"}),CSS.registerProperty({name:"--n-color-end",syntax:"<color>",inherits:!1,initialValue:"#0000"})}catch{}}function $(e){const{heightSmall:t,heightMedium:r,heightLarge:a,borderRadius:n}=e;return{color:"#eee",colorEnd:"#ddd",borderRadius:n,heightSmall:t,heightMedium:r,heightLarge:a}}const j={common:R,self:$};var G=b([E("skeleton",`
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
 `)]);const I={...y.props,text:Boolean,round:Boolean,circle:Boolean,height:[String,Number],width:[String,Number],size:String,repeat:{type:Number,default:1},animated:{type:Boolean,default:!0},sharp:{type:Boolean,default:!0}};var W=N({name:"Skeleton",inheritAttrs:!1,props:I,setup(e){T();const{mergedClsPrefixRef:t,mergedComponentPropsRef:r}=M(e),a=p(()=>{var o,i;return e.size||((i=(o=r==null?void 0:r.value)==null?void 0:o.Skeleton)==null?void 0:i.size)}),n=y("Skeleton","-skeleton",G,j,e,t);return{mergedClsPrefix:t,style:p(()=>{const o=n.value,{common:{cubicBezierEaseInOut:i}}=o,g=o.self,{color:v,colorEnd:z,borderRadius:x}=g;let s;const{circle:l,sharp:B,round:w,width:c,height:d,text:h,animated:C}=e,f=a.value;f!==void 0&&(s=g[O("height",f)]);const u=l?c??d??s:c,m=(l?c??d:d)??s;return{display:h?"inline-block":"",verticalAlign:h?"-0.125em":"",borderRadius:l?"50%":w?"4096px":B?"":x,width:typeof u=="number"?k(u):u,height:typeof m=="number"?k(m):m,animation:C?"":"none","--n-bezier":i,"--n-color-start":v,"--n-color-end":z}})}},render(){const{repeat:e,style:t,mergedClsPrefix:r,$attrs:a}=this,n=V("div",_({class:`${r}-skeleton`,style:t},a));return e>1?(H(),A(L,{key:1},[D(()=>F(e,null).map(o=>[n,`
`]))],64)):n}});export{W as S};
