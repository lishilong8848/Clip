import{ct as P,by as R,aM as b,aO as N,bw as V,e$ as S,cf as E,cN as _,dv as H,ba as I,df as L,dV as $,L as A,eI as M,b3 as p,be as O,dI as k}from"./app-v2-BKv35RPY.js";let y=!1;function T(){if(P&&window.CSS&&!y&&(y=!0,"registerProperty"in(window==null?void 0:window.CSS)))try{CSS.registerProperty({name:"--n-color-start",syntax:"<color>",inherits:!1,initialValue:"#0000"}),CSS.registerProperty({name:"--n-color-end",syntax:"<color>",inherits:!1,initialValue:"#0000"})}catch{}}function F(e){const{heightSmall:t,heightMedium:r,heightLarge:a,borderRadius:n}=e;return{color:"#eee",colorEnd:"#ddd",borderRadius:n,heightSmall:t,heightMedium:r,heightLarge:a}}const K={common:R,self:F};var W=b([N("skeleton",`
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
 `)]);const j={...S.props,text:Boolean,round:Boolean,circle:Boolean,height:[String,Number],width:[String,Number],size:String,repeat:{type:Number,default:1},animated:{type:Boolean,default:!0},sharp:{type:Boolean,default:!0}};var D=V({name:"Skeleton",inheritAttrs:!1,props:j,setup(e){T();const{mergedClsPrefixRef:t,mergedComponentPropsRef:r}=M(e),a=p(()=>{var o,i;return e.size||((i=(o=r==null?void 0:r.value)==null?void 0:o.Skeleton)==null?void 0:i.size)}),n=S("Skeleton","-skeleton",W,K,e,t);return{mergedClsPrefix:t,style:p(()=>{const o=n.value,{common:{cubicBezierEaseInOut:i}}=o,h=o.self,{color:v,colorEnd:z,borderRadius:x}=h;let s;const{circle:l,sharp:w,round:B,width:c,height:d,text:g,animated:C}=e,f=a.value;f!==void 0&&(s=h[O("height",f)]);const u=l?c??d??s:c,m=(l?c??d:d)??s;return{display:g?"inline-block":"",verticalAlign:g?"-0.125em":"",borderRadius:l?"50%":B?"4096px":w?"":x,width:typeof u=="number"?k(u):u,height:typeof m=="number"?k(m):m,animation:C?"":"none","--n-bezier":i,"--n-color-start":v,"--n-color-end":z}})}},render(){const{repeat:e,style:t,mergedClsPrefix:r,$attrs:a}=this,n=E("div",_({class:`${r}-skeleton`,style:t},a));return e>1?(H(),I(A,{key:1},[L(()=>$(e,null).map(o=>[n,`
`]))],64)):n}});export{D as S};
