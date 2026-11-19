import{cv as C,bA as R,aN as b,aP as N,by as E,f6 as S,ch as V,cQ as _,dC as A,bc as H,dh as L,e0 as M,M as T,eP as $,b5 as p,bg as F,dP as k}from"./app-v2-C-KrkZYP.js";let y=!1;function I(){if(C&&window.CSS&&!y&&(y=!0,"registerProperty"in(window==null?void 0:window.CSS)))try{CSS.registerProperty({name:"--n-color-start",syntax:"<color>",inherits:!1,initialValue:"#0000"}),CSS.registerProperty({name:"--n-color-end",syntax:"<color>",inherits:!1,initialValue:"#0000"})}catch{}}function K(e){const{heightSmall:t,heightMedium:r,heightLarge:a,borderRadius:n}=e;return{color:"#eee",colorEnd:"#ddd",borderRadius:n,heightSmall:t,heightMedium:r,heightLarge:a}}const O={common:R,self:K};var Q=b([N("skeleton",`
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
 `)]);const W={...S.props,text:Boolean,round:Boolean,circle:Boolean,height:[String,Number],width:[String,Number],size:String,repeat:{type:Number,default:1},animated:{type:Boolean,default:!0},sharp:{type:Boolean,default:!0}};var q=E({name:"Skeleton",inheritAttrs:!1,props:W,setup(e){I();const{mergedClsPrefixRef:t,mergedComponentPropsRef:r}=$(e),a=p(()=>{var o,i;return e.size||((i=(o=r==null?void 0:r.value)==null?void 0:o.Skeleton)==null?void 0:i.size)}),n=S("Skeleton","-skeleton",Q,O,e,t);return{mergedClsPrefix:t,style:p(()=>{const o=n.value,{common:{cubicBezierEaseInOut:i}}=o,m=o.self,{color:v,colorEnd:z,borderRadius:x}=m;let s;const{circle:l,sharp:P,round:B,width:c,height:d,text:g,animated:w}=e,f=a.value;f!==void 0&&(s=m[F("height",f)]);const u=l?c??d??s:c,h=(l?c??d:d)??s;return{display:g?"inline-block":"",verticalAlign:g?"-0.125em":"",borderRadius:l?"50%":B?"4096px":P?"":x,width:typeof u=="number"?k(u):u,height:typeof h=="number"?k(h):h,animation:w?"":"none","--n-bezier":i,"--n-color-start":v,"--n-color-end":z}})}},render(){const{repeat:e,style:t,mergedClsPrefix:r,$attrs:a}=this,n=V("div",_({class:`${r}-skeleton`,style:t},a));return e>1?(A(),H(T,{key:1},[L(()=>M(e,null).map(o=>[n,`
`]))],64)):n}});export{q as S};
