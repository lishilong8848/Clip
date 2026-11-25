import{cE as C,bG as E,aN as b,aP as R,bE as N,fD as S,cq as V,cZ as _,dT as H,bh as T,dy as q,eq as A,G,fk as L,b9 as k,bl as $,eb as p}from"./app-v2-DWPuzWwt.js";let y=!1;function D(){if(C&&window.CSS&&!y&&(y=!0,"registerProperty"in(window==null?void 0:window.CSS)))try{CSS.registerProperty({name:"--n-color-start",syntax:"<color>",inherits:!1,initialValue:"#0000"}),CSS.registerProperty({name:"--n-color-end",syntax:"<color>",inherits:!1,initialValue:"#0000"})}catch{}}function F(e){const{heightSmall:t,heightMedium:r,heightLarge:a,borderRadius:n}=e;return{color:"#eee",colorEnd:"#ddd",borderRadius:n,heightSmall:t,heightMedium:r,heightLarge:a}}const I={common:E,self:F};var K=b([R("skeleton",`
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
 `)]);const M={...S.props,text:Boolean,round:Boolean,circle:Boolean,height:[String,Number],width:[String,Number],size:String,repeat:{type:Number,default:1},animated:{type:Boolean,default:!0},sharp:{type:Boolean,default:!0}};var W=N({name:"Skeleton",inheritAttrs:!1,props:M,setup(e){D();const{mergedClsPrefixRef:t,mergedComponentPropsRef:r}=L(e),a=k(()=>{var o,i;return e.size||((i=(o=r==null?void 0:r.value)==null?void 0:o.Skeleton)==null?void 0:i.size)}),n=S("Skeleton","-skeleton",K,I,e,t);return{mergedClsPrefix:t,style:k(()=>{const o=n.value,{common:{cubicBezierEaseInOut:i}}=o,m=o.self,{color:v,colorEnd:z,borderRadius:x}=m;let s;const{circle:l,sharp:B,round:w,width:c,height:d,text:g,animated:P}=e,f=a.value;f!==void 0&&(s=m[$("height",f)]);const u=l?c??d??s:c,h=(l?c??d:d)??s;return{display:g?"inline-block":"",verticalAlign:g?"-0.125em":"",borderRadius:l?"50%":w?"4096px":B?"":x,width:typeof u=="number"?p(u):u,height:typeof h=="number"?p(h):h,animation:P?"":"none","--n-bezier":i,"--n-color-start":v,"--n-color-end":z}})}},render(){const{repeat:e,style:t,mergedClsPrefix:r,$attrs:a}=this,n=V("div",_({class:`${r}-skeleton`,style:t},a));return e>1?(H(),T(G,{key:1},[q(()=>A(e,null).map(o=>[n,`
`]))],64)):n}});export{W as S};
