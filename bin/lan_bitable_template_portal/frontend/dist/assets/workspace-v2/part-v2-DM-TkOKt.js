import{cu as C,bz as L,aL as b,aN as R,bx as N,f2 as y,cg as E,cP as V,dx as _,bb as H,dh as A,dY as T,L as $,eL as F,b4 as p,bf as I,dL as k}from"./app-v2-CXizHh9x.js";let S=!1;function K(){if(C&&window.CSS&&!S&&(S=!0,"registerProperty"in(window==null?void 0:window.CSS)))try{CSS.registerProperty({name:"--n-color-start",syntax:"<color>",inherits:!1,initialValue:"#0000"}),CSS.registerProperty({name:"--n-color-end",syntax:"<color>",inherits:!1,initialValue:"#0000"})}catch{}}function M(e){const{heightSmall:t,heightMedium:r,heightLarge:a,borderRadius:n}=e;return{color:"#eee",colorEnd:"#ddd",borderRadius:n,heightSmall:t,heightMedium:r,heightLarge:a}}const O={common:L,self:M};var W=b([R("skeleton",`
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
 `)]);const Y={...y.props,text:Boolean,round:Boolean,circle:Boolean,height:[String,Number],width:[String,Number],size:String,repeat:{type:Number,default:1},animated:{type:Boolean,default:!0},sharp:{type:Boolean,default:!0}};var q=N({name:"Skeleton",inheritAttrs:!1,props:Y,setup(e){K();const{mergedClsPrefixRef:t,mergedComponentPropsRef:r}=F(e),a=p(()=>{var o,i;return e.size||((i=(o=r==null?void 0:r.value)==null?void 0:o.Skeleton)==null?void 0:i.size)}),n=y("Skeleton","-skeleton",W,O,e,t);return{mergedClsPrefix:t,style:p(()=>{const o=n.value,{common:{cubicBezierEaseInOut:i}}=o,m=o.self,{color:v,colorEnd:z,borderRadius:x}=m;let s;const{circle:l,sharp:B,round:w,width:c,height:d,text:g,animated:P}=e,f=a.value;f!==void 0&&(s=m[I("height",f)]);const u=l?c??d??s:c,h=(l?c??d:d)??s;return{display:g?"inline-block":"",verticalAlign:g?"-0.125em":"",borderRadius:l?"50%":w?"4096px":B?"":x,width:typeof u=="number"?k(u):u,height:typeof h=="number"?k(h):h,animation:P?"":"none","--n-bezier":i,"--n-color-start":v,"--n-color-end":z}})}},render(){const{repeat:e,style:t,mergedClsPrefix:r,$attrs:a}=this,n=E("div",V({class:`${r}-skeleton`,style:t},a));return e>1?(_(),H($,{key:1},[A(()=>T(e,null).map(o=>[n,`
`]))],64)):n}});export{q as S};
