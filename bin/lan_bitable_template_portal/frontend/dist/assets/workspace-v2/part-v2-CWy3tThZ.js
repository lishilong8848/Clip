import{cu as C,bw as R,aF as b,aH as N,bu as E,f7 as S,cg as H,cP as V,dA as _,b7 as $,dg as A,d$ as F,y as L,eQ as T,a$ as p,bb as I,dN as k}from"./app-v2-BFIkV8bv.js";let y=!1;function K(){if(C&&window.CSS&&!y&&(y=!0,"registerProperty"in(window==null?void 0:window.CSS)))try{CSS.registerProperty({name:"--n-color-start",syntax:"<color>",inherits:!1,initialValue:"#0000"}),CSS.registerProperty({name:"--n-color-end",syntax:"<color>",inherits:!1,initialValue:"#0000"})}catch{}}function M(e){const{heightSmall:t,heightMedium:r,heightLarge:a,borderRadius:n}=e;return{color:"#eee",colorEnd:"#ddd",borderRadius:n,heightSmall:t,heightMedium:r,heightLarge:a}}const O={common:R,self:M};var Q=b([N("skeleton",`
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
 `)]);const W={...S.props,text:Boolean,round:Boolean,circle:Boolean,height:[String,Number],width:[String,Number],size:String,repeat:{type:Number,default:1},animated:{type:Boolean,default:!0},sharp:{type:Boolean,default:!0}};var q=E({name:"Skeleton",inheritAttrs:!1,props:W,setup(e){K();const{mergedClsPrefixRef:t,mergedComponentPropsRef:r}=T(e),a=p(()=>{var o,i;return e.size||((i=(o=r==null?void 0:r.value)==null?void 0:o.Skeleton)==null?void 0:i.size)}),n=S("Skeleton","-skeleton",Q,O,e,t);return{mergedClsPrefix:t,style:p(()=>{const o=n.value,{common:{cubicBezierEaseInOut:i}}=o,m=o.self,{color:v,colorEnd:z,borderRadius:x}=m;let s;const{circle:l,sharp:w,round:B,width:c,height:d,text:h,animated:P}=e,f=a.value;f!==void 0&&(s=m[I("height",f)]);const u=l?c??d??s:c,g=(l?c??d:d)??s;return{display:h?"inline-block":"",verticalAlign:h?"-0.125em":"",borderRadius:l?"50%":B?"4096px":w?"":x,width:typeof u=="number"?k(u):u,height:typeof g=="number"?k(g):g,animation:P?"":"none","--n-bezier":i,"--n-color-start":v,"--n-color-end":z}})}},render(){const{repeat:e,style:t,mergedClsPrefix:r,$attrs:a}=this,n=H("div",V({class:`${r}-skeleton`,style:t},a));return e>1?(_(),$(L,{key:1},[A(()=>F(e,null).map(o=>[n,`
`]))],64)):n}});export{q as S};
