import{bT as C,bi as R,ax as b,az as N,bg as _,e2 as y,bJ as E,c7 as V,cN as A,aW as H,cA as T,da as L,w as M,dM as W,aP as p,aZ as $,c_ as k}from"./app-v2-dLiaPnr4.js";let S=!1;function F(){if(C&&window.CSS&&!S&&(S=!0,"registerProperty"in(window==null?void 0:window.CSS)))try{CSS.registerProperty({name:"--n-color-start",syntax:"<color>",inherits:!1,initialValue:"#0000"}),CSS.registerProperty({name:"--n-color-end",syntax:"<color>",inherits:!1,initialValue:"#0000"})}catch{}}function I(e){const{heightSmall:t,heightMedium:r,heightLarge:a,borderRadius:n}=e;return{color:"#eee",colorEnd:"#ddd",borderRadius:n,heightSmall:t,heightMedium:r,heightLarge:a}}const J={common:R,self:I};var K=b([N("skeleton",`
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
 `)]);const O={...y.props,text:Boolean,round:Boolean,circle:Boolean,height:[String,Number],width:[String,Number],size:String,repeat:{type:Number,default:1},animated:{type:Boolean,default:!0},sharp:{type:Boolean,default:!0}};var j=_({name:"Skeleton",inheritAttrs:!1,props:O,setup(e){F();const{mergedClsPrefixRef:t,mergedComponentPropsRef:r}=W(e),a=p(()=>{var o,i;return e.size||((i=(o=r==null?void 0:r.value)==null?void 0:o.Skeleton)==null?void 0:i.size)}),n=y("Skeleton","-skeleton",K,J,e,t);return{mergedClsPrefix:t,style:p(()=>{const o=n.value,{common:{cubicBezierEaseInOut:i}}=o,g=o.self,{color:v,colorEnd:z,borderRadius:x}=g;let s;const{circle:l,sharp:w,round:B,width:c,height:d,text:h,animated:P}=e,f=a.value;f!==void 0&&(s=g[$("height",f)]);const u=l?c??d??s:c,m=(l?c??d:d)??s;return{display:h?"inline-block":"",verticalAlign:h?"-0.125em":"",borderRadius:l?"50%":B?"4096px":w?"":x,width:typeof u=="number"?k(u):u,height:typeof m=="number"?k(m):m,animation:P?"":"none","--n-bezier":i,"--n-color-start":v,"--n-color-end":z}})}},render(){const{repeat:e,style:t,mergedClsPrefix:r,$attrs:a}=this,n=E("div",V({class:`${r}-skeleton`,style:t},a));return e>1?(A(),H(M,{key:1},[T(()=>L(e,null).map(o=>[n,`
`]))],64)):n}});export{j as S};
