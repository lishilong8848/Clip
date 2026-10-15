import{bK as C,b9 as R,aq as b,as as _,b7 as E,dR as y,bz as N,b_ as V,cD as H,aP as q,cq as A,cZ as I,t as K,dz as L,aI as p,aS as O,cO as k}from"./app-DXg8PPQD.js";let S=!1;function T(){if(C&&window.CSS&&!S&&(S=!0,"registerProperty"in(window==null?void 0:window.CSS)))try{CSS.registerProperty({name:"--n-color-start",syntax:"<color>",inherits:!1,initialValue:"#0000"}),CSS.registerProperty({name:"--n-color-end",syntax:"<color>",inherits:!1,initialValue:"#0000"})}catch{}}function $(e){const{heightSmall:t,heightMedium:r,heightLarge:a,borderRadius:n}=e;return{color:"#eee",colorEnd:"#ddd",borderRadius:n,heightSmall:t,heightMedium:r,heightLarge:a}}const D={common:R,self:$};var F=b([_("skeleton",`
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
 `)]);const M={...y.props,text:Boolean,round:Boolean,circle:Boolean,height:[String,Number],width:[String,Number],size:String,repeat:{type:Number,default:1},animated:{type:Boolean,default:!0},sharp:{type:Boolean,default:!0}};var Z=E({name:"Skeleton",inheritAttrs:!1,props:M,setup(e){T();const{mergedClsPrefixRef:t,mergedComponentPropsRef:r}=L(e),a=p(()=>{var o,i;return e.size||((i=(o=r==null?void 0:r.value)==null?void 0:o.Skeleton)==null?void 0:i.size)}),n=y("Skeleton","-skeleton",F,D,e,t);return{mergedClsPrefix:t,style:p(()=>{const o=n.value,{common:{cubicBezierEaseInOut:i}}=o,h=o.self,{color:v,colorEnd:z,borderRadius:x}=h;let s;const{circle:l,sharp:B,round:w,width:c,height:d,text:g,animated:P}=e,f=a.value;f!==void 0&&(s=h[O("height",f)]);const u=l?c??d??s:c,m=(l?c??d:d)??s;return{display:g?"inline-block":"",verticalAlign:g?"-0.125em":"",borderRadius:l?"50%":w?"4096px":B?"":x,width:typeof u=="number"?k(u):u,height:typeof m=="number"?k(m):m,animation:P?"":"none","--n-bezier":i,"--n-color-start":v,"--n-color-end":z}})}},render(){const{repeat:e,style:t,mergedClsPrefix:r,$attrs:a}=this,n=N("div",V({class:`${r}-skeleton`,style:t},a));return e>1?(H(),q(K,{key:1},[A(()=>I(e,null).map(o=>[n,`
`]))],64)):n}});export{Z as S};
