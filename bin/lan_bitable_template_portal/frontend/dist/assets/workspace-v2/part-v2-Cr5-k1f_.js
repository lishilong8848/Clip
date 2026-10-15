import{bP as C,be as E,av as b,ax as R,bc as N,dW as S,bE as V,c3 as _,cI as H,aU as T,cv as A,d2 as I,w as L,dE as W,aN as p,aX as $,cT as k}from"./app-v2-BtC_jOn5.js";let v=!1;function F(){if(C&&window.CSS&&!v&&(v=!0,"registerProperty"in(window==null?void 0:window.CSS)))try{CSS.registerProperty({name:"--n-color-start",syntax:"<color>",inherits:!1,initialValue:"#0000"}),CSS.registerProperty({name:"--n-color-end",syntax:"<color>",inherits:!1,initialValue:"#0000"})}catch{}}function K(e){const{heightSmall:t,heightMedium:r,heightLarge:a,borderRadius:n}=e;return{color:"#eee",colorEnd:"#ddd",borderRadius:n,heightSmall:t,heightMedium:r,heightLarge:a}}const M={common:E,self:K};var O=b([R("skeleton",`
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
 `)]);const U={...S.props,text:Boolean,round:Boolean,circle:Boolean,height:[String,Number],width:[String,Number],size:String,repeat:{type:Number,default:1},animated:{type:Boolean,default:!0},sharp:{type:Boolean,default:!0}};var j=N({name:"Skeleton",inheritAttrs:!1,props:U,setup(e){F();const{mergedClsPrefixRef:t,mergedComponentPropsRef:r}=W(e),a=p(()=>{var o,i;return e.size||((i=(o=r==null?void 0:r.value)==null?void 0:o.Skeleton)==null?void 0:i.size)}),n=S("Skeleton","-skeleton",O,M,e,t);return{mergedClsPrefix:t,style:p(()=>{const o=n.value,{common:{cubicBezierEaseInOut:i}}=o,h=o.self,{color:y,colorEnd:z,borderRadius:x}=h;let s;const{circle:l,sharp:w,round:B,width:c,height:d,text:g,animated:P}=e,f=a.value;f!==void 0&&(s=h[$("height",f)]);const u=l?c??d??s:c,m=(l?c??d:d)??s;return{display:g?"inline-block":"",verticalAlign:g?"-0.125em":"",borderRadius:l?"50%":B?"4096px":w?"":x,width:typeof u=="number"?k(u):u,height:typeof m=="number"?k(m):m,animation:P?"":"none","--n-bezier":i,"--n-color-start":y,"--n-color-end":z}})}},render(){const{repeat:e,style:t,mergedClsPrefix:r,$attrs:a}=this,n=V("div",_({class:`${r}-skeleton`,style:t},a));return e>1?(H(),T(L,{key:1},[A(()=>I(e,null).map(o=>[n,`
`]))],64)):n}});export{j as S};
