import{aO as i,aR as f,b1 as I,at as V,a0 as E,u as S,v as H,bm as C,bb as W,R as A,aW as y,aY as D}from"./index-Cckjdmtm.js";import{s as L,r as O}from"./c-BqSz5JJU.js";import{z as x,a6 as g,s as b,p as B,I as T,U as K,F as M,n as R}from"./c-1SqeWcLE.js";const U=["value","name","checked","disabled","onChange","onFocus","onBlur"];var ee=x({name:"RadioButton",props:O,setup:L,render(){const{mergedClsPrefix:e}=this;return g(),b("label",{class:i([`${e}-radio-button`,this.mergedDisabled&&`${e}-radio-button--disabled`,this.renderSafeChecked&&`${e}-radio-button--checked`,this.focus&&[`${e}-radio-button--focus`]])},[B("input",{ref:"inputRef",type:"radio",class:i(`${e}-radio-input`),value:this.value,name:this.mergedName,checked:this.renderSafeChecked,disabled:this.mergedDisabled,onChange:this.handleRadioInputChange,onFocus:this.handleRadioInputFocus,onBlur:this.handleRadioInputBlur},null,42,U),B("div",{class:i(`${e}-radio-button__state-border`)},null,2),f(()=>I(this.$slots.default,t=>!t&&!this.label?null:(g(),b("div",{ref:"labelRef",class:i(`${e}-radio__label`)},[f(()=>t||this.label)],2))))],2)}});let z=!1;function Y(){if(V&&window.CSS&&!z&&(z=!0,"registerProperty"in(window==null?void 0:window.CSS)))try{CSS.registerProperty({name:"--n-color-start",syntax:"<color>",inherits:!1,initialValue:"#0000"}),CSS.registerProperty({name:"--n-color-end",syntax:"<color>",inherits:!1,initialValue:"#0000"})}catch{}}function j(e){const{heightSmall:t,heightMedium:r,heightLarge:n,borderRadius:a}=e;return{color:"#eee",colorEnd:"#ddd",borderRadius:a,heightSmall:t,heightMedium:r,heightLarge:n}}const q={common:E,self:j};var G=S([H("skeleton",`
 height: 1em;
 width: 100%;
 transition:
 --n-color-start .3s var(--n-bezier),
 --n-color-end .3s var(--n-bezier),
 background-color .3s var(--n-bezier);
 animation: 2s skeleton-loading infinite cubic-bezier(0.36, 0, 0.64, 1);
 background-color: var(--n-color-start);
 `),S("@keyframes skeleton-loading",`
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
 `)]);const J={...C.props,text:Boolean,round:Boolean,circle:Boolean,height:[String,Number],width:[String,Number],size:String,repeat:{type:Number,default:1},animated:{type:Boolean,default:!0},sharp:{type:Boolean,default:!0}};var te=x({name:"Skeleton",inheritAttrs:!1,props:J,setup(e){Y();const{mergedClsPrefixRef:t,mergedComponentPropsRef:r}=W(e),n=R(()=>{var o,s;return e.size||((s=(o=r==null?void 0:r.value)==null?void 0:o.Skeleton)==null?void 0:s.size)}),a=C("Skeleton","-skeleton",G,q,e,t);return{mergedClsPrefix:t,style:R(()=>{const o=a.value,{common:{cubicBezierEaseInOut:s}}=o,p=o.self,{color:_,colorEnd:$,borderRadius:P}=p;let l;const{circle:d,sharp:w,round:N,width:c,height:u,text:k,animated:F}=e,v=n.value;v!==void 0&&(l=p[A("height",v)]);const h=d?c??u??l:c,m=(d?c??u:u)??l;return{display:k?"inline-block":"",verticalAlign:k?"-0.125em":"",borderRadius:d?"50%":N?"4096px":w?"":P,width:typeof h=="number"?y(h):h,height:typeof m=="number"?y(m):m,animation:F?"":"none","--n-bezier":s,"--n-color-start":_,"--n-color-end":$}})}},render(){const{repeat:e,style:t,mergedClsPrefix:r,$attrs:n}=this,a=T("div",K({class:`${r}-skeleton`,style:t},n));return e>1?(g(),b(M,{key:1},[f(()=>D(e,null).map(o=>[a,`
`]))],64)):a}});export{ee as R,te as S};
