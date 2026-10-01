import{i as l,m as c,D as d,G as u,J as f,f as k,T as m,e as p,l as b,a0 as v,P as B,g as h,L as y,v as V,k as x}from"./index-CyF1RT03.js";/**
 * @license lucide-vue-next v0.468.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const A=l("ArrowLeftIcon",[["path",{d:"m12 19-7-7 7-7",key:"1l729n"}],["path",{d:"M19 12H5",key:"x3x0zl"}]]),C=["disabled","title"],N=c({__name:"VnetBackButton",props:{to:{},hard:{type:Boolean},disabled:{type:Boolean},title:{}},emits:["click"],setup(t,{emit:s}){const a=y(!0);d(()=>{a.value=!0}),u(()=>{a.value=!1});const e=t,n=s;function r(){if(!e.disabled){if(e.to){V(e.to,e.hard);return}n("click")}}return(i,o)=>a.value?(f(),k(m,{key:0,defer:"",to:"#page-back-slot"},[p("button",{type:"button",class:"vnet-back-button",disabled:t.disabled,title:t.title||"返回","aria-label":"返回",onClick:r},[b(v(A),{size:16,"aria-hidden":"true"}),B(i.$slots,"default",{},()=>[o[0]||(o[0]=x("返回",-1))])],8,C)])):h("",!0)}});export{A,N as _};
