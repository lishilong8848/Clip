import{h as i,l as c,u as d,w as u,z as f,e as k,T as p,d as b,k as m,P as v,I as B,f as h,D as y,p as V,j as x}from"./index-DvHLAflD.js";/**
 * @license lucide-vue-next v0.468.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const C=i("ArrowLeftIcon",[["path",{d:"m12 19-7-7 7-7",key:"1l729n"}],["path",{d:"M19 12H5",key:"x3x0zl"}]]),N=["disabled","title"],z=c({__name:"VnetBackButton",props:{to:{},hard:{type:Boolean},disabled:{type:Boolean},title:{}},emits:["click"],setup(t,{emit:s}){const a=y(!0);d(()=>{a.value=!0}),u(()=>{a.value=!1});const e=t,n=s;function r(){if(!e.disabled){if(e.to){V(e.to,e.hard);return}n("click")}}return(l,o)=>a.value?(f(),k(p,{key:0,defer:"",to:"#page-back-slot"},[b("button",{type:"button",class:"vnet-back-button",disabled:t.disabled,title:t.title||"返回","aria-label":"返回",onClick:r},[m(v(C),{size:16,"aria-hidden":"true"}),B(l.$slots,"default",{},()=>[o[0]||(o[0]=x("返回",-1))])],8,N)])):h("",!0)}});export{z as _};
