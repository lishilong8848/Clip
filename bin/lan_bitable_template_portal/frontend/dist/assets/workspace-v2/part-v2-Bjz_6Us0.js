import{bm as c,bE as h,dT as l,bh as s,G as u,en as y,be as m,bf as b,eB as k,dn as p,aw as v}from"./app-v2-DWPuzWwt.js";/**
 * @license lucide-vue-next v0.468.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const x=c("LayoutGridIcon",[["rect",{width:"7",height:"7",x:"3",y:"3",rx:"1",key:"1g98yp"}],["rect",{width:"7",height:"7",x:"14",y:"3",rx:"1",key:"6d4xhi"}],["rect",{width:"7",height:"7",x:"14",y:"14",rx:"1",key:"nxv5o0"}],["rect",{width:"7",height:"7",x:"3",y:"14",rx:"1",key:"1bb6yr"}]]);/**
 * @license lucide-vue-next v0.468.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const w=c("ListIcon",[["path",{d:"M3 12h.01",key:"nlz23k"}],["path",{d:"M3 18h.01",key:"1tta3j"}],["path",{d:"M3 6h.01",key:"1rqtza"}],["path",{d:"M8 12h13",key:"1za7za"}],["path",{d:"M8 18h13",key:"1lx6n3"}],["path",{d:"M8 6h13",key:"ik3vkj"}]]),_={class:"record-view-switch",role:"group","aria-label":"显示方式"},f=["title","aria-label","aria-pressed","onClick"],V=h({__name:"RecordViewSwitch",props:{modelValue:{}},emits:["update:modelValue"],setup(a,{emit:r}){const i=a,o=r,d=[{value:"cards",label:"卡片显示",icon:x},{value:"table",label:"表单显示",icon:w}];function n(t){t!==i.modelValue&&o("update:modelValue",t)}return(t,g)=>(l(),s("div",_,[(l(),s(u,null,y(d,e=>m("button",{key:e.value,type:"button",class:p(["rvs-btn",{"is-active":a.modelValue===e.value}]),title:e.label,"aria-label":e.label,"aria-pressed":a.modelValue===e.value,onClick:z=>n(e.value)},[(l(),b(k(e.icon),{size:16,"aria-hidden":"true",focusable:"false"}))],10,f)),64))]))}}),M=v(V,[["__scopeId","data-v-0d52a877"]]);export{M as R};
