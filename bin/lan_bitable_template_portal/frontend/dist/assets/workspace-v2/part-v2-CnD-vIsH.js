import{bl as c,bD as u,dW as l,bg as s,I as h,ew as m,bd as b,be as k,eK as p,dq as v,az as y}from"./app-v2-BzzgZfzX.js";import{L as _}from"./part-v2-DLdmX83D.js";/**
 * @license lucide-vue-next v0.468.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const f=c("ListIcon",[["path",{d:"M3 12h.01",key:"nlz23k"}],["path",{d:"M3 18h.01",key:"1tta3j"}],["path",{d:"M3 6h.01",key:"1rqtza"}],["path",{d:"M8 12h13",key:"1za7za"}],["path",{d:"M8 18h13",key:"1lx6n3"}],["path",{d:"M8 6h13",key:"ik3vkj"}]]);/**
 * @license lucide-vue-next v0.468.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const x=c("PenLineIcon",[["path",{d:"M12 20h9",key:"t2du7b"}],["path",{d:"M16.376 3.622a1 1 0 0 1 3.002 3.002L7.368 18.635a2 2 0 0 1-.855.506l-2.872.838a.5.5 0 0 1-.62-.62l.838-2.872a2 2 0 0 1 .506-.854z",key:"1ykcvy"}]]),L={class:"record-view-switch",role:"group","aria-label":"显示方式"},V=["title","aria-label","aria-pressed","onClick"],z=u({__name:"RecordViewSwitch",props:{modelValue:{}},emits:["update:modelValue"],setup(a,{emit:o}){const n=a,i=o,r=[{value:"cards",label:"卡片显示",icon:_},{value:"table",label:"表单显示",icon:f}];function d(t){t!==n.modelValue&&i("update:modelValue",t)}return(t,M)=>(l(),s("div",L,[(l(),s(h,null,m(r,e=>b("button",{key:e.value,type:"button",class:v(["rvs-btn",{"is-active":a.modelValue===e.value}]),title:e.label,"aria-label":e.label,"aria-pressed":a.modelValue===e.value,onClick:w=>d(e.value)},[(l(),k(p(e.icon),{size:16,"aria-hidden":"true",focusable:"false"}))],10,V)),64))]))}}),B=y(z,[["__scopeId","data-v-0d52a877"]]);export{x as P,B as R};
