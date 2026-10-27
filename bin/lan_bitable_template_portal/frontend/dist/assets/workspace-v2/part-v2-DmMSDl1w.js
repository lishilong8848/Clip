import{bc as c}from"./app-v2-DPWSSTwB.js";/**
 * @license lucide-vue-next v0.468.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const s=c("ChevronUpIcon",[["path",{d:"m18 15-6-6-6 6",key:"153udz"}]]),r=new WeakSet;function l(t){const a="clipboardData"in t?t.clipboardData:t.dataTransfer,e=Array.from((a==null?void 0:a.files)||[]);if(!e.length)return[];const o=e.filter(n=>n.type.startsWith("image/"));return t.stopPropagation(),(t.type==="drop"||o.length)&&t.preventDefault(),r.has(t)?[]:(r.add(t),o)}function f(t,a=!1){var o;const e=t.target;a||!(e instanceof Element)||e.closest("button,a,input,label,textarea,select,[contenteditable],.n-image")||(o=t.currentTarget)==null||o.focus({preventScroll:!0})}export{s as C,f,l as n};
