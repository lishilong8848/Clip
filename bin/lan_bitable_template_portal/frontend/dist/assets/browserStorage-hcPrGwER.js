import{h as r}from"./index-DFlHy_S8.js";/**
 * @license lucide-vue-next v0.468.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const s=r("FilesIcon",[["path",{d:"M20 7h-3a2 2 0 0 1-2-2V2",key:"x099mo"}],["path",{d:"M9 18a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h7l4 4v10a2 2 0 0 1-2 2Z",key:"18t6ie"}],["path",{d:"M3 7.6v12.8A1.6 1.6 0 0 0 4.6 22h9.8",key:"1nja0z"}]]);/**
 * @license lucide-vue-next v0.468.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const d=r("PencilIcon",[["path",{d:"M21.174 6.812a1 1 0 0 0-3.986-3.987L3.842 16.174a2 2 0 0 0-.5.83l-1.321 4.352a.5.5 0 0 0 .623.622l4.353-1.32a2 2 0 0 0 .83-.497z",key:"1a8usu"}],["path",{d:"m15 5 4 4",key:"1mk7zo"}]]);function c(t,a){const n=new Map;return{getItem(e){if(n.has(e))return n.get(e)??null;try{return window[t].getItem(e)}catch{return a(),null}},setItem(e,o){n.set(e,o);try{window[t].setItem(e,o),n.delete(e)}catch{a()}},removeItem(e){n.set(e,null);try{window[t].removeItem(e),n.delete(e)}catch{a()}}}}function l(){const t=typeof globalThis<"u"?globalThis.crypto:void 0;if(typeof(t==null?void 0:t.randomUUID)=="function")return t.randomUUID().replace(/-/g,"");if(typeof(t==null?void 0:t.getRandomValues)=="function"){const a=t.getRandomValues(new Uint8Array(16));return Array.from(a,n=>n.toString(16).padStart(2,"0")).join("")}return`${Date.now().toString(16)}${Math.random().toString(16).slice(2)}${Math.random().toString(16).slice(2)}`.padEnd(32,"0").slice(0,32)}export{s as F,d as P,c as a,l as r};
