import{bs as q,dr as r,b6 as a,b3 as g,da as S,d4 as l,db as f,I as D,b4 as k,K as A,M as L,ap as X,x as Y,ab as V,a$ as w,cY as F,c3 as N,aI as I,aK as d,aM as B,eW as E,d9 as G,eD as K,dB as J,eX as Z,ba as M}from"./app-v2-B_JvFXr_.js";const H=["id"],Q=["stop-color"],U=["stop-color"],ee=["viewBox"],re=["d","stroke-width"],te=["d","stroke-width"],ie={success:(r(),k(V)),error:(r(),k(Y)),warning:(r(),k(X)),info:(r(),k(L))};var oe=q({name:"ProgressCircle",props:{clsPrefix:{type:String,required:!0},status:{type:String,required:!0},strokeWidth:{type:Number,required:!0},fillColor:[String,Object],railColor:String,railStyle:[String,Object],percentage:{type:Number,default:0},offsetDegree:{type:Number,default:0},showIndicator:{type:Boolean,required:!0},indicatorTextColor:String,unit:String,viewBoxWidth:{type:Number,required:!0},gapDegree:{type:Number,required:!0},gapOffsetDegree:{type:Number,default:0}},setup(e,{slots:y}){const h=w(()=>{const t="gradient",{fillColor:i}=e;return typeof i=="object"?`${t}-${F(JSON.stringify(i))}`:t});function x(t,i,s,p){const{gapDegree:v,viewBoxWidth:m,strokeWidth:b}=e,n=50,$=0,c=n,o=0,_=100,P=50+b/2,C=`M ${P},${P} m ${$},${c}
      a ${n},${n} 0 1 1 ${o},-100
      a ${n},${n} 0 1 1 0,${_}`,z=Math.PI*2*n;return{pathString:C,pathStyle:{stroke:p==="rail"?s:typeof e.fillColor=="object"?`url(#${h.value})`:s,strokeDasharray:`${Math.min(t,100)/100*(z-v)}px ${m*8}px`,strokeDashoffset:`-${v/2}px`,transformOrigin:i?"center":void 0,transform:i?`rotate(${i}deg)`:void 0}}}const u=()=>{const t=typeof e.fillColor=="object",i=t?e.fillColor.stops[0]:"",s=t?e.fillColor.stops[1]:"";return t&&(r(),a("defs",null,[g("linearGradient",{id:h.value,x1:"0%",y1:"100%",x2:"100%",y2:"0%"},[g("stop",{offset:"0%","stop-color":i},null,8,Q),g("stop",{offset:"100%","stop-color":s},null,8,U)],8,H)]))};return()=>{const{fillColor:t,railColor:i,strokeWidth:s,offsetDegree:p,status:v,percentage:m,showIndicator:b,indicatorTextColor:n,unit:$,gapOffsetDegree:c,clsPrefix:o}=e,{pathString:_,pathStyle:P}=x(100,0,i,"rail"),{pathString:C,pathStyle:z}=x(m,p,t,"fill"),R=100+s;return r(),a("div",{class:l(`${o}-progress-content`),role:"none"},[g("div",{class:l(`${o}-progress-graph`),"aria-hidden":!0},[g("div",{class:l(`${o}-progress-graph-circle`),style:S({transform:c?`rotate(${c}deg)`:void 0})},[(r(),a("svg",{viewBox:`0 0 ${R} ${R}`},[f(()=>u()),g("g",null,[g("path",{class:l(`${o}-progress-graph-circle-rail`),d:_,"stroke-width":s,"stroke-linecap":"round",fill:"none",style:S(P)},null,14,re)]),g("g",null,[g("path",{class:l([`${o}-progress-graph-circle-fill`,m===0&&`${o}-progress-graph-circle-fill--empty`]),d:C,"stroke-width":s,"stroke-linecap":"round",fill:"none",style:S(z)},null,14,te)])],8,ee))],6)],2),b?(r(),a("div",{key:0},[y.default?(r(),a("div",{key:0,class:l(`${o}-progress-custom-content`),role:"none"},[f(()=>y.default())],2)):(r(),a(D,{key:1},[v!=="default"?(r(),a("div",{key:0,class:l(`${o}-progress-icon`),"aria-hidden":!0},[(r(),k(A,{clsPrefix:o},{default:()=>ie[v]},1032,["clsPrefix"]))],2)):(r(),a("div",{key:1,class:l(`${o}-progress-text`),style:S({color:n}),role:"none"},[g("span",{class:l(`${o}-progress-text__percentage`)},[f(()=>m)],2),g("span",{class:l(`${o}-progress-text__unit`)},[f(()=>$)],2)],6))],64))])):f(()=>null)],2)}}});const se={success:(r(),k(V)),error:(r(),k(Y)),warning:(r(),k(X)),info:(r(),k(L))};var le=q({name:"ProgressLine",props:{clsPrefix:{type:String,required:!0},percentage:{type:Number,default:0},railColor:String,railStyle:[String,Object],fillColor:[String,Object],status:{type:String,required:!0},indicatorPlacement:{type:String,required:!0},indicatorTextColor:String,unit:{type:String,default:"%"},processing:{type:Boolean,required:!0},showIndicator:{type:Boolean,required:!0},height:[String,Number],railBorderRadius:[String,Number],fillBorderRadius:[String,Number]},setup(e,{slots:y}){const h=w(()=>N(e.height)),x=w(()=>{var i,s;return typeof e.fillColor=="object"?`linear-gradient(to right, ${(i=e.fillColor)==null?void 0:i.stops[0]} , ${(s=e.fillColor)==null?void 0:s.stops[1]})`:e.fillColor}),u=w(()=>e.railBorderRadius!==void 0?N(e.railBorderRadius):e.height!==void 0?N(e.height,{c:.5}):""),t=w(()=>e.fillBorderRadius!==void 0?N(e.fillBorderRadius):e.railBorderRadius!==void 0?N(e.railBorderRadius):e.height!==void 0?N(e.height,{c:.5}):"");return()=>{const{indicatorPlacement:i,railColor:s,railStyle:p,percentage:v,unit:m,indicatorTextColor:b,status:n,showIndicator:$,processing:c,clsPrefix:o}=e;return r(),a("div",{class:l(`${o}-progress-content`),role:"none"},[g("div",{class:l(`${o}-progress-graph`),"aria-hidden":!0},[g("div",{class:l([`${o}-progress-graph-line`,{[`${o}-progress-graph-line--indicator-${i}`]:!0}])},[g("div",{class:l(`${o}-progress-graph-line-rail`),style:S([{backgroundColor:s,height:h.value,borderRadius:u.value},p])},[g("div",{class:l([`${o}-progress-graph-line-fill`,c&&`${o}-progress-graph-line-fill--processing`]),style:S({maxWidth:`${e.percentage}%`,background:x.value,height:h.value,lineHeight:h.value,borderRadius:t.value})},[i==="inside"?(r(),a("div",{key:0,class:l(`${o}-progress-graph-line-indicator`),style:S({color:b})},[y.default?(r(),a(D,{key:0},[f(()=>y.default())],64)):(r(),a(D,{key:1},[f(()=>`${v}${m}`)],64))],6)):f(()=>null)],6)],6)],2)],2),$&&i==="outside"?(r(),a("div",{key:0},[y.default?(r(),a("div",{key:0,class:l(`${o}-progress-custom-content`),style:S({color:b}),role:"none"},[f(()=>y.default())],6)):(r(),a(D,{key:1},[n==="default"?(r(),a("div",{key:0,role:"none",class:l(`${o}-progress-icon ${o}-progress-icon--as-text`),style:S({color:b})},[f(()=>v),f(()=>m)],6)):(r(),a("div",{key:1,class:l(`${o}-progress-icon`),"aria-hidden":!0},[(r(),k(A,{clsPrefix:o},{default:()=>se[n]},1032,["clsPrefix"]))],2))],64))])):f(()=>null)],2)}}});const ae=["id"],ne=["stop-color"],ce=["stop-color"],de=["d","stroke-width"],ge=["d","stroke-width"],ue=["viewBox"];function T(e,y,h=100){return`m ${h/2} ${h/2-e} a ${e} ${e} 0 1 1 0 ${2*e} a ${e} ${e} 0 1 1 0 -${2*e}`}var pe=q({name:"ProgressMultipleCircle",props:{clsPrefix:{type:String,required:!0},viewBoxWidth:{type:Number,required:!0},percentage:{type:Array,default:[0]},strokeWidth:{type:Number,required:!0},circleGap:{type:Number,required:!0},showIndicator:{type:Boolean,required:!0},fillColor:{type:Array,default:()=>[]},railColor:{type:Array,default:()=>[]},railStyle:{type:Array,default:()=>[]}},setup(e,{slots:y}){const h=w(()=>e.percentage.map((u,t)=>`${Math.PI*u/100*(e.viewBoxWidth/2-e.strokeWidth/2*(1+2*t)-e.circleGap*t)*2}, ${e.viewBoxWidth*8}`)),x=(u,t)=>{const i=e.fillColor[t],s=typeof i=="object"?i.stops[0]:"",p=typeof i=="object"?i.stops[1]:"";return typeof e.fillColor[t]=="object"&&(r(),a("linearGradient",{id:`gradient-${t}`,x1:"100%",y1:"0%",x2:"0%",y2:"100%"},[g("stop",{offset:"0%","stop-color":s},null,8,ne),g("stop",{offset:"100%","stop-color":p},null,8,ce)],8,ae))};return()=>{const{viewBoxWidth:u,strokeWidth:t,circleGap:i,showIndicator:s,fillColor:p,railColor:v,railStyle:m,percentage:b,clsPrefix:n}=e;return r(),a("div",{class:l(`${n}-progress-content`),role:"none"},[g("div",{class:l(`${n}-progress-graph`),"aria-hidden":!0},[g("div",{class:l(`${n}-progress-graph-circle`)},[(r(),a("svg",{viewBox:`0 0 ${u} ${u}`},[g("defs",null,[f(()=>b.map(($,c)=>x($,c)))]),f(()=>b.map(($,c)=>(r(),a("g",{key:c},[g("path",{class:l(`${n}-progress-graph-circle-rail`),d:T(u/2-t/2*(1+2*c)-i*c,t,u),"stroke-width":t,"stroke-linecap":"round",fill:"none",style:S([{strokeDashoffset:0,stroke:v[c]},m[c]])},null,14,de),g("path",{class:l([`${n}-progress-graph-circle-fill`,$===0&&`${n}-progress-graph-circle-fill--empty`]),d:T(u/2-t/2*(1+2*c)-i*c,t,u),"stroke-width":t,"stroke-linecap":"round",fill:"none",style:S({strokeDasharray:h.value[c],strokeDashoffset:0,stroke:typeof p[c]=="object"?`url(#gradient-${c})`:p[c]})},null,14,ge)]))))],8,ue))],2)],2),s&&y.default?(r(),a("div",{key:0},[g("div",{class:l(`${n}-progress-text`)},[f(()=>y.default())],2)])):f(()=>null)],2)}}}),fe=I([d("progress",{display:"inline-block"},[d("progress-icon",`
 color: var(--n-icon-color);
 transition: color .3s var(--n-bezier);
 `),B("line",`
 width: 100%;
 display: block;
 `,[d("progress-content",`
 display: flex;
 align-items: center;
 `,[d("progress-graph",{flex:1})]),d("progress-custom-content",{marginLeft:"14px"}),d("progress-icon",`
 width: 30px;
 padding-left: 14px;
 height: var(--n-icon-size-line);
 line-height: var(--n-icon-size-line);
 font-size: var(--n-icon-size-line);
 `,[B("as-text",`
 color: var(--n-text-color-line-outer);
 text-align: center;
 width: 40px;
 font-size: var(--n-font-size);
 padding-left: 4px;
 transition: color .3s var(--n-bezier);
 `)])]),B("circle, dashboard",{width:"120px"},[d("progress-custom-content",`
 position: absolute;
 left: 50%;
 top: 50%;
 transform: translateX(-50%) translateY(-50%);
 display: flex;
 align-items: center;
 justify-content: center;
 `),d("progress-text",`
 position: absolute;
 left: 50%;
 top: 50%;
 transform: translateX(-50%) translateY(-50%);
 display: flex;
 align-items: center;
 color: inherit;
 font-size: var(--n-font-size-circle);
 color: var(--n-text-color-circle);
 font-weight: var(--n-font-weight-circle);
 transition: color .3s var(--n-bezier);
 white-space: nowrap;
 `),d("progress-icon",`
 position: absolute;
 left: 50%;
 top: 50%;
 transform: translateX(-50%) translateY(-50%);
 display: flex;
 align-items: center;
 color: var(--n-icon-color);
 font-size: var(--n-icon-size-circle);
 `)]),B("multiple-circle",`
 width: 200px;
 color: inherit;
 `,[d("progress-text",`
 font-weight: var(--n-font-weight-circle);
 color: var(--n-text-color-circle);
 position: absolute;
 left: 50%;
 top: 50%;
 transform: translateX(-50%) translateY(-50%);
 display: flex;
 align-items: center;
 justify-content: center;
 transition: color .3s var(--n-bezier);
 `)]),d("progress-content",{position:"relative"}),d("progress-graph",{position:"relative"},[d("progress-graph-circle",[I("svg",{verticalAlign:"bottom"}),d("progress-graph-circle-fill",`
 stroke: var(--n-fill-color);
 transition:
 opacity .3s var(--n-bezier),
 stroke .3s var(--n-bezier),
 stroke-dasharray .3s var(--n-bezier);
 `,[B("empty",{opacity:0})]),d("progress-graph-circle-rail",`
 transition: stroke .3s var(--n-bezier);
 overflow: hidden;
 stroke: var(--n-rail-color);
 `)]),d("progress-graph-line",[B("indicator-inside",[d("progress-graph-line-rail",`
 height: 16px;
 line-height: 16px;
 border-radius: 10px;
 `,[d("progress-graph-line-fill",`
 height: inherit;
 border-radius: 10px;
 `),d("progress-graph-line-indicator",`
 background: #0000;
 white-space: nowrap;
 text-align: right;
 margin-left: 14px;
 margin-right: 14px;
 height: inherit;
 font-size: 12px;
 color: var(--n-text-color-line-inner);
 transition: color .3s var(--n-bezier);
 `)])]),B("indicator-inside-label",`
 height: 16px;
 display: flex;
 align-items: center;
 `,[d("progress-graph-line-rail",`
 flex: 1;
 transition: background-color .3s var(--n-bezier);
 `),d("progress-graph-line-indicator",`
 background: var(--n-fill-color);
 font-size: 12px;
 transform: translateZ(0);
 display: flex;
 vertical-align: middle;
 height: 16px;
 line-height: 16px;
 padding: 0 10px;
 border-radius: 10px;
 position: absolute;
 white-space: nowrap;
 color: var(--n-text-color-line-inner);
 transition:
 right .2s var(--n-bezier),
 color .3s var(--n-bezier),
 background-color .3s var(--n-bezier);
 `)]),d("progress-graph-line-rail",`
 position: relative;
 overflow: hidden;
 height: var(--n-rail-height);
 border-radius: 5px;
 background-color: var(--n-rail-color);
 transition: background-color .3s var(--n-bezier);
 `,[d("progress-graph-line-fill",`
 background: var(--n-fill-color);
 position: relative;
 border-radius: 5px;
 height: inherit;
 width: 100%;
 max-width: 0%;
 transition:
 background-color .3s var(--n-bezier),
 max-width .2s var(--n-bezier);
 `,[B("processing",[I("&::after",`
 content: "";
 background-image: var(--n-line-bg-processing);
 animation: progress-processing-animation 2s var(--n-bezier) infinite;
 `)])])])])])]),I("@keyframes progress-processing-animation",`
 0% {
 position: absolute;
 left: 0;
 top: 0;
 bottom: 0;
 right: 100%;
 opacity: 1;
 }
 66% {
 position: absolute;
 left: 0;
 top: 0;
 bottom: 0;
 right: 0;
 opacity: 0;
 }
 100% {
 position: absolute;
 left: 0;
 top: 0;
 bottom: 0;
 right: 0;
 opacity: 0;
 }
 `)]);const he=["aria-valuenow","role"],ye={...E.props,processing:Boolean,type:{type:String,default:"line"},gapDegree:Number,gapOffsetDegree:Number,status:{type:String,default:"default"},railColor:[String,Array],railStyle:[String,Array],color:[String,Array,Object],viewBoxWidth:{type:Number,default:100},strokeWidth:{type:Number,default:7},percentage:[Number,Array],unit:{type:String,default:"%"},showIndicator:{type:Boolean,default:!0},indicatorPosition:{type:String,default:"outside"},indicatorPlacement:{type:String,default:"outside"},indicatorTextColor:String,circleGap:{type:Number,default:1},height:Number,borderRadius:[String,Number],fillBorderRadius:[String,Number],offsetDegree:Number};var me=q({name:"Progress",props:ye,setup(e){const y=w(()=>e.indicatorPlacement||e.indicatorPosition),h=w(()=>{if(e.gapDegree||e.gapDegree===0)return e.gapDegree;if(e.type==="dashboard")return 75}),{mergedClsPrefixRef:x,inlineThemeDisabled:u}=K(e),t=E("Progress","-progress",fe,J,e,x),i=w(()=>{const{status:p}=e,{common:{cubicBezierEaseInOut:v},self:{fontSize:m,fontSizeCircle:b,railColor:n,railHeight:$,iconSizeCircle:c,iconSizeLine:o,textColorCircle:_,textColorLineInner:P,textColorLineOuter:C,lineBgProcessing:z,fontWeightCircle:R,[M("iconColor",p)]:j,[M("fillColor",p)]:W}}=t.value;return{"--n-bezier":v,"--n-fill-color":W,"--n-font-size":m,"--n-font-size-circle":b,"--n-font-weight-circle":R,"--n-icon-color":j,"--n-icon-size-circle":c,"--n-icon-size-line":o,"--n-line-bg-processing":z,"--n-rail-color":n,"--n-rail-height":$,"--n-text-color-circle":_,"--n-text-color-line-inner":P,"--n-text-color-line-outer":C}}),s=u?Z("progress",w(()=>e.status[0]),i,e):void 0;return{mergedClsPrefix:x,mergedIndicatorPlacement:y,gapDeg:h,cssVars:u?void 0:i,themeClass:s==null?void 0:s.themeClass,onRender:s==null?void 0:s.onRender}},render(){const{type:e,cssVars:y,indicatorTextColor:h,showIndicator:x,status:u,railColor:t,railStyle:i,color:s,percentage:p,viewBoxWidth:v,strokeWidth:m,mergedIndicatorPlacement:b,unit:n,borderRadius:$,fillBorderRadius:c,height:o,processing:_,circleGap:P,mergedClsPrefix:C,gapDeg:z,gapOffsetDegree:R,themeClass:j,$slots:W,onRender:O}=this;return O==null||O(),r(),a("div",{class:l([j,`${C}-progress`,`${C}-progress--${e}`,`${C}-progress--${u}`]),style:S(y),"aria-valuemax":100,"aria-valuemin":0,"aria-valuenow":p,role:e==="circle"||e==="line"||e==="dashboard"?"progressbar":"none"},[e==="circle"||e==="dashboard"?(r(),k(oe,{key:0,clsPrefix:C,status:u,showIndicator:x,indicatorTextColor:h,railColor:t,fillColor:s,railStyle:i,offsetDegree:this.offsetDegree,percentage:p,viewBoxWidth:v,strokeWidth:m,gapDegree:z===void 0?e==="dashboard"?75:0:z,gapOffsetDegree:R,unit:n},G(W),1032,["clsPrefix","status","showIndicator","indicatorTextColor","railColor","fillColor","railStyle","offsetDegree","percentage","viewBoxWidth","strokeWidth","gapDegree","gapOffsetDegree","unit"])):(r(),a(D,{key:1},[e==="line"?(r(),k(le,{key:0,clsPrefix:C,status:u,showIndicator:x,indicatorTextColor:h,railColor:t,fillColor:s,railStyle:i,percentage:p,processing:_,indicatorPlacement:b,unit:n,fillBorderRadius:c,railBorderRadius:$,height:o},G(W),1032,["clsPrefix","status","showIndicator","indicatorTextColor","railColor","fillColor","railStyle","percentage","processing","indicatorPlacement","unit","fillBorderRadius","railBorderRadius","height"])):(r(),a(D,{key:1},[e==="multiple-circle"?(r(),k(pe,{key:0,clsPrefix:C,strokeWidth:m,railColor:t,fillColor:s,railStyle:i,viewBoxWidth:v,percentage:p,showIndicator:x,circleGap:P},G(W),1032,["clsPrefix","strokeWidth","railColor","fillColor","railStyle","viewBoxWidth","percentage","showIndicator","circleGap"])):f(()=>null)],64))],64))],14,he)}});export{me as P};
