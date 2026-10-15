import{be as F,bc as q,cI as r,aU as n,aR as u,cu as S,cp as s,cv as y,w as W,aS as $,x as A,z as M,ai as X,s as E,a6 as Y,aN as w,ci as H,bw as N,av as I,ax as g,az as B,dW as V,ct as G,dE as J,dX as K,aX as L}from"./app-v2-BtC_jOn5.js";function U(e){const{infoColor:f,successColor:h,warningColor:v,errorColor:d,textColor2:t,progressRailColor:o,fontSize:l,fontWeight:p}=e;return{fontSize:l,fontSizeCircle:"28px",fontWeightCircle:p,railColor:o,railHeight:"8px",iconSizeCircle:"36px",iconSizeLine:"18px",iconColor:f,iconColorInfo:f,iconColorSuccess:h,iconColorWarning:v,iconColorError:d,textColorCircle:t,textColorLineInner:"rgb(255, 255, 255)",textColorLineOuter:t,fillColor:f,fillColorInfo:f,fillColorSuccess:h,fillColorWarning:v,fillColorError:d,lineBgProcessing:"linear-gradient(90deg, rgba(255, 255, 255, .3) 0%, rgba(255, 255, 255, .5) 100%)"}}const Z={common:F,self:U},Q=["id"],ee=["stop-color"],re=["stop-color"],te=["viewBox"],oe=["d","stroke-width"],ie=["d","stroke-width"],le={success:(r(),$(Y)),error:(r(),$(E)),warning:(r(),$(X)),info:(r(),$(M))};var se=q({name:"ProgressCircle",props:{clsPrefix:{type:String,required:!0},status:{type:String,required:!0},strokeWidth:{type:Number,required:!0},fillColor:[String,Object],railColor:String,railStyle:[String,Object],percentage:{type:Number,default:0},offsetDegree:{type:Number,default:0},showIndicator:{type:Boolean,required:!0},indicatorTextColor:String,unit:String,viewBoxWidth:{type:Number,required:!0},gapDegree:{type:Number,required:!0},gapOffsetDegree:{type:Number,default:0}},setup(e,{slots:f}){const h=w(()=>{const t="gradient",{fillColor:o}=e;return typeof o=="object"?`${t}-${H(JSON.stringify(o))}`:t});function v(t,o,l,p){const{gapDegree:m,viewBoxWidth:b,strokeWidth:x}=e,a=50,C=0,c=a,i=0,_=100,z=50+x/2,k=`M ${z},${z} m ${C},${c}
      a ${a},${a} 0 1 1 ${i},-100
      a ${a},${a} 0 1 1 0,${_}`,P=Math.PI*2*a;return{pathString:k,pathStyle:{stroke:p==="rail"?l:typeof e.fillColor=="object"?`url(#${h.value})`:l,strokeDasharray:`${Math.min(t,100)/100*(P-m)}px ${b*8}px`,strokeDashoffset:`-${m/2}px`,transformOrigin:o?"center":void 0,transform:o?`rotate(${o}deg)`:void 0}}}const d=()=>{const t=typeof e.fillColor=="object",o=t?e.fillColor.stops[0]:"",l=t?e.fillColor.stops[1]:"";return t&&(r(),n("defs",null,[u("linearGradient",{id:h.value,x1:"0%",y1:"100%",x2:"100%",y2:"0%"},[u("stop",{offset:"0%","stop-color":o},null,8,ee),u("stop",{offset:"100%","stop-color":l},null,8,re)],8,Q)]))};return()=>{const{fillColor:t,railColor:o,strokeWidth:l,offsetDegree:p,status:m,percentage:b,showIndicator:x,indicatorTextColor:a,unit:C,gapOffsetDegree:c,clsPrefix:i}=e,{pathString:_,pathStyle:z}=v(100,0,o,"rail"),{pathString:k,pathStyle:P}=v(b,p,t,"fill"),R=100+l;return r(),n("div",{class:s(`${i}-progress-content`),role:"none"},[u("div",{class:s(`${i}-progress-graph`),"aria-hidden":!0},[u("div",{class:s(`${i}-progress-graph-circle`),style:S({transform:c?`rotate(${c}deg)`:void 0})},[(r(),n("svg",{viewBox:`0 0 ${R} ${R}`},[y(()=>d()),u("g",null,[u("path",{class:s(`${i}-progress-graph-circle-rail`),d:_,"stroke-width":l,"stroke-linecap":"round",fill:"none",style:S(z)},null,14,oe)]),u("g",null,[u("path",{class:s([`${i}-progress-graph-circle-fill`,b===0&&`${i}-progress-graph-circle-fill--empty`]),d:k,"stroke-width":l,"stroke-linecap":"round",fill:"none",style:S(P)},null,14,ie)])],8,te))],6)],2),x?(r(),n("div",{key:0},[f.default?(r(),n("div",{key:0,class:s(`${i}-progress-custom-content`),role:"none"},[y(()=>f.default())],2)):(r(),n(W,{key:1},[m!=="default"?(r(),n("div",{key:0,class:s(`${i}-progress-icon`),"aria-hidden":!0},[(r(),$(A,{clsPrefix:i},{default:()=>le[m]},1032,["clsPrefix"]))],2)):(r(),n("div",{key:1,class:s(`${i}-progress-text`),style:S({color:a}),role:"none"},[u("span",{class:s(`${i}-progress-text__percentage`)},[y(()=>b)],2),u("span",{class:s(`${i}-progress-text__unit`)},[y(()=>C)],2)],6))],64))])):y(()=>null)],2)}}});const ne={success:(r(),$(Y)),error:(r(),$(E)),warning:(r(),$(X)),info:(r(),$(M))};var ae=q({name:"ProgressLine",props:{clsPrefix:{type:String,required:!0},percentage:{type:Number,default:0},railColor:String,railStyle:[String,Object],fillColor:[String,Object],status:{type:String,required:!0},indicatorPlacement:{type:String,required:!0},indicatorTextColor:String,unit:{type:String,default:"%"},processing:{type:Boolean,required:!0},showIndicator:{type:Boolean,required:!0},height:[String,Number],railBorderRadius:[String,Number],fillBorderRadius:[String,Number]},setup(e,{slots:f}){const h=w(()=>N(e.height)),v=w(()=>{var o,l;return typeof e.fillColor=="object"?`linear-gradient(to right, ${(o=e.fillColor)==null?void 0:o.stops[0]} , ${(l=e.fillColor)==null?void 0:l.stops[1]})`:e.fillColor}),d=w(()=>e.railBorderRadius!==void 0?N(e.railBorderRadius):e.height!==void 0?N(e.height,{c:.5}):""),t=w(()=>e.fillBorderRadius!==void 0?N(e.fillBorderRadius):e.railBorderRadius!==void 0?N(e.railBorderRadius):e.height!==void 0?N(e.height,{c:.5}):"");return()=>{const{indicatorPlacement:o,railColor:l,railStyle:p,percentage:m,unit:b,indicatorTextColor:x,status:a,showIndicator:C,processing:c,clsPrefix:i}=e;return r(),n("div",{class:s(`${i}-progress-content`),role:"none"},[u("div",{class:s(`${i}-progress-graph`),"aria-hidden":!0},[u("div",{class:s([`${i}-progress-graph-line`,{[`${i}-progress-graph-line--indicator-${o}`]:!0}])},[u("div",{class:s(`${i}-progress-graph-line-rail`),style:S([{backgroundColor:l,height:h.value,borderRadius:d.value},p])},[u("div",{class:s([`${i}-progress-graph-line-fill`,c&&`${i}-progress-graph-line-fill--processing`]),style:S({maxWidth:`${e.percentage}%`,background:v.value,height:h.value,lineHeight:h.value,borderRadius:t.value})},[o==="inside"?(r(),n("div",{key:0,class:s(`${i}-progress-graph-line-indicator`),style:S({color:x})},[f.default?(r(),n(W,{key:0},[y(()=>f.default())],64)):(r(),n(W,{key:1},[y(()=>`${m}${b}`)],64))],6)):y(()=>null)],6)],6)],2)],2),C&&o==="outside"?(r(),n("div",{key:0},[f.default?(r(),n("div",{key:0,class:s(`${i}-progress-custom-content`),style:S({color:x}),role:"none"},[y(()=>f.default())],6)):(r(),n(W,{key:1},[a==="default"?(r(),n("div",{key:0,role:"none",class:s(`${i}-progress-icon ${i}-progress-icon--as-text`),style:S({color:x})},[y(()=>m),y(()=>b)],6)):(r(),n("div",{key:1,class:s(`${i}-progress-icon`),"aria-hidden":!0},[(r(),$(A,{clsPrefix:i},{default:()=>ne[a]},1032,["clsPrefix"]))],2))],64))])):y(()=>null)],2)}}});const ce=["id"],de=["stop-color"],ge=["stop-color"],ue=["d","stroke-width"],fe=["d","stroke-width"],pe=["viewBox"];function T(e,f,h=100){return`m ${h/2} ${h/2-e} a ${e} ${e} 0 1 1 0 ${2*e} a ${e} ${e} 0 1 1 0 -${2*e}`}var he=q({name:"ProgressMultipleCircle",props:{clsPrefix:{type:String,required:!0},viewBoxWidth:{type:Number,required:!0},percentage:{type:Array,default:[0]},strokeWidth:{type:Number,required:!0},circleGap:{type:Number,required:!0},showIndicator:{type:Boolean,required:!0},fillColor:{type:Array,default:()=>[]},railColor:{type:Array,default:()=>[]},railStyle:{type:Array,default:()=>[]}},setup(e,{slots:f}){const h=w(()=>e.percentage.map((d,t)=>`${Math.PI*d/100*(e.viewBoxWidth/2-e.strokeWidth/2*(1+2*t)-e.circleGap*t)*2}, ${e.viewBoxWidth*8}`)),v=(d,t)=>{const o=e.fillColor[t],l=typeof o=="object"?o.stops[0]:"",p=typeof o=="object"?o.stops[1]:"";return typeof e.fillColor[t]=="object"&&(r(),n("linearGradient",{id:`gradient-${t}`,x1:"100%",y1:"0%",x2:"0%",y2:"100%"},[u("stop",{offset:"0%","stop-color":l},null,8,de),u("stop",{offset:"100%","stop-color":p},null,8,ge)],8,ce))};return()=>{const{viewBoxWidth:d,strokeWidth:t,circleGap:o,showIndicator:l,fillColor:p,railColor:m,railStyle:b,percentage:x,clsPrefix:a}=e;return r(),n("div",{class:s(`${a}-progress-content`),role:"none"},[u("div",{class:s(`${a}-progress-graph`),"aria-hidden":!0},[u("div",{class:s(`${a}-progress-graph-circle`)},[(r(),n("svg",{viewBox:`0 0 ${d} ${d}`},[u("defs",null,[y(()=>x.map((C,c)=>v(C,c)))]),y(()=>x.map((C,c)=>(r(),n("g",{key:c},[u("path",{class:s(`${a}-progress-graph-circle-rail`),d:T(d/2-t/2*(1+2*c)-o*c,t,d),"stroke-width":t,"stroke-linecap":"round",fill:"none",style:S([{strokeDashoffset:0,stroke:m[c]},b[c]])},null,14,ue),u("path",{class:s([`${a}-progress-graph-circle-fill`,C===0&&`${a}-progress-graph-circle-fill--empty`]),d:T(d/2-t/2*(1+2*c)-o*c,t,d),"stroke-width":t,"stroke-linecap":"round",fill:"none",style:S({strokeDasharray:h.value[c],strokeDashoffset:0,stroke:typeof p[c]=="object"?`url(#gradient-${c})`:p[c]})},null,14,fe)]))))],8,pe))],2)],2),l&&f.default?(r(),n("div",{key:0},[u("div",{class:s(`${a}-progress-text`)},[y(()=>f.default())],2)])):y(()=>null)],2)}}}),ye=I([g("progress",{display:"inline-block"},[g("progress-icon",`
 color: var(--n-icon-color);
 transition: color .3s var(--n-bezier);
 `),B("line",`
 width: 100%;
 display: block;
 `,[g("progress-content",`
 display: flex;
 align-items: center;
 `,[g("progress-graph",{flex:1})]),g("progress-custom-content",{marginLeft:"14px"}),g("progress-icon",`
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
 `)])]),B("circle, dashboard",{width:"120px"},[g("progress-custom-content",`
 position: absolute;
 left: 50%;
 top: 50%;
 transform: translateX(-50%) translateY(-50%);
 display: flex;
 align-items: center;
 justify-content: center;
 `),g("progress-text",`
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
 `),g("progress-icon",`
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
 `,[g("progress-text",`
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
 `)]),g("progress-content",{position:"relative"}),g("progress-graph",{position:"relative"},[g("progress-graph-circle",[I("svg",{verticalAlign:"bottom"}),g("progress-graph-circle-fill",`
 stroke: var(--n-fill-color);
 transition:
 opacity .3s var(--n-bezier),
 stroke .3s var(--n-bezier),
 stroke-dasharray .3s var(--n-bezier);
 `,[B("empty",{opacity:0})]),g("progress-graph-circle-rail",`
 transition: stroke .3s var(--n-bezier);
 overflow: hidden;
 stroke: var(--n-rail-color);
 `)]),g("progress-graph-line",[B("indicator-inside",[g("progress-graph-line-rail",`
 height: 16px;
 line-height: 16px;
 border-radius: 10px;
 `,[g("progress-graph-line-fill",`
 height: inherit;
 border-radius: 10px;
 `),g("progress-graph-line-indicator",`
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
 `,[g("progress-graph-line-rail",`
 flex: 1;
 transition: background-color .3s var(--n-bezier);
 `),g("progress-graph-line-indicator",`
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
 `)]),g("progress-graph-line-rail",`
 position: relative;
 overflow: hidden;
 height: var(--n-rail-height);
 border-radius: 5px;
 background-color: var(--n-rail-color);
 transition: background-color .3s var(--n-bezier);
 `,[g("progress-graph-line-fill",`
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
 `)]);const ve=["aria-valuenow","role"],me={...V.props,processing:Boolean,type:{type:String,default:"line"},gapDegree:Number,gapOffsetDegree:Number,status:{type:String,default:"default"},railColor:[String,Array],railStyle:[String,Array],color:[String,Array,Object],viewBoxWidth:{type:Number,default:100},strokeWidth:{type:Number,default:7},percentage:[Number,Array],unit:{type:String,default:"%"},showIndicator:{type:Boolean,default:!0},indicatorPosition:{type:String,default:"outside"},indicatorPlacement:{type:String,default:"outside"},indicatorTextColor:String,circleGap:{type:Number,default:1},height:Number,borderRadius:[String,Number],fillBorderRadius:[String,Number],offsetDegree:Number};var xe=q({name:"Progress",props:me,setup(e){const f=w(()=>e.indicatorPlacement||e.indicatorPosition),h=w(()=>{if(e.gapDegree||e.gapDegree===0)return e.gapDegree;if(e.type==="dashboard")return 75}),{mergedClsPrefixRef:v,inlineThemeDisabled:d}=J(e),t=V("Progress","-progress",ye,Z,e,v),o=w(()=>{const{status:p}=e,{common:{cubicBezierEaseInOut:m},self:{fontSize:b,fontSizeCircle:x,railColor:a,railHeight:C,iconSizeCircle:c,iconSizeLine:i,textColorCircle:_,textColorLineInner:z,textColorLineOuter:k,lineBgProcessing:P,fontWeightCircle:R,[L("iconColor",p)]:j,[L("fillColor",p)]:D}}=t.value;return{"--n-bezier":m,"--n-fill-color":D,"--n-font-size":b,"--n-font-size-circle":x,"--n-font-weight-circle":R,"--n-icon-color":j,"--n-icon-size-circle":c,"--n-icon-size-line":i,"--n-line-bg-processing":P,"--n-rail-color":a,"--n-rail-height":C,"--n-text-color-circle":_,"--n-text-color-line-inner":z,"--n-text-color-line-outer":k}}),l=d?K("progress",w(()=>e.status[0]),o,e):void 0;return{mergedClsPrefix:v,mergedIndicatorPlacement:f,gapDeg:h,cssVars:d?void 0:o,themeClass:l==null?void 0:l.themeClass,onRender:l==null?void 0:l.onRender}},render(){const{type:e,cssVars:f,indicatorTextColor:h,showIndicator:v,status:d,railColor:t,railStyle:o,color:l,percentage:p,viewBoxWidth:m,strokeWidth:b,mergedIndicatorPlacement:x,unit:a,borderRadius:C,fillBorderRadius:c,height:i,processing:_,circleGap:z,mergedClsPrefix:k,gapDeg:P,gapOffsetDegree:R,themeClass:j,$slots:D,onRender:O}=this;return O==null||O(),r(),n("div",{class:s([j,`${k}-progress`,`${k}-progress--${e}`,`${k}-progress--${d}`]),style:S(f),"aria-valuemax":100,"aria-valuemin":0,"aria-valuenow":p,role:e==="circle"||e==="line"||e==="dashboard"?"progressbar":"none"},[e==="circle"||e==="dashboard"?(r(),$(se,{key:0,clsPrefix:k,status:d,showIndicator:v,indicatorTextColor:h,railColor:t,fillColor:l,railStyle:o,offsetDegree:this.offsetDegree,percentage:p,viewBoxWidth:m,strokeWidth:b,gapDegree:P===void 0?e==="dashboard"?75:0:P,gapOffsetDegree:R,unit:a},G(D),1032,["clsPrefix","status","showIndicator","indicatorTextColor","railColor","fillColor","railStyle","offsetDegree","percentage","viewBoxWidth","strokeWidth","gapDegree","gapOffsetDegree","unit"])):(r(),n(W,{key:1},[e==="line"?(r(),$(ae,{key:0,clsPrefix:k,status:d,showIndicator:v,indicatorTextColor:h,railColor:t,fillColor:l,railStyle:o,percentage:p,processing:_,indicatorPlacement:x,unit:a,fillBorderRadius:c,railBorderRadius:C,height:i},G(D),1032,["clsPrefix","status","showIndicator","indicatorTextColor","railColor","fillColor","railStyle","percentage","processing","indicatorPlacement","unit","fillBorderRadius","railBorderRadius","height"])):(r(),n(W,{key:1},[e==="multiple-circle"?(r(),$(he,{key:0,clsPrefix:k,strokeWidth:b,railColor:t,fillColor:l,railStyle:o,viewBoxWidth:m,percentage:p,showIndicator:v,circleGap:z},G(D),1032,["clsPrefix","strokeWidth","railColor","fillColor","railStyle","viewBoxWidth","percentage","showIndicator","circleGap"])):y(()=>null)],64))],64))],14,ve)}});export{xe as P};
