<template><div class="tool-desk"><slot/></div></template>
<style>
.tool-desk {
  --td-ink:var(--v2-ink,#203b39); --td-muted:color-mix(in srgb,var(--td-ink) 73%,var(--soft-face,#fff));
  --td-face:var(--soft-face,#fcfdfb); --td-field:var(--soft-field,#edf3f1);
  --td-line:color-mix(in srgb,var(--td-ink) 15%,var(--td-face));
  --td-green:#176449; --td-mint:color-mix(in srgb,var(--td-face) 80%,#82dbb7);
  --td-blue:var(--v2-blue,#306986); --td-sky:var(--v2-palette-selection,#dfedf6);
  color:var(--td-ink);font:inherit;line-height:1.55;min-width:0;max-width:100%;container-type:inline-size;
}
.tool-desk *, .tool-desk *::before,.tool-desk *::after {box-sizing:border-box}
.tool-desk :is(h1,h2,h3,h4,p){margin:0}
.tool-desk h1{font-size:clamp(1.5rem,4cqi,2.7rem);letter-spacing:-.05em;line-height:1.16}
.tool-desk h2{font-size:1.18rem;letter-spacing:-.025em}.tool-desk h3{font-size:1rem}
.tool-desk small,.tool-desk .td-muted{color:var(--td-muted);font-size:.85rem}
.tool-desk :is(button,a,summary,input,textarea,select):focus-visible{outline:3px solid var(--td-blue);outline-offset:2px}
.tool-desk :is(button,.td-button),.tool-desk .td-link{
  display:inline-flex;align-items:center;justify-content:center;gap:.4rem;min-height:38px;padding:.4rem .85rem;
  border:1px solid var(--td-line);border-radius:10px;background:var(--td-face);color:var(--td-ink);
  font:inherit;font-size:.89rem;font-weight:600;line-height:1.35;text-decoration:none;cursor:pointer;
}
.tool-desk button:hover:enabled,.tool-desk a:hover{border-color:var(--td-blue)}
.tool-desk button:disabled,.tool-desk fieldset:disabled button{opacity:.54;cursor:default}
.tool-desk .td-primary,.tool-desk button.td-primary{background:var(--v2-palette-accent);color:var(--v2-palette-on-accent);border-color:var(--v2-palette-accent)}
.tool-desk .td-link{border-color:transparent;background:transparent;color:var(--td-blue);padding-inline:.4rem}
.tool-desk .td-icon{aspect-ratio:1;padding:.45rem}.tool-desk .td-danger{color:#a33e31}
.tool-desk :is(input:not([type=checkbox]):not([type=radio]),select,textarea){
  min-height:40px;max-width:100%;min-width:0;padding:.5rem .7rem;border:1px solid var(--td-line);border-radius:10px;
  color:var(--td-ink);background-color:var(--td-field);box-shadow:var(--soft-inset,inset 1px 2px 3px #274a3d0c);font:inherit;font-size:.92rem
}
.tool-desk textarea{resize:vertical;min-height:4.3rem}.tool-desk input[type=checkbox]{width:17px;height:17px;accent-color:var(--td-green)}
.tool-desk .td-page,.tool-desk .td-stack,.tool-desk .td-field{display:grid;gap:1rem;min-width:0}
.tool-desk .td-field{align-content:start;gap:.35rem;font-size:.88rem;font-weight:600}
.tool-desk .td-page{width:100%;padding-block:clamp(5px,.7cqi,14px)}
.tool-desk .td-panel,.tool-desk.td-dialog{padding:clamp(14px,2.3cqi,26px);border:1px solid var(--td-line);border-radius:20px;
  background:var(--td-face);box-shadow:var(--soft-raised,-3px -3px 10px #ffffffa0,4px 5px 12px #3a5a5012)}
.tool-desk .td-panel{display:grid;align-content:start;gap:1rem;min-width:0}
.tool-desk .td-heading,.tool-desk .td-row,.tool-desk .td-actions,.tool-desk .td-check{display:flex;align-items:center;gap:.6rem;flex-wrap:wrap}
.tool-desk .td-heading,.tool-desk .td-row{justify-content:space-between}.tool-desk .td-actions{gap:.45rem}
.tool-desk .td-heading > div,.tool-desk .td-row > div{min-width:0}
.tool-desk .td-list{display:grid;min-width:0}
.tool-desk .td-list > *{padding:.8rem 0;border-block-start:1px solid var(--td-line);overflow-wrap:anywhere}
.tool-desk .td-notice,.tool-desk .td-empty{padding:.75rem 1rem;background:var(--td-sky);border-radius:12px;overflow-wrap:anywhere}
.tool-desk .td-empty{text-align:center;color:var(--td-muted);background:var(--td-field)}
.tool-desk .td-error{border-inline-start:4px solid #be6252}
.tool-desk .td-chip{display:inline-flex;align-items:center;min-height:25px;padding:.12rem .55rem;background:var(--td-mint);border-radius:7px;font-size:.8rem}
.tool-desk .td-tabs{display:flex;flex-wrap:wrap;gap:.4rem}
.tool-desk .td-tabs button{background:var(--td-field);border:1px solid transparent}
.tool-desk .td-tabs [aria-pressed=true],.tool-desk .td-tabs [aria-current=page]{background:var(--td-mint);border-color:color-mix(in srgb,var(--td-green) 50%,var(--td-face))}
.tool-desk .td-cols{display:grid;grid-template-columns:minmax(0,1.6fr) minmax(235px,1fr);gap:1rem;align-items:start}
.tool-desk .td-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,195px),1fr));gap:.85rem}
.tool-desk fieldset{border:0;padding:0;margin:0;min-width:0}
.tool-desk summary{cursor:pointer;font-weight:600}
.tool-desk .td-count{font-variant-numeric:tabular-nums}
.tool-desk.td-dialog{width:min(640px,calc(100vw - 24px));max-height:88dvh;overflow:auto;display:grid;gap:1rem}
.tool-desk .td-page-title{padding:.25rem 0}.tool-desk .td-page-title h1{font-size:clamp(1.35rem,2.3cqi,1.85rem);letter-spacing:-.025em}
.tool-desk .td-record-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,27rem),1fr));gap:.6rem}
.tool-desk .td-record-grid>article{min-width:0;padding:.8rem;border:1px solid var(--td-line);border-radius:12px;overflow-wrap:anywhere}
.tool-desk .td-divider{border:0;border-top:1px solid var(--td-line);margin:0}
.tool-desk img{max-width:100%;object-fit:contain}
@container(max-width:700px){.tool-desk .td-cols{grid-template-columns:minmax(0,1fr)}.tool-desk .td-panel{border-radius:15px}}
@container(max-width:420px){.tool-desk .td-row{align-items:flex-start}.tool-desk .td-actions{gap:.35rem}.tool-desk .td-grid{grid-template-columns:minmax(0,1fr)}}
</style>
