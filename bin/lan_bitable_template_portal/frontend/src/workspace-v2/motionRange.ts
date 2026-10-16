import type { ObjectDirective } from 'vue';

function updateRange(input:HTMLInputElement) {
  const min=input.min===''?0:Number(input.min),max=input.max===''?100:Number(input.max);
  const value=Number(input.value),percent=max>min?Math.max(0,Math.min(100,(value-min)/(max-min)*100)):0;
  input.style.setProperty('--val',percent+'%');
}
function onInput(event:Event){updateRange(event.currentTarget as HTMLInputElement);}
export const vMotionRange:ObjectDirective<HTMLInputElement>={
  mounted(input){updateRange(input);input.addEventListener('input',onInput);input.addEventListener('change',onInput);},
  updated: updateRange,
  beforeUnmount(input){input.removeEventListener('input',onInput);input.removeEventListener('change',onInput);},
};
