import assert from 'node:assert/strict';
import {build} from 'esbuild';
import {chromium} from 'playwright';
import {fileURLToPath} from 'node:url';

const helper=await build({entryPoints:[fileURLToPath(new URL('../src/workspace-v2/cardActivation.ts',import.meta.url))],bundle:true,write:false,format:'iife',globalName:'cards'});
const browser=await chromium.launch({headless:true});
try{
  const page=await browser.newPage();
  await page.setContent('<article style="padding:30px;width:400px"><p>可以选择的通告说明</p><button>打开详情</button><label><input type="checkbox">选择结果</label><a href="#">附件</a><button disabled>不可用</button></article>');
  await page.addScriptTag({content:helper.outputFiles[0].text});
  await page.evaluate(()=>{
    window.opened=0;window.activated=0;window.locked=false;
    const card=document.querySelector('article');
    card.addEventListener('click',event=>{if(!window.locked&&cards.activateCard(event))window.opened++;});
    card.querySelector('button').addEventListener('click',()=>window.activated++);
    card.querySelector('a').addEventListener('click',event=>event.preventDefault());
  });
  await page.locator('article').click({position:{x:5,y:5}});
  assert.equal(await page.evaluate(()=>window.opened),1);
  await page.getByRole('button',{name:'打开详情',exact:true}).click();
  assert.equal(await page.evaluate(()=>window.activated),1);
  await page.getByText('选择结果',{exact:false}).click();
  assert(await page.getByRole('checkbox').isChecked());
  await page.getByRole('link',{name:'附件',exact:true}).click();
  await page.locator('article').click({position:{x:5,y:5},modifiers:['Control']});
  await page.evaluate(()=>{const range=document.createRange();range.selectNodeContents(document.querySelector('p'));const selection=getSelection();selection.removeAllRanges();selection.addRange(range);document.querySelector('p').click();});
  assert.equal(await page.evaluate(()=>window.opened),1,'controls, links, modified clicks and text selection do not open another editor');
  await page.evaluate(()=>{getSelection().removeAllRanges();window.locked=true;});
  await page.locator('article').click({position:{x:5,y:5}});
  assert.equal(await page.evaluate(()=>window.opened),1,'disabled cards cannot open');
  console.log('PASS: card whitespace activates once; nested actions, checkbox labels, attachment links, modified clicks, selection and disabled state stay independent.');
}finally{await browser.close();}
