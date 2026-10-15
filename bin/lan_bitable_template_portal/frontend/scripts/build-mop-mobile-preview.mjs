import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { build } from 'vite';
import vue from '@vitejs/plugin-vue';

// Compile only the real MOP page and its dependencies while other modules are being edited.
export async function buildMopPreview() {
  const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
  const outDir = path.resolve(root, '../../../output/mop-mobile-isolated');
  await build({ root, configFile: false, logLevel: 'error', publicDir: false, plugins: [{
    name: 'isolated-mop-preview',
    enforce: 'post',
    resolveId(id) { if (id === 'virtual:mop-preview') return '\0mop-preview'; },
    load(id) {
      if (id !== '\0mop-preview') return;
      return `import {createApp} from 'vue';
        import Mop from '/src/components/EngineerMopPage.vue';
        import WorkspaceMop from '/src/workspace-v2/MopWorkspace.vue';
        import LoadingIndicator from '/src/components/LoadingIndicator.vue';
        import UiTransition from '/src/components/UiTransition.vue';
        import {installFeedbackMotion} from '/src/workspace-v2/feedbackMotion';
        import '/src/global.css'; import '/src/workspace-v2/workspace.css'; import '/src/workspace-v2/feedbackToasts.css';
        const v2=new URLSearchParams(location.search).get('version')==='v2';
        if(v2){document.documentElement.classList.add('workspace-v2');installFeedbackMotion();}
        createApp(v2?WorkspaceMop:Mop,{checking:false,loggedIn:true,loginUrl:'/',scopeOptions:[{value:'A',label:'A楼'}]})
          .component('LoadingIndicator',LoadingIndicator).component('UiTransition',UiTransition).mount('#app');`;
    },
    generateBundle(_options, bundle) {
      const entry = Object.values(bundle).find(item => item.type === 'chunk' && item.isEntry);
      const css = Object.values(bundle).filter(item => item.fileName.endsWith('.css'));
      this.emitFile({ type: 'asset', fileName: 'index.html', source: `<!doctype html><html lang="zh-CN"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>维护单手机填写预览</title>${css.map(item => `<link rel="stylesheet" href="/${item.fileName}">`).join('')}</head><body style="margin:0"><div id="page-back-slot"></div><div id="app"></div><script type="module" src="/${entry.fileName}"></script></body></html>` });
    },
  }, vue()], build: { outDir, emptyOutDir: true, cssCodeSplit: false, rollupOptions: { input: 'virtual:mop-preview' } } });
  return outDir;
}
