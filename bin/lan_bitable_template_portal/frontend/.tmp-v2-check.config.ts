import { defineConfig } from 'vite';
import vue from '@vitejs/plugin-vue';
import { fileURLToPath, URL } from 'node:url';
import { existsSync, readFileSync, readdirSync, unlinkSync } from 'node:fs';
import { resolve, join } from 'node:path';
// @ts-ignore build helper is plain ESM, separately covered by the route contract test.
import { adaptBrowserReferences } from './scripts/workspace-build-adapter.mjs';
// @ts-ignore isolated SFC adaptation is validated by the workspace event tests.
import { adaptEventPeriodPage } from './scripts/workspace-event-adapter.mjs';
// @ts-ignore V2-only presentation adapter, covered by browser tests.
import { adaptCabinetBatchPage, adaptCabinetPage } from './scripts/workspace-cabinet-adapter.mjs';
// @ts-ignore marks binary settings only, never selection groups.
import { markBinaryControls, adaptPasswordLogin, fuseLearningCharts, portalFloatingControls, adaptMotionControls } from './scripts/workspace-controls-adapter.mjs';
// @ts-ignore V2-only response adaptation; the legacy client source stays unchanged.
import { adaptNoticeParserClient,adaptNoticeSopControls } from './scripts/workspace-notice-adapter.mjs';
// @ts-ignore V2-only alert metadata; source components and business states stay unchanged.
import { markFeedbackAlerts } from './scripts/workspace-feedback-adapter.mjs';
// @ts-ignore build-only controller reuse is checked with compiler and browser fixtures.
import { adaptRepairController, controllerBindings } from './scripts/workspace-repair-adapter.mjs';
// @ts-ignore V2-only learning presentation; API and grading stay in the existing controller.
import { adaptLearningPage } from './scripts/workspace-learning-adapter.mjs';
// @ts-ignore V2-only sharing layout, retaining the existing data controller.
import { adaptLearningSharing } from './scripts/workspace-sharing-adapter.mjs';
// @ts-ignore build-only admin navigation presentation, original permissions are preserved.
import { adaptAccounts, adaptAdminAccountLoading, adaptModuleAdminEntry } from './scripts/workspace-admin-adapter.mjs';
// @ts-ignore V2-only filter placement; original form bindings and legacy views stay intact.
import { adaptPageFilters } from './scripts/workspace-filters-adapter.mjs';
// @ts-ignore V2-only native single-select bridge keeps original form handlers.
import { adaptSingleSelects } from './scripts/workspace-select-adapter.mjs';
// @ts-ignore reader presentation retains existing document/version APIs.
import { adaptKnowledgeReader, adaptKnowledgeFolders } from './scripts/workspace-knowledge-adapter.mjs';
// @ts-ignore isolated presentation adapters are compiler and browser checked.
import { adaptCompactWorkspaces } from './scripts/workspace-compact-adapter.mjs';
// @ts-ignore account grouping does not extend the existing permissions.
import { adaptAccountPresentation } from './scripts/workspace-account-presentation-adapter.mjs';
// @ts-ignore V2-only browser location presentation.
import { adaptBrowserWeather } from './scripts/workspace-weather-adapter.mjs';
// @ts-ignore V2-only layout slots preserve the existing business controllers.
import { adaptPageLayouts } from './scripts/workspace-page-layout-adapter.mjs';
// @ts-ignore V2-only drill and guide presentation, covered by isolated browser checks.
import { adaptDrillPresentation } from './scripts/workspace-drill-adapter.mjs';
// @ts-ignore scopes reused module navigation when mounted as a dashboard component.
import { adaptEmbeddedNavigation } from './scripts/workspace-embedded-adapter.mjs';
// @ts-ignore V2-only history import view retains the original scan/save controller.
import { adaptHistoryMemory } from './scripts/workspace-history-adapter.mjs';
// @ts-ignore compile-time night palette leaves the legacy build untouched.
import workspaceNightTheme from './scripts/workspace-night-theme.mjs';
const root = fileURLToPath(new URL('.', import.meta.url)).replace(/\\/g, '/');
const outputDir=process.env.CLIPFLOW_WORKSPACE_BUILD_DIR?resolve(root,process.env.CLIPFLOW_WORKSPACE_BUILD_DIR):resolve(root,'dist/assets/workspace-v2');
/* guard off */
let generated=new Set<string>();
export default defineConfig({
  root, publicDir:false, base: '/assets/workspace-v2/',
  css:{postcss:{plugins:[workspaceNightTheme()]}},
  preview: { proxy: { '/api': 'http://127.0.0.1:18766', '/workbench-lite': 'http://127.0.0.1:18766', '/sop-work-order': 'http://127.0.0.1:18766' } },
  plugins: [{
    name: 'workspace-business-imports', enforce: 'pre',
    resolveId(source, importer) {
      if(importer?.replace(/\\/g,'/').startsWith(root+'src/')&&source.endsWith('/VnetSelect.vue')&&!importer.includes('/workspace-v2/SingleSelect.vue'))return root+'src/workspace-v2/SingleSelect.vue';
      if (importer?.replace(/\\/g, '/').startsWith(root + 'src/') && source.endsWith('/VnetBackButton.vue')) return root + 'src/workspace-v2/ModuleBackButton.vue';
      if (importer?.replace(/\\/g, '/').startsWith(root + 'src/') && source.endsWith('/AdminWorkspaceChrome.vue')) return root + 'src/workspace-v2/AdminSettingsPicker.vue';
      if (importer?.replace(/\\/g, '/').startsWith(root + 'src/') && source.endsWith('/WaterManagementPage.vue')) return root + 'src/workspace-v2/WaterWorkspace.vue';
      if (/^\/assets\/[\w.-]+\.(?:png|svg|webp|jpg|gif)$/.test(source) && existsSync(join(root,'public',source))) return '\0workspace-public:' + source;
      if (importer?.replace(/\\/g, '/').startsWith(root + 'src/') && source.endsWith('/LighthouseReply.vue') && !importer.includes('/workspace-v2/')) return root + 'src/workspace-v2/AnimatedReply.vue';
      if (importer?.replace(/\\/g, '/').startsWith(root + 'src/') && source.endsWith('/LighthouseBot.vue') && !importer.includes('/workspace-v2/')) return root + 'src/workspace-v2/CompactBot.vue';
      if (importer?.replace(/\\/g, '/').startsWith(root + 'src/') && /(?:^|\/)navigation(?:\.ts)?$/.test(source) && !importer.includes('/workspace-v2/')) return root + 'src/workspace-v2/navigation.ts';
      if (importer?.replace(/\\/g, '/').startsWith(root + 'src/') && /(?:^|\/)api\/client(?:\.ts)?$/.test(source) && !importer.includes('/workspace-v2/client.ts')) return root + 'src/workspace-v2/client.ts';
    },
    load(id){if(id.startsWith('\0workspace-public:'))return 'export default '+JSON.stringify(id.slice('\0workspace-public:'.length));},
    transform(code,id){
      const path=id.replace(/\\/g,'/');
      if(path===root+'src/api/client.ts')return {code:adaptNoticeParserClient(code),map:null};
      if(!path.startsWith(root+'src/')||!path.endsWith('.vue'))return;
      let result=code;
      if(path===root+'src/components/HistoryMemoryPage.vue')result=adaptHistoryMemory(result,readFileSync(join(root,'src/workspace-v2/HistoryMemoryWorkspace.vue'),'utf8'));
      if(path===root+'src/components/LearningPage.vue')result=adaptLearningPage(result);
      if(path===root+'src/components/LearningPage.vue')result=result.replace('<VnetBackButton :to="undefined"', '<VnetBackButton :within-module="viewMode === \'overview\' && !!learner" :to="undefined"');
      if(path===root+'src/components/EventManagementPage.vue')result=result.replace('<VnetBackButton v-if="showingDetails"', '<VnetBackButton within-module v-if="showingDetails"');
      if(['MopPreviewHeader.vue','RepairStatusPage.vue','AuthPanels.vue'].some(name=>path===root+'src/components/'+name))result=result.replaceAll('<VnetBackButton ', '<VnetBackButton within-module ');
      if(path===root+'src/components/AuthPanels.vue')result=result.replace('title="返回首页"', 'title="返回个人页面"');
      if(path===root+'src/components/AdminTools.vue')result=result.replace('<button type="button" class="btn ghost" @click="$emit(\'close\')"><ArrowLeft :size="17" />返回首页</button>', '');
      if(path===root+'src/components/LearningSharing.vue')result=adaptLearningSharing(result);
      result=adaptModuleAdminEntry(result,path);
      result=adaptDrillPresentation(result,path);
      result=adaptKnowledgeReader(result,path);
      result=adaptKnowledgeFolders(result,path);
      result=adaptBrowserWeather(result,path)||result;
      if(path===root+'src/components/AdminAccounts.vue')result=adaptAccounts(result);
      if(path===root+'src/components/AdminTools.vue')result=adaptAdminAccountLoading(result);
      if(path===root+'src/components/RepairManagementPage.vue')result=adaptRepairController(result,'management',controllerBindings(readFileSync(join(root,'src/workspace-v2/RepairWorkspace.vue'),'utf8')));
      if(path===root+'src/components/RepairFollowupPanel.vue')result=adaptRepairController(result,'followup',controllerBindings(readFileSync(join(root,'src/workspace-v2/RepairFollowups.vue'),'utf8')));
      if(path===root+'src/components/EventManagementPage.vue')result=adaptEventPeriodPage(result);
      if(path===root+'src/components/CabinetPowerBatchPage.vue')result=adaptCabinetBatchPage(result);
      if(path===root+'src/components/CabinetPowerPage.vue')result=adaptCabinetPage(result);
      if(path===root+'src/components/PersonnelPasswordLogin.vue')result=adaptPasswordLogin(result);
      if(path===root+'src/components/LearningDashboard.vue')result=fuseLearningCharts(result);
      if(path===root+'src/components/LighthouseNoticeSop.vue')result=adaptNoticeSopControls(result);
      result=adaptCompactWorkspaces(result,path);
      result=adaptPageFilters(result,path);
      result=adaptPageLayouts(result,path);
      result=adaptAccountPresentation(result,path);
      result=adaptEmbeddedNavigation(result,path);
      result=markBinaryControls(result);
      result=adaptMotionControls(result);
      result=markFeedbackAlerts(result);
      result=portalFloatingControls(result);
      result=adaptSingleSelects(result,path);
      if(result!==code)return {code:result,map:null};
    },
  }, vue(), {
    name: 'workspace-readonly-business-adapter', enforce: 'post',
    transform(code, id) {
      const path = id.replace(/\\/g, '/');
      if (!path.startsWith(root + 'src/') || path.includes('/workspace-v2/') || path.includes('type=style') || !/\.(?:vue|ts)(?:\?|$)/.test(path)) return null;
      const result = adaptBrowserReferences(code, id, root + 'src/workspace-v2/browser.ts');
      return result ? { code: result, map: null } : null;
    },
    generateBundle(_options, bundle) {
      const entry = Object.values(bundle).find(item => item.type === 'chunk' && item.isEntry);
      if (!entry) throw new Error('Workspace entry is missing');
      const css = Object.values(bundle).filter(item => item.type === 'asset' && item.fileName.endsWith('.css')).map(item => item.fileName);
      this.emitFile({ type: 'asset', fileName: 'entry.json', source: JSON.stringify({ entry: entry.fileName, css }) });
      this.emitFile({ type: 'asset', fileName: 'index.html', source: readFileSync(new URL('./src/workspace-v2/entry.html', import.meta.url), 'utf8') });
      generated=new Set(Object.keys(bundle));
    },
    writeBundle(options){
      const expected=outputDir;
      /* guard off */
      for(const name of readdirSync(expected))if(/^(?:app|part|res)-[\w-]+\.(?:js|css)$/.test(name)&&!generated.has(name))unlinkSync(join(expected,name));
    },
  }],
  build: { outDir: outputDir, emptyOutDir: true, cssCodeSplit: false,
    // Fresh names prevent older installers from pruning this generation on their first upgrade.
    rollupOptions: { input: root + 'src/workspace-v2/main.ts', output: { entryFileNames: 'app-v2-[hash].js', chunkFileNames: 'part-v2-[hash].js', assetFileNames: 'res-v2-[hash][extname]' } } },
});
