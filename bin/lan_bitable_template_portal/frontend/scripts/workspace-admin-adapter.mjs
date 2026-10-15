import { parse } from '@vue/compiler-sfc';

export function adaptAccounts(source) {
  const { descriptor } = parse(source), edits = [];
  function visit(node) {
    const classes = node.props?.find(p=>p.type===6&&p.name==='class')?.value?.content.split(' ') || [];
    if (node.type===1 && classes.includes('metrics')) {
      edits.push([node.loc.start.offset,node.loc.end.offset,`<div class="v2-account-scope"><strong>{{levelLabel(myLevel)}}</strong><span>管理范围：{{scopesLabel(policy.scope_rules || [])}}</span><span>{{items.length}} 个可管理账号</span><span v-if="pendingCount">{{pendingCount}} 条待审批</span></div>`]);return;
    }
    if (node.type===1 && classes.includes('category-tabs')) { edits.push([node.loc.start.offset,node.loc.end.offset,'']);return; }
    if (node.type===1 && classes.includes('filters')) {
      const at=node.loc.end.offset-'</div>'.length;
      edits.push([at,at,`<label><span>权限级别</span><select v-model="v2Level" aria-label="筛选权限级别"><option value="">全部级别</option><option v-for="level in [...new Set(items.map(item=>item.level))].filter(Boolean).sort()" :key="level" :value="level">{{levelLabel(level)}}</option></select></label><label><span>岗位</span><select v-model="category" aria-label="筛选岗位"><option v-for="option in categoryTabs" :key="option.value" :value="option.value">{{option.label}} · {{option.count}}</option></select></label>`]);
    }
    if (node.type===1 && classes.includes('account-header')) {
      const at=node.loc.end.offset-'</header>'.length;
      edits.push([at,at,`<button v-if="myLevel && myLevel !== 'L0'" type="button" :disabled="saving" @click="v2RequestAccess"><ShieldCheck :size="16" />申请修改本人权限</button>`]);
    }
    for (const child of node.children||[]) visit(child);
  }
  visit(descriptor.template.ast);
  if (edits.length!==4) throw new Error('Account V2 sections changed');
  for(const [start,end,text] of edits.sort((a,b)=>b[0]-a[0])) source=source.slice(0,start)+text+source.slice(end);
  source=source.replace('<script setup lang="ts">',`<script setup lang="ts">
import { ShieldCheck } from 'lucide-vue-next';
import { useDialog as useV2Dialog, useMessage as useV2Message } from 'naive-ui';
import PermissionReview from '../workspace-v2/PermissionReview.vue';
import { navigate as v2Navigate, registerNavigationGuard } from '../workspace-v2/navigation';`);
  source=source.replace("const search = ref(''),", "const v2Level=ref('');\nconst search = ref(''),");
  source=source.replace('items.value.filter(x => (!search.value', 'items.value.filter(x => (!v2Level.value || x.level === v2Level.value) && (!search.value');
  source=source.replace('当前级别只能查看该账号，不能修改。角色与可访问楼栋由HR统一管理。', '该账号权限受保护，不能在此直接修改；本人权限通过上级审批生效。');
  source=source.replace('const own = new Set(draft.value.own_scopes || []);', 'const own = new Set(policy.value.scope_rules || []);');
  source=source.replace('own.has(option.value) || option.value === draft.value?.home_scope', 'own.has(option.value)');
  source=source.replace("return categoryOptions.value.find(x => x.value === value)?.label || value || '待分类';", "return categoryOptions.value.find(x => x.value === value)?.label || ({manager:'经理',supervisor:'主管',engineer:'工程师',operator:'值班员',shift_leader:'值班长',duty:'值班账号',other:'待分类'} as Record<string,string>)[value] || '待分类';");
  const patch=(before,after)=>{if(!source.includes(before))throw new Error('Account workflow contract changed: '+before.slice(0,80));source=source.replace(before,after);};
  patch('<slot v-else-if="tab === \'requests\'" />', '<PermissionReview v-else-if="tab === \'requests\'" :scopes="scopeOptions" :modules="moduleOptions" :categories="categoryOptions" @changed="load();emit(\'changed\')" />');
  patch("if (level !== 'L3') list.push", 'list.push');
  patch('if (isSuperAdmin.value) return options;', "if (isSuperAdmin.value) return options.filter(option=>option.value!=='ALL'||draft.value?.level==='L1');");
  patch("function closeEditor() { if (saving.value) return;", "function closeEditor(force: unknown = false) { if (saving.value && force !== true) return; if(force !== true && v2Dirty.value){v2Discard(()=>closeEditor(true));return;} v2Baseline.value=''; v2Stale.value=false;");
  patch("saving.value = false; closeEditor(); await load();", "saving.value = false; closeEditor(true); await load();");
  patch('if (!fresh || !canEdit(fresh)) closeEditor();', 'if (!fresh || !canEdit(fresh)) closeEditor(true);');
  patch("} catch (e: any) { if (!abort.signal.aborted) error.value = e.message || '账号加载失败'; }", "} catch (e: any) { if (!abort.signal.aborted) {error.value = e.message || '账号加载失败'; if([401,403].includes(e.status)){items.value=[];audits.value=[];closeEditor(true);}} }");
  const restoreStart=source.indexOf('    // A rejected save must keep'), restoreEnd=source.indexOf('\n  }\n  finally { saving.value = false; }',restoreStart);
  if(restoreStart<0||restoreEnd<0)throw new Error('Account save failure contract changed');
  source=source.slice(0,restoreStart)+`    if([401,403,409].includes(e.status)||!e.status||e.status>=500){v2Stale.value=true;editorError.value+=' 请重新读取账号状态后再处理。';}
`+source.slice(restoreEnd);
  patch("  draftOriginal = {", "  v2Stale.value=false; v2Baseline.value=JSON.stringify(accessChange(draft.value));\n  draftOriginal = {");
  patch('<p v-if="editorError" class="error" role="alert">{{ editorError }}</p>', '<p v-if="editorError" class="error" role="alert">{{ editorError }} <button v-if="v2Stale" type="button" :disabled="saving" @click="v2ReloadEditor">重新读取账号</button></p>');
  patch('class="account-editor" role="dialog"', 'class="account-editor" data-v2-inline-feedback role="dialog"');
  patch(':disabled="saving" @click="saveAccess"', ':disabled="saving || v2Stale || !v2AccessDirty" @click="saveAccess"');
  patch('@click="saveEnabled"', ':disabled="v2Stale || v2PermissionsDirty || !v2AccessDirty" @click="saveEnabled"');
  patch('if (draft.value) void savePermissions([accessChange(draft.value)])', 'if (draft.value && !v2Stale.value && canEditAccess(draft.value)) void savePermissions([accessChange(draft.value)])');
  patch('if (draft.value) void savePermissions([{ ...permissionChange(draft.value)', 'if (draft.value && !v2Stale.value && canEdit(draft.value)) void savePermissions([{ ...permissionChange(draft.value)');
  patch("if (!draft.value || saving.value) return;", "if (!draft.value || saving.value || v2Stale.value) return;");
  patch("} catch (e: any) { editorError.value = e.message || '密码修改未完成'; }", "} catch (e: any) { editorError.value = e.message || '密码修改未完成'; if([401,403,409].includes(e.status)||!e.status||e.status>=500){v2Stale.value=true;editorError.value+=' 请先核验账号状态，不要重复修改密码。';} }");
  patch('<button type="submit" class="primary"><KeyRound', '<button type="submit" class="primary" :disabled="v2Stale"><KeyRound');
  patch('function escape(event: KeyboardEvent) {', 'function escape(event: KeyboardEvent) {\n  if(v2ConfirmOpen)return;');
  patch('function switchTab(value: string) { tab.value', "function switchTab(value: string) { if(saving.value)return;notice.value='';error.value='';tab.value");
  source=source.replace('</script>', `
const v2Dialog=useV2Dialog(),v2Message=useV2Message(),v2Baseline=ref(''),v2Stale=ref(false);
const v2AccessDirty=computed(()=>!!draft.value && JSON.stringify(accessChange(draft.value))!==v2Baseline.value);
const v2PermissionsDirty=computed(()=>!!draft.value && !!draftOriginal && JSON.stringify({level:draft.value.level,category:draft.value.category,scopes:draft.value.scopes,modules:draft.value.modules,role:draft.value.role})!==JSON.stringify(draftOriginal));
const v2Dirty=computed(()=>v2AccessDirty.value || !!newPassword.value || !!confirmPassword.value);
let v2ConfirmOpen=false;
function v2Discard(proceed:()=>void){if(v2ConfirmOpen)return;v2ConfirmOpen=true;v2Dialog.warning({zIndex:5100,title:'放弃未保存的账号修改？',positiveText:'放弃修改',negativeText:'继续编辑',onPositiveClick:proceed,onAfterLeave:()=>{v2ConfirmOpen=false;}});}
async function v2ReloadEditor(){const id=draft.value?.id;v2Discard(async()=>{closeEditor(true);await load();const fresh=items.value.find(item=>item.id===id);if(fresh)openAccount(fresh);});}
const v2Stop=registerNavigationGuard((_url,proceed)=>{if(saving.value){v2Message.warning('正在保存账号，请等待结果');return false;}if(v2Dirty.value){v2Discard(()=>{closeEditor(true);proceed();});return false;}return true;});
function v2Unload(event:BeforeUnloadEvent){if(saving.value||v2Dirty.value){event.preventDefault();event.returnValue='';}}
onMounted(()=>window.addEventListener('beforeunload',v2Unload));
onBeforeUnmount(()=>{v2Stop();window.removeEventListener('beforeunload',v2Unload);});
watch(v2Level,()=>{page.value=1;});
function v2RequestAccess(){if(!saving.value)v2Navigate('/?view=permission');}
</script>`);
  return source;
}

export function adaptAdminAccountLoading(source) {
  const start=source.indexOf('async function loadPermissions(): Promise<void> {');
  const end=source.indexOf('async function loadPermissionRequests(',start);
  if(start<0||end<0)throw new Error('Admin permission loader changed');
  return source.slice(0,start)+'async function loadPermissions(): Promise<void> {}\n\n'+source.slice(end);
}

export function adaptModuleAdminEntry(source, filename) {
  const file = filename.replaceAll('\\','/').split('/').pop();
  if (!['CriticalGuardPage.vue','DrillManagementPage.vue'].includes(file)) return source;
  const drill = file === 'DrillManagementPage.vue';
  const { descriptor } = parse(source), edits = [];
  function visit(node) {
    const classes = node.props?.find(p => p.type === 6 && p.name === 'class')?.value?.content.split(' ') || [];
    if (node.type === 1 && classes.includes(drill ? 'drill-page__header' : 'guard-page__header')) {
      const click = drill ? "requestDiscardChanges('进入管理员页面会放弃当前未保存修改。', openAdmin)" : 'requestSwitch(openAdmin)';
      const button = `<V2AdminButton v-if="isAdmin" data-v2-navigation class="v2-module-admin" secondary size="small" :type="viewMode === 'admin' ? 'primary' : 'default'" :disabled="${drill ? 'busy' : 'saving || loading'} || viewMode === 'admin'" :aria-current="viewMode === 'admin' ? 'page' : undefined" @click="${click}"><Settings2 :size="16" />管理员入口</V2AdminButton>`;
      const end = node.loc.end.offset - '</header>'.length;
      edits.push([end,end,button]);
    }
    if (node.type === 1 && classes.includes('building-card') && classes.includes('admin')) edits.push([node.loc.start.offset,node.loc.end.offset,'']);
    for (const child of node.children || []) visit(child);
  }
  visit(descriptor.template.ast);
  if (edits.length !== 2) throw new Error('Module admin entry contract changed: '+file);
  for (const [start,end,text] of edits.sort((a,b)=>b[0]-a[0])) source=source.slice(0,start)+text+source.slice(end);
  return source.replace('<script setup lang="ts">', '<script setup lang="ts">\nimport { NButton as V2AdminButton } from "naive-ui";');
}
