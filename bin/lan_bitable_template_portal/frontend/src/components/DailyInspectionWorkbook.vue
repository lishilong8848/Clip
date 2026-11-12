<template>
  <section ref="rootElement" class="inspection-workbook inspection-theme" :class="{'full-height':mobileSheet,'rotate-landscape':rotation==='landscape','rotate-portrait':rotation==='portrait','compact-editor-view':mobileSheet && editing && (availableHeight<260 || rotatedWidth<300)}" :style="{'--available-height':availableHeight+'px','--rotated-width':rotatedWidth+'px'}" aria-label="巡检原表">
    <div v-if="mobileSheet" class="mobile-header">
    <div class="mobile-sheet-bar">
      <button aria-label="退出巡检表" @click="emit('exit')">返回</button>
      <div class="mobile-sheet-context">
        <span>{{ scope }}楼 · {{ day.slice(5) }}</span>
        <strong aria-live="polite">{{ mobilePhase }}</strong>
      </div>
      <button :aria-label="screenLayout==='landscape'?'切换竖屏':'切换横屏'" @click="toggleScreenLayout">{{ screenLayout==='landscape'?'竖屏':'横屏' }}</button>
      <button :aria-expanded="compactMenu" aria-label="巡检菜单" @click="compactMenu=!compactMenu">{{ compactMenu?'收起':'菜单' }}</button>
    </div>
      <div v-if="orderedBase.length" class="sheet-navigation">
        <div ref="sheetTabs" class="sheet-tabs" role="group" aria-label="巡检工作表">
          <button v-for="item in orderedBase" :key="item.key" type="button" :aria-pressed="pageKey===item.key"
            :aria-label="item.name.trim()" :title="item.name.trim()" @click="selectPage(item.key)">{{ item.name.trim() }}
            <small>{{ item.profession }}{{ item.profession!==item.homeProfession?' · 已调整':'' }}</small>
          </button>
        </div>
        <button class="sheet-sort" aria-label="调整工作表顺序" :disabled="!preferencesReady || saving || dirty || orderedBase.length<2" @click="openSort">排序</button>
      </div>
    </div>
    <div class="book-options" :class="{expanded:compactMenu}">
    <div class="book-tools" aria-label="巡检楼栋与日期">
      <label :title="lockReason"><span class="toolbar-label">楼栋</span> <select aria-label="楼栋" :value="scope" data-native-select :disabled="saving || dirty || !!editing || !!abnormal || sortOpen" @change="changeScope"><option v-for="s in scopes" :key="s" :value="s">{{ s }}楼</option></select></label>
      <label :title="lockReason"><span class="toolbar-label">日期</span> <input aria-label="巡检日期" :value="day" type="date" :disabled="saving || dirty || !!editing || !!abnormal || sortOpen" @change="changeDay"></label>
      <NButton size="small" :disabled="loading || saving || dirty || !!editing" :title="lockReason" @click="load">刷新</NButton>
      <NButton size="small" :disabled="loading || saving || dirty || !!editing || !report?.rounds.length" :title="!report?.rounds.length ? '本楼本日尚无巡检记录' : lockReason" @click="download">导出</NButton>
      <NButton size="small" class="book-settings" @click="openSettings">表格设置</NButton>
      <NButton v-if="canGroup" size="small" @click="openGroups">专业分工<template v-if="Object.keys(currentGroups).length"> · {{ Object.keys(currentGroups).length }} 已调整</template></NButton>
    </div>
    <div class="book-tools" aria-label="工作表导航">
      <label><span class="toolbar-label">专业</span><select aria-label="筛选工作表专业" data-native-select :value="professionView" @change="changeProfessionView"><option value="ALL">全部</option><option value="电气">电气</option><option value="暖通">暖通</option></select></label>
      <div v-if="!mobileSheet && report" class="sheet-picker">
        <NSelect :value="pageKey || null" :options="sheetOptions" filterable placeholder="搜索或切换工作表" :input-props="{'aria-label':'搜索或切换工作表'}" @update:value="selectPage" />
      </div>
      <NButton size="small" :disabled="!basePages.length" @click="openLookup('search')">定位单元格</NButton>
      <NButton size="small" :disabled="!viewedRound || !sheetLoaded" @click="openLookup('abnormal')">异常 {{ roundAbnormal.length }}</NButton>
    </div>
    <div v-if="mobileSheet" class="mobile-zoom-tools" role="group" aria-label="巡检表缩放">
      <button aria-label="缩小巡检表" :disabled="zoom<=7" @click="resizeSheet(-2)">−</button>
      <button class="zoom-reset" aria-label="恢复巡检表原始大小" @click="resizeSheet(14-zoom)">{{ Math.round(scale*100) }}%</button>
      <button aria-label="放大巡检表" :disabled="zoom>=28" @click="resizeSheet(2)">＋</button>
    </div>
    <slot name="start" v-if="isCurrentDate && !active" />
    <span v-if="preferencesError" class="book-error">{{ preferencesError }}<button @click="loadPreferences">重读排序</button></span>
    <div class="book-status">
      <span v-if="currentActive?.started_by">{{ currentActive.started_by }} 开轮<template v-if="presence.length"> · {{ presence.length }} 人在看</template></span>
      <span class="status-legend" aria-hidden="true"><i class="current-key"></i>本次巡检<i class="history-key"></i>历史巡检</span>
      <span v-if="currentActive && !readOnly" class="status-review">
        <button type="button" :disabled="!pendingRows.length" :title="pendingRows.length?'跳到本表下一个待填写单元格':'本表没有待填写项'" @click="gotoNextEmpty">待填 {{ pendingRows.length }}</button>
        <button type="button" title="核对本表待填项与上一轮差异" @click="reviewOpen=true">本表核对</button>
      </span>
      <span class="status-save" role="status">
        <strong v-if="editing" class="editing-address">正在编辑 {{ editing }}</strong>
        <span v-if="lockReason" class="lock-reason">{{ lockReason }}</span>
        <span>{{ saving ? '正在保存…' : dirty ? '有未保存修改' : '单元格修改自动保存' }}</span>
        <button v-if="dirty && !saving" @click="flush">重试保存</button><button v-if="dirty && !saving" @click="compareLatest">核对最新数据</button>
      </span>
    </div>
    <div v-if="report && (!mobileSheet || compactMenu)" class="round-tabs" role="group" :aria-label="isChiller?'冷机巡检轮次':'巡检轮次与状态'">
      <button v-for="state in roundStates" :key="state.slot" type="button" :class="{selected:shownSlot===state.slot}" :aria-pressed="shownSlot===state.slot" :disabled="!isChiller && !roundsBySlot.get(state.slot)" @click="selectSlot(state.slot)">
        <strong>{{ state.slot }}</strong><span>{{ state.label }}</span><small v-if="state.autoClosed">自动收尾</small><small v-if="state.sourceClock">取数 {{ state.sourceClock }}</small><small v-else-if="state.scheduledClock && state.label==='未到时点'">{{ state.scheduledClock }}</small>
      </button>
      <span v-if="needsFreeRound" class="round-hint">自由格请先选择已归档轮次</span>
    </div>
    </div>
    <div v-if="mobileSheet && !compactMenu && dirty && !saving" class="mobile-save-recovery"><span>修改尚未保存</span><button @click="flush">重试保存</button><button @click="compareLatest">核对最新数据</button></div>
    <div v-if="mobileSheet && !compactMenu && isCurrentDate && !active" class="mobile-start"><slot name="start" /></div>
    <p v-if="report?.archive_warning" class="book-pending">{{ report.archive_warning }}</p>
    <p v-if="report?.rounds.some(r=>r.status==='自动确认·待上传')" class="book-pending">上轮数据已在本地保存并自动收尾，飞书上传待重试。</p>
    <p v-if="invalidationText" class="book-pending" role="status">{{ invalidationText }} <NButton v-if="sheetLoaded" size="tiny" @click="openComparison">对照上一轮</NButton></p>
    <p v-if="error" class="book-error" role="alert">{{ error }}</p>
    <section v-if="comparison" class="book-error" aria-label="核对单元格冲突">
      <p>请核对共享数据，选择保留哪一份；未选择的修改不会自动覆盖。</p>
      <div v-for="job in jobs" :key="job.round.round_id">
        <p v-for="cell in job.cells" :key="cell.template+cell.sheet+cell.cell">{{ cell.sheet.trim() }} {{ cell.cell }}：共享 {{ latestValue(job.round,cell) || '空白' }} / 我的 {{ cell.value || '空白' }}</p>
        <button @click="resolveJob(job,true)">核对后使用我的填写</button><button @click="resolveJob(job,false)">使用共享数据</button>
      </div>
    </section>
    <p v-if="loading && !report" role="status">正在打开巡检表…</p>
    <template v-if="report">
      <p v-if="currentPage && !sheetLoaded && loading" class="book-pending" role="status">正在读取当前工作表数据…</p>
      <div v-if="currentPage" ref="scrollElement" class="book-scroll" tabindex="0" aria-label="可编辑巡检表格" :key="viewKey" @touchstart="startPinch" @touchmove="movePinch" @touchend="endPinch" @touchcancel="endPinch">
        <div class="book-canvas" :style="{width:(columnWidths.reduce((a,b)=>a+b,0)+42)+'px'}">
        <table :style="{fontSize:zoom+'px',width:(columnWidths.reduce((a,b)=>a+b,0)+42)+'px'}"><colgroup><col style="width:42px"><col v-for="i in columnCount" :key="i" :style="{width:columnWidths[i-1]+'px'}"></colgroup><thead><tr><th class="grid-corner" aria-label="行列编号"></th><th v-for="i in columnCount" :key="i" class="column-number" scope="col">{{ address(0,i-1).replace(/\d+$/,'') }}</th></tr></thead><tbody>
          <tr v-for="(row,i) in grid" :key="i" :style="{height:rowHeight(i)+'px',display:rowHeight(i)===0?'none':undefined}"><th class="row-number" scope="row"><span>{{ i+1 }}</span></th><template v-for="cell in row" :key="cell.address">
            <td v-if="!cell.hidden" :data-address="cell.address" :style="originalCellStyle(cell)" :rowspan="cell.rowspan" :colspan="cell.colspan" :class="{'current-cell':cell.current,'history-cell':cell.historical,'empty-cell':cell.mapped && !cell.text,'mobile-editing-cell':mobileSheet && editing===cell.address}"
              :tabindex="cell.editable?0:undefined" :aria-label="cell.address+' '+cell.text" @click="beginEdit(cell)" @keydown.enter.prevent="beginEdit(cell)">
              <select v-if="!mobileSheet && editing===cell.address && cell.stateCell" v-model="editValue" v-focus data-native-select :aria-label="'编辑 '+cell.address" @click.stop @blur="commit(cell)" @change="completeEdit" @keydown.enter.stop @keydown.esc.stop="cancelEdit" @keydown.alt.up.stop.prevent="moveEdit(-1)" @keydown.alt.down.stop.prevent="moveEdit(1)"><option value="">{{ cell.mapped ? '待填写' : '空白' }}</option><option value="√">√ 正常</option><option value="×">× 异常</option><option value="△">△ 未运行</option></select>
              <input v-else-if="!mobileSheet && editing===cell.address" v-model="editValue" v-focus :inputmode="cell.mapped ? 'decimal' : 'text'" enterkeyhint="done" @click.stop :aria-label="'编辑 '+cell.address" @blur="commit(cell)" @keydown.enter.stop.prevent="completeEdit" @keydown.esc.stop="cancelEdit" @keydown.alt.up.stop.prevent="moveEdit(-1)" @keydown.alt.down.stop.prevent="moveEdit(1)">
              <span v-else class="cell-content" :style="cellContentStyle(cell)"><span>{{ cell.displayText || '\u00a0' }}</span></span>
              <svg v-if="cell.diagonal?.up || cell.diagonal?.down" class="cell-diagonal" viewBox="0 0 100 100" preserveAspectRatio="none" aria-hidden="true"><line v-if="cell.diagonal.up" x1="0" y1="100" x2="100" y2="0" :stroke="cell.diagonal.color" vector-effect="non-scaling-stroke"/><line v-if="cell.diagonal.down" x1="0" y1="0" x2="100" y2="100" :stroke="cell.diagonal.color" vector-effect="non-scaling-stroke"/></svg>
            </td>
          </template></tr>
        </tbody></table>
        <svg v-if="currentPage.display_artwork?.length" class="template-art" aria-hidden="true" :width="columnWidths.reduce((a,b)=>a+b,0)" :height="(currentPage.row_heights||[]).reduce((a,b)=>a+b,0)*scale" :viewBox="'0 0 '+(currentPage.column_widths||[]).reduce((a,b)=>a+b,0)+' '+(currentPage.row_heights||[]).reduce((a,b)=>a+b,0)">
          <g v-for="(art,index) in currentPage.display_artwork" :key="index">
            <image v-if="art.image" :href="art.image" :x="art.x" :y="art.y" :width="art.width" :height="art.height" preserveAspectRatio="none" />
            <text v-else :x="art.x+(art.left||0)" :y="art.y+(art.top||0)" :font-family="art.font" :font-size="art.size" fill="#000" dominant-baseline="text-before-edge"><tspan v-for="(line,li) in art.lines" :key="li" :x="art.x+(art.left||0)" :dy="li?(art.size||14)*1.2:0">{{ line }}</tspan></text>
          </g>
        </svg>
        </div>
      </div>
      <p v-if="loading && !report.draft_key" role="status">模板已就绪，正在读取本轮及历史数据…</p>
      <p v-else-if="!report.rounds.length">尚无巡检记录；开始本轮后即可填写。原表已显示。</p>
      <div v-if="editing && editCell" class="cell-editor-tools" role="group" aria-label="单元格编辑" @keydown.esc.stop="cancelEdit" @keydown.alt.up.prevent="moveEdit(-1)" @keydown.alt.down.prevent="moveEdit(1)">
        <strong>{{ editing }} · {{ slotOf(editCell.round) }}</strong>
        <template v-if="mobileSheet">
          <select v-if="editCell.stateCell" :key="editing" v-model="editValue" v-focus data-native-select :aria-label="'编辑 '+editing" @change="completeEdit"><option value="">{{ editCell.mapped ? '待填写' : '空白' }}</option><option value="√">√ 正常</option><option value="×">× 异常</option><option value="△">△ 未运行</option></select>
          <input v-else :key="editing" v-model="editValue" v-focus :inputmode="editCell.mapped ? 'decimal' : 'text'" enterkeyhint="done" :aria-label="'编辑 '+editing" @keydown.enter.prevent="completeEdit">
        </template>
        <div class="cell-editor-actions">
          <button type="button" :disabled="!editNeighbors[0]" aria-keyshortcuts="Alt+ArrowUp" @mousedown.prevent @click="moveEdit(-1)">上一格</button>
          <button type="button" :disabled="!editNeighbors[1]" aria-keyshortcuts="Alt+ArrowDown" @mousedown.prevent @click="moveEdit(1)">下一格</button>
          <button type="button" class="primary" @mousedown.prevent @click="completeEdit">完成</button>
        </div>
      </div>
      <footer v-if="currentActive && !readOnly" class="book-submit">
        <div v-if="!currentActive.pending" class="book-signoffs" aria-label="专业确认">
          <template v-for="name in selectedProfessions" :key="name">
            <span v-if="confirmedProfessions.includes(name)" :title="confirmationLabel(name)">{{ name }} · {{ confirmationLabel(name) }}</span>
            <NButton v-else size="small" :disabled="actionLocked || !hasViewed(name)" :title="progressLabel(name)" @click="confirmProfession(name)">{{ name }}确认 · {{ progressLabel(name) }}</NButton>
          </template>
          <NButton v-for="name in joinableProfessions" :key="name" size="small" :disabled="actionLocked" @click="joinProfession(name)">本轮加入{{ name }}</NButton>
          <span v-if="!selectedProfessions.length" :title="unvisitedNames">已查看 {{ visitedCount }} / {{ requiredPages.length }} 页</span>
          <NButton v-if="nextUnvisited" size="tiny" :title="'还需查看 '+unvisitedNames" :disabled="actionLocked" @click="openPage(nextUnvisited.key)">下一未查看表</NButton>
          <span v-if="saving || dirty" role="status">{{ saving?'正在保存…':'修改待保存' }}</span>
        </div>
        <NButton type="primary" size="small" class="submit-action" :disabled="!canSubmit || actionLocked"
          :title="submitReason" @click="confirmSubmit=true">{{ currentActive.pending?'重试提交归档':'确认提交' }}</NButton>
      </footer>
    </template>
    <NModal v-model:show="settingsOpen" :to="rootElement" preset="card" title="表格设置" style="width:min(94%,460px)">
      <div class="book-settings-content">
        <div class="settings-section"><strong>表格字号</strong><div class="settings-choices" aria-label="表格字号"><button v-for="(label,i) in ['紧凑','标准','放大','更大']" :key="i" :aria-pressed="zoom===[12,14,18,22][i]" @click="zoom=[12,14,18,22][i]">{{ label }}</button></div></div>
        <div v-if="!mobileSheet" class="settings-section"><strong>个人阅读顺序</strong><span>当前：{{ professionView==='ALL'?'全部专业':professionView }}，与账号同步</span><NButton block :disabled="!preferencesReady || saving || dirty" @click="openSort">调整工作表顺序</NButton></div>
        <div v-if="canGroup && showGroups" ref="groupPanel" class="settings-section">
          <strong>本班专业归属</strong><small>仅影响{{ scope }}楼{{ defaultShift }}班，下一班恢复默认。</small>
          <div class="group-list">
            <label v-for="page in basePages" :key="page.key"><span>{{ page.name.trim() }}<small>默认 {{ page.homeProfession }}{{ groupValue(page)!==page.homeProfession?' · 已调整':'' }}</small></span>
              <select :value="groupValue(page)" :disabled="actionLocked" :aria-label="page.name.trim()+'归属'" @change="stageGroup(page,$event)"><option v-for="name in professions" :key="name">{{ name }}</option></select>
            </label>
          </div>
          <div class="group-actions"><NButton size="small" :disabled="actionLocked" @click="restoreGroups">恢复全部默认</NButton><NButton size="small" type="primary" :disabled="actionLocked || !Object.keys(groupDraft).length" @click="requestGroupSave">保存归属</NButton></div>
        </div>
        <NButton v-else-if="canGroup" size="small" @click="openGroups">查看本班专业分工</NButton>
        <div v-else-if="currentPage && frozenProfession" class="settings-section"><strong>该轮专业归属</strong><span>{{ currentPage.name.trim() }} · {{ frozenProfession }}<template v-if="shownSlot"> · {{ shownSlot }}</template></span><small v-if="frozenProfession!==currentGroup(currentPage)">本班对应归属为 {{ currentGroup(currentPage) }}。</small></div>
        <div v-if="currentActive" class="settings-section"><strong>本轮协作</strong><span v-if="currentActive.started_by">开轮 {{ currentActive.started_by }} · {{ roundClock.format(currentActive.started || 0) }}</span><span v-for="person in presence" :key="person.actor+person.sheet">{{ person.actor }} · 正在查看 {{ basePages.find(page=>page.key===person.sheet)?.name.trim() }}</span></div>
      </div>
      <template #footer><NButton type="primary" block @click="settingsOpen=false">完成</NButton></template>
    </NModal>
    <NModal v-model:show="sortOpen" :to="rootElement" preset="card" title="个人工作表排序" style="width:min(94%,560px)" :mask-closable="false">
      <p>{{ professionView==='ALL'?'全部':professionView }}视图 · 排序同步到当前账号，手机和电脑通用</p>
      <p v-if="preferencesError" class="book-error" role="alert">{{ preferencesError }}</p>
      <div class="sort-list"><div v-for="(key,index) in sortDraft" :key="key" class="sort-row"><span>{{ basePages.find(p=>p.baseKey===key)?.name.trim() }}</span><button :disabled="index===0" @click="shiftSort(index,-1)">上移</button><button :disabled="index===sortDraft.length-1" @click="shiftSort(index,1)">下移</button></div>
      </div><button :disabled="saving" @click="saveSort">保存排序</button>
    </NModal>
    <NModal v-model:show="confirmSubmit" :to="rootElement" preset="dialog" title="确认提交本轮巡检" content="保存已填写内容及可用源值并上传归档。没有读数的测点保留空白，上传未确认时可重试原轮次。" positive-text="确认提交" negative-text="继续查看" @positive-click="submit" />
    <NModal :show="!!abnormal" :to="rootElement" preset="dialog" title="填写异常说明" positive-text="保存" negative-text="返回修改" @positive-click="saveAbnormal" @negative-click="abnormal=undefined" @update:show="v=>{if(!v)abnormal=undefined}"><textarea v-model="abnormalNote" placeholder="请描述该设备异常，随单元格修改保存" rows="3" /></NModal>
    <NModal :show="!!pendingSwap" :to="rootElement" preset="dialog" type="warning" title="调整本班专业归属？"
      :content="swapWarning" positive-text="仍要调整" negative-text="取消"
      @positive-click="applySwap" @update:show="v=>{if(!v)pendingSwap=false}" />
    <NModal v-model:show="reviewOpen" :to="rootElement" preset="card" :title="'本表核对 · '+(currentPage?.name||'').trim()" style="width:min(94%,560px)" @after-leave="applyPendingFocus">
      <div class="review-filters" role="group" aria-label="核对筛选">
        <button type="button" :aria-pressed="reviewFilter==='all'" @click="reviewFilter='all'">全部 {{ reviewRows.length }}</button>
        <button type="button" :aria-pressed="reviewFilter==='pending'" @click="reviewFilter='pending'">待填 {{ pendingRows.length }}</button>
        <button type="button" :aria-pressed="reviewFilter==='changed'" @click="reviewFilter='changed'">与上轮有差异 {{ changedRows.length }}</button>
      </div>
      <p class="settings-note">{{ previousSlot ? '上轮＝'+previousSlot+'，按测点身份比对，不受各轮所在列不同的影响。' : '本日该时点之前没有可比对的轮次，仅列出待填项。' }}</p>
      <p class="settings-note">待填表示本轮没有可用读数。归档时保留空白，可用源值会随本轮记录保存。</p>
      <div class="review-list">
        <button v-for="row in shownReviewRows" :key="row.address" type="button" class="review-row" @click="focusReviewCell(row)">
          <span class="review-label">{{ row.label }}<small>{{ row.address }}</small></span>
          <span class="review-values">
            <i :class="{'review-empty':row.empty}">{{ row.current || '待填写' }}</i>
            <em v-if="row.hasPrevious">{{ row.previous || '空' }}</em>
            <b v-if="row.changed">变化</b>
          </span>
        </button>
        <p v-if="!shownReviewRows.length" class="settings-note">{{ reviewFilter==='pending' ? '本表没有待填写项。' : reviewFilter==='changed' ? '与上一轮没有差异。' : '本表没有需要核对的映射单元格。' }}</p>
      </div>
    </NModal>
    <NModal :show="!!lookupMode" :to="rootElement" preset="card" :title="lookupMode==='abnormal'?'本轮异常 · '+shownSlot:'定位工作表单元格'" style="width:min(94%,560px)" @update:show="v=>{if(!v)lookupMode=''}" @after-leave="applyLookupTarget">
      <input v-if="lookupMode==='search'" v-model.trim="lookupQuery" maxlength="100" type="search" aria-label="查找表名、表内文字或格号" placeholder="搜索表名、设备、测点或格号">
      <p v-if="lookupPending" class="settings-note" role="status">正在查询本轮设备和测点…</p>
      <p v-else-if="lookupError" class="settings-note" role="status">{{ lookupError }}</p>
      <div class="review-list">
        <button v-for="row in lookupRows" :key="row.key+row.cell" type="button" class="review-row" @click="selectLookup(row)"><span class="review-label">{{ row.label }}<small>{{ row.sheet.trim() }} · {{ row.cell }}</small></span></button>
        <p v-if="!lookupRows.length && !lookupPending" class="settings-note">{{ lookupMode==='abnormal'?'本轮无已保存的异常状态。':lookupQuery?'没有匹配的单元格。':'输入关键字定位原表。' }}</p>
      </div>
    </NModal>
  </section>
</template>
<script setup lang="ts">
import {computed,nextTick,onBeforeUnmount,onMounted,ref,shallowRef,watch} from 'vue';
import {NModal,NButton,NSelect} from 'naive-ui';
import './inspection.css';
import {requestJson,downloadFile} from '../api/client';
import {registerNavigationGuard} from '../navigation';
import {inspectionCellText} from './inspectionCellFormat';
import {configuredInspectionPositions,manuallyFilledInspectionPositions,inspectionPosition,type InspectionVisibilityActive} from './inspectionCellVisibility';
type CellEdit={template:string;sheet:string;cell:string;value:string;note?:string};
type Reading=CellEdit & {value:any;read_only?:boolean;parameter?:string;value_type?:string;device?:string;device_name?:string;kind?:string;point_id?:string;location?:string;form?:string;members?:Reading[]};
type Round={id?:string;round_id:string;shift:string;round:number;version:string|number;status:string;auto_closed?:boolean;sheet_groups?:Record<string,string>;abnormal_cells?:Reading[];original_cells:Reading[];extra_cells?:Reading[]};
type Page={key:string;baseKey:string;template:string;profession:string;homeProfession?:string;name:string;rows:any[][];merges:string[];round_rows?:Record<string,string>;column_widths?:number[];row_heights?:number[];styles?:Record<string,any>[];excel_styles?:{number_format:string;alignment:string;diagonal?:{up:boolean;down:boolean;color:string}}[];style_ids?:number[][];source_range?:string;excluded_ranges?:string[];display_artwork?:{x:number;y:number;width:number;height:number;image?:string;lines?:string[];font?:string;size?:number;left?:number;top?:number}[]};
type SlotState={slot:string;status:string;scheduled_at:number;source_time?:number|null;auto_closed?:boolean};
type Report={template_version?:string;archive_warning?:string;draft_key:string;can_edit_history:boolean;scope:string;date:string;rounds:Round[];slot_states?:SlotState[];sheet_groups?:Record<string,string>;summary_only?:boolean;sheet_filter?:[string,string]|null;original_templates:{name:string;profession:string;sheets:Omit<Page,'key'|'baseKey'|'template'|'profession'>[]}[]};
type RoundMeta=Pick<Round,'id'|'round_id'|'shift'|'round'|'version'|'status'>;
type Job={scope:string;date:string;round:RoundMeta;cells:CellEdit[];attempted?:boolean};
type Presence={actor:string;sheet:string;seen_at:number};
type Progress={visited_sheets?:string[];presence?:Presence[]};
type Active=InspectionVisibilityActive & Progress & {
 id:string;pending:boolean;duty:{scope:string;date:string;shift:string};round:number;review_version:number;professions?:string[]|null;
 started_by?:string;started?:number;available_professions?:string[];sheet_groups?:Record<string,string>;worksheet_mode?:boolean;
 profession_confirmations?:Record<string,{actor?:string;confirmed?:number;version?:string}>;
 profession_versions?:Record<string,string>;confirmation_invalidations?:Record<string,string>;
};
const props=defineProps<{immersive?:boolean;scopes:string[];templateVersion?:string;archiveRevision?:string;defaultScope?:string;defaultDate?:string;defaultShift?:string;nextRound?:number;active?:Active;plannedGroups?:Record<string,string>;readOnly:boolean;parentBusy:boolean}>();
const emit=defineEmits<{changed:[];scope:[scope:string];exit:[];'round-state':[state:any]}>();
const compactMenu=ref(false);
const phoneMedia=window.matchMedia('(max-width:760px), (max-height:500px) and (pointer:coarse)');
const phoneScreen=ref(phoneMedia.matches);
const mobileSheet=computed(()=>!!props.immersive||phoneScreen.value);
const screenLayout=ref<'landscape'|'portrait'>('landscape');
try{if(sessionStorage.getItem('inspection-screen-layout')==='portrait')screenLayout.value='portrait';}catch{/* Default to landscape. */}
function screenIsPortrait(){
 const type=window.screen.orientation?.type||'';
 if(type.startsWith('portrait'))return true;
 if(type.startsWith('landscape'))return false;
 const legacy=(window as Window & {orientation?:number}).orientation;
 if(typeof legacy==='number')return Math.abs(legacy)%180===0;
 return window.innerHeight>=window.innerWidth;
}
const viewportPortrait=ref(screenIsPortrait());
const rotation=computed(()=>!mobileSheet.value?'':screenLayout.value==='landscape'&&viewportPortrait.value?'landscape':screenLayout.value==='portrait'&&!viewportPortrait.value?'portrait':'');
function toggleScreenLayout(){finishEdit();if(editing.value||abnormal.value)return;screenLayout.value=screenLayout.value==='landscape'?'portrait':'landscape';try{sessionStorage.setItem('inspection-screen-layout',screenLayout.value);}catch{}void nextTick(measureHeight);}
function updatePhoneScreen(){phoneScreen.value=phoneMedia.matches;void nextTick(measureHeight);}
const scope=ref(props.defaultScope||props.scopes[0]||'');
const day=ref(props.defaultDate||new Intl.DateTimeFormat('sv-SE',{timeZone:'Asia/Shanghai',dateStyle:'short'}).format(new Date()));
const cachedTemplates=shallowRef<Report['original_templates']>([]),layoutVersion=ref('');
const report=shallowRef<Report>();const comparison=shallowRef<Report>();const error=ref(''),loading=ref(false),saving=ref(false),zoom=ref(14);
const pageKey=ref(''),visited=ref(new Set<string>()),editing=ref(''),editValue=ref('');
const rootElement=ref<HTMLElement>(),availableHeight=ref(600),rotatedWidth=ref(window.innerHeight);
const professionView=ref('ALL'),preferencesReady=ref(false),preferencesError=ref(''),preferencesRevision=ref(0);
const orders=ref<Record<string,string[]>>({ALL:[],电气:[],暖通:[]}),sortOpen=ref(false),settingsOpen=ref(false),sortDraft=ref<string[]>([]);
const jobs=ref<Job[]>([]);const savingCells=ref(false);const dirty=computed(()=>jobs.value.length>0);const confirmSubmit=ref(false);
const abnormal=ref<{round:RoundMeta;edit:CellEdit}>(),abnormalNote=ref('');
const professions=['电气','暖通'];
// Disabled toolbar controls must say why; dirty/saving are already reported by the save status.
const lockReason=computed(()=>abnormal.value?'填写异常说明后即可切换楼栋或日期'
 :editing.value?'完成当前单元格编辑后即可切换楼栋或日期'
 :sortOpen.value?'关闭排序窗口后即可切换楼栋或日期':'');
const slots=['白1','白2','夜1','夜2'];
const selectedSlot=ref(''),chillerSelection=ref('');
const slotOf=(round?:Pick<Round,'shift'|'round'>)=>round?round.shift+round.round:'';
const isCurrentDate=computed(()=>scope.value===props.defaultScope && day.value===props.defaultDate);
const currentActive=computed(()=>props.active?.duty.scope===scope.value && props.active?.duty.date===day.value?props.active:undefined);
const activeSlot=computed(()=>currentActive.value?currentActive.value.duty.shift+currentActive.value.round:'');
const currentSlot=computed(()=>activeSlot.value||(isCurrentDate.value&&props.defaultShift?props.defaultShift+Math.min(props.nextRound||1,2):''));
const activeRound=computed(()=>report.value?.rounds.find(r=>r.round_id===currentActive.value?.id));
const roundsBySlot=computed(()=>{
 const result=new Map<string,Round|undefined>();
 for(const round of report.value?.rounds||[]){const slot=slotOf(round);result.set(slot,result.has(slot)?undefined:round);}
 return result;
});
function hasArchiveReceipt(round:RoundMeta){return round.status==='已归档'&&!!round.id&&typeof round.version==='string'&&!!round.version;}
const roundClock=new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',hour:'2-digit',minute:'2-digit',hourCycle:'h23'});
const roundStates=computed(()=>slots.map(slot=>{
 const state=report.value?.slot_states?.find(item=>item.slot===slot),round=roundsBySlot.value.get(slot);
 const status=state?.status||round?.status||(slot===activeSlot.value?(currentActive.value?.pending?'待归档核验':'进行中'):'未开始');
 const label=['待归档','自动确认·待上传'].includes(status)?'待上传':status;
 return {slot,label,autoClosed:state?.auto_closed??(round?.auto_closed||status==='自动确认·待上传'),
  sourceClock:state?.source_time?roundClock.format(state.source_time):'',
  scheduledClock:state?.scheduled_at?roundClock.format(state.scheduled_at):''};
}));
const currentGroups=computed(()=>currentActive.value?.sheet_groups || (isCurrentDate.value ? props.plannedGroups : undefined) || report.value?.sheet_groups || {});
const historicalGroupRound=computed(()=>roundsBySlot.value.get(
 (report.value?.original_templates.some(t=>t.sheets.some(s=>s.name.trim()==='暖通冷机系统巡检表'&&JSON.stringify([t.name,s.name])===pageKey.value))
  ? chillerSelection.value : selectedSlot.value))
 || (!currentActive.value ? report.value?.rounds.slice(-1)[0] : undefined));
const displayedGroups=computed(()=>historicalGroupRound.value && historicalGroupRound.value.round_id!==currentActive.value?.id
 ? historicalGroupRound.value.sheet_groups || {} : currentGroups.value);
const basePages=computed<Page[]>(()=>report.value?.original_templates.flatMap(t=>t.sheets.filter(s=>s.name.trim()!=='封面').map(s=>{const key=JSON.stringify([t.name,s.name]);return {...s,template:t.name,profession:displayedGroups.value[key]||t.profession,homeProfession:t.profession,key,baseKey:key};}))||[]);
const orderedBase=computed(()=>{
 const order=orders.value[professionView.value]||[];
 return basePages.value.filter(p=>professionView.value==='ALL'||p.profession===professionView.value).slice().sort((a,b)=>{
 const x=order.indexOf(a.baseKey),y=order.indexOf(b.baseKey);return (x<0?9999:x)-(y<0?9999:y);});
});
function preferredPage(){return orderedBase.value[0]?.key||'';}
const currentPage=computed(()=>orderedBase.value.find(p=>p.key===pageKey.value));
const sheetLoaded=computed(()=>!!report.value?.draft_key && !report.value.summary_only &&
 (!report.value.sheet_filter || JSON.stringify(report.value.sheet_filter)===currentPage.value?.baseKey));
const sheetOptions=computed(()=>orderedBase.value.map(p=>({value:p.key,label:`${p.profession} · ${p.name.trim()}${currentActive.value?(visited.value.has(p.baseKey)?' · 已查看':' · 未查看'):''}`})));
const selectedProfessions=computed(()=>currentActive.value?.professions || []);
const requiredPages=computed(()=>basePages.value.filter(p=>!selectedProfessions.value.length || selectedProfessions.value.includes(currentGroup(p))));
const allVisited=computed(()=>!!report.value?.draft_key && requiredPages.value.length>0 && requiredPages.value.every(p=>visited.value.has(p.baseKey)));
const visitedCount=computed(()=>requiredPages.value.filter(p=>visited.value.has(p.baseKey)).length);
// Naming the remaining sheets turns a missing submit button into an actionable checklist.
const unvisitedNames=computed(()=>{
 const names=requiredPages.value.filter(p=>!confirmedProfessions.value.includes(currentGroup(p)) && !visited.value.has(p.baseKey)).map(p=>p.name.trim());
 if(!names.length)return '';
 return names.length>3 ? names.slice(0,3).join('、')+' 等 '+names.length+' 张' : names.join('、');
});
const nextUnvisited=computed(()=>requiredPages.value.find(page=>!confirmedProfessions.value.includes(currentGroup(page))&&!visited.value.has(page.key)));
const isChiller=computed(()=>currentPage.value?.name.trim()==='暖通冷机系统巡检表');
const chillerSlot=computed(()=>chillerSelection.value||(mobileSheet.value?currentSlot.value:activeSlot.value)||slots.find(slot=>roundsBySlot.value.get(slot))||slots[0]);
const viewKey=computed(()=>JSON.stringify([pageKey.value,isChiller.value?chillerSlot.value:'']));
const freeRound=computed(()=>{
 if(selectedSlot.value)return roundsBySlot.value.get(selectedSlot.value);
 if(currentActive.value)return activeRound.value;
 return report.value?.rounds.length===1?report.value.rounds[0]:undefined;
});
const shownSlot=computed(()=>isChiller.value?chillerSlot.value:slotOf(freeRound.value));
// Top-level sheet_groups reflects a single shift (the active one, else the day's last round), so a
// round's own frozen grouping is the only trustworthy source when reviewing an archived round.
const viewedRound=computed(()=>isChiller.value?roundsBySlot.value.get(chillerSlot.value):freeRound.value);
const frozenProfession=computed(()=>currentPage.value && viewedRound.value
 ? viewedRound.value.sheet_groups?.[currentPage.value.baseKey] || currentPage.value.homeProfession : '');
const actionLocked=computed(()=>saving.value||dirty.value||!!editing.value||!!abnormal.value||loading.value||props.parentBusy);
const currentGroup=(page:Page)=>currentGroups.value[page.key]||page.homeProfession||page.profession;
const pagesFor=(name:string)=>requiredPages.value.filter(page=>currentGroup(page)===name);
const hasViewed=(name:string)=>!!report.value?.draft_key && pagesFor(name).every(page=>
 (currentActive.value?.worksheet_mode ? syncedVisited.value : visited.value).has(page.key));
function progressLabel(name:string){
 const pages=pagesFor(name),count=pages.filter(page=>visited.value.has(page.key)).length;
 return hasViewed(name) ? '可确认' : count===pages.length ? '进度待同步' : `已查看 ${count}/${pages.length}`;
}
const canSubmit=computed(()=>!!currentActive.value && (currentActive.value.pending ||
 (selectedProfessions.value.length ? selectedProfessions.value.every(name=>confirmedProfessions.value.includes(name)) : allVisited.value)));
const submitReason=computed(()=>canSubmit.value ? lockReason.value : selectedProfessions.value.length
 ? '请逐页查看并完成所选专业确认' : '还需查看 '+unvisitedNames.value);
const joinableProfessions=computed(()=>selectedProfessions.value.length && !confirmedProfessions.value.length
 ? (currentActive.value?.available_professions || professions).filter(name=>!selectedProfessions.value.includes(name)) : []);
const invalidationText=computed(()=>Object.entries(currentActive.value?.confirmation_invalidations || {})
 .filter(([name])=>selectedProfessions.value.includes(name)).map(([name,reason])=>name+' · '+reason).join(' '));
const mobilePhase=computed(()=>{
 const slot=currentSlot.value;
 const viewing=shownSlot.value;
 if(!slot)return viewing?`历史巡检 · ${viewing}`:'历史巡检';
 const state=roundStates.value.find(item=>item.slot===slot);
 return viewing&&viewing!==slot?`当前 ${slot} · 查看 ${viewing}`:`${slot} · ${state?.label||'未开始'}`;
});
const needsFreeRound=computed(()=>!isChiller.value&&!currentActive.value&&!freeRound.value&&!!report.value?.can_edit_history&&[...roundsBySlot.value.values()].some(r=>r&&hasArchiveReceipt(r)));
const vFocus={mounted:(el:HTMLElement)=>{el.focus({preventScroll:true});if(el instanceof HTMLInputElement)el.select();}};
function address(r:number,c:number){let s='';for(c++;c;c=Math.floor((c-1)/26))s=String.fromCharCode(65+(c-1)%26)+s;return s+(r+1);}
function coords(a:string){const m=/^([A-Z]+)(\d+)$/.exec(a)!;let c=0;for(const ch of m[1])c=c*26+ch.charCodeAt(0)-64;return [Number(m[2])-1,c-1];}
const text=(v:any)=>v==null?'':String(v);
const visibleRounds=computed(()=>report.value?.rounds.filter(r=>!isChiller.value || slotOf(r)===chillerSlot.value)||[]);
type LayoutCell={address:string;rowIndex:number;colIndex:number;raw:any;rowspan:number;colspan:number;hidden:boolean};
const layoutCache=new WeakMap<any[][],LayoutCell[][]>();
const columnWidth=computed(()=>Math.max(88,zoom.value*6));
const columnCount=computed(()=>{
 const p=currentPage.value;if(!p)return 0;if(p.column_widths?.length)return p.column_widths.length;let end=1;
 for(const row of p.rows)row.forEach((v,i)=>{if(v!==null&&v!==undefined&&v!=='')end=Math.max(end,i+1);});
 for(const merge of p.merges)end=Math.max(end,coords(merge.split(':').pop()!)[1]+1);
 for(const round of report.value?.rounds||[])for(const c of [...round.original_cells,...(round.extra_cells||[])])if(c.template===p.template&&c.sheet===p.name)end=Math.max(end,coords(c.cell)[1]+1);
 for(const job of jobs.value)for(const c of job.cells)if(c.template===p.template&&c.sheet===p.name)end=Math.max(end,coords(c.cell)[1]+1);
 return end;
});
const scale=computed(()=>zoom.value/14);
const columnWidths=computed(()=>Array.from({length:columnCount.value},(_,i)=>currentPage.value?.column_widths ? currentPage.value.column_widths[i]*scale.value : columnWidth.value));
function rowHeight(index:number){return (currentPage.value?.row_heights?.[index]??32)*scale.value;}
function mergedHeight(cell:LayoutCell){let height=0;for(let i=cell.rowIndex;i<cell.rowIndex+cell.rowspan;i++)height+=rowHeight(i);return Math.max(0,height-2);}
function originalCellStyle(cell:GridCell):Record<string,any>{const p=currentPage.value;const source=p?.styles?.[p.style_ids?.[cell.rowIndex]?.[cell.colIndex]??0];if(!source)return {};
 const alignment=p?.excel_styles?.[p.style_ids?.[cell.rowIndex]?.[cell.colIndex]??0]?.alignment||'';
 const horizontal=alignment.match(/horizontal="([^"]*)"/)?.[1];
 return {...source,...(!horizontal||horizontal==='general'?{textAlign:typeof cell.value==='number'?'right':'left'}:{}),fontSize:typeof source.fontSize==='number'?source.fontSize*scale.value+'px':undefined,
 ...(cell.current?{backgroundColor:'#fff0b8',color:'#111827'}:{}),height:mergedHeight(cell)+'px',padding:'0 2px'};
}
function cellContentStyle(cell:GridCell){const style=originalCellStyle(cell);return {justifyContent:style.verticalAlign==='middle'?'center':style.verticalAlign==='top'?'flex-start':'flex-end'};}
const cellLayout=computed(()=>{
 const p=currentPage.value;if(!p)return [];
 const cached=layoutCache.get(p.rows);if(cached)return cached;
 const merges=new Map<string,{rowspan:number;colspan:number}>(),hidden=new Set<string>();
 for(const range of p.merges){const [a,b=a]=range.split(':');const [r,c]=coords(a),[rr,cc]=coords(b);merges.set(a,{rowspan:rr-r+1,colspan:cc-c+1});for(let y=r;y<=rr;y++)for(let x=c;x<=cc;x++)if(y!==r||x!==c)hidden.add(address(y,x));}
 const layout=p.rows.map((row,ri)=>row.map((raw,ci)=>{const a=address(ri,ci);return {address:a,rowIndex:ri,colIndex:ci,raw,...(merges.get(a)||{rowspan:1,colspan:1}),hidden:hidden.has(a)};}));layoutCache.set(p.rows,layout);return layout;
});
const sheetReadings=computed(()=>{
 const groups=new Map<string,{reading:Reading;round:Round}[]>();
 for(const r of report.value?.rounds||[])for(const c of r.original_cells||[]){const key=JSON.stringify([c.template,c.sheet]);const entries=groups.get(key)||[];entries.push({reading:c,round:r});groups.set(key,entries);}
 return groups;
});
const configuredPositions=computed(()=>configuredInspectionPositions(currentActive.value));
const manualPositions=computed(()=>manuallyFilledInspectionPositions(currentActive.value,
 (report.value?.rounds.find(round=>round.round_id===currentActive.value?.id)?.original_cells||[])));
const acknowledgedManual=ref(new Set<string>());
const roundPosition=(round:string,template:string,sheet:string,cell:string)=>round+'|'+inspectionPosition(template,sheet,cell);
const grid=computed(()=>{
 const p=currentPage.value;if(!p)return [];
 const values=new Map<string,{value:any;round:Round;readonly?:boolean;mapped:boolean;parameter?:string;value_type?:string;visible:boolean}>();
 const collisions=new Set<string>(),visibleById=new Map(visibleRounds.value.map(round=>[round.round_id,round]));
 for(const {reading:c,round:r} of sheetReadings.value.get(p.baseKey)||[]){
  if(!visibleById.has(r.round_id))continue;
  if(values.has(c.cell)){collisions.add(c.cell);continue;}
  const current=r.round_id===currentActive.value?.id,key=inspectionPosition(c.template,c.sheet,c.cell);
  const configured=!current||configuredPositions.value.has(key),manual=manualPositions.value.has(key)||acknowledgedManual.value.has(roundPosition(r.round_id,c.template,c.sheet,c.cell));
  values.set(c.cell,{value:c.value,round:r,readonly:c.read_only,mapped:configured,visible:configured||manual,parameter:c.parameter,value_type:c.value_type});
 }
 const active=activeRound.value;
 if(active&&visibleById.has(active.round_id))for(const key of configuredPositions.value){
  const [template,sheet,cell]=JSON.parse(key) as string[];
  if(template!==p.template||sheet!==p.name.trim()||values.has(cell))continue;
  const [ri,ci]=coords(cell),rowSlot=p.round_rows?.[String(ri+1)];
  if(text(p.rows[ri]?.[ci])||(!isChiller.value&&rowSlot&&rowSlot!==slotOf(active)))continue;
  values.set(cell,{value:'',round:active,mapped:true,visible:true});
 }
 const overrides=new Map<string,Map<string,CellEdit>>();
 const {template,name:sheet}=p;
 function overlay(round:Round,edit:CellEdit){
  if(edit.template!==template||edit.sheet!==sheet)return;
  const byRound=overrides.get(edit.cell)||new Map();
  byRound.set(round.round_id,edit);overrides.set(edit.cell,byRound);
 }
 for(const round of visibleRounds.value)for(const cell of round.extra_cells||[])overlay(round,cell);
 for(const job of jobs.value){const round=visibleById.get(job.round.round_id);if(round)for(const cell of job.cells)overlay(round,cell);}
 return cellLayout.value.map(row=>row.slice(0,columnCount.value).map(cell=>{
  const a=cell.address,v=values.get(a),rowSlot=isChiller.value?chillerSlot.value:p.round_rows?.[String(cell.rowIndex+1)];
  // Existing mappings keep their owner. An assigned row never inherits a different round.
  const round=v?.round||(rowSlot?roundsBySlot.value.get(rowSlot):freeRound.value);
  const byRound=overrides.get(a);
  let override=round?byRound?.get(round.round_id):undefined;
  if(!round&&!rowSlot&&!selectedSlot.value&&!currentActive.value&&byRound?.size===1)override=byRound.values().next().value;
  const current=!!round&&round.round_id===currentActive.value?.id;
  const excluded=(p.excluded_ranges||[]).some(range=>{const [x,y=x]=range.split(':');const [r,c]=coords(x),[rr,cc]=coords(y);return cell.rowIndex>=r&&cell.rowIndex<=rr&&cell.colIndex>=c&&cell.colIndex<=cc;});
  const editable=sheetLoaded.value&&!!round&&!collisions.has(a)&&!v?.readonly&&(current
   ? !excluded&&!props.readOnly&&!currentActive.value?.pending&&round.status==='进行中'
     && (!currentActive.value?.worksheet_mode || requiredPages.value.some(page=>page.key===p.key))
   : report.value?.can_edit_history&&hasArchiveReceipt(round));
  const value=collisions.has(a)?'位置冲突，请核对映射':override?override.value:v?(v.visible?v.value:''):cell.raw;
  const style=p.excel_styles?.[p.style_ids?.[cell.rowIndex]?.[cell.colIndex]??0],format=style?.number_format;
  return {...cell,value,diagonal:style?.diagonal,text:text(value),displayText:current&&v?.mapped&&!text(value)?'待填写':inspectionCellText(value,format),round,
   current:current&&!!(v?.visible||override),historical:!!(v||override)&&!current,mapped:!!v?.mapped,
   stateCell:v?.value_type==='状态'||/运行状态|启停状态|开关状态|设备状态|告警状态|有无滴漏/.test(v?.parameter||'')||['√','×','△'].includes(text(override?override.value:v?.value)),editable:!!editable};
 }));
});
type GridCell=(typeof grid.value)[number][number];
type ReviewRow={cell:GridCell;address:string;label:string;current:string;previous:string;hasPrevious:boolean;empty:boolean;changed:boolean};
// Except for the chiller sheet, one sheet holds all four slots in different columns, so the same
// measurement never repeats a cell address across rounds. Compare by semantic identity, not address.
function readingIdentity(reading:Reading):string{
 if(reading.kind==='paired'&&reading.members?.length)return reading.members.map(readingIdentity).sort().join('+');
 if(reading.kind==='point')return 'point|'+(reading.device||'')+'|'+(reading.point_id||'')+'|'+(reading.parameter||'');
 if(reading.kind==='section')return 'section|'+(reading.device||'')+'|'+(reading.location||'')+'|'+(reading.form||'')+'|'+(reading.parameter||'');
 return 'cell|'+reading.cell;
}
const reviewOpen=ref(false),reviewFilter=ref<'all'|'pending'|'changed'>('all'),pendingFocus=ref('');
const previousSlot=computed(()=>{
 const index=slots.indexOf(shownSlot.value||activeSlot.value);
 for(let i=index-1;i>=0;i--)if(roundsBySlot.value.get(slots[i]))return slots[i];
 return '';
});
const reviewRows=computed<ReviewRow[]>(()=>{
 const page=currentPage.value,active=activeRound.value;
 if(!page||!active)return [];
 const entries=sheetReadings.value.get(page.baseKey)||[];
 const previous=previousSlot.value?roundsBySlot.value.get(previousSlot.value):undefined;
 const previousByIdentity=new Map<string,string>();
 if(previous)for(const {reading,round} of entries)if(round.round_id===previous.round_id)previousByIdentity.set(readingIdentity(reading),text(reading.value));
 const identityByCell=new Map<string,string>(),parameterByCell=new Map<string,string>();
 for(const {reading,round} of entries)if(round.round_id===active.round_id){
  identityByCell.set(reading.cell,readingIdentity(reading));
  parameterByCell.set(reading.cell,reading.parameter||'');
 }
 const rows:ReviewRow[]=[];
 for(const row of grid.value)for(const cell of row){
  if(cell.hidden||!cell.mapped||cell.round?.round_id!==active.round_id||rowHeight(cell.rowIndex)===0)continue;
  const identity=identityByCell.get(cell.address),previousValue=identity?previousByIdentity.get(identity):undefined;
  // The sheet's own row header is what an inspector actually reads; device ids are opaque.
  const header=text(page.rows[cell.rowIndex]?.[0]).trim();
  const current=cell.text.trim();
  rows.push({cell,address:cell.address,current,
   label:[header,parameterByCell.get(cell.address)].filter(Boolean).join(' · ')||cell.address,
   previous:(previousValue||'').trim(),hasPrevious:previousValue!==undefined,
   empty:!current,changed:previousValue!==undefined&&(previousValue||'').trim()!==current});
 }
 return rows;
});
const pendingRows=computed(()=>reviewRows.value.filter(row=>row.cell.editable&&row.empty));
const changedRows=computed(()=>reviewRows.value.filter(row=>row.changed));
const shownReviewRows=computed(()=>reviewFilter.value==='pending'?pendingRows.value:reviewFilter.value==='changed'?changedRows.value:reviewRows.value);
function openComparison(){reviewFilter.value=previousSlot.value?'changed':'all';reviewOpen.value=true;}
type Locator={key:string;sheet:string;cell:string;label:string;abnormal?:Reading};
const lookupMode=ref<''|'search'|'abnormal'>(''),lookupQuery=ref('');
const lookupTarget=ref<Locator>();
const lookupMatches=ref<Reading[]>([]),lookupPending=ref(false),lookupError=ref('');
const roundAbnormal=computed(()=>viewedRound.value?.abnormal_cells || []);
const lookupRows=computed<Locator[]>(()=>{
 if(lookupMode.value==='abnormal')return roundAbnormal.value.map(row=>({key:JSON.stringify([row.template,row.sheet]),
  sheet:row.sheet,cell:row.cell,abnormal:row,label:[row.device_name,row.parameter,row.note || '未填写备注'].filter(Boolean).join(' · ')}));
 const query=lookupQuery.value.toLowerCase();
 if(!query)return [];
 const results:Locator[]=[];
 for(const page of basePages.value){
  if(page.name.trim().toLowerCase().includes(query))results.push({key:page.key,sheet:page.name,cell:'A1',label:page.name.trim()});
  page.rows.some((row,i)=>row.some((value,j)=>{
   const label=text(value),cell=address(i,j);
   if((label && label.toLowerCase().includes(query))||cell.toLowerCase()===query)results.push({key:page.key,sheet:page.name,cell,label:label||cell});
   return results.length>=100;
  }));
  if(results.length>=100)break;
 }
 return [...new Map([...lookupMatches.value.map(row=>({key:JSON.stringify([row.template,row.sheet]),
  sheet:row.sheet,cell:row.cell,label:[row.device_name,row.parameter].filter(Boolean).join(' · ')})),...results]
  .map(row=>[row.key+row.cell,row])).values()].slice(0,100);
});
watch([lookupMode,lookupQuery,()=>viewedRound.value?.round_id],([mode,query,id],_,cleanup)=>{
 lookupMatches.value=[];lookupPending.value=false;lookupError.value='';
 if(mode!=='search'||!query||!id)return;
 const ctl=new AbortController(),key=context();
 lookupPending.value=true;
 const timer=window.setTimeout(async()=>{
  try{
   const data=await requestJson('/api/daily-inspection/day-sheet',{method:'POST',body:JSON.stringify({
    scope:scope.value,date:day.value,drafts:{},form_only:true,lookup:query,lookup_round:id,
   }),signal:ctl.signal,timeoutMs:15000}) as {matches?:Reading[]};
   if(!ctl.signal.aborted&&key===context())lookupMatches.value=data.matches || [];
  }catch{if(!ctl.signal.aborted)lookupError.value='设备和测点查询暂未完成，仍可查看原表匹配。';}
  finally{if(!ctl.signal.aborted)lookupPending.value=false;}
 },250);
 cleanup(()=>{window.clearTimeout(timer);ctl.abort();});
});
function openLookup(mode:'search'|'abnormal'){
 finishEdit();if(abnormal.value)return;
 lookupQuery.value='';lookupMode.value=mode;
}
function selectLookup(row:Locator){lookupTarget.value=row;lookupMode.value='';}
function applyLookupTarget(){
 const target=lookupTarget.value;
 if(!target)return;
 if(!basePages.value.some(page=>page.key===target.key)){lookupTarget.value=undefined;return;}
 if(!orderedBase.value.some(page=>page.key===target.key))professionView.value='ALL';
 selectPage(target.key);
 void nextTick(focusLookupTarget);
}
function focusLookupTarget(){
 const target=lookupTarget.value;
 if(!target||target.key!==pageKey.value||!sheetLoaded.value)return;
 lookupTarget.value=undefined;
 const [row,col]=coords(target.cell);
 const cell=cellLayout.value[row]?.[col];
 const merged=cell?.hidden ? grid.value.flat().find(entry=>!entry.hidden && entry.rowIndex<=row && entry.colIndex<=col &&
  entry.rowIndex+entry.rowspan>row && entry.colIndex+entry.colspan>col) : cell;
 const element=cellElement(merged?.address || target.cell);
 element?.scrollIntoView({block:'center',inline:'center'});
 if(element){element.tabIndex=element.tabIndex<0?-1:element.tabIndex;element.focus({preventScroll:true});}
 if(target.abnormal){
  const reading=grid.value[row]?.[col];
  if(reading?.editable && reading.round && reading.mapped && reading.text==='×'){
   abnormal.value={round:roundMeta(reading.round),edit:{template:target.abnormal.template,sheet:target.sheet,cell:target.cell,value:'×'}};
   abnormalNote.value=target.abnormal.note || '';
  }
 }
}
function jumpToCell(address:string){
 if(abnormal.value)return;
 finishEdit();
 if(abnormal.value)return;
 // Re-resolve from the live grid: committing an edit can rebuild it underneath us.
 void nextTick(()=>{
  const fresh=grid.value.flat().find(cell=>cell.address===address);
  cellElement(address)?.scrollIntoView({block:'center',inline:'center'});
  if(fresh)beginEdit(fresh);
 });
}
// NModal hands focus back to its trigger on close, which would blur the editor we just opened
// and commit it away. Wait for the leave transition instead of racing it.
function focusReviewCell(row:ReviewRow){
 if(abnormal.value)return;
 if(reviewOpen.value){pendingFocus.value=row.address;reviewOpen.value=false;return;}
 jumpToCell(row.address);
}
function applyPendingFocus(){const address=pendingFocus.value;pendingFocus.value='';if(address)jumpToCell(address);}
function gotoNextEmpty(){
 const rows=pendingRows.value;
 if(!rows.length)return;
 const index=rows.findIndex(row=>row.address===editing.value);
 focusReviewCell(rows[(index+1)%rows.length]||rows[0]);
}
let timer:number|undefined,controller:AbortController|undefined,disposed=false,storageKey='',storageUnverified=false;
let requestedArchiveRevision=props.archiveRevision;
const presence=ref<Presence[]>([]);
const syncedVisited=ref(new Set<string>());
let progressRequest:AbortController|undefined;
function persist(){try{if(storageKey&&!storageUnverified){if(dirty.value)localStorage.setItem(storageKey,JSON.stringify(jobs.value));else localStorage.removeItem(storageKey);}}catch{error.value='浏览器暂存失败，请保持页面打开直到保存完成。';}}
function viewedKey(){return report.value?.scope===scope.value && report.value.date===day.value && report.value.draft_key && currentActive.value ? 'inspection-viewed:'+report.value.draft_key+':'+currentActive.value.id : '';}
function restoreViewed(){
 const key=viewedKey();let saved:string[]=[];
 if(key)try{const value=JSON.parse(localStorage.getItem(key)||'[]');if(Array.isArray(value))saved=value.filter((v):v is string=>typeof v==='string');}catch{/* Reading progress is optional; never affect saved data. */}
 const valid=new Set(basePages.value.map(page=>page.baseKey));
 if(key){
  syncedVisited.value=new Set([...(currentActive.value?.visited_sheets || []),...syncedVisited.value]);
  presence.value=currentActive.value?.presence ?? presence.value;
 }
 visited.value=new Set([...saved,...syncedVisited.value].filter(value=>valid.has(value)));
}
function markViewed(){
 const key=viewedKey();if(!key||!currentPage.value||!sheetLoaded.value||!activeRound.value||readOnly.value
  ||currentActive.value?.pending||(isChiller.value&&chillerSlot.value!==activeSlot.value))return;
 visited.value=new Set([...visited.value,currentPage.value.baseKey]);
 try{localStorage.setItem(key,JSON.stringify([...visited.value]));}catch{/* The user can still confirm after viewing again. */}
 void syncProgress();
}
const readOnly=computed(()=>props.readOnly);
async function syncProgress(){
 const active=currentActive.value,page=currentPage.value;
 if(!active||!page||active.pending||props.readOnly||!activeRound.value||!sheetLoaded.value||document.hidden
  ||(isChiller.value&&chillerSlot.value!==activeSlot.value)||progressRequest)return;
 const controller=progressRequest=new AbortController();
 const key=context(),id=active.id;
 try{
  const data=await requestJson('/api/daily-inspection/view-sheet?view_scope='+encodeURIComponent(scope.value),{
   method:'POST',body:JSON.stringify({id,sheet:page.key,visited_sheets:[...visited.value]}),signal:controller.signal,timeoutMs:8000,
  }) as Progress;
  if(disposed||controller.signal.aborted||key!==context()||id!==currentActive.value?.id)return;
  syncedVisited.value=new Set([...syncedVisited.value,...(data.visited_sheets || [])]);
  visited.value=new Set([...visited.value,...syncedVisited.value]);
  presence.value=data.presence || [];
 }catch{/* Viewing progress is retried by the next poll. It cannot modify readings. */}
 finally{
  if(progressRequest===controller){
   progressRequest=undefined;
   if(key===context()&&id===currentActive.value?.id&&page.key!==currentPage.value?.key)void syncProgress();
  }
 }
}
function roundMeta(r:RoundMeta):RoundMeta{return {id:r.id,round_id:r.round_id,shift:r.shift,round:r.round,version:r.version,status:r.status};}
function context(){return JSON.stringify([scope.value,day.value]);}
let mutation=0,inFlight:Job|undefined;
let editor:{cell:GridCell;template:string;sheet:string;scope:string;date:string}|undefined;
const editCell=computed(()=>editing.value?editor?.cell:undefined);
const editNeighbors=computed(()=>{
 const target=editCell.value;if(!target)return [];
 const cells=grid.value.flat().filter(cell=>!cell.hidden&&rowHeight(cell.rowIndex)>0&&cell.editable&&cell.round?.round_id===target.round?.round_id&&
  (cell.address===target.address||(target.mapped?cell.mapped:!cell.mapped&&!text(cell.raw))));
 const index=cells.findIndex(cell=>cell.address===target.address);
 return index<0?[]:[cells[index-1],cells[index+1]];
});
function finishEdit(){if(editor)commit(editor.cell);}
function selectPage(key:string){
 if(key!==pageKey.value){
  finishEdit();if(abnormal.value)return;
  rememberScroll();pageKey.value=key;void nextTick(restoreScroll);
 }
 if(mobileSheet.value)compactMenu.value=false;
}
function openPage(key:string){
 if(!orderedBase.value.some(page=>page.key===key))professionView.value='ALL';
 selectPage(key);
}
function selectSlot(slot:string){
 finishEdit();if(abnormal.value)return;
 if(isChiller.value){rememberScroll();chillerSelection.value=slot;void nextTick(restoreScroll);}
 else selectedSlot.value=selectedSlot.value===slot?'':slot;
 if(mobileSheet.value)compactMenu.value=false;
}
watch(viewKey,markViewed);
function beginEdit(cell:GridCell){
 if(editing.value===cell.address)return;
 if(storageUnverified||!sheetLoaded.value||!cell.editable||(saving.value&&!savingCells.value)||props.parentBusy||!currentPage.value||loading.value)return;
 finishEdit();if(abnormal.value)return;
 editor={cell,template:currentPage.value.template,sheet:currentPage.value.name,scope:scope.value,date:day.value};
 editValue.value=cell.text;editing.value=cell.address;
}
function queue(round:RoundMeta,edit:CellEdit){
 mutation++;
 let job=jobs.value.find(j=>j!==inFlight&&!j.attempted&&j.round.round_id===round.round_id);
 if(!job){job={scope:scope.value,date:day.value,round:roundMeta(report.value?.rounds.find(r=>r.round_id===round.round_id)||round),cells:[]};jobs.value.push(job);}
 const existing=job.cells.find(c=>c.template===edit.template&&c.sheet===edit.sheet&&c.cell===edit.cell);
 if(existing)Object.assign(existing,edit);else job.cells.push(edit);
 persist();scheduleSave();
}
function scheduleSave(){window.clearTimeout(timer);timer=window.setTimeout(()=>void flush(),650);}
function commit(cell:GridCell){
 if(editing.value!==cell.address||!editor)return;
 const target=editor;editing.value='';editor=undefined;
 if(editValue.value===target.cell.text||!target.cell.round){if(dirty.value&&!error.value)scheduleSave();return;}
 if(target.scope!==scope.value||target.date!==day.value){error.value='填写所属楼栋或日期已变化，请重新核对。';return;}
 const edit={template:target.template,sheet:target.sheet,cell:target.cell.address,value:editValue.value};
 if(edit.value==='×'){abnormal.value={round:target.cell.round,edit};abnormalNote.value='';return;}
 queue(target.cell.round,edit);
}
function cellElement(address:string){return scrollElement.value?.querySelector<HTMLElement>(`td[data-address="${address}"]`);}
function completeEdit(){const cell=cellElement(editing.value);finishEdit();if(!abnormal.value)void nextTick(()=>cell?.focus({preventScroll:true}));}
function moveEdit(direction:number){
 const next=editNeighbors.value[direction<0?0:1];if(!next)return;
 finishEdit();if(abnormal.value)return;
 cellElement(next.address)?.scrollIntoView({block:'nearest',inline:'nearest'});
 beginEdit(next);
}
function cancelEdit(){const cell=cellElement(editing.value);editing.value='';editor=undefined;if(dirty.value&&!error.value)scheduleSave();void nextTick(()=>cell?.focus({preventScroll:true}));}
function saveAbnormal(){if(!abnormal.value||!abnormalNote.value.trim())return false;queue(abnormal.value.round,{...abnormal.value.edit,note:abnormalNote.value.trim()});abnormal.value=undefined;return true;}
function cacheTemplates(templates:Report['original_templates'],version:string){cachedTemplates.value=templates;layoutVersion.value=version;try{localStorage.setItem('inspection-layout:'+scope.value,JSON.stringify({version,templates}));}catch{/* Layout caching is optional. */}}
function attachTemplates(value:Report){if(value.original_templates?.length){cacheTemplates(value.original_templates,value.template_version||'');return;}if(value.template_version!==layoutVersion.value)throw new Error('巡检模板已更新，请刷新后核对。');value.original_templates=cachedTemplates.value;}
function showTemplate(){if(!report.value?.draft_key&&cachedTemplates.value.length)report.value={scope:scope.value,date:day.value,draft_key:'',can_edit_history:false,rounds:[],original_templates:cachedTemplates.value};}
async function loadTemplates(){const key=context();try{const cached=JSON.parse(localStorage.getItem('inspection-layout:'+scope.value)||'null');if(cached?.version&&cached.version===props.templateVersion&&Array.isArray(cached.templates)&&cached.templates.every((t:{scope?:string})=>t.scope===scope.value)){cachedTemplates.value=cached.templates;layoutVersion.value=cached.version;showTemplate();return;}}catch{}
 try{const data=await requestJson('/api/daily-inspection/templates?scope='+scope.value+'&v='+encodeURIComponent(props.templateVersion||''),{cache:'no-store'}) as {scope:string;version:string;templates:Report['original_templates']};if(disposed||key!==context()||data.scope!==scope.value||(props.templateVersion&&data.version!==props.templateVersion)||!Array.isArray(data.templates))return;cacheTemplates(data.templates,data.version);showTemplate();await nextTick();measureHeight();}catch{/* The data response can still provide the template; its error remains visible. */}}
async function load(){
 if(disposed||dirty.value||saving.value||editing.value||abnormal.value)return;
 if(loading.value&&controller&&!controller.signal.aborted)return;
 controller?.abort();const ctl=controller=new AbortController(),key=context(),stamp=mutation;
 requestedArchiveRevision=props.archiveRevision;
  const selected=currentPage.value;
  const selection=selected ? {view_sheet:[selected.template,selected.name]} : {summary_only:true};
  let accepted=false;
 loading.value=true;error.value='';
 try{
   const value=await requestJson('/api/daily-inspection/day-sheet',{method:'POST',body:JSON.stringify({scope:scope.value,date:day.value,drafts:{},form_only:true,template_version:layoutVersion.value,...selection}),signal:ctl.signal,timeoutMs:180000}) as Report;
  if(disposed||ctl.signal.aborted||controller!==ctl||key!==context()||stamp!==mutation||dirty.value||editing.value)return;
  if(value.scope!==scope.value||value.date!==day.value||!value.draft_key)throw new Error('巡检表返回的楼栋或日期不一致，未替换当前数据。');
   if(value.sheet_filter && JSON.stringify(value.sheet_filter)!==selected?.baseKey)throw new Error('返回的巡检工作表不一致，请刷新。');
  attachTemplates(value);report.value=value;storageUnverified=false;storageKey='inspection-sheet-edits:'+value.draft_key;
   accepted=true;
  try{const pending=JSON.parse(localStorage.getItem(storageKey)||'[]');
   if(!Array.isArray(pending))throw new Error();
   jobs.value=pending.map((job:any)=>{
    if(!job.round?.round_id||!Array.isArray(job.cells)||(job.scope&&job.scope!==scope.value)||(job.date&&job.date!==day.value))throw new Error();
    return {scope:scope.value,date:day.value,round:roundMeta(job.round),cells:job.cells,attempted:job.attempted!==false};
   });
  }catch{storageUnverified=true;error.value='暂存修改无法核验，请先核对本机数据；未自动提交。';}
  restoreViewed();
  if(!currentPage.value)pageKey.value=preferredPage();
  await nextTick();markViewed();measureHeight();restoreScroll();focusLookupTarget();
 }catch(e){if(!ctl.signal.aborted&&key===context())error.value=e instanceof Error?e.message:'巡检表读取失败';}
  finally{if(controller===ctl){
   loading.value=false;
   if(accepted&&currentPage.value&&!sheetLoaded.value&&!storageUnverified)void load();
  }}
}
function applySaved(job:Job,version:string|number){
 const value=report.value;if(!value)return;
 report.value={...value,rounds:value.rounds.map(r=>{
  if(r.round_id!==job.round.round_id)return r;
  const readings=r.original_cells.map(c=>({...c})),extras=(r.extra_cells||[]).map(c=>({...c}));
  const abnormal=[...(r.abnormal_cells || [])];
  for(const edit of job.cells){
   const matches=(cell:Reading)=>cell.template===edit.template&&cell.sheet===edit.sheet&&cell.cell===edit.cell;
   const existing=[...readings,...extras].find(matches);
   if(existing)Object.assign(existing,{value:edit.value,...(edit.note ? {note:edit.note} : {})});
   else extras.push({...edit});
   const index=abnormal.findIndex(matches);
   if(index>=0)abnormal.splice(index,1);
   if(edit.value==='×')abnormal.push({...existing,...edit});
  }
  return {...r,version,original_cells:readings,extra_cells:extras,abnormal_cells:abnormal};
 })};
 for(const queued of jobs.value)if(queued!==job&&queued.round.round_id===job.round.round_id)queued.round.version=version;
}
async function flush(){
 if(saving.value||props.parentBusy||editing.value||abnormal.value||comparison.value||!dirty.value||disposed)return;
 saving.value=true;savingCells.value=true;error.value='';window.clearTimeout(timer);controller?.abort();loading.value=false;
 const key=context();
 try{
  while(jobs.value.length&&!disposed){
   const job=jobs.value[0],r=job.round;
   if(job.scope!==scope.value||job.date!==day.value)throw new Error('暂存修改不属于当前楼栋和日期，已停止发送');
   inFlight=job;job.attempted=true;persist();
   let result:any,version:string|number;
   if(r.round_id===currentActive.value?.id){
    result=await requestJson('/api/daily-inspection/draft?view_scope='+encodeURIComponent(job.scope),{method:'POST',body:JSON.stringify({id:r.round_id,review_version:r.version,sheet_cells:job.cells}),timeoutMs:180000});
    if(result.active?.id!==r.round_id||typeof result.active.review_version!=='number')throw new Error('保存回执未确认原巡检轮次');
    version=result.active.review_version;
   }else{
    if(r.status!=='已归档'||!r.id)throw new Error('原巡检状态已变化，请核对最新数据');
    result=await requestJson('/api/daily-inspection/history-edit?scope='+encodeURIComponent(job.scope),{method:'POST',body:JSON.stringify({id:r.id,version:r.version,cells:job.cells}),timeoutMs:180000});
    if(result.id!==r.id||result.scope!==job.scope||typeof result.version!=='string')throw new Error('历史修改回执未确认原记录');
    version=result.version;
   }
   if(disposed||key!==context())return;
   for(const cell of job.cells)acknowledgedManual.value.add(roundPosition(r.round_id,cell.template,cell.sheet,cell.cell));
   acknowledgedManual.value=new Set(acknowledgedManual.value);applySaved(job,version);jobs.value.shift();inFlight=undefined;persist();
   if(editing.value||abnormal.value)break;
  }
  if(!dirty.value)emit('changed');
 }catch(e){if(!disposed&&key===context())error.value=(e instanceof Error?e.message:'保存尚未确认')+'；未确认修改已保留。';}
 finally{inFlight=undefined;saving.value=false;savingCells.value=false;}
}
async function compareLatest(){
 if(saving.value||loading.value)return;finishEdit();controller?.abort();const ctl=controller=new AbortController(),key=context();loading.value=true;
 try{const value=await requestJson('/api/daily-inspection/day-sheet',{method:'POST',body:JSON.stringify({scope:scope.value,date:day.value,drafts:{},form_only:true,template_version:layoutVersion.value}),signal:ctl.signal,timeoutMs:180000}) as Report;
 if(!disposed&&!ctl.signal.aborted&&key===context()&&value.scope===scope.value&&value.date===day.value){attachTemplates(value);comparison.value=value;}}
 catch(e){if(!disposed&&!ctl.signal.aborted&&key===context())error.value=e instanceof Error?e.message:'最新数据读取失败';}
 finally{if(controller===ctl)loading.value=false;}
}
function latestValue(round:RoundMeta,cell:CellEdit){const r=comparison.value?.rounds.find(r=>r.round_id===round.round_id);const c=[...(r?.original_cells||[]),...(r?.extra_cells||[])].find(c=>c.template===cell.template&&c.sheet===cell.sheet&&c.cell===cell.cell);if(c)return text(c.value);
 const sheet=comparison.value?.original_templates.find(t=>t.name===cell.template)?.sheets.find(s=>s.name===cell.sheet);const [ri,ci]=coords(cell.cell);return text(sheet?.rows[ri]?.[ci]);}
function resolveJob(job:Job,keep:boolean){
 const latest=comparison.value?.rounds.find(r=>r.round_id===job.round.round_id);if(keep&&!latest){error.value='原巡检记录已不存在，请先核对后选择共享数据。';return;}
 if(keep){job.round=roundMeta(latest!);job.attempted=false;}else jobs.value=jobs.value.filter(j=>j!==job);
 const unresolved=jobs.value.some(j=>j!==job&&(!comparison.value?.rounds.some(r=>r.round_id===j.round.round_id)||comparison.value?.rounds.some(r=>r.round_id===j.round.round_id&&r.version!==j.round.version)));
 if(!unresolved){report.value=comparison.value;comparison.value=undefined;error.value='';persist();if(dirty.value)void flush();}else persist();
}
async function submit(){if(!currentActive.value||actionLocked.value||!canSubmit.value)return;saving.value=true;error.value='';
 try{await requestJson('/api/daily-inspection/finish?view_scope='+scope.value,{method:'POST',body:JSON.stringify({id:currentActive.value.id,review_version:currentActive.value.review_version,sheet_workflow:true,visited_sheets:[...visited.value]}),timeoutMs:180000});emit('changed');}
 catch(e){error.value=e instanceof Error?e.message:'提交未确认，请重试原任务。';}finally{saving.value=false;await load();}}
async function download(){try{await downloadFile('/api/daily-inspection/day-export',{method:'POST',body:JSON.stringify({scope:scope.value,date:day.value,drafts:{}}),timeoutMs:180000});}catch(e){error.value=e instanceof Error?e.message:'导出失败';}}
function changeScope(e:Event){if(storageUnverified||dirty.value||saving.value||editing.value||abnormal.value||sortOpen.value)return;scope.value=(e.target as HTMLSelectElement).value;emit('scope',scope.value);void loadPreferences();reset();}
function changeDay(e:Event){if(storageUnverified||dirty.value||saving.value||editing.value||abnormal.value||sortOpen.value)return;day.value=(e.target as HTMLInputElement).value;reset();}
function clearProgress(){progressRequest?.abort();progressRequest=undefined;syncedVisited.value=new Set();presence.value=[];}
function reset(){cachedTemplates.value=[];layoutVersion.value='';settingsOpen.value=false;sortOpen.value=false;lookupMode.value='';lookupTarget.value=undefined;reviewOpen.value=false;groupDraft.value={};sortTarget=undefined;controller?.abort();window.clearTimeout(timer);storageKey='';mutation++;scrollPositions.clear();editor=undefined;editing.value='';comparison.value=undefined;report.value=undefined;visited.value=new Set();clearProgress();pageKey.value='';selectedSlot.value='';chillerSelection.value='';void loadTemplates();void load();}
watch(()=>props.defaultScope,v=>{if(v&&v!==scope.value&&!dirty.value&&!saving.value&&!editing.value&&!abnormal.value){scope.value=v;void loadPreferences();reset();}});
watch(()=>currentActive.value?.id,()=>{clearProgress();restoreViewed();void nextTick(markViewed);if(!saving.value&&!dirty.value)void load();});
watch([()=>currentActive.value?.visited_sheets,()=>currentActive.value?.presence],restoreViewed);
watch(()=>props.active?.review_version,version=>{if(!saving.value&&!dirty.value&&!editing.value&&report.value?.rounds.find(r=>r.round_id===currentActive.value?.id)?.version!==version)void load();});
const archiveRefreshIdle=computed(()=>!loading.value&&!saving.value&&!dirty.value&&!editing.value&&!abnormal.value&&!comparison.value&&
 !sortOpen.value&&!settingsOpen.value&&!confirmSubmit.value&&!props.parentBusy);
watch([()=>props.archiveRevision,archiveRefreshIdle],([revision,idle])=>{
 if(idle&&!storageUnverified&&revision!==requestedArchiveRevision)void load();
});
watch(()=>props.parentBusy,busy=>{if(!busy&&dirty.value&&!error.value)scheduleSave();});
watch([()=>currentPage.value?.baseKey,dirty],()=>{if(currentPage.value&&!sheetLoaded.value&&!storageUnverified)void load();});
const scrollElement=ref<HTMLElement>();const scrollPositions=new Map<string,{left:number;top:number}>();
const sheetTabs=ref<HTMLElement>();
watch([sheetTabs,pageKey,orderedBase,rotation],()=>{
 const tabs=sheetTabs.value,tab=tabs?.querySelector<HTMLElement>('button[aria-pressed="true"]');
 if(!tabs||!tab)return;
 const left=tab.offsetLeft,right=left+tab.offsetWidth;
 if(left<tabs.scrollLeft)tabs.scrollLeft=left;
 else if(right>tabs.scrollLeft+tabs.clientWidth)tabs.scrollLeft=right-tabs.clientWidth;
},{flush:'post'});
function rememberScroll(){const el=scrollElement.value;if(el)scrollPositions.set(viewKey.value,{left:el.scrollLeft,top:el.scrollTop});}
function restoreScroll(){const el=scrollElement.value,pos=scrollPositions.get(viewKey.value);if(el){el.scrollLeft=pos?.left||0;el.scrollTop=pos?.top||0;}}
function resizeSheet(delta:number){const el=scrollElement.value,previous=zoom.value;const next=Math.max(7,Math.min(28,previous+delta));const left=el?.scrollLeft||0,top=el?.scrollTop||0;zoom.value=next;void nextTick(()=>{if(el){el.scrollLeft=left*next/previous;el.scrollTop=top*next/previous;}});}
let pinch:{element:HTMLElement;distance:number;zoom:number;x:number;y:number}|undefined;
let pinchFrame=0;
function touchPair(event:TouchEvent){if(event.touches.length!==2)return;const [a,b]=Array.from(event.touches);return {distance:Math.hypot(a.clientX-b.clientX,a.clientY-b.clientY),x:(a.clientX+b.clientX)/2,y:(a.clientY+b.clientY)/2};}
function localTouchPoint(el:HTMLElement,x:number,y:number){const box=el.getBoundingClientRect();if(rotation.value==='landscape')return {x:y-box.top,y:box.right-x};if(rotation.value==='portrait')return {x:box.bottom-y,y:x-box.left};return {x:x-box.left,y:y-box.top};}
function startPinch(event:TouchEvent){const pair=touchPair(event),el=scrollElement.value;if(!pair||!el||pair.distance<8)return;if(event.cancelable)event.preventDefault();const pos=localTouchPoint(el,pair.x,pair.y);pinch={element:el,distance:pair.distance,zoom:zoom.value,x:el.scrollLeft+pos.x,y:el.scrollTop+pos.y};}
function movePinch(event:TouchEvent){const pair=touchPair(event),state=pinch;if(!pair||!state)return;if(event.cancelable)event.preventDefault();cancelAnimationFrame(pinchFrame);pinchFrame=requestAnimationFrame(()=>{pinchFrame=0;if(disposed||state.element!==scrollElement.value)return;const next=Math.max(7,Math.min(28,Math.round(state.zoom*pair.distance/state.distance*100)/100)),ratio=next/state.zoom;zoom.value=next;void nextTick(()=>{if(disposed||state.element!==scrollElement.value)return;const pos=localTouchPoint(state.element,pair.x,pair.y);state.element.scrollLeft=state.x*ratio-pos.x;state.element.scrollTop=state.y*ratio-pos.y;});});}
function endPinch(event:TouchEvent){if(event.touches.length<2)pinch=undefined;}
function changeProfessionView(event:Event){
 const select=event.target as HTMLSelectElement;
 finishEdit();if(abnormal.value){select.value=professionView.value;return;}
 rememberScroll();professionView.value=select.value;void nextTick(restoreScroll);
}
function measureHeight(){
 viewportPortrait.value=screenIsPortrait();
 if(!rootElement.value)return;
 const visibleHeight=Math.min(window.innerHeight,window.visualViewport?.height||window.innerHeight);
 rotatedWidth.value=visibleHeight;
 const height=rotation.value?window.innerWidth:visibleHeight;
 availableHeight.value=Math.max(0,height-(mobileSheet.value?0:rootElement.value.getBoundingClientRect().top)-2);
}
onMounted(()=>{measureHeight();phoneMedia.addEventListener('change',updatePhoneScreen);window.addEventListener('resize',measureHeight);window.addEventListener('orientationchange',measureHeight);window.screen.orientation?.addEventListener?.('change',measureHeight);window.visualViewport?.addEventListener('resize',measureHeight);});
watch(orderedBase,()=>{if(!currentPage.value)pageKey.value=preferredPage();});
let preferenceGeneration=0;
function preferencesUrl(){return '/api/workspace-layouts/desktop/inspection-sheets-'+scope.value;}
async function loadPreferences(){const generation=++preferenceGeneration;preferencesReady.value=false;preferencesError.value='';orders.value={ALL:[],电气:[],暖通:[]};
 try{const result=await requestJson(preferencesUrl()) as {revision:number;value:{orders:Record<string,string[]>}|null};if(disposed||generation!==preferenceGeneration)return;preferencesRevision.value=result.revision;orders.value=result.value?.orders||{ALL:[],电气:[],暖通:[]};preferencesReady.value=true;}
 catch(e){if(generation===preferenceGeneration)preferencesError.value=e instanceof Error?e.message:'个人排序读取失败';}}
const showGroups=ref(false),groupPanel=ref<HTMLElement>();
function openSettings(){finishEdit();if(!abnormal.value){showGroups.value=false;settingsOpen.value=true;}}
function openGroups(){finishEdit();if(!abnormal.value){showGroups.value=true;settingsOpen.value=true;void nextTick(()=>groupPanel.value?.scrollIntoView({block:'nearest'}));}}
let sortTarget:{scope:string;view:string;revision:number;orders:Record<string,string[]>}|undefined;
function openSort(){finishEdit();if(abnormal.value||dirty.value)return;settingsOpen.value=false;sortTarget={scope:scope.value,view:professionView.value,revision:preferencesRevision.value,orders:JSON.parse(JSON.stringify(orders.value))};sortDraft.value=orderedBase.value.map(p=>p.baseKey);sortOpen.value=true;}
function shiftSort(index:number,delta:number){const list=sortDraft.value.slice();[list[index],list[index+delta]]=[list[index+delta],list[index]];sortDraft.value=list;}
async function saveSort(){if(!preferencesReady.value||saving.value||!sortTarget||sortTarget.scope!==scope.value)return;const target=sortTarget,key=context();saving.value=true;const value={version:1,orders:{...target.orders,[target.view]:sortDraft.value}};
 try{const result=await requestJson('/api/workspace-layouts/desktop/inspection-sheets-'+target.scope,{method:'PUT',body:JSON.stringify({revision:target.revision,value})}) as {revision:number};if(disposed||key!==context())return;preferencesRevision.value=result.revision;orders.value=value.orders;sortOpen.value=false;preferencesError.value='';}
 catch(e){preferencesError.value=e instanceof Error?e.message:'排序保存失败，调整仍保留';}finally{saving.value=false;}}
const groupDraft=ref<Record<string,string|null>>({});
const pendingSwap=ref(false);
const confirmedProfessions=computed(()=>Object.entries(currentActive.value?.profession_confirmations || {})
 .filter(([name,item])=>item.version && (!currentActive.value?.profession_versions || item.version===currentActive.value.profession_versions[name]))
 .map(([name])=>name));
const canGroup=computed(()=>isCurrentDate.value && !props.readOnly && !!props.defaultShift &&
 (!currentActive.value || (!currentActive.value.pending && viewedRound.value?.round_id===currentActive.value.id)));
const groupValue=(page:Page)=>page.key in groupDraft.value ? groupDraft.value[page.key] || page.homeProfession : currentGroup(page);
const swapWarning=computed(()=>`调整归属会使 ${confirmedProfessions.value.join('、')} 的已有确认作废，需要重新核对。`);
function stageGroup(page:Page,event:Event){
 const value=(event.target as HTMLSelectElement).value;
 const next={...groupDraft.value};
 if(value===currentGroup(page))delete next[page.key];
 else next[page.key]=value===page.homeProfession ? null : value;
 groupDraft.value=next;
}
function restoreGroups(){
 groupDraft.value=Object.fromEntries(basePages.value.filter(page=>currentGroup(page)!==page.homeProfession).map(page=>[page.key,null]));
}
function requestGroupSave(){
 if(!canGroup.value||actionLocked.value||!Object.keys(groupDraft.value).length)return;
 if(confirmedProfessions.value.length){pendingSwap.value=true;return;}
 void applySwap();
}
async function applySwap(){
 pendingSwap.value=false;
 if(!canGroup.value)return;
 const active=currentActive.value;
 const duty={scope:scope.value,date:day.value,shift:props.defaultShift};
 if(await writeRoundAction('sheet-group',{id:active?.id || '',duty,review_version:active?.review_version,groups:groupDraft.value})){
  groupDraft.value={};settingsOpen.value=false;
 }
}
function confirmationLabel(name:string){
 const receipt=currentActive.value?.profession_confirmations?.[name];
 return [receipt?.actor || '已确认',receipt?.confirmed ? roundClock.format(receipt.confirmed) : ''].filter(Boolean).join(' · ');
}
async function writeRoundAction(action:string,body:object){
 if(actionLocked.value||props.readOnly)return false;
 saving.value=true;error.value='';
 try{
  const key=context(),activeId=currentActive.value?.id;
  const state=await requestJson('/api/daily-inspection/'+action+'?view_scope='+encodeURIComponent(scope.value),{
   method:'POST',body:JSON.stringify(body),timeoutMs:30000,
  });
  if(!disposed&&key===context()&&activeId===currentActive.value?.id)emit('round-state',state);
  return true;
 }catch(e){
  error.value=e instanceof Error?e.message:'本轮操作未确认，请刷新状态后重试。';
  emit('changed');
  return false;
 }finally{saving.value=false;}
}
function confirmProfession(profession:string){
 const active=currentActive.value,version=active?.profession_versions?.[profession];
 if(!active||active.pending||!version||!hasViewed(profession))return;
 void writeRoundAction('confirm-profession',{id:active.id,profession,profession_version:version});
}
function joinProfession(profession:string){
 const active=currentActive.value;
 if(!active||active.pending||!joinableProfessions.value.includes(profession))return;
 void writeRoundAction('append-profession',{id:active.id,profession,review_version:active.review_version});
}
function allowNavigation(){finishEdit();if(storageUnverified||dirty.value||saving.value||editing.value||abnormal.value||sortOpen.value){error.value='请先保存单元格修改，再离开巡检表。';return false;}return true;}
const unregister=registerNavigationGuard(allowNavigation);
defineExpose({allowNavigation});
function unload(e:BeforeUnloadEvent){if(dirty.value||saving.value||editing.value||abnormal.value)e.preventDefault();}
window.addEventListener('beforeunload',unload);
const progressTimer=window.setInterval(()=>void syncProgress(),12000);
void loadTemplates();void load();void loadPreferences();
onBeforeUnmount(()=>{disposed=true;controller?.abort();clearProgress();window.clearInterval(progressTimer);window.clearTimeout(timer);cancelAnimationFrame(pinchFrame);pinch=undefined;persist();unregister();phoneMedia.removeEventListener('change',updatePhoneScreen);window.removeEventListener('beforeunload',unload);window.removeEventListener('resize',measureHeight);window.removeEventListener('orientationchange',measureHeight);window.screen.orientation?.removeEventListener?.('change',measureHeight);window.visualViewport?.removeEventListener('resize',measureHeight);});
</script>
<style scoped>
.inspection-workbook{
  display:grid;gap:8px;min-width:0}.book-tools,.book-status,.round-tabs{display:flex;align-items:center;gap:6px;flex-wrap:wrap}.book-tools label{display:flex;gap:4px;align-items:center}button,input,select,textarea{font:inherit;color:inherit;border:1px solid var(--inspection-line);background:var(--inspection-surface);padding:6px;min-height:34px;box-sizing:border-box}button{cursor:pointer}button:disabled{opacity:.5;cursor:default}.selected{background:var(--inspection-accent);color:var(--inspection-on-accent)}p{margin:0}.book-scroll{overflow:auto;max-height:calc(100dvh - 230px);min-height:240px;border:1px solid var(--inspection-line);overscroll-behavior:contain}table{border-collapse:collapse;min-width:100%;width:max-content;table-layout:fixed}td{border:1px solid var(--inspection-line);height:30px;min-width:72px;max-width:210px;padding:4px;white-space:pre-wrap;text-align:center;overflow-wrap:anywhere}td[tabindex]{cursor:text}td:focus{outline:2px solid var(--inspection-accent);outline-offset:-2px}td input{width:100%;min-width:72px;max-width:210px;padding:2px}.current-cell,.current-key{background:color-mix(in srgb,var(--inspection-surface) 75%,#ffc83d)}.history-cell,.history-key{background:color-mix(in srgb,var(--inspection-surface) 92%,#5783a9)}.current-key,.history-key{display:inline-block;width:14px;height:14px;border:1px solid var(--inspection-line)}.empty-cell{color:var(--cf-warning)}.book-status{font-size:12px}.book-submit{display:flex;align-items:center;justify-content:space-between;gap:8px;position:sticky;bottom:0;padding:8px 0 max(8px,env(safe-area-inset-bottom));background:var(--inspection-surface)}.primary{background:var(--inspection-accent);color:var(--inspection-on-accent);padding:9px 25px;margin-left:auto}.book-error{color:var(--cf-danger)}textarea{width:100%}@media(max-width:760px){.toolbar-label{display:none}input,select{font-size:16px}.book-tools{font-size:13px}.book-tools input[type=date]{width:142px}.book-scroll{max-height:calc(100dvh - 250px)}}
.book-settings-content{display:grid;gap:12px}.book-settings-content label{display:flex;justify-content:space-between;align-items:center;gap:8px}.book-settings-content select{font-size:16px;min-height:40px}.book-error{max-height:20dvh;overflow:auto;flex-shrink:0}.sort-list{max-height:55dvh;overflow:auto}.sort-row{display:flex;align-items:center;gap:5px;margin:5px 0}.sort-row span{flex:1}.full-height{height:var(--available-height);display:flex;flex-direction:column;overflow:hidden}.full-height>.book-tools,.full-height>.book-status,.full-height>.book-submit{flex-shrink:0}.full-height>.book-scroll{flex:1;min-height:0;max-height:none;touch-action:pan-x pan-y}.full-height .book-submit{position:static}
@media(max-height:500px) and (hover:none){.full-height .book-status{display:none}.full-height .book-tools{gap:3px}.full-height button,.full-height input,.full-height select{min-height:30px}.full-height .book-submit{padding:3px 0}.full-height{gap:4px}}

/* The colour legend and the save state are different things; they must not read as one sentence. */
.status-legend{display:inline-flex;align-items:center;gap:5px;opacity:.78;white-space:nowrap}
.status-save{display:inline-flex;align-items:center;gap:6px;flex-wrap:wrap;min-width:0;margin-left:auto}
.lock-reason{color:var(--inspection-muted)}
/* The submit footer stays visible on phones so a gated button explains itself instead of vanishing. */
.book-signoffs,.group-actions{display:flex;flex-wrap:wrap;align-items:center;gap:5px;min-width:0}
.book-signoffs{font-size:12px}
.sheet-tabs button small{margin-inline-start:5px;font-size:10px;opacity:.78}
.submit-action{margin-left:auto;flex:none}
.group-list{max-height:32dvh;overflow:auto;display:grid;gap:4px}
.group-list label{padding:4px 0;border-bottom:1px solid var(--inspection-line)}
.group-list small{display:block;color:var(--inspection-muted)}
.submit-remaining{flex:1 1 auto;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--cf-warning)}
.book-submit .primary:disabled{cursor:not-allowed}

/* Review drawer: pending cells and cross-round differences, joined by measurement identity. */
.status-review{display:inline-flex;align-items:center;gap:5px}
.status-review button{min-height:28px;padding:2px 9px;font-size:12px}
.review-filters{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:7px;margin-bottom:8px}
.review-filters button{min-height:38px;padding:5px 6px;font-size:13px;border:1px solid var(--inspection-line);border-radius:7px;background:transparent;color:inherit;cursor:pointer}
.review-filters button[aria-pressed=true]{background:var(--inspection-accent);border-color:var(--inspection-accent);color:var(--inspection-on-accent)}
.settings-note{margin:0 0 8px;font-size:12px;line-height:1.5;color:var(--inspection-muted)}
.review-list{display:grid;gap:2px;max-height:min(56dvh,420px);overflow:auto}
.review-list .settings-note{padding:8px 6px}
.review-row{display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:center;gap:8px;width:100%;
  padding:7px 6px;border:0;border-bottom:1px solid var(--inspection-line);background:transparent;color:inherit;font:inherit;text-align:left;cursor:pointer}
.review-row:hover{background:var(--inspection-soft)}
.review-label{min-width:0;font-size:13px;overflow-wrap:anywhere}
.review-label small{display:block;font-size:11px;color:var(--inspection-muted)}
.review-values{display:inline-flex;align-items:baseline;gap:7px;flex:none;font-size:13px;font-variant-numeric:tabular-nums}
.review-values i{font-style:normal;font-weight:600}
.review-values i.review-empty{color:var(--cf-warning)}
.review-values em{font-style:normal;font-size:12px;color:var(--inspection-muted)}
.review-values b{font-size:11px;font-weight:600;color:var(--cf-danger)}

.book-scroll{width:100%;max-width:100%;min-width:0;box-sizing:border-box;isolation:isolate}
.book-scroll table{min-width:0;max-width:none;table-layout:fixed}.book-scroll td{min-width:0;max-width:none;box-sizing:border-box;overflow:hidden}.book-scroll td>span{display:block;overflow:hidden;white-space:inherit;overflow-wrap:normal;line-height:normal}.book-scroll td input,.book-scroll td select{min-width:0;max-width:100%;width:100%;box-sizing:border-box}.book-scroll td:focus-within{outline:2px solid var(--inspection-accent);outline-offset:-2px}
.settings-section{display:flex;flex-direction:column;gap:10px;padding-bottom:16px;border-bottom:1px solid rgba(130,145,165,.22)}.settings-section span,.settings-section small{font-size:13px;opacity:.75}.book-pending{font-size:12px;color:var(--cf-warning);flex-shrink:0}

.book-scroll th{height:28px;border:1px solid var(--inspection-line);background:var(--inspection-soft,var(--inspection-surface));font-size:12px;font-weight:500;text-align:center;box-sizing:border-box}.column-number{position:sticky;top:0;z-index:3}.row-number{position:sticky;left:0;z-index:2;min-width:42px}.grid-corner{position:sticky;left:0;top:0;z-index:4}.editing-address{font-weight:600;color:var(--inspection-accent)}
.cell-editor-tools{display:flex;align-items:center;gap:7px;padding:6px 0;flex-shrink:0;background:var(--inspection-surface)}.cell-editor-tools strong{font-size:13px;margin-right:auto;white-space:nowrap}.cell-editor-actions{display:flex;gap:4px}.cell-editor-tools button{min-height:40px;padding:7px 12px;white-space:nowrap}.cell-editor-tools .primary{margin-left:0}.cell-editor-tools~.book-submit{display:none}
.settings-choices{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:7px}.settings-choices.two{grid-template-columns:repeat(2,minmax(0,1fr))}.settings-choices>button{appearance:none;display:flex;align-items:center;justify-content:center;position:relative;width:100%;height:40px;min-width:0;padding:6px;border:1px solid rgba(130,145,165,.4);border-radius:7px;background:transparent;color:inherit;line-height:1.4}.settings-choices>button[aria-pressed=true]{background:var(--inspection-accent);border-color:var(--inspection-accent);color:var(--inspection-on-accent)}.book-settings-content{max-height:min(70dvh,calc(var(--available-height) - 180px));overflow:auto;padding:2px}
.book-options{display:contents}
.full-height{position:fixed;inset:0 0 auto;z-index:1000;box-sizing:border-box;gap:2px;padding:env(safe-area-inset-top) env(safe-area-inset-right) env(safe-area-inset-bottom) env(safe-area-inset-left);background:var(--inspection-surface,#fff);width:100%;max-width:100vw}
.full-height.rotate-landscape,.full-height.rotate-portrait{inset:0 auto auto 0;width:var(--rotated-width);max-width:none;transform-origin:top left}
.full-height.rotate-landscape{transform:translateX(100dvw) rotate(90deg)}
.full-height.rotate-portrait{transform:translateY(var(--rotated-width)) rotate(-90deg)}
.full-height :deep(.n-modal){max-height:calc(var(--available-height) - 12px);overflow-y:auto}
.mobile-sheet-bar{display:flex;align-items:center;gap:2px;flex:0 0 auto;min-width:0;padding:2px;border-bottom:1px solid var(--inspection-line)}
.mobile-sheet-bar>button{flex:0 0 auto;min-height:44px;min-width:44px;padding:3px;font-size:13px;white-space:nowrap}
.mobile-zoom-tools{display:flex;gap:4px}
.mobile-zoom-tools button{min-width:44px}
.mobile-sheet-context{display:flex;flex:1;flex-wrap:wrap;align-items:baseline;gap:0 8px;min-width:0;padding:0 5px;line-height:1.35}
.mobile-sheet-context>span{font-size:11px;color:var(--inspection-muted);white-space:nowrap}
.mobile-sheet-context>strong{font-size:13px;font-weight:650}
.sheet-navigation{display:flex;flex-shrink:0;min-width:0;gap:4px;align-items:stretch;padding:0 2px 2px;border-bottom:1px solid var(--inspection-line)}
.sheet-tabs{position:relative;display:flex;flex:1;min-width:0;gap:4px;overflow:auto hidden;scrollbar-width:none}
.sheet-tabs::-webkit-scrollbar{display:none}
.sheet-tabs>button{flex:0 0 auto;max-width:min(90%,320px);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.sheet-tabs>button,.sheet-sort{min-height:36px;padding:4px 8px;font-size:13px}
.sheet-tabs>button[aria-pressed=true]{border-color:var(--inspection-accent);background:var(--inspection-accent);color:var(--inspection-on-accent)}
.sheet-sort{flex:0 0 auto;min-width:44px}
.mobile-header{min-width:0;display:grid;gap:2px;flex-shrink:0}
.rotate-landscape .mobile-header{grid-template-columns:auto minmax(0,1fr)}
@media(max-height:500px) and (pointer:coarse){
 .full-height:not(.rotate-portrait) .mobile-header{grid-template-columns:auto minmax(0,1fr)}
}
.full-height>.book-options{display:none}
.full-height>.book-options.expanded{display:flex;flex-direction:column;gap:6px;flex-shrink:0;max-height:min(38dvh,38%);overflow:auto;padding:6px;border-bottom:1px solid var(--inspection-line)}
.full-height .sort-list{max-height:min(55dvh,max(80px,calc(var(--available-height) - 170px)))}
.full-height>.book-submit{min-height:30px;padding:2px 6px;flex-shrink:0;font-size:11px;gap:4px;border-top:1px solid var(--inspection-line)}
.full-height>.book-submit .primary{min-height:38px;padding:6px 16px}
.mobile-save-recovery,.mobile-start{display:flex;align-items:center;gap:4px;flex-shrink:0;font-size:12px;padding:2px 5px}
.sheet-picker{flex:1 1 220px;min-width:180px;max-width:360px}
.round-tabs{font-size:13px}
.round-tabs button{display:flex;align-items:center;justify-content:center;gap:5px;white-space:nowrap}
.round-tabs button:disabled{opacity:1;background:var(--inspection-soft,var(--inspection-surface))}
.round-tabs button:disabled.selected{background:var(--inspection-accent);color:var(--inspection-on-accent)}
.round-tabs small,.round-hint{font-size:12px}
.full-height .round-tabs{order:-1;display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:3px;flex-shrink:0}
.full-height .round-tabs button{flex-wrap:wrap;gap:1px 4px;min-height:36px;padding:2px;font-size:12px;line-height:1.15;white-space:normal}
.full-height .round-tabs small{font-size:11px}
.full-height .round-hint{grid-column:1/-1;padding-inline:3px}
.full-height>.cell-editor-tools{flex-wrap:wrap;gap:4px;padding:4px 6px;border-top:1px solid var(--inspection-line);max-height:100%;overflow:auto}
.full-height .cell-editor-tools :is(input,select){flex:1 1 110px;min-width:0;min-height:40px;font-size:16px}
.full-height .cell-editor-tools strong{margin:0}
.full-height .cell-editor-actions{margin-left:auto;min-width:0;flex-wrap:wrap;justify-content:flex-end}
.full-height .cell-editor-actions button{min-height:40px;padding-inline:8px}
.book-scroll td.mobile-editing-cell{outline:2px solid var(--inspection-accent);outline-offset:-2px}
.compact-editor-view>.mobile-header,.compact-editor-view>.book-options.expanded{display:none}
@media(max-height:500px) and (pointer:coarse){.mobile-sheet-bar{padding:0 2px}.mobile-sheet-bar>button,.sheet-tabs>button,.sheet-sort{min-height:34px}.full-height>.book-submit{min-height:22px;padding:0 6px}.full-height>.book-submit .primary{min-height:34px}}
/* Cell contents must not enlarge Excel's saved row heights, including merged cells. */
.book-scroll td{position:relative;line-height:normal}
.book-scroll td>.cell-content{position:absolute;inset:0 2px;display:flex;flex-direction:column;overflow:hidden;line-height:normal}
.cell-content>span{flex:0 0 auto;max-height:100%;overflow:hidden}
.book-scroll tbody .row-number{height:auto;padding:0;line-height:1}
.row-number>span{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;overflow:hidden}
.book-scroll td>input,.book-scroll td>select{position:absolute;inset:0;width:100%;height:100%;min-height:0;z-index:1}
.book-canvas{position:relative;min-width:0}
.template-art{position:absolute;left:42px;top:29px;pointer-events:none;overflow:hidden;z-index:1}
.cell-diagonal{position:absolute;inset:0;width:100%;height:100%;pointer-events:none}
</style>
