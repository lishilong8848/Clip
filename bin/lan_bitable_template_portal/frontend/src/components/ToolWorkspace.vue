<template>
  <section class="tool-workspace" aria-label="工具工作台" data-v2-inline-feedback>
    <NAlert v-if="error" type="error" :bordered="false" role="alert">{{ error }}</NAlert>
    <NAlert v-for="warning in sourceWarnings" :key="warning" type="warning" :bordered="false" role="status">{{ warning }}</NAlert>
    <div v-if="!session && !recordsOpen" class="workspace-grid" :class="{ 'is-external': kind === 'external' }">
      <section class="workspace-panel" aria-labelledby="tool-assets-title">
        <header class="panel-heading">
          <h3 id="tool-assets-title" tabindex="-1">工具台账</h3>
          <small>{{ kind === 'self' ? '本组勾选' : '已勾选' }} {{ selectedIds.length }} 台<span v-if="kind === 'self' && assignedCount"> · 已配模板 {{ assignedCount }} 台</span></small>
        </header>
        <NRadioGroup :value="kind" size="small" aria-label="办理类型" :disabled="!!busy" @update:value="chooseKind">
          <NRadioButton value="self">维护</NRadioButton><NRadioButton value="external">送检</NRadioButton>
        </NRadioGroup>
        <div class="asset-filters">
          <NSelect v-model:value="scope" :options="scopeOptions" aria-label="筛选楼栋" size="small" />
          <NInput v-model:value="search" size="small" clearable placeholder="搜索工具、型号、品牌" :input-props="{ 'aria-label': '搜索工具、型号或品牌' }" />
        </div>
        <NSpin :show="loading && !board">
          <div v-if="visibleAssets.length" class="asset-list">
            <table class="assets">
              <thead><tr>
                <th scope="col"><NCheckbox :checked="allVisibleSelected" :indeterminate="someVisibleSelected && !allVisibleSelected"
                  :disabled="!!busy || !visibleEligible.length" aria-label="选择筛选后的工具" @update:checked="toggleVisible" /></th>
                <th scope="col">工具 / 楼栋</th><th scope="col">型号 / 品牌</th><th scope="col">周期</th>
              </tr></thead>
              <tbody>
                <tr v-for="asset in visibleAssets" :key="asset.id"
                  :class="{ 'is-selected': selected.has(asset.id) || assigned[asset.id],
                    'is-selectable': !busy && selectable(asset) }"
                  @click="chooseAssetRow(asset, $event)">
                  <td><NCheckbox :checked="selected.has(asset.id) || !!assigned[asset.id]" :disabled="!!busy || !selectable(asset)"
                    :aria-label="'选择工具 ' + asset.name" @update:checked="toggleAsset(asset.id, $event)" /></td>
                  <td><strong>{{ asset.name }}</strong><small>{{ scopeLabel(asset.scope) }} · <NButton text size="tiny" @click="emit('asset-history', asset.id)">履历</NButton></small>
                    <small v-if="kind === 'self' && assigned[asset.id]">
                      {{ templateName(assigned[asset.id]) }} · <NButton text size="tiny" :disabled="!!busy" @click="delete assigned[asset.id]">移出本次</NButton>
                    </small>
                  </td>
                  <td>{{ asset.model || '—' }}<small>{{ asset.brand || '—' }}</small></td>
                  <td>{{ cycleText(asset) }}<small v-if="reason(asset)" class="asset-reason">{{ reason(asset) }}</small>
                    <small class="workspace-actions asset-actions">
                      <NButton text size="tiny" :disabled="!!busy" @click="showRecords(asset)">办理记录</NButton>
                      <NButton v-if="asset.plan?.mode === 'transfer'" size="tiny" :disabled="!!busy" @click="transferAsset(asset)">接收移交</NButton>
                    </small>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
          <NEmpty v-else-if="!loading" description="没有匹配的工具" />
        </NSpin>
      </section>

      <section v-if="kind === 'self'" class="workspace-panel" aria-labelledby="tool-templates-title">
        <header class="panel-heading"><h3 id="tool-templates-title">自检模板库</h3></header>
        <p class="workspace-muted">{{ selectedIds.length ? '为当前勾选的工具选择模板，可继续配下一组。' : '勾选左侧工具后，选择这组工具使用的模板。' }}</p>
        <NRadioGroup v-if="templates.length" :value="null" :disabled="!!busy || !selectedIds.length" aria-label="选择自检模板" @update:value="chooseTemplate">
          <div class="template-list">
            <NRadio v-if="allAssociated" :value="existingTemplate">使用各工具已关联模板</NRadio>
            <table class="templates" aria-label="自检模板记录">
              <thead><tr><th scope="col">选择</th><th scope="col">模板名称</th></tr></thead>
              <tbody><tr v-for="template in templates" :key="template.id">
                <td><NRadio :value="template.id" :aria-label="'选用模板 ' + (template.label || template.name)" /></td>
                <td><label @click="chooseTemplate(template.id)">{{ template.label || template.name }}</label></td>
              </tr></tbody>
            </table>
          </div>
        </NRadioGroup>
        <NEmpty v-else description="暂无可用的飞书模板" />
      </section>

      <aside class="workspace-panel opening-panel" aria-label="办理操作">
        <header class="panel-heading"><h3>本次办理</h3></header>
        <NButton type="primary" :disabled="!!busy || !canOpen" :loading="busy === 'open'" @click="requestOpen">{{ kind === 'self' ? '打开填写' : '打开送检' }}</NButton>
        <span v-if="kind === 'self' && pendingIds.length" class="workspace-muted">本次 {{ pendingIds.length }} 台<span v-if="unassignedCount"> · {{ unassignedCount }} 台待选模板</span></span>
        <p v-if="!pendingAssets.length" class="workspace-muted">先在左侧勾选工具。</p>
        <ul v-else class="selection-review" aria-label="本次工具和模板">
          <li v-for="asset in pendingAssets" :key="asset.id">
            <div>
              <strong>{{ asset.name }}</strong><small>{{ [scopeLabel(asset.scope), asset.model].filter(Boolean).join(' · ') }}</small>
              <small v-if="kind === 'self'">{{ templateAssignments[asset.id] ? templateName(templateAssignments[asset.id]) : '待选模板' }}</small>
              <small v-else>共用本批送检报告</small>
            </div>
            <NButton text size="tiny" :disabled="!!busy" :aria-label="'移出本次 ' + asset.name" @click="removeSelection(asset.id)">移出</NButton>
          </li>
        </ul>
        <span v-if="mixedReportBuildings" class="workspace-muted" role="status">送检报告请按楼栋分批上传。</span>
        <NButton :disabled="!!busy" @click="emit('records')">查看办理记录</NButton>
        <label v-if="sessionOptions.length">办理记录
          <NSelect :value="null" :options="sessionOptions" size="small" placeholder="选择记录" aria-label="办理记录" :disabled="!!busy" @update:value="id => openSaved(id)" />
        </label>
        <NButton v-if="board?.history_cursor" size="small" :loading="busy === 'history'" :disabled="!!busy" @click="loadMoreHistory">加载更早的办理记录</NButton>
        <span v-if="busy === 'read'" class="workspace-muted" role="status">正在打开办理记录…</span>
      </aside>
    </div>
    <section v-else-if="!session" class="workspace-panel records-panel" aria-labelledby="tool-records-title">
      <header class="panel-heading">
        <div class="workspace-actions">
          <NButton size="small" :disabled="!!busy" @click="recordsOnly ? emit('choose') : (recordsOpen = false)">返回选择</NButton>
          <h3 id="tool-records-title" tabindex="-1">办理记录</h3>
        </div>
        <div class="workspace-actions">
          <NButton v-if="recordAsset" size="small" :disabled="!!busy" @click="showRecords()">查看全部</NButton>
          <NButton size="small" :disabled="!!busy" :loading="busy === 'records'" @click="reloadRecords">刷新记录</NButton>
        </div>
      </header>
      <slot name="followups" />
      <h4 v-if="recordsOnly" class="record-section-title">维护与送检</h4>
      <p v-if="recordAsset" class="workspace-muted">{{ recordAsset.name }} · {{ scopeLabel(recordAsset.scope) }}</p>
      <NSpin :show="busy === 'records'">
        <div v-if="visibleRecordSessions.length" class="record-list">
          <article v-for="batch in visibleRecordSessions" :key="batch.id" class="record-batch">
            <div class="record-heading">
              <strong>{{ batch.kind === 'self' ? '维护' : '送检' }} · {{ batch.created_at?.slice(0, 16).replace('T', ' ') }}</strong>
              <NTag size="small">{{ sessionStatus(batch) }}</NTag>
              <span v-if="batch.owner_name" class="workspace-muted">{{ batch.owner_name }}</span>
            </div>
            <ul class="record-tools">
              <li v-for="tool in batch.history_items" :key="tool.id">
                <div><strong>{{ tool.asset_name }}</strong><small v-if="tool.model || tool.location" class="workspace-muted">{{ [tool.model, tool.location].filter(Boolean).join(' · ') }}</small>
                  <small class="workspace-muted">{{ scopeLabel(tool.scope) }} · {{ recordStatus(tool) }}</small>
                  <small v-if="tool.tool_status_label" class="workspace-muted">{{ tool.result_label }} · {{ tool.tool_status_label }}</small>
                  <small v-if="tool.result_notice" role="status">{{ tool.result_notice }}</small>
                  <small v-if="tool.deletion_pending" role="status">{{ tool.deletion_error || '等待飞书确认删除' }}</small>
                </div>
                <div class="workspace-actions record-actions">
                  <NButton size="small" :disabled="!!busy || tool.deletion_pending" @click="openSaved(batch.id, tool.id)">查看记录</NButton>
                  <NButton v-if="tool.can_release" size="small" :disabled="!!busy" @click="manageRecord(batch, tool, 'release')">取消办理</NButton>
                  <NButton v-if="tool.can_delete && !tool.deletion_pending" size="small" type="error" secondary :disabled="!!busy" @click="manageRecord(batch, tool, 'delete')">删除</NButton>
                </div>
              </li>
            </ul>
          </article>
        </div>
        <NEmpty v-else-if="busy !== 'records'" :description="recordsOnly ? '暂无维护或送检记录' : '暂无办理记录'" />
      </NSpin>
      <NButton v-if="recordCursor" size="small" :disabled="!!busy" @click="loadRecords(true)">加载更早的记录</NButton>
    </section>
    <section v-else class="workspace-panel editor-panel" aria-labelledby="tool-editor-title">
        <header class="panel-heading">
          <div class="workspace-actions">
            <NButton size="small" :disabled="!!busy" @click="returnToSelection">返回选择</NButton>
            <h3 id="tool-editor-title" tabindex="-1">{{ session.kind === 'self' ? '自检记录' : '送检报告' }}</h3>
          </div>
          <NTag size="small" :type="session.status === 'archived' ? 'success' : session.status === 'pending' ? 'warning' : 'default'" role="status">{{ sessionStatus(session) }}</NTag>
        </header>
          <div v-if="session.kind === 'self'" class="workspace-editor">
            <div class="tool-toolbar">
              <div class="item-tabs tool-tabs" aria-label="本批工具" tabindex="0">
                <NButton v-for="entry in session.items" :key="entry.id" size="small" :aria-pressed="entry.id === itemId"
                  :disabled="!!busy" :type="entry.id === itemId ? 'primary' : 'default'" @click="selectItem(entry.id)">
                  {{ entry.asset.name }} · {{ scopeLabel(entry.asset.scope) }}
                </NButton>
              </div>
              <NButton v-if="reviewable.length > 1" size="small" :disabled="!!busy" @click="openBatchReview">批量审核</NButton>
              <NButton v-if="item?.can_edit && activeSheet?.name === item.template?.schema?.sheet" size="small" :disabled="!!busy" @click="normalFill">一键正常</NButton>
            </div>
            <template v-if="item">
              <p v-if="item.result_notice" role="status">{{ item.result_notice }}</p>
              <div v-if="item.can_edit || item.can_reassign || item.can_release || item.can_withdraw" class="workspace-actions item-actions">
                <label v-if="item.can_edit" class="workspace-actions">完成情况
                  <select data-native-select aria-label="填写完成情况" :value="item.completion || ''" :disabled="!!busy" @change="setCompletion">
                    <option value="">请选择</option><option value="completed">已完成</option><option value="incomplete">未完成</option>
                  </select>
                </label>
                <NButton v-if="item.can_reassign" :disabled="!!busy" size="small" @click="startReassign">改派审核人</NButton>
                <NButton v-if="item.can_withdraw" :disabled="!!busy" size="small" @click="confirmItemAction('withdraw')">撤回审核</NButton>
                <NButton v-if="item.can_release" :disabled="!!busy" size="small" @click="confirmItemAction('release')">取消本台办理</NButton>
              </div>
              <div class="review-line">
                <span>审核人 {{ item.reviewer || item.reviewer_name || '本楼楼长' }}</span>
                <span v-if="item.reviewed_at">{{ item.reviewed_at.replace('T', ' ') }}</span>
                <NButton v-if="item.can_review" size="small" :disabled="!!busy" @click="openSignature(false)">{{ reviewSignatures[item.id] ? '重签' : '手绘审核签名' }}</NButton>
                <img v-if="isPng(item.signature)" class="signature-preview" :src="item.signature" alt="审核签名" />
                <img v-else-if="reviewSignatures[item.id]" class="signature-preview" :src="reviewSignatures[item.id]" alt="待提交审核签名" />
                <small v-if="item.review_note">{{ item.review_note }}</small>
              </div>
              <div v-if="item.can_review" class="review-line">
                <label>审核时间（北京时间）
                  <input type="datetime-local" :value="reviewDates[item.id] || ''" :disabled="!!busy" @input="setReviewDate" />
                </label>
                <small v-if="!reviewDates[item.id]" class="workspace-muted">留空时使用审核通过的时间</small>
              </div>
              <div v-if="item.can_review" class="workspace-actions">
                <NCheckbox v-model:checked="savedSignature" :disabled="!!busy">使用我的已登记签名</NCheckbox>
                <NInput v-model:value="reviewNote" size="small" placeholder="审核意见，驳回时必填" aria-label="审核意见" />
                <NButton :disabled="!!busy || !reviewNote.trim()" @click="reviewItems([item], false)">驳回修改</NButton>
                <NButton type="primary" :disabled="!!busy || (!reviewSignatures[item.id] && !savedSignature)" @click="reviewItems([item], true)">审核通过</NButton>
              </div>
              <p v-if="item.binding_pending" class="workspace-muted" role="status">正在核验本台工具与模板的关联…</p>
              <div v-if="item.binding_error && item.order_status !== 'cancelled'" class="workspace-actions">
                <span>{{ item.binding_error }}</span>
                <NButton :disabled="!!busy" @click="retryBindings">重试关联</NButton>
                <NSelect v-model:value="replacementTemplate" size="small" :options="templates.map(row => ({label:row.label || row.name,value:row.id}))" placeholder="选择替换模板" />
                <NButton :disabled="!!busy || !replacementTemplate" @click="resolveBinding(false)">重新关联</NButton>
                <NButton :disabled="!!busy" @click="resolveBinding(true)">移出本批</NButton>
              </div>
              <div v-if="sheets.length > 1" class="item-tabs" aria-label="模板工作表">
                <NButton v-for="sheet in sheets" :key="sheet.name" size="small" :aria-pressed="sheet.name === activeSheet?.name"
                  :type="sheet.name === activeSheet?.name ? 'primary' : 'default'" @click="sheetName = sheet.name">{{ sheet.name }}</NButton>
              </div>
              <div v-if="activeSheet" class="item-tabs sheet-tools" aria-label="原表缩放">
                <NButton size="tiny" aria-label="缩小原表" :disabled="zoom <= 60" @click="zoom -= 10">−</NButton>
                <NButton size="tiny" aria-label="恢复原表比例" @click="zoom = 100">{{ zoom }}%</NButton>
                <NButton size="tiny" aria-label="放大原表" :disabled="zoom >= 150" @click="zoom += 10">+</NButton>
              </div>
              <div v-if="activeSheet" :key="item.id + '!' + activeSheet.name" class="sheet-scroll" tabindex="0" :aria-label="activeSheet.name + '原表'">
                <div class="sheet-frame" :style="{ width: sheetWidth + 'px', zoom: zoom / 100 }">
                  <table class="sheet-table">
                    <colgroup><col v-for="(width, index) in activeSheet.columns" :key="index" :style="{ width: width + 'px' }" /></colgroup>
                    <tbody>
                      <tr v-for="row in activeSheet.rows" :key="row.index" :style="{ height: row.height + 'px' }">
                        <td v-for="cell in row.cells" :key="cell.address" :rowspan="cell.rowspan" :colspan="cell.colspan"
                          :style="cell.style" :class="{ 'fillable-cell': cell.editable || cell.check }">
                          <select v-if="cell.check" data-native-select :value="item.checks[cellKey(cell)] || ''" :disabled="!item.can_edit || !!busy" :aria-label="'检查 ' + cellKey(cell)" @change="setCheck(cell, $event)">
                            <option value="">请选择</option><option value="是">√ 是</option><option value="否">× 否</option>
                          </select>
                          <select v-else-if="reviewCell(cell) === 'result' && item.can_edit" data-native-select :value="item.completion || ''"
                            :disabled="!!busy" aria-label="完成情况" @change="setCompletion">
                            <option value="">请选择</option><option value="completed">已完成[√]　未完成[ ]</option><option value="incomplete">已完成[ ]　未完成[√]</option>
                          </select>
                          <select v-else-if="reviewCell(cell) === 'reviewer' && item.can_edit" data-native-select :value="item.reviewer_id || ''"
                            :disabled="!!busy || reviewerLoading" :aria-busy="reviewerLoading"
                            aria-label="选择审核人" @focus="loadReviewers()" @change="setReviewer">
                            <option value="">{{ reviewerLoading ? '正在读取审核人' : '本楼楼长' }}</option>
                            <option v-if="item.reviewer_id && !reviewerOptions.some(person => person.id === item?.reviewer_id)" :value="item.reviewer_id">{{ item.reviewer_name }}</option>
                            <option v-for="person in reviewerOptions" :key="person.id" :value="person.id">{{ person.name }}</option>
                          </select>
                          <input v-else-if="reviewCell(cell) === 'time' && item.can_review" type="datetime-local"
                            aria-label="审核年月日时" :value="reviewDates[item.id] || ''" :disabled="!!busy" @input="setReviewDate" />
                          <ToolWorkbookInput v-else-if="!reviewCell(cell) && item.can_edit && editable && specialCell(cell)" :key="cellKey(cell)"
                            :kind="cellDateKind(cell)" :label="'填写 '+cellKey(cell)" :value="String(inputCellValue(cell))" :original="String(cell.value ?? '')"
                            :disabled="!!busy" @change="value=>setCellValue(cell,value)"/>
                          <input v-else-if="cell.editable" type="text" :value="inputCellValue(cell)"
                            :disabled="!item.can_edit || !!busy" :aria-label="'填写 ' + cellKey(cell)" @input="setCell(cell, $event)" />
                          <template v-else-if="reviewCell(cell)">
                            <span v-if="isSignatureCell(cell) && isPng(item.signature || reviewSignatures[item.id])" class="signed-cell">
                              <span :title="String(reviewText(cell) ?? '')">{{ reviewText(cell) }}</span>
                              <img class="signature-in-cell" :src="item.signature || reviewSignatures[item.id]" alt="表内审核签名"
                                :style="{ maxHeight: Math.max(0, row.height - 6) + 'px' }" />
                            </span>
                            <span v-else>{{ reviewText(cell) }}</span>
                          </template>
                          <textarea v-else-if="cell.text_editable && item.can_edit && editingText === item.id + ':' + cellKey(cell)"
                            v-focus-cell class="sheet-text-editor" :value="String(inputCellValue(cell))" :aria-label="'填写 ' + cellKey(cell)"
                            :disabled="!!busy" @input="setCell(cell, $event)" @blur="editingText = ''" @keydown.esc.stop="editingText = ''" />
                          <button v-else-if="cell.text_editable && item.can_edit" type="button" class="sheet-text-button"
                            :aria-label="'编辑 ' + cellKey(cell) + ' ' + inputCellValue(cell)" :disabled="!!busy"
                            @click="editingText = item.id + ':' + cellKey(cell)">{{ inputCellValue(cell) }}</button>
                          <span v-else>{{ sheetText(cell) }}</span>
                        </td>
                      </tr>
                    </tbody>
                  </table>
                  <img v-for="image in sheetImages" :key="image.key" :src="image.src" :style="image.style" alt="" />
                  <div v-for="title in activeSheet.titles" :key="title.key" class="form-title-box"
                    :style="{ left:title.x+'px', top:title.y+'px', width:title.width+'px', height:title.height+'px', fontSize:title.size+'px',
                              color:title.color || undefined, backgroundColor:title.background || undefined }">
                    <textarea v-if="item.can_edit && editingText === item.id + ':' + title.key" v-focus-cell
                      class="sheet-text-editor" :aria-label="'填写表格标题'" :disabled="!!busy"
                      :value="item.values[activeSheet.name+'!'+title.key] ?? title.text" @input="setCell({address:title.key}, $event)"
                      @blur="editingText = ''" @keydown.esc.stop="editingText = ''" />
                    <button v-else-if="item.can_edit" type="button" class="sheet-text-button" aria-label="编辑表格标题" :disabled="!!busy"
                      @click="editingText = item.id + ':' + title.key">{{ item.values[activeSheet.name+'!'+title.key] ?? title.text }}</button>
                    <span v-else>{{ item.values[activeSheet.name+'!'+title.key] ?? title.text }}</span>
                  </div>
                </div>
              </div>
              <NAlert v-else type="warning" :bordered="false">未读取到该工具的原表，请刷新记录核对。</NAlert>
              <div class="review-line">
                <span id="tool-qualified-label">工具检测是否合格</span>
                <NRadioGroup class="qualified-options" :value="item.outcome || null" size="small"
                  aria-labelledby="tool-qualified-label" :disabled="!item.can_edit || !!busy" @update:value="item.outcome = $event">
                  <NRadioButton value="normal">合格</NRadioButton><NRadioButton value="repair">不合格 · 转检修</NRadioButton><NRadioButton value="scrap">不合格 · 报废</NRadioButton>
                </NRadioGroup>
                <NInput v-if="item.outcome === 'repair' || item.outcome === 'scrap'" v-model:value="item.reason"
                  placeholder="处理原因" aria-label="处理原因" :maxlength="1000" show-count :disabled="!item.can_edit || !!busy" />
                <label v-if="item.outcome === 'scrap'">报废数量
                  <input v-model.number="item.quantity" type="number" min="1" :disabled="!item.can_edit || !!busy" />
                </label>
              </div>
              <div v-if="item.outcome === 'scrap'" class="review-line scrap-photos" role="group" aria-labelledby="tool-scrap-photos-label">
                <span id="tool-scrap-photos-label">报废现场照片</span>
                <template v-if="item.can_edit">
                  <input ref="photoInput" type="file" accept="image/jpeg,image/png" multiple hidden
                    :disabled="!!busy || !canAddScrapPhoto" @change="pickScrapPhotos(item, $event)" />
                  <NButton size="small" :disabled="!!busy || !canAddScrapPhoto" @click="photoInput?.click()">选择照片</NButton>
                  <small class="workspace-muted">JPEG 或 PNG，最多 {{ scrapPhotoLimit }} 张，每张不超过 1MB</small>
                </template>
                <small v-else class="workspace-muted">审核前不可修改</small>
                <ul v-if="item.scrap_photos?.length" class="photo-strip">
                  <li v-for="(photo, index) in item.scrap_photos" :key="index" class="photo-item">
                    <button type="button" class="photo-button" :aria-label="'查看报废现场照片 ' + (index + 1)" @click="photoPreview = photo">
                      <img :src="photo" :alt="'报废现场照片 ' + (index + 1)" loading="lazy" />
                    </button>
                    <NButton v-if="item.can_edit" text size="tiny" :disabled="!!busy" @click="removeScrapPhoto(item, index)">删除</NButton>
                  </li>
                </ul>
              </div>
              <p v-if="item.outcome === 'scrap'" class="workspace-muted scrap-approval-note">自检审核后提交报废申请，梅冰冰、马进宇任一位同意即可报废。</p>
            </template>
          </div>
          <div v-else class="workspace-editor">
            <p class="workspace-muted">{{ session.items.length }} 台工具共用一份送检报告。</p>
            <ul class="archive-list"><li v-for="entry in session.items" :key="entry.id">{{ entry.asset.name }} · {{ scopeLabel(entry.asset.scope) }} · {{ entry.asset.model || '—' }}</li></ul>
            <div v-if="session.report_name" class="workspace-actions">
              <span role="status">{{ session.report_name }}</span>
              <NButton v-if="session.report_available !== false" size="small" tag="a" :href="sessionUrl + '/report-file'" target="_blank" rel="noopener">查看报告</NButton>
              <small v-else class="workspace-muted">报告包含当前账号无权查看的工具，仅显示已授权登记信息。</small>
            </div>
            <div v-if="editable" class="workspace-actions">
              <label>实际检测日期 <input v-model="actualDate" aria-label="实际检测日期" type="date" :disabled="!!busy" /></label>
              <input ref="reportInput" type="file" accept=".pdf,.docx,.xlsx,.jpg,.jpeg,.png" hidden @change="pickReport" />
              <NButton size="small" :disabled="!!busy" @click="reportInput?.click()">{{ session.report_name ? '更换报告' : '选择报告' }}</NButton>
              <span v-if="report">{{ report.name }}</span>
              <NButton v-if="report" size="small" :loading="busy === 'report'" :disabled="!!busy" @click="uploadReport">上传报告</NButton>
              <NButton v-if="report" size="small" :disabled="!!busy" @click="report = undefined">取消选择报告</NButton>
            </div>
            <p v-else-if="session.actual">实际检测日期 {{ session.actual }}</p>
            <ToolInspectionReportFields :model="reportCheck" :asset-ids="reportAssets" :actual="actualDate"
              :assets="session.items.filter(entry => !editable || entry.can_edit).map(entry => entry.asset)"
              :expires-on="session.inspection_expires_on" :editable="editable" :disabled="!!busy || !session.report_name || !!report" />
          </div>
          <ul v-if="session.status !== 'draft'" class="archive-list">
            <li v-for="entry in session.items" :key="entry.id">{{ entry.asset.name }} · {{ archiveStatus(entry.archive_status) }}<span v-if="entry.archive_error"> · {{ entry.archive_error }}</span></li>
          </ul>
          <footer class="workspace-actions editor-footer">
            <span v-if="editable" class="workspace-muted" role="status">{{ dirty ? '有未保存修改' : '草稿已保存' }}</span>
            <NButton v-if="editable && session.kind === 'self'" size="small" :disabled="!!busy || !dirty" :loading="busy === 'save'" @click="saveDraft">保存草稿</NButton>
            <NPopconfirm v-if="editable && session.status !== 'opening' && (session.kind !== 'self' || item?.can_edit)" positive-text="确认提交" negative-text="取消" @positive-click="submit">
              <template #trigger><NButton size="small" type="primary" :loading="busy === 'submit'"
                :disabled="!!busy || !!report || (session.kind === 'external' && (!session.report_name || !inspectionReady(reportCheck, reportAssets, actualDate)))">{{ resubmitAsset ? '重新提交本台' : session.kind === 'self' ? '提交审核' : '提交归档' }}</NButton></template>
              {{ resubmitAsset ? '将「' + item?.asset.name + '」重新提交审核。' : session.kind === 'self' ? '提交待填写工具，由楼长或授权人员审核。' : '提交送检报告归档。' }}
            </NPopconfirm>
            <span v-if="session.status === 'opening'" class="workspace-muted">本批模板核验完成后可提交审核</span>
            <NButton v-if="session.status === 'archived' && session.report_available !== false" tag="a" size="small" :href="sessionUrl + '/download'">下载归档</NButton>
          </footer>
    </section>
    <NModal :show="!!photoPreview" preset="card" title="报废现场照片" class="tool-workspace-dialog" @update:show="show => { if (!show) photoPreview = '' }">
      <img class="photo-preview" :src="photoPreview" alt="报废现场照片" />
    </NModal>
    <NModal :show="!!confirmAction" preset="card" :title="confirmAction?.title" class="tool-workspace-dialog" @update:show="closeConfirm">
      <p class="confirmation-summary">{{ confirmAction?.text }}</p>
      <template #footer><div class="workspace-actions dialog-actions">
        <NButton @click="closeConfirm(false)">取消</NButton><NButton type="primary" @click="runConfirm">确认</NButton>
      </div></template>
    </NModal>
    <NModal v-model:show="reassignOpen" preset="card" title="改派审核人" class="tool-workspace-dialog">
      <NSelect v-model:value="reassignReviewer" :loading="reviewerLoading" :disabled="!!busy || reviewerLoading"
        aria-label="改派给" :options="[{label:'由本楼楼长审核',value:''}, ...reviewerOptions.map(person => ({label:person.name,value:person.id}))]" />
      <template #footer><div class="workspace-actions dialog-actions">
        <NButton :disabled="!!busy" @click="reassignOpen = false">取消</NButton>
        <NButton type="primary" :disabled="!!busy || reviewerLoading" @click="manageItem('reassign')">确认改派</NButton>
      </div></template>
    </NModal>
    <NModal v-model:show="batchOpen" preset="card" title="批量审核" class="tool-workspace-dialog">
      <div class="review-line">
        <NCheckbox v-model:checked="batchSavedSignature">使用我的已登记签名</NCheckbox>
        <label>审核时间（北京时间） <input v-model="batch.reviewed_at" type="datetime-local" /></label>
        <NButton size="small" @click="openSignature(true)">{{ batch.signature ? '重签批量签名' : '绘制批量签名' }}</NButton>
        <img v-if="isPng(batch.signature)" class="signature-preview" :src="batch.signature" alt="批量审核签名" />
      </div>
      <p class="workspace-muted">审核 {{ reviewable.length }} 台获授权工具。已单独绘制的签名优先使用。</p>
      <template #footer><div class="workspace-actions dialog-actions"><NButton @click="batchOpen = false">取消</NButton>
        <NButton type="primary" :disabled="(!batch.signature && !batchSavedSignature) || !!busy" @click="applyBatchReview">审核通过</NButton>
      </div></template>
    </NModal>
    <NModal v-model:show="signatureOpen" preset="card" title="手绘审核签名" class="tool-workspace-dialog" @after-enter="resetCanvas">
      <canvas ref="pad" width="480" height="170" aria-label="手绘审核签名画板"
        @pointerdown="startStroke" @pointermove="moveStroke" @pointerup="stopStroke" @pointercancel="stopStroke" @lostpointercapture="stopStroke" />
      <template #footer><div class="workspace-actions dialog-actions"><NButton @click="resetCanvas">清空</NButton>
        <NButton @click="signatureOpen = false">取消</NButton><NButton type="primary" :disabled="!signed" @click="applySignature">使用签名</NButton>
      </div></template>
    </NModal>
  </section>
</template>

<script setup lang="ts">
import { computed, markRaw, nextTick, onBeforeUnmount, onMounted, reactive, ref, shallowRef } from 'vue';
import ToolWorkbookInput from './ToolWorkbookInput.vue';
import {boxParts,displayToolDate,type ToolDateKind} from './toolWorkbookFields';
import { NAlert, NButton, NCheckbox, NEmpty, NInput, NModal, NPopconfirm, NRadio, NRadioButton, NRadioGroup, NSelect, NSpin, NTag } from 'naive-ui';
import { requestBinaryJson, requestJson } from '../api/client';
import ToolInspectionReportFields, { emptyInspectionReport, inspectionReady, type InspectionReport } from './ToolInspectionReportFields.vue';

type Kind = 'self' | 'external';
type OrderPlan = { mode: string; workspace_id: string; order_id?: string; revision?: number };
type Asset = { id: string; name: string; scope: string; model?: string; brand?: string; location?: string; plan?: OrderPlan;
  self_cycle?: string | number; external_cycle?: string | number; template_id?: string; eligible_kinds?: Kind[];
  disabled_reason?: string };
type Template = { id: string; name?: string; label?: string };
type Cell = { address: string; value: string | number | boolean | null; rowspan: number; colspan: number;
  style?: Record<string, string | number>; editable?: boolean; text_editable?: boolean; check?: boolean; input_kind?:ToolDateKind; picker_default?:string };
type Sheet = { name: string; columns: number[]; rows: { index: number; height: number; cells: Cell[] }[];
  titles?: {key:string;text:string;x:number;y:number;width:number;height:number;size:number;color?:string;background?:string}[];
  review?: { reviewer_address: string; reviewed_at_address: string; signature_address: string };
  images?: { address: string; src: string; offsetX: number; offsetY: number; width: number; height: number }[] };
type Review = { reviewer: string; reviewed_at: string; signature: string };
type Layout = { sheets: Sheet[]; schema?: { sheet: string; layout?: string; review_row: number; result_row: number };
  prefill_fields?: { inspector: string; checked_at: string } };
type Item = Review & { id: string; asset_id: string; asset: Asset; template?: Layout | null; layout_key?: string;
  values: Record<string, string>; checks: Record<string, string>; qualified: boolean | null; archive_status?: string; archive_error?: string;
  outcome?: string; reason?: string; quantity?: number; scrap_photos?: string[]; order_revision?: number; can_edit?: boolean; can_review?: boolean;
  can_release?: boolean; can_withdraw?: boolean; can_reassign?: boolean; result_notice?: string;
  review_note?: string; review_status?: string; binding_error?: string; binding_pending?: boolean; order_status?: string; completion?: string; reviewer_id?: string; reviewer_name?: string };
type HistoryItem = { id: string; order_id?: string; asset_name: string; scope: string; order_status?: string; archive_status?: string;
  binding_pending?: boolean; binding_error?: string; result_label?: string; tool_status_label?: string;
  location?: string; model?: string; deletion_pending?: boolean; deletion_error?: string; result_notice?: string;
  order_revision?: number; can_release?: boolean; can_delete?: boolean };
type Summary = { id: string; kind: Kind; status: string; revision?: number; items?: Item[]; history_items?: HistoryItem[];
  created_at?: string; owner_name?: string; item_count?: number; archived_count?: number; operation_id?: string };
type Session = Summary & { revision: number; can_edit: boolean; items: Item[]; report_name?: string; actual?: string; legacy_draft?: boolean;
  inspection?: InspectionReport; inspection_expires_on?: string; layouts?: Record<string, Layout>; report_available?: boolean; binding_pending?: boolean };
const emit = defineEmits<{ 'asset-history': [id: string]; 'inventory-status': [warning: string]; records: []; choose: []; 'records-refresh': [] }>();
const props = defineProps<{ sessionId?: string; assetId?: string; scope?: string; recordsOnly?: boolean; followupOrderIds?: string[]; inventoryWarning?: string }>();
const reviewSignatures = reactive<Record<string, string>>({});
const reviewDates = reactive<Record<string, string>>({});
const reviewers = shallowRef<Record<string, { id: string; name: string }[]>>();
const reviewerLoading = ref(false);
let reviewerRequest: AbortController | undefined;
const savedSignature = ref(false);
const batchSavedSignature = ref(false);
const reviewNote = ref('');
const reassignOpen = ref(false);
const reassignReviewer = ref('');
const actualDate = ref('');
const reportCheck = ref(emptyInspectionReport());
const reportSnapshot = computed(() => JSON.stringify([actualDate.value, reportCheck.value]));
const savedReport = ref(reportSnapshot.value);
const reportAssets = computed(() => session.value?.items.filter(entry => entry.can_edit).map(entry => entry.asset_id) || []);
const replacementTemplate = ref<string | null>(null);
const savedItems = new Map<string, string>();
type HistoryPage = { sessions: Summary[]; history_cursor?: { before: string; id: string } | null };
type Board = HistoryPage & { assets: Asset[]; templates: Template[]; warning?: string; inventory_error?: string };

const api = '/api/tool-self-check/workspace';
const existingTemplate = '__associated__';
const board = shallowRef<Board>();
const session = ref<Session>();
const kind = ref<Kind>('self');
const scope = ref(props.scope && props.scope !== 'ALL' ? props.scope : '');
const search = ref('');
const selectedIds = ref<string[]>([]);
const selected = computed(() => new Set(selectedIds.value));
const assigned = reactive<Record<string, string>>({});
const itemId = ref('');
const sheetName = ref('');
const editingText = ref('');
const vFocusCell = { mounted: (element: HTMLTextAreaElement) => element.focus() };
const zoom = ref(100);
const error = ref('');
const sourceWarnings = computed(() => [...new Set(
  [board.value?.inventory_error, board.value?.warning, props.inventoryWarning]
    .filter((warning): warning is string => !!warning && warning !== error.value),
)]);
const busy = ref('');
const loading = ref(false);
const saved = ref('');
const report = shallowRef<File>();
const reportInput = ref<HTMLInputElement>();
const confirmAction = shallowRef<{ title: string; text: string; run: () => void }>();
const batchOpen = ref(false);
const batch = reactive<Review>({ reviewer: '', reviewed_at: '', signature: '' });
const signatureOpen = ref(false);
const signatureForBatch = ref(false);
const pad = ref<HTMLCanvasElement>();
const signed = ref(false);
const lifetime = new AbortController();
let pollTimer: ReturnType<typeof setTimeout> | undefined;
let recordsTimer: ReturnType<typeof setInterval> | undefined;
let recordRequest = 0;
let loadedRecordPages = 1;
let pollVersion = '';
let boardRead: AbortController | undefined;
let openOperation: { key: string; id: string } | undefined;
let strokeId: number | undefined;
const recordsOpen = ref(!!props.recordsOnly);
const recordAsset = ref<Asset>();
const recordSessions = shallowRef<Summary[]>([]);
const visibleRecordSessions = computed(() => {
  const followups = new Set(props.followupOrderIds || []);
  const building = props.scope && !['ALL', 'CAMPUS'].includes(props.scope) ? props.scope : '';
  if (!props.recordsOnly) return recordSessions.value;
  return recordSessions.value.map(batch => ({ ...batch, history_items: (batch.history_items || []).filter(tool =>
    !followups.has(tool.order_id || '') && (!building || tool.scope === building)),
  })).filter(batch => batch.history_items.length);
});
const recordCursor = shallowRef<HistoryPage['history_cursor']>();
const scrapPhotoLimit = 3;
const scrapPhotoMaxBytes = 1024 * 1024;
const photoInput = ref<HTMLInputElement>();
const photoPreview = ref('');

const assets = computed(() => board.value?.assets || []);
const templates = computed(() => board.value?.templates || []);
const scopeOptions = computed(() => [{ value: '', label: '全部楼栋' },
  ...[...new Set(assets.value.map(asset => asset.scope).filter(Boolean))].sort().map(value => ({ value, label: scopeLabel(value) }))]);
const visibleAssets = computed(() => {
  const terms = search.value.trim().toLowerCase().split(/\s+/).filter(Boolean);
  return assets.value.filter(asset => (!scope.value || asset.scope === scope.value)
    && (!onlyCycle(asset) || onlyCycle(asset) === kind.value)
    && terms.every(term => [asset.name, asset.model, asset.brand, asset.location, scopeLabel(asset.scope)].join(' ').toLowerCase().includes(term)));
});
const visibleEligible = computed(() => visibleAssets.value.filter(selectable));
const allVisibleSelected = computed(() => !!visibleEligible.value.length && visibleEligible.value.every(asset => selected.value.has(asset.id)));
const someVisibleSelected = computed(() => visibleEligible.value.some(asset => selected.value.has(asset.id)));
const selectedAssets = computed(() => assets.value.filter(asset => selected.value.has(asset.id)));
const templatesById = computed(() => new Map(templates.value.map(template => [template.id, template])));
const allAssociated = computed(() => selectedAssets.value.length > 0 &&
  selectedAssets.value.every(asset => templatesById.value.has(asset.template_id || '')));
const assignedCount = computed(() => Object.keys(assigned).length);
const pendingIds = computed(() => [...new Set([...Object.keys(assigned), ...selectedIds.value])]);
const pendingAssets = computed(() => {
  const byId = new Map(assets.value.map(asset => [asset.id, asset]));
  return pendingIds.value.flatMap(id => byId.get(id) || []);
});
const templateAssignments = computed(() => {
  const result: Record<string, string> = { ...assigned };
  for (const asset of selectedAssets.value) if (!result[asset.id] && asset.template_id) result[asset.id] = asset.template_id;
  return result;
});
const unassignedCount = computed(() => pendingIds.value.filter(id => !templatesById.value.has(templateAssignments.value[id])).length);
const mixedReportBuildings = computed(() => kind.value === 'external' &&
  new Set(assets.value.filter(asset => pendingIds.value.includes(asset.id)).map(asset => asset.scope)).size > 1);
const canOpen = computed(() => pendingIds.value.length > 0 && !mixedReportBuildings.value && (kind.value === 'external' || unassignedCount.value === 0));
const sessionOptions = computed(() => (board.value?.sessions || []).filter(entry => entry.kind === kind.value)
  .map(entry => ({ value: entry.id, label: `${sessionStatus(entry)} · ${entry.item_count ?? entry.items?.length ?? 0} 台 · ${entry.created_at?.slice(0, 16).replace('T', ' ') || entry.id.slice(0, 8)}` })));
const sessionUrl = computed(() => api + '/' + encodeURIComponent(session.value?.id || ''));
const item = computed(() => session.value?.items.find(entry => entry.id === itemId.value));
const resubmitAsset = computed(() => item.value?.can_edit && item.value.review_status === 'rejected' ? item.value.id : '');
const reviewerOptions = computed(() => reviewers.value?.[item.value?.asset.scope || ''] || []);
const sheets = computed(() => item.value?.template?.sheets || []);
const canAddScrapPhoto = computed(() => (item.value?.scrap_photos?.length || 0) < scrapPhotoLimit);
const activeSheet = computed(() => {
  return sheets.value.find(sheet => sheet.name === sheetName.value) || sheets.value.find(sheet => sheet.rows.some(row => row.cells.some(cell => cell.check || cell.editable))) || sheets.value[0];
});
const sheetWidth = computed(() => activeSheet.value?.columns.reduce((sum, width) => sum + width, 0) || 0);
const sheetImages = computed(() => {
  const sheet = activeSheet.value;
  return (sheet?.images || []).flatMap((image, index) => {
    const anchor = /^([A-Z]+)(\d+)$/.exec(image.address);
    if (!sheet || !anchor || !/^data:image\/(?:png|jpeg|webp|gif);base64,/.test(image.src)) return [];
    const col = [...anchor[1]].reduce((number, letter) => number * 26 + letter.charCodeAt(0) - 64, 0);
    const left = sheet.columns.slice(0, col - 1).reduce((sum, width) => sum + width, image.offsetX);
    const top = sheet.rows.filter(row => row.index < Number(anchor[2])).reduce((sum, row) => sum + row.height, image.offsetY);
    return [{ key: image.address + index, src: image.src, style: { left: left + 'px', top: top + 'px', width: image.width + 'px', height: image.height + 'px' } }];
  });
});
const editable = computed(() => !!session.value && ['draft', 'opening'].includes(session.value.status) &&
  session.value.can_edit && session.value.report_available !== false);
const reviewable = computed(() => session.value?.items.filter(entry => entry.can_review) || []);
const dirty = computed(() => !!report.value || Object.keys(reviewSignatures).length > 0 || Object.values(reviewDates).some(Boolean) ||
  !!editable.value && (session.value?.kind === 'external' ? reportSnapshot.value !== savedReport.value : JSON.stringify(fields()) !== saved.value));

function scopeLabel(value: string) {
  if (!value) return '—';
  if (value === '110') return '110站';
  return /^[A-Z]$/.test(value) ? value + '楼' : value;
}
function onlyCycle(asset: Asset): Kind | undefined {
  const self = !!asset.self_cycle && asset.self_cycle !== '停用';
  const external = !!asset.external_cycle && asset.external_cycle !== '停用';
  if (self === external) return;
  return self ? 'self' : 'external';
}
function eligible(asset: Asset) {
  return asset.eligible_kinds?.includes(kind.value) ?? (onlyCycle(asset) === kind.value);
}
function selectable(asset: Asset) {
  return eligible(asset) && !assigned[asset.id];
}
function reason(asset: Asset) {
  if (asset.disabled_reason) return asset.disabled_reason;
  if (asset.self_cycle && asset.external_cycle) return '同时设置了两种周期';
  if (!asset.self_cycle && !asset.external_cycle) return '未设置周期';
  return eligible(asset) ? '' : '当前账号不可办理';
}
function cycleText(asset: Asset) {
  return (kind.value === 'self' ? asset.self_cycle : asset.external_cycle) || '—';
}
function sessionStatus(entry: Summary) {
  return ({ opening: '模板关联待核对', draft: '草稿', review_pending: '待审核', pending: '归档处理中',
    cancelled: '已取消', archived: '已归档' } as Record<string, string>)[entry.status] || entry.status;
}
function archiveStatus(status?: string) {
  return ({ archived: '已归档', verifying: '正在核对归档结果', retrying: '归档失败，待重试',
    review_pending: '待审核', cancelled: '已取消', pending: '待处理' } as Record<string, string>)[status || ''] || '待处理';
}
function recordStatus(tool: HistoryItem) {
  if (tool.order_status === 'pending') return tool.binding_pending || tool.binding_error ? '模板关联待核对' : '待填写';
  if (tool.order_status === 'submitted' && tool.archive_status === 'pending') return '待归档';
  return archiveStatus(tool.archive_status || tool.order_status);
}
function chooseKind(value: Kind) {
  if (value === kind.value) return;
  leave(() => {
    kind.value = value;
    selectedIds.value = [];
    for (const id of Object.keys(assigned)) delete assigned[id];
    clearSession();
  });
}
function chooseTemplate(value: string) {
  if (busy.value || !selectedIds.value.length) return;
  if (value === existingTemplate ? !allAssociated.value : !templatesById.value.has(value)) return;
  for (const asset of selectedAssets.value) assigned[asset.id] = value === existingTemplate ? asset.template_id! : value;
  selectedIds.value = [];
}
function templateName(id: string) {
  const template = templatesById.value.get(id);
  return template?.label || template?.name || '模板待核对';
}
function toggleAsset(id: string, checked: boolean) {
  if (assigned[id]) return;
  if (selected.value.has(id) === checked) return;
  if (checked && !assigned[id] && pendingIds.value.length >= 200) {
    error.value = '每批最多选择 200 台工具，请分批办理。';
    return;
  }
  selectedIds.value = checked ? [...selectedIds.value, id] : selectedIds.value.filter(value => value !== id);
}
function removeSelection(id: string) {
  delete assigned[id];
  toggleAsset(id, false);
}
function chooseAssetRow(asset: Asset, event: MouseEvent) {
  if (busy.value || !selectable(asset) ||
      (event.target instanceof Element && event.target.closest('button,a,input,[role="checkbox"]'))) return;
  toggleAsset(asset.id, !selected.value.has(asset.id));
}
function transferAsset(asset: Asset) {
  ask('接收工具移交？', `「${asset.name}」的原楼栋办理会结束，已保存草稿保留在原记录。接收后可在本楼重新填写。`,
    () => void perform('transfer', async () => {
      await post(api + '/transfer', { asset_id: asset.id, scope: asset.scope,
        order_id: asset.plan?.order_id, revision: asset.plan?.revision });
      await readBoard();
    }));
}
function toggleVisible(checked: boolean) {
  const ids = new Set(visibleEligible.value.map(asset => asset.id));
  const selection = checked ? [...new Set([...selectedIds.value, ...ids])] : selectedIds.value.filter(id => !ids.has(id));
  if (new Set([...selection, ...Object.keys(assigned)]).size > 200) {
    error.value = '筛选结果超过每批 200 台上限，请缩小筛选范围。';
    return;
  }
  selectedIds.value = selection;
}
function cellKey(cell: Pick<Cell,'address'>) {
  return activeSheet.value?.name + '!' + cell.address;
}
function reviewCell(cell: Cell, sheet = activeSheet.value) {
  const schema = item.value?.template?.schema;
  if (!sheet || schema?.sheet !== sheet.name) return '';
  if (cell.address === (sheet.review?.reviewer_address || 'E' + schema.review_row)) return 'reviewer';
  if (cell.address === (sheet.review?.reviewed_at_address || 'G' + schema.review_row)) return 'time';
  if (cell.address === 'E' + schema.result_row) return 'result';
  if (isSignatureCell(cell, sheet)) return 'signature';
  return '';
}
function isSignatureCell(cell: Cell, sheet = activeSheet.value) {
  const schema = item.value?.template?.schema;
  return !!sheet && schema?.sheet === sheet.name && cell.address === (sheet.review?.signature_address || 'E' + schema.review_row);
}
function reviewText(cell: Cell) {
  const current = item.value;
  const sheet = activeSheet.value;
  if (!current || !sheet) return cell.value;
  switch (reviewCell(cell)) {
    case 'reviewer': return current.reviewer || current.reviewer_name || cell.value;
    case 'time': return current.reviewed_at?.replace('T', ' ') || cell.value;
    case 'result': {
      if (current.completion === 'completed') return '已完成[√]　未完成[ ]';
      if (current.completion === 'incomplete') return '已完成[ ]　未完成[√]';
      const checks = sheet.rows.flatMap(row => row.cells.filter(entry => entry.check)).map(entry => current.checks[cellKey(entry)]);
      if (!checks.length || checks.some(value => value !== '是' && value !== '否')) return cell.value;
      return checks.every(value => value === '是') ? '已完成[√]　未完成[ ]' : '已完成[ ]　未完成[√]';
    }
    default: return cell.value;
  }
}
function cellDateKind(cell: Cell):ToolDateKind|undefined {
  if (cell.input_kind) return cell.input_kind;
  const layout = item.value?.template;
  if (!layout || layout.schema?.sheet !== activeSheet.value?.name) return;
  const dateAddress = layout.prefill_fields?.checked_at || (layout.schema?.layout === 'hvac' ? 'G11' : 'G8');
  if (cell.address === dateAddress) return String(cell.value).includes('时') ? 'datetime-local' : 'date';
}
function inputCellValue(cell: Cell) {
  return item.value?.values[cellKey(cell)] ?? cell.picker_default ?? cell.value ?? '';
}
function sheetText(cell:Cell) {
  const value = inputCellValue(cell);
  const kind = cellDateKind(cell);
  return kind ? displayToolDate(kind,String(value),String(cell.value ?? '')) : value;
}
function specialCell(cell:Cell) {
  return !!cellDateKind(cell) || boxParts(String(cell.value ?? '')).parts.length>0 || boxParts(String(inputCellValue(cell))).parts.length>0;
}
function setCompletion(event: Event) {
  if (item.value?.can_edit) item.value.completion = (event.target as HTMLSelectElement).value;
}
function setReviewer(event: Event) {
  if (!item.value?.can_edit) return;
  const id = (event.target as HTMLSelectElement).value;
  item.value.reviewer_id = id;
  item.value.reviewer_name = reviewerOptions.value.find(person => person.id === id)?.name || '';
}
function setReviewDate(event: Event) {
  if (item.value?.can_review) reviewDates[item.value.id] = (event.target as HTMLInputElement).value;
}
async function loadReviewers(showError = true) {
  const current = session.value;
  if (!current || reviewers.value || reviewerLoading.value) return;
  const request = new AbortController();
  reviewerRequest = request;
  reviewerLoading.value = true;
  const path = sessionUrl.value + '/people';
  try {
    const people = await read(path, request.signal) as NonNullable<typeof reviewers.value>;
    if (!request.signal.aborted && session.value?.id === current.id) reviewers.value = people;
  } catch (cause) {
    if (showError && !request.signal.aborted && session.value?.id === current.id) error.value = message(cause);
  } finally {
    if (reviewerRequest === request) reviewerLoading.value = false;
  }
}
function normalFill() {
  const current = item.value;
  const sheet = activeSheet.value;
  if (!current?.can_edit || !sheet) return;
  for (const row of sheet.rows) for (const cell of row.cells) {
    if (cell.check) current.checks[cellKey(cell)] = '是';
  }
  current.completion = 'completed';
  current.outcome = 'normal';
}
function setCell(cell: Pick<Cell,'address'>, event: Event) {
  setCellValue(cell,(event.target as HTMLInputElement).value);
}
function setCellValue(cell: Pick<Cell,'address'>, value:string) {
  if (item.value?.can_edit && editable.value && !busy.value) item.value.values[cellKey(cell)] = value;
}
function setCheck(cell: Cell, event: Event) {
  if (!item.value || !editable.value) return;
  const value = (event.target as HTMLSelectElement).value;
  if (value) item.value.checks[cellKey(cell)] = value;
  else delete item.value.checks[cellKey(cell)];
}
function readPhoto(file: File) {
  return new Promise<string>((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => typeof reader.result === 'string' ? resolve(reader.result) : reject(new Error('照片读取失败'));
    reader.onerror = () => reject(reader.error || new Error('照片读取失败'));
    reader.readAsDataURL(file);
  });
}
async function pickScrapPhotos(entry: Item, event: Event) {
  const input = event.target as HTMLInputElement;
  const files = Array.from(input.files || []);
  input.value = '';
  if (!files.length || !entry.can_edit || busy.value) return;
  const photos = entry.scrap_photos = entry.scrap_photos || [];
  const room = scrapPhotoLimit - photos.length;
  if (room <= 0) {
    error.value = `报废现场照片最多 ${scrapPhotoLimit} 张。`;
    return;
  }
  const accepted: string[] = [];
  for (const file of files.slice(0, room)) {
    if (!['image/jpeg', 'image/png'].includes(file.type)) { error.value = `「${file.name}」不是 JPEG 或 PNG 图片。`; continue; }
    if (file.size > scrapPhotoMaxBytes) { error.value = `「${file.name}」超过 1MB。`; continue; }
    try { accepted.push(await readPhoto(file)); }
    catch (cause) { error.value = `「${file.name}」${message(cause)}。`; }
  }
  if (files.length > room) error.value = `报废现场照片最多 ${scrapPhotoLimit} 张，已加入前 ${room} 张。`;
  if (accepted.length && session.value?.items.includes(entry)) entry.scrap_photos = [...photos, ...accepted];
}
function removeScrapPhoto(entry: Item, index: number) {
  if (!entry.can_edit || busy.value) return;
  entry.scrap_photos = (entry.scrap_photos || []).filter((photo, position) => position !== index);
}
function selectItem(id: string) {
  itemId.value = id;
  replacementTemplate.value = null;
  sheetName.value = '';
}
function fields() {
  return (session.value?.items || []).filter(entry => entry.can_edit).map(({ id, asset_id, values, checks, outcome, reason, quantity, scrap_photos, completion, reviewer_id }) =>
    ({ id, asset_id, values, checks, outcome, reason, quantity, scrap_photos, completion, reviewer_id }));
}
function clearSession() {
  clearTimeout(pollTimer);
  session.value = undefined;
  report.value = undefined;
  itemId.value = '';
  sheetName.value = '';
  actualDate.value = '';
  reportCheck.value = emptyInspectionReport();
  savedReport.value = reportSnapshot.value;
  reviewNote.value = '';
  savedSignature.value = false;
  reviewers.value = undefined;
  reviewerRequest?.abort();
  reviewerLoading.value = false;
  for (const id of Object.keys(reviewDates)) delete reviewDates[id];
  for (const id of Object.keys(reviewSignatures)) delete reviewSignatures[id];
}
function returnToSelection() {
  leave(() => {
    clearSession();
    error.value = '';
    if (recordsOpen.value) void loadRecords();
    void nextTick(() => {
      const heading = document.getElementById(recordsOpen.value ? 'tool-records-title' : 'tool-assets-title');
      heading?.focus({ preventScroll: true });
      heading?.scrollIntoView({ block: 'nearest' });
    });
  });
}
function applySession(value: Session, retainReportDraft = false) {
  const opened = session.value?.id !== value.id;
  if (opened) clearSession();
  for (const entry of value.items) {
    const layout = entry.layout_key ? value.layouts?.[entry.layout_key] : entry.template;
    if (layout) entry.template = markRaw(layout);
  }
  session.value = value;
  pollVersion = '';
  if (value.kind === 'self' && value.items.some(entry => entry.can_edit || entry.can_reassign)) void loadReviewers(false);
  if (!retainReportDraft) {
    actualDate.value = value.actual || '';
    reportCheck.value = emptyInspectionReport(value.inspection);
    savedReport.value = reportSnapshot.value;
  }
  if (!value.items.some(entry => entry.id === itemId.value)) selectItem((value.items.find(entry => entry.can_edit) || value.items[0])?.id || '');
  saved.value = JSON.stringify(fields());
  savedItems.clear();
  for (const entry of fields()) savedItems.set(entry.id, JSON.stringify(entry));
  if (board.value) {
    const { id, kind, status, item_count, created_at } = value;
    const summary = { id, kind, status, item_count: item_count ?? value.items.length, created_at };
    board.value = { ...board.value, sessions: [summary, ...board.value.sessions.filter(entry => entry.id !== value.id)] };
  }
  if (opened) void nextTick(() => {
    const heading = document.getElementById('tool-editor-title');
    heading?.focus({ preventScroll: true });
    heading?.scrollIntoView({ block: 'nearest' });
  });
  schedulePoll();
}
function message(cause: unknown) {
  return cause instanceof Error ? cause.message : '操作尚未确认，请刷新核对。';
}
function read(path: string, signal = lifetime.signal) {
  return requestJson(compactPath(path), { signal, cache: 'no-store', fresh: true });
}
function compactPath(path: string) { return path + (path.includes('?') ? '&' : '?') + 'compact=1'; }
function post(path: string, payload: object, timeoutMs?: number) {
  return requestJson(compactPath(path), { method: 'POST', body: JSON.stringify(payload), signal: lifetime.signal, timeoutMs });
}
async function readBoard(refresh = false) {
  boardRead?.abort();
  const controller = new AbortController();
  boardRead = controller;
  loading.value = true;
  try {
    const data = await (refresh
      ? requestJson(api + '/refresh', { method: 'POST', signal: controller.signal })
      : read(api, controller.signal)) as Board;
    if (!controller.signal.aborted) {
      board.value = data;
      if (typeof data.inventory_error === 'string') emit('inventory-status', data.inventory_error);
      const ids = new Set(data.assets.filter(eligible).map(asset => asset.id));
      selectedIds.value = selectedIds.value.filter(id => ids.has(id));
      for (const id of Object.keys(assigned)) if (!ids.has(id)) delete assigned[id];
    }
  } catch (cause) {
    if (!controller.signal.aborted) error.value = message(cause);
  } finally {
    if (boardRead === controller) loading.value = false;
  }
}
async function perform(action: string, operation: () => Promise<void>) {
  if (busy.value || lifetime.signal.aborted) return;
  busy.value = action;
  error.value = '';
  try { await operation(); }
  catch (cause) { if (!lifetime.signal.aborted) error.value = message(cause); }
  finally { busy.value = ''; schedulePoll(); }
}
function loadMoreHistory() {
  const cursor = board.value?.history_cursor;
  if (!cursor) return;
  void perform('history', async () => {
    const page = await read(api + '/history?' + new URLSearchParams(cursor)) as HistoryPage;
    if (!board.value) return;
    const known = new Set(board.value.sessions.map(entry => entry.id));
    board.value = { ...board.value, history_cursor: page.history_cursor,
      sessions: [...board.value.sessions, ...page.sessions.filter(entry => !known.has(entry.id))] };
  });
}
async function fetchRecords(more = false, preservePages = false) {
  const cursor = more ? recordCursor.value : null;
  if (more && !cursor) return;
  const request = ++recordRequest;
  const query = new URLSearchParams(cursor || {});
  if (recordAsset.value) query.set('asset_id', recordAsset.value.id);
  let page = await read(api + '/history?' + query) as HistoryPage;
  const entries = new Map((more ? recordSessions.value : []).map(batch => [batch.id, batch]));
  let pages = 1;
  while (true) {
    if (request !== recordRequest || lifetime.signal.aborted) return;
    for (const batch of page.sessions) entries.set(batch.id, batch);
    if (!preservePages || !page.history_cursor || pages >= loadedRecordPages) break;
    for (const [key, value] of Object.entries(page.history_cursor)) query.set(key, value);
    page = await read(api + '/history?' + query) as HistoryPage;
    pages += 1;
  }
  loadedRecordPages = more ? loadedRecordPages + 1 : pages;
  recordSessions.value = [...entries.values()];
  recordCursor.value = page.history_cursor;
}
function loadRecords(more = false) {
  return perform('records', () => fetchRecords(more));
}
function reloadRecords() {
  emit('records-refresh');
  return loadRecords();
}
function showRecords(asset?: Asset) {
  recordAsset.value = asset;
  recordSessions.value = [];
  recordCursor.value = null;
  recordsOpen.value = true;
  void loadRecords();
  void nextTick(() => document.getElementById('tool-records-title')?.focus());
}
function manageRecord(batch: Summary, tool: HistoryItem, action: 'release' | 'delete') {
  const removing = action === 'delete';
  const description = !removing ? '取消办理后仍可查看原草稿。' :
    tool.archive_status === 'archived' ? '将删除本次办理及对应的飞书台账记录。' :
      '将取消这笔办理并从列表移除，原草稿会保留。';
  ask(removing ? '删除这笔办理记录？' : '取消本台工具办理？', `「${tool.asset_name}」${description}`,
    () => void perform('manage', async () => {
      const updated = await post(api + '/' + encodeURIComponent(batch.id) + '/manage', { action, asset_id: tool.id,
        revision: tool.order_revision ?? batch.revision, note: '工作台取消本台办理，保留原草稿。' }) as
        { status?: string; batch_hidden?: boolean };
      if ((updated.status === 'cancelled' || updated.batch_hidden) && openOperation?.id === batch.operation_id) openOperation = undefined;
      await fetchRecords();
      void readBoard();
    }));
}
function requestOpen() {
  if (!canOpen.value) return;
  leave(() => {
    const mapping = kind.value === 'self' ? { ...templateAssignments.value } : {};
    const changed = assets.value.filter(asset => mapping[asset.id] && asset.template_id && mapping[asset.id] !== asset.template_id);
    const run = () => void openSelection(mapping);
    if (changed.length) ask('更换已关联模板？', `将更新 ${changed.length} 台工具与原模板的关联。`, run);
    else run();
  });
}
async function openSelection(template_ids: Record<string, string>) {
  await perform('open', async () => {
    const payload = { asset_ids: [...pendingIds.value], template_ids, kind: kind.value, defer_bindings: true, new_record: true };
    const key = JSON.stringify(payload);
    if (openOperation?.key !== key) openOperation = { key, id: Array.from(crypto.getRandomValues(new Uint8Array(16)), part => part.toString(16).padStart(2, '0')).join('') };
    let result: Session;
    try {
      result = await post(api + '/open', { ...payload, operation_id: openOperation.id }, 120000) as Session;
    } catch (cause) {
      void readBoard();
      throw cause;
    }
    openOperation = undefined;
    report.value = undefined;
    applySession(result);
    selectedIds.value = [];
    for (const id of Object.keys(assigned)) delete assigned[id];
    void readBoard();
  });
}
function openSaved(id: string, assetId = '') {
  if (!id || id === session.value?.id) return;
  leave(() => void perform('read', async () => {
    const path = api + '/' + encodeURIComponent(id);
    let data = await read(path) as Session;
    const targetAsset = assetId || (id === props.sessionId ? props.assetId : '');
    if (targetAsset && !data.items.some(entry => entry.id === targetAsset))
      throw new Error('当前账号无法查看链接里的自检记录，或该记录已移出原批次。');
    if (data.legacy_draft) {
      ask('接续旧草稿', '保留原表填写内容，审核签名由审核人确认。', () =>
        void perform('read', async () => applySession(await post(path + '/resume', {}, 120000) as Session)));
    } else if (data.status === 'opening' && !data.binding_pending) {
      try {
        data = await requestJson(compactPath(path + '/resume'), { method: 'POST', signal: lifetime.signal, timeoutMs: 120000 }) as Session;
      } catch (cause) {
        await readBoard();
        throw cause;
      }
    }
    clearSession();
    applySession(data);
    if (targetAsset) selectItem(targetAsset);
  }));
}
function refresh() {
  if (recordsOpen.value && !session.value) return perform('records', async () => {
    await readBoard(true);
    emit('records-refresh');
    await fetchRecords();
  });
  leave(() => void perform('read', async () => {
    await readBoard(true);
    if (session.value) {
      const data = await read(sessionUrl.value) as Session;
      report.value = undefined;
      applySession(data);
    }
  }));
}
async function save() {
  if (!session.value || !editable.value || !dirty.value) return;
  const changed = fields().filter(entry => JSON.stringify(entry) !== savedItems.get(entry.id));
  if (changed.length) applySession(await post(sessionUrl.value + '/save', { revision: session.value.revision, items: changed }) as Session);
}
function saveDraft() {
  return perform('save', save);
}
function pickReport(event: Event) {
  const input = event.target as HTMLInputElement;
  report.value = input.files?.[0] || report.value;
  input.value = '';
}
function uploadReport() {
  const file = report.value;
  const current = session.value;
  if (!file || !current || !editable.value) return;
  return perform('report', async () => {
    const query = new URLSearchParams({ name: file.name, revision: String(current.revision) });
    const data = await requestBinaryJson(sessionUrl.value + '/report?' + query, file, {
      headers: { 'Content-Type': file.type || 'application/octet-stream' }, signal: lifetime.signal,
    }) as Session;
    report.value = undefined;
    reportCheck.value.confirmed_asset_ids = [];
    applySession(data, true);
  });
}
function submit() {
  if (!editable.value || report.value) return;
  return perform('submit', async () => {
    const target = resubmitAsset.value;
    if (session.value?.kind === 'self') {
      if (!item.value?.can_edit) return;
      for (const entry of session.value.items.filter(entry => entry.can_edit && (!target || entry.id === target))) {
        const missing = !entry.outcome;
        const incomplete = !entry.template?.sheets.length || entry.template.sheets.some(sheet => sheet.rows.some(row =>
          row.cells.some(cell => cell.check && !['是', '否'].includes(entry.checks[sheet.name + '!' + cell.address] || ''))));
        const inconsistent = entry.outcome === 'normal' && Object.values(entry.checks).includes('否');
        if (missing || incomplete || inconsistent) {
          selectItem(entry.id);
          throw new Error(`「${entry.asset.name}」${missing ? '请选择检查结果。' : incomplete ? '请完成原表中的所有检查项。' : '检查项存在「否」，检测结果不能选「正常」。'}`);
        }
      }
    }
    if (session.value?.kind === 'self') await save();
    if (session.value && editable.value) applySession(await post(sessionUrl.value + '/submit', {
      revision: session.value.revision, actual: actualDate.value, inspection: reportCheck.value,
      ...(target ? { resubmit_asset_id: target } : {}),
    }) as Session);
  });
}
function schedulePoll(delay = 3000) {
  clearTimeout(pollTimer);
  const current = session.value;
  if (!current || lifetime.signal.aborted || (!current.binding_pending && !['pending', 'review_pending'].includes(current.status)
      && !current.items.some(entry => entry.result_notice || ['review_pending', 'verifying', 'retrying'].includes(entry.archive_status || '')
        || entry.order_status === 'submitted' && entry.archive_status === 'pending'))) return;
  pollTimer = setTimeout(async () => {
    if (busy.value) return schedulePoll();
    try {
      const data = await read(api + '/' + encodeURIComponent(current.id) + '/status?known=' + encodeURIComponent(pollVersion)) as
        Session & { status_version?: string; unchanged?: boolean };
      if (session.value === current && !lifetime.signal.aborted) {
        if (busy.value) return schedulePoll();
        error.value = '';
        if (data.unchanged) return schedulePoll();
        const statuses = new Map(data.items.map(entry => [entry.id, entry]));
        if (data.status !== current.status || data.revision !== current.revision ||
            current.items.some(entry => {
              const next = statuses.get(entry.id);
              return next?.order_revision !== entry.order_revision || next?.binding_pending !== entry.binding_pending ||
                next?.binding_error !== entry.binding_error;
            })) {
          if (!busy.value && !dirty.value) {
            const latest = await read(api + '/' + encodeURIComponent(current.id)) as Session;
            if (session.value !== current || lifetime.signal.aborted) return;
            if (!busy.value && !dirty.value) {
              applySession(latest);
              return;
            }
          }
          schedulePoll();
        } else {
          pollVersion = data.status_version || '';
          current.items.forEach(entry => {
            const status = statuses.get(entry.id);
            if (status) Object.assign(entry, status);
          });
          schedulePoll();
        }
      }
    } catch (cause) {
      if (session.value === current && !lifetime.signal.aborted) {
        error.value = message(cause);
        schedulePoll(6000);
      }
    }
  }, delay);
}

function openBatchReview() {
  Object.assign(batch, { reviewer: '', reviewed_at: '', signature: '' });
  batchSavedSignature.value = savedSignature.value;
  batchOpen.value = true;
}
function applyBatchReview() {
  if (batch.signature || batchSavedSignature.value) void reviewItems(reviewable.value, true, batch.signature, batchSavedSignature.value, batch.reviewed_at);
}
function startReassign() {
  reassignReviewer.value = item.value?.reviewer_id || '';
  reassignOpen.value = true;
  void loadReviewers();
}
function confirmItemAction(action: 'withdraw' | 'release') {
  const release = action === 'release';
  ask(release ? '取消本台工具办理？' : '撤回本台审核？',
    `${item.value?.asset.name || ''} · 已填写内容会保留，${release ? '该工具可重新办理' : '撤回后可以继续填写'}。`,
    () => void manageItem(action));
}
async function manageItem(action: 'reassign' | 'withdraw' | 'release') {
  const id = item.value?.id;
  if (!id) return;
  await perform('manage', async () => {
    if (editable.value) await save();
    const entry = session.value?.items.find(tool => tool.id === id);
    if (!entry) return;
    applySession(await post(sessionUrl.value + '/manage', { action, asset_id: entry.id, revision: entry.order_revision,
      reviewer_id: reassignReviewer.value, note: '工作台取消本台办理，保留原草稿。' }) as Session);
    delete reviewSignatures[entry.id];
    delete reviewDates[entry.id];
    reassignOpen.value = false;
    void readBoard();
  });
}
async function reviewItems(entries: Item[], approve: boolean, signature = '', useSaved = savedSignature.value, reviewedAt = '') {
  await perform('review', async () => {
    const items = entries.map(entry => ({asset_id: entry.id, revision: entry.order_revision,
      ...(approve && reviewSignatures[entry.id] ? {signature: reviewSignatures[entry.id]} : {}),
      ...(approve && reviewDates[entry.id] ? {reviewed_at: reviewDates[entry.id]} : {})}));
    applySession(await post(sessionUrl.value + '/review', {action: approve ? 'approve' : 'reject',
      note: reviewNote.value, signature, use_saved_signature: approve && useSaved && !signature &&
        entries.some(entry => !reviewSignatures[entry.id]), reviewed_at: reviewedAt, items}, 120000) as Session);
    for (const entry of entries) { delete reviewSignatures[entry.id]; delete reviewDates[entry.id]; }
    reviewNote.value = '';
    batchOpen.value = false;
  });
}
function resolveBinding(remove: boolean) {
  const current = item.value;
  if (!current || !session.value) return;
  const run = () => void perform('binding', async () => {
    if (!session.value) return;
    if (!remove) await readBoard(true);
    const result = await post(sessionUrl.value + '/binding', {asset_id: current.id, revision: session.value.revision,
      template_id: remove ? '' : replacementTemplate.value}, 120000) as Session & {removed?: boolean};
    if (result.removed) clearSession();
    else applySession(result);
    await readBoard();
  });
  ask(remove ? '移出本批？' : '重新关联模板？', current.asset.name, run);
}
function retryBindings() {
  void perform('binding', async () => {
    if (editable.value) await save();
    applySession(await requestJson(compactPath(sessionUrl.value + '/resume'), {
      method: 'POST', signal: lifetime.signal, timeoutMs: 120000,
    }) as Session);
  });
}
function isPng(value: string) {
  return /^data:image\/png;base64,/.test(value);
}
function openSignature(forBatch: boolean) {
  signatureForBatch.value = forBatch;
  signed.value = false;
  signatureOpen.value = true;
}
function resetCanvas() {
  const canvas = pad.value;
  canvas?.getContext('2d')?.clearRect(0, 0, canvas.width, canvas.height);
  signed.value = false;
  strokeId = undefined;
}
function point(event: PointerEvent, canvas: HTMLCanvasElement) {
  const box = canvas.getBoundingClientRect();
  return [(event.clientX - box.left) * canvas.width / box.width, (event.clientY - box.top) * canvas.height / box.height] as const;
}
function startStroke(event: PointerEvent) {
  const canvas = pad.value;
  const context = canvas?.getContext('2d');
  if (!canvas || !context || strokeId !== undefined || event.button !== 0) return;
  strokeId = event.pointerId;
  canvas.setPointerCapture(event.pointerId);
  const [x, y] = point(event, canvas);
  context.strokeStyle = '#111';
  context.lineWidth = 4;
  context.lineCap = 'round';
  context.lineJoin = 'round';
  context.beginPath();
  context.moveTo(x, y);
}
function moveStroke(event: PointerEvent) {
  const canvas = pad.value;
  const context = canvas?.getContext('2d');
  if (!canvas || !context || strokeId !== event.pointerId) return;
  const [x, y] = point(event, canvas);
  context.lineTo(x, y);
  context.stroke();
  context.beginPath();
  context.moveTo(x, y);
  signed.value = true;
}
function stopStroke(event: PointerEvent) {
  if (strokeId !== event.pointerId) return;
  strokeId = undefined;
  const canvas = pad.value;
  const pixels = canvas?.getContext('2d')?.getImageData(0, 0, canvas.width, canvas.height).data;
  signed.value = !!pixels?.some((value, index) => index % 4 === 3 && value > 0);
}
function applySignature() {
  const value = signed.value ? pad.value?.toDataURL('image/png') : '';
  if (!value) return;
  if (signatureForBatch.value) batch.signature = value;
  else if (item.value?.can_review) reviewSignatures[item.value.id] = value;
  signatureOpen.value = false;
}

function ask(title: string, text: string, run: () => void) {
  confirmAction.value = { title, text, run };
}
function closeConfirm(show: boolean) {
  if (!show) confirmAction.value = undefined;
}
function runConfirm() {
  const action = confirmAction.value;
  confirmAction.value = undefined;
  action?.run();
}
function askDiscard(proceed: () => void) {
  ask('放弃未保存内容？', '请先保存草稿，或确认放弃本次修改。', proceed);
}
function leave(proceed: () => void) {
  if (busy.value) return;
  if (dirty.value) askDiscard(proceed);
  else proceed();
}
defineExpose({ refresh, reloadRecords: loadRecords, leave, busy, dirty });
onMounted(() => {
  if (props.sessionId) openSaved(props.sessionId);
  if (props.recordsOnly) void loadRecords();
  else void readBoard();
  recordsTimer = setInterval(() => {
    if (!recordsOpen.value || session.value || busy.value || document.hidden) return;
    if (props.recordsOnly) emit('records-refresh');
    void fetchRecords(false, true).catch(() => { /* Keep the last successful list until the next refresh. */ });
  }, 20_000);
});
onBeforeUnmount(() => {
  lifetime.abort();
  boardRead?.abort();
  reviewerRequest?.abort();
  clearTimeout(pollTimer);
  clearInterval(recordsTimer);
});
</script>

<style scoped>
.tool-workspace { --tool-border: var(--v2-border, #dbe3e9); --tool-muted: var(--v2-muted, #617180); min-width: 0; }
.tool-workspace > .n-alert { margin-bottom: 10px; }
.confirmation-summary { max-height: min(55vh, 340px); overflow: auto; white-space: pre-line; }
.workspace-grid { display: grid; grid-template-columns: minmax(0, 6fr) minmax(0, 3fr) minmax(128px, 1.4fr); gap: 12px; align-items: start; }
.workspace-grid.is-external { grid-template-columns: minmax(0, 1fr) minmax(138px, 22%); }
.workspace-panel { min-width: 0; padding: 11px; border: 1px solid var(--tool-border); border-radius: 8px; background: var(--v2-card, var(--cf-surface, transparent)); }
.panel-heading, .workspace-actions, .review-line, .item-tabs { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; }
.panel-heading { justify-content: space-between; min-height: 24px; margin-bottom: 8px; }
.panel-heading h3 { font-size: 15px; margin: 0; }
.panel-heading small, .workspace-muted, .assets small { color: var(--tool-muted, #617180); }
.workspace-muted { margin: 4px 0; font-size: 13px; }
.asset-filters { display: grid; grid-template-columns: minmax(87px, .58fr) minmax(0, 1fr); gap: 6px; margin: 8px 0; }
.asset-list { overflow: auto; max-height: max(280px, calc(100dvh - 290px)); }
.assets, .templates { border-collapse: collapse; width: 100%; text-align: left; font-size: 12px; }
.assets th, .assets td, .templates th, .templates td { padding: 6px 4px; border-bottom: 1px solid var(--tool-border); vertical-align: top; overflow-wrap: anywhere; }
.templates th:first-child { width: 32px; }
.templates label { cursor: pointer; }
.assets th:first-child, .assets td:first-child { width: 22px; }
.assets small { display: block; margin-top: 3px; }
.assets tr.is-selected, .templates tr.is-selected { background: color-mix(in srgb, var(--v2-blue, #2364d2) 9%, transparent); }
.assets tr.is-selectable { cursor: pointer; }
.asset-reason { font-weight: 500; }
.template-list { display: grid; gap: 8px; max-height: max(280px, calc(100dvh - 280px)); overflow: auto; padding-block: 6px; }
.template-list :deep(.n-radio__label) { white-space: normal; overflow-wrap: anywhere; }
.workspace-panel > .n-radio-group { width: 100%; }
.workspace-editor { display: grid; gap: 9px; margin-top: 12px; min-width: 0; }
.tool-toolbar { display: flex; align-items: center; gap: 8px; min-width: 0; }
.tool-tabs { flex: 1; min-width: 0; flex-wrap: nowrap; overflow-x: auto; padding-block: 3px; }
.tool-tabs > .n-button, .tool-toolbar > .n-button { flex-shrink: 0; }
.review-line > label { display: grid; gap: 3px; min-width: 0; font-size: 12px; }
.item-actions select { min-height: 32px; max-width: 100%; padding: 2px 7px; background: var(--v2-card, var(--cf-surface, transparent)); color: inherit; border: 1px solid var(--tool-border); border-radius: 4px; }
.qualified-options { display: inline-flex; align-items: center; flex-wrap: nowrap; }
.qualified-options :deep(.n-radio-button) { display: inline-flex; align-items: center; }
.review-line label .n-input { max-width: 142px; }
.review-line input[type=datetime-local] { border: 1px solid var(--tool-border, #dbe3e9); color: inherit; color-scheme: inherit; background: transparent; border-radius: 4px; min-height: 28px; max-width: 100%; font: inherit; }
.signature-preview { width: 90px; height: 36px; object-fit: contain; background: #fff; border: 1px solid #dbe3e9; }
.scrap-photos { align-items: flex-start; }
.photo-strip { display: flex; flex-wrap: wrap; gap: 8px; margin: 0; padding: 0; list-style: none; }
.photo-item { display: grid; justify-items: center; gap: 2px; }
.photo-button { padding: 0; background: transparent; border: 0; cursor: zoom-in; }
.photo-preview { display: block; max-width: 100%; max-height: 75vh; margin: auto; }
.photo-item img { display: block; width: 72px; height: 72px; object-fit: cover; background: #fff; border: 1px solid var(--tool-border); border-radius: 4px; }
.scrap-approval-note { margin: 0; }
.opening-panel { display: grid; gap: 10px; }
.selection-review { max-height: 42vh; overflow: auto; list-style: none; margin: 0; padding: 0; }
.selection-review li { display: flex; justify-content: space-between; align-items: start; gap: 6px; padding: 7px 0; border-bottom: 1px solid var(--tool-border); }
.selection-review li div { min-width: 0; overflow-wrap: anywhere; }
.selection-review small { display: block; color: var(--tool-muted); margin-top: 2px; }
.opening-panel label { display: grid; gap: 4px; font-size: 12px; }
.records-panel,.record-list { display: grid; gap: 10px; }
.record-batch { border: 1px solid var(--tool-border); padding: 10px 12px; border-radius: 6px; }
.record-section-title { margin: 16px 0 8px; font-size: 14px; }
.record-heading,.record-tools li { display: flex; align-items: center; flex-wrap: wrap; gap: 8px 12px; }
.record-tools { list-style: none; padding: 0; margin: 8px 0 0; }
.record-tools li { border-top: 1px solid var(--tool-border); justify-content: space-between; padding: 8px 0 2px; }
.record-tools small { display: block; margin-top: 2px; }
.record-actions { margin-left: auto; }
.editor-footer { justify-content: flex-end; margin-top: 12px; }
.sheet-scroll { overflow: auto; max-height: min(60vh, 620px); max-width: 100%; }
.sheet-tools { justify-content: flex-end; }
.sheet-frame { position: relative; }
.sheet-table { border-collapse: collapse; table-layout: fixed; width: 100%; background: #fff; color: #141414; font-size: 13px; }
.sheet-table td { border: 1px solid #cbd1d8; vertical-align: middle; white-space: pre-wrap; overflow-wrap: anywhere; }
.sheet-table input, .sheet-table select, .sheet-table :deep(.tool-workbook-input input:not([type=checkbox]):not([type=radio])) { box-sizing: border-box; width: 100%; min-width: 0; min-height: 22px; padding: 1px 3px; border: 1px solid #9ab6e7; border-radius: 2px !important; color: #182333; background: #eff6ff; font: inherit; }
.sheet-frame > img { position: absolute; object-fit: contain; pointer-events: none; }
.form-title-box { position:absolute;display:grid;align-items:center;text-align:center;white-space:pre-wrap;line-height:1.2;font-weight:700;color:#171717; }
.sheet-text-button,.sheet-text-editor { display:block;box-sizing:border-box;width:100%;min-width:0;min-height:1.4em;padding:0 2px;font:inherit;color:inherit;text-align:inherit;white-space:pre-wrap;overflow-wrap:anywhere; }
.sheet-text-button { border:0;background:transparent;cursor:text; }
.sheet-text-button:hover,.sheet-text-button:focus-visible { outline:1px solid #85a7de;outline-offset:-1px; }
.sheet-text-editor { min-height:3em;background:#fff;color:#182333;border:1px solid #7298d6;resize:vertical; }
.signed-cell { display: flex; align-items: center; gap: 3px; min-width: 0; }
.signed-cell > span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.signature-in-cell { display: block; max-width: 55%; height: 35px; object-fit: contain; }
.archive-list { margin: 10px 0; padding-left: 20px; font-size: 13px; }
.dialog-actions { justify-content: flex-end; }
canvas { display: block; width: 100%; height: auto; touch-action: none; background: #fff; border: 1px solid #8592a1; }
@media (max-width: 680px) {
  .workspace-grid, .workspace-grid.is-external { grid-template-columns: minmax(0, 1fr); }
  .workspace-panel { padding: 8px; }
  :global(.workspace-v2 .tool-workspace .editor-footer) { padding-inline-end: 38px; }
  .asset-list { max-height: 52dvh; }
  .template-list { max-height: 28dvh; }
  .tool-workspace :deep(.n-button) { min-height: 40px; }
  .assets :deep(.n-checkbox), .templates :deep(.n-radio) { padding-block: 10px; }
  .review-line input[type=datetime-local] { min-height: 40px; }
  .item-actions select { min-height: 40px; }
  .qualified-options :deep(.n-radio-button) { padding-inline: 7px; min-height: 40px; }
}
</style>

<style>
.tool-workspace-dialog { width: min(520px, calc(100vw - 24px)); }
</style>
