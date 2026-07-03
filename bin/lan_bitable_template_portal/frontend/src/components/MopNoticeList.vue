<template>
  <aside class="panel notice-panel">
    <div class="panel-head">
      <div>
        <h2>本月维保通告</h2>
      </div>
      <span>{{ items.length }}</span>
    </div>
    <div class="filters">
      <input v-model="searchModel" placeholder="搜索通告、楼栋、专业" />
      <select v-model="statusModel">
        <option value="">全部状态</option>
        <option value="ongoing">进行中/未闭环</option>
        <option value="closed">已完成/已闭环</option>
        <option value="pending">待绑定或待上传</option>
        <option value="bound">已绑定表格</option>
        <option value="unbound">未绑定</option>
        <option value="uploaded">已上传维护单</option>
      </select>
    </div>
    <div v-if="!items.length" class="empty-box">
      暂无维保通告
    </div>
    <div v-else class="list notice-list">
      <button
        v-for="notice in items"
        :key="notice.notice_key"
        type="button"
        class="notice-row"
        :class="{ active: notice.notice_key === selectedNoticeKey, closed: noticeIsEnded(notice), pending: mopNoticeNeedsAction(notice) }"
        @click="$emit('select', String(notice.notice_key || ''))"
      >
        <span class="row-status" :class="{ closed: noticeIsEnded(notice) }">{{ notice.status || "进行中" }}</span>
        <strong>{{ notice.title || "未命名维保通告" }}</strong>
        <small>
          {{ notice.building || "未识别楼栋" }}
          <template v-if="notice.specialty"> · {{ notice.specialty }}</template>
          <template v-if="notice.maintenance_cycle"> · {{ notice.maintenance_cycle }}</template>
        </small>
        <span class="notice-mop-tags">
          <em :class="{ success: notice.mop_binding, warning: !notice.mop_binding }">
            <template v-if="notice.mop_binding">
              {{ notice.mop_binding.inherited ? "已继承绑定" : "已绑定" }}：{{ notice.mop_binding.mop_title || "已选 MOP 表格" }}
            </template>
            <template v-else>未绑定 MOP 表格</template>
          </em>
          <em :class="{ success: noticeMopUploaded(notice), warning: !noticeMopUploaded(notice) }">
            {{ noticeMopUploaded(notice) ? `已上传维护单${Number(notice.mop_attachment_count || 0) ? ` ${notice.mop_attachment_count}份` : ""}` : "未上传维护单" }}
          </em>
        </span>
      </button>
    </div>
  </aside>
</template>

<script setup lang="ts">
import { computed } from "vue";
import type { Dict } from "../api/client";

const props = defineProps<{
  items: Dict[];
  selectedNoticeKey: string;
  noticeSearch: string;
  noticeStatusFilter: string;
}>();

const emit = defineEmits<{
  "update:noticeSearch": [value: string];
  "update:noticeStatusFilter": [value: string];
  select: [noticeKey: string];
}>();

const searchModel = computed({
  get: () => props.noticeSearch,
  set: (value: string) => emit("update:noticeSearch", value),
});

const statusModel = computed({
  get: () => props.noticeStatusFilter,
  set: (value: string) => emit("update:noticeStatusFilter", value),
});

function noticeMopUploaded(notice: Dict): boolean {
  return Boolean(notice?.mop_uploaded || Number(notice?.mop_attachment_count || 0) > 0);
}

function mopNoticeNeedsAction(notice: Dict): boolean {
  return !notice?.mop_binding || !noticeMopUploaded(notice);
}

function noticeIsEnded(notice: Dict): boolean {
  const status = String(notice?.status || "").trim();
  if (!status || /未(结束|完成|闭环)/.test(status)) return false;
  return /(已结束|正常结束|维修完成|已完成|闭环)/.test(status);
}
</script>

<style scoped>
.panel {
  height: min(760px, calc(100vh - 300px));
  min-height: min(560px, calc(100vh - 220px));
  overflow: hidden;
  padding: 18px;
  display: grid;
  grid-template-rows: auto auto 1fr;
  gap: 14px;
  border: 1px solid #d8e5f7;
  border-radius: 22px;
  background: rgba(255, 255, 255, 0.92);
  box-shadow: 0 16px 36px rgba(0, 47, 135, 0.08);
}

.panel-head {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 12px;
}

.panel-head h2 {
  margin: 0;
  color: #0f172a;
  font-size: 18px;
}

.panel-head p {
  margin: 6px 0 0;
  color: #64748b;
  font-size: 12px;
}

.panel-head > span {
  padding: 5px 10px;
  border: 1px solid #cfe0ff;
  border-radius: 999px;
  color: #005bff;
  background: #eff6ff;
  font-weight: 800;
}

.filters {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(156px, 0.36fr);
  gap: 10px;
}

.filters input,
.filters select {
  min-height: 38px;
  border: 1px solid #c8dcf3;
  border-radius: 14px;
  background: #fbfdff;
  padding: 0 12px;
  color: #0f172a;
}

.list {
  min-height: 0;
  overflow: auto;
  display: grid;
  gap: 10px;
  align-content: start;
  padding-right: 4px;
}

.empty-box {
  display: grid;
  place-items: center;
  min-height: 180px;
  border: 1px dashed #bdd2f4;
  border-radius: 18px;
  padding: 18px;
  background: rgba(248, 251, 255, 0.86);
  color: #516a88;
  font-size: 13px;
  font-weight: 850;
  line-height: 1.6;
  text-align: center;
}

.notice-row {
  position: relative;
  width: 100%;
  border: 1px solid #d8e5f7;
  border-radius: 16px;
  padding: 13px 14px 13px 18px;
  text-align: left;
  color: #0f172a;
  background: #ffffff;
  cursor: pointer;
  transition: border-color 0.16s ease, box-shadow 0.16s ease, transform 0.16s ease;
}

.notice-row::before {
  content: "";
  position: absolute;
  top: 14px;
  bottom: 14px;
  left: 8px;
  width: 3px;
  border-radius: 999px;
  background: #cfe0ff;
}

.notice-row:hover,
.notice-row.active {
  border-color: #1e63ff;
  box-shadow: 0 10px 24px rgba(30, 99, 255, 0.13);
  transform: translateY(-1px);
}

.notice-row.pending {
  border-color: #fde68a;
  background: linear-gradient(135deg, #ffffff 0%, #fffbeb 100%);
}

.notice-row.pending::before {
  background: #f59e0b;
}

.notice-row.active::before {
  background: #1e63ff;
}

.notice-row.active {
  background:
    linear-gradient(135deg, rgba(239, 246, 255, 0.98), rgba(255, 255, 255, 0.98)),
    #ffffff;
}

.notice-row strong {
  display: block;
  margin-top: 8px;
  line-height: 1.45;
}

.notice-row small,
.notice-row em {
  display: block;
  margin-top: 7px;
  color: #64748b;
  font-size: 12px;
  font-style: normal;
}

.notice-mop-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin-top: 8px;
}

.notice-mop-tags em {
  display: inline-flex;
  align-items: center;
  max-width: 100%;
  min-height: 24px;
  margin-top: 0;
  padding: 4px 8px;
  border-radius: 999px;
  border: 1px solid #cfe0ff;
  background: #eff6ff;
  color: #1d4ed8;
  line-height: 1.25;
}

.notice-mop-tags em.success {
  border-color: #bbf7d0;
  background: #ecfdf5;
  color: #047857;
}

.notice-mop-tags em.warning {
  border-color: #fde68a;
  background: #fffbeb;
  color: #92400e;
}

.row-status {
  display: inline-flex;
  align-items: center;
  min-height: 24px;
  padding: 4px 9px;
  border-radius: 999px;
  color: #047857;
  background: #ecfdf5;
  font-size: 12px;
  font-weight: 800;
}

.row-status.closed,
.notice-row.closed .row-status {
  color: #475569;
  background: #f1f5f9;
}

@media (max-width: 960px) {
  .panel {
    height: auto;
    min-height: 0;
  }

  .filters {
    grid-template-columns: 1fr;
  }
}
</style>
