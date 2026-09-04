<template>
  <section class="signature-management">
    <header class="signature-hero">
      <div class="hero-copy">
        <span class="eyebrow"><ShieldCheck :size="15" />人员签名中心</span>
        <h2><Fingerprint :size="30" />统一签名管理</h2>
        <p>集中查看正式与临时人员签名，签名由本人通过安全链接完成。</p>
      </div>
      <div class="hero-actions">
        <a class="btn hero-ghost" href="/">返回首页</a>
        <button v-if="loggedIn" class="btn hero-ghost" :disabled="loading" @click="load(true)">
          <RefreshCw :size="16" :class="{ spin: loading }" />刷新签名
        </button>
        <button v-if="loggedIn" class="btn hero-primary" @click="openCreate">
          <Plus :size="16" />新增临时人员
        </button>
        <button v-if="loggedIn && isAdmin" class="btn hero-ghost" :disabled="duplicateBusy" @click="loadDuplicates">
          <UsersRound :size="16" />重复人员核对
        </button>
      </div>
    </header>

    <section v-if="checking" class="state-panel" role="status">
      <RefreshCw :size="22" class="spin" /><strong>正在核验登录状态</strong>
    </section>
    <section v-else-if="!loggedIn" class="state-panel">
      <ShieldCheck :size="28" />
      <div><strong>登录后管理人员签名</strong><p>登录用于核验管理权限和记录操作人。</p></div>
      <a class="btn blue" :href="loginUrl">飞书登录</a>
    </section>

    <template v-else>
      <section class="stats" aria-label="签名状态统计">
        <article><span class="stat-icon success"><CheckCircle2 :size="19" /></span><div><small>已有可用签名</small><b>{{ counts.signed || 0 }}</b></div></article>
        <article><span class="stat-icon pending"><UserRound :size="19" /></span><div><small>未签名</small><b>{{ counts.unsigned || 0 }}</b></div></article>
        <article><span class="stat-icon warning"><CircleAlert :size="19" /></span><div><small>需重新签名</small><b>{{ counts.resign || 0 }}</b></div></article>
        <article><span class="stat-icon neutral"><UsersRound :size="19" /></span><div><small>当前筛选人员</small><b>{{ count }}</b></div></article>
      </section>

      <p v-if="notice" class="notice" :class="{ error: noticeError }" role="status" aria-live="polite">
        <CircleAlert v-if="noticeError" :size="17" />
        <CheckCircle2 v-else :size="17" />
        {{ notice }}
      </p>

      <section class="directory-panel" :aria-busy="loading">
        <header class="section-head">
          <div><h3>人员目录</h3><p>支持姓名、工号、岗位、班组和楼栋组合搜索。</p></div>
          <span class="result-count">{{ resultRange }}</span>
        </header>

        <div class="filters">
          <label class="search-field">
            <span>搜索人员</span>
            <div class="search-input">
              <Search :size="17" aria-hidden="true" />
              <input v-model="query" type="search" autocomplete="off" spellcheck="false" placeholder="搜索姓名、工号、楼栋、岗位或班组" @keyup.enter="load()" />
              <button v-if="query" type="button" aria-label="清空搜索" title="清空搜索" @click="query = ''"><X :size="16" /></button>
            </div>
          </label>
          <label><span>楼栋</span><select v-model="scope"><option value="">全部楼栋</option><option v-for="b in buildings" :key="b" :value="b">{{ buildingLabel(b) }}</option></select></label>
          <label><span>人员来源</span><select v-model="source"><option value="">全部人员</option><option value="staff">正式人员</option><option value="external">临时人员</option></select></label>
          <label><span>签名状态</span><select v-model="status"><option value="">全部状态</option><option value="unsigned">未签名</option><option value="signed">已有签名</option><option value="resign">需重新签名</option></select></label>
          <button v-if="hasFilters" type="button" class="btn clear-filter" @click="clearFilters"><X :size="15" />清除筛选</button>
        </div>

        <div class="source-status">
          <span v-for="(item, key) in sources" :key="key" :class="{ error: !item.ok }">
            <i :class="{ bad: !item.ok }"></i>{{ key === 'staff' ? '正式人员表' : '临时人员表' }}
            <template v-if="item.ok">更新于 {{ dateText(item.loaded_at) }}</template>
            <template v-else>刷新失败，保留上次数据：{{ item.error }}</template>
          </span>
        </div>

        <div v-if="loading" class="loading-line" aria-hidden="true"></div>
        <div v-if="loading && !people.length" class="empty" role="status"><RefreshCw :size="22" class="spin" />正在读取两张人员表…</div>
        <div v-else-if="!people.length" class="empty"><Search :size="24" /><strong>没有找到匹配人员</strong><span>请减少关键词或清除筛选条件。</span></div>
        <div v-else class="people">
          <article v-for="person in people" :key="person.person_key" class="person-card">
            <div class="person-info">
              <span class="avatar">{{ personInitial(person) }}</span>
              <div><strong>{{ person.name }}</strong><small>{{ person.employee_no || '工号未填写' }}　{{ person.building || '楼栋未填写' }}</small></div>
            </div>
            <div class="person-tags">
              <span :class="person.source === 'staff' ? 'source-staff' : 'source-temp'">{{ person.source === 'staff' ? '正式人员' : '临时人员' }}</span>
              <span v-if="person.position || person.specialty">{{ person.position || person.specialty }}</span>
              <span v-if="person.source === 'staff'">{{ person.account_nature || '账号性质未填写' }}</span>
              <span v-if="person.source === 'staff'" :class="person.can_receive_message ? 'message-ready' : 'message-relay'">{{ person.can_receive_message ? '可直接收消息' : '需代收消息' }}</span>
              <span v-if="person.origin_staff_record_id">已关联正式人员</span>
            </div>
            <div class="signature-state">
              <div class="state-title" :class="{ good: person.has_signature, warning: person.signature_status === 'resign' }">
                <CheckCircle2 v-if="person.has_signature" :size="16" />
                <CircleAlert v-else :size="16" />{{ stateLabel(person.signature_status) }}
              </div>
              <small v-if="person.signature_reason">{{ person.signature_reason }}</small>
              <small v-if="person.effective_has_signature">默认使用{{ person.effective_source === 'staff' ? '正式表' : '临时表' }}签名</small>
              <small v-if="person.has_signature" class="protected-signature"><ShieldCheck :size="13" />签名内容仅供后端生成文件，不在网页展示</small>
            </div>
            <div v-if="person.request" class="request-state">
              <span><Link2 :size="14" />{{ requestLabel(person.request.status) }}</span>
              <small v-if="person.request.recipient_label">接收：{{ person.request.recipient_label }}</small>
              <small v-if="person.request.expires_at">有效期至 {{ dateText(person.request.expires_at) }}</small>
              <small v-if="person.request.error" class="error">{{ person.request.error }}</small>
            </div>
            <div class="row-actions">
              <button class="btn blue" @click="openSend(person)">{{ person.has_signature ? '重新签名' : '发送签名链接' }}</button>
              <button v-if="isAdmin && person.source === 'external'" class="btn" @click="openAssociate(person)">关联正式人员</button>
            </div>
          </article>
        </div>

        <footer class="pagination">
          <span>共 {{ count }} 人</span>
          <div><button class="btn" :disabled="page <= 1 || loading" @click="page--">上一页</button><span>第 {{ page }} / {{ totalPages }} 页</span><button class="btn" :disabled="page * 50 >= count || loading" @click="page++">下一页</button></div>
        </footer>
      </section>

      <section v-if="showDuplicates" class="duplicates">
        <header><div><h3>重复人员预检</h3><p>同名不代表同一人，请核对工号、楼栋及签名。</p></div><button class="btn" @click="showDuplicates = false">收起</button></header>
        <div v-if="!duplicateGroups.length" class="empty">未发现同名的临时人员记录。</div>
        <article v-for="group in duplicateGroups" :key="group.name">
          <strong>{{ group.name }}</strong>
          <label v-for="person in group.people" :key="person.record_id" class="duplicate-row">
            <input v-model="group.selected" type="checkbox" :value="person.record_id" />
            <span>{{ person.name }}　{{ person.employee_no || '无工号' }}　{{ person.building }}　{{ person.record_id }}</span>
            <span v-if="person.has_signature" class="protected-signature"><ShieldCheck :size="13" />已有签名</span>
          </label>
          <div class="duplicate-options">
            <label><span>保留人员记录</span><select v-model="group.keep"><option v-for="p in group.people" :key="p.record_id" :value="p.record_id">{{ p.record_id }}</option></select></label>
            <label><span>保留签名</span><select v-model="group.signature"><option v-for="p in group.people.filter((p: Dict) => p.has_signature)" :key="p.record_id" :value="p.record_id">{{ p.record_id }}</option></select></label>
          </div>
          <label class="check-row"><input v-model="group.confirmed" type="checkbox" />已核实勾选记录属于同一个人</label>
          <label class="check-row"><input v-model="group.upgraded" type="checkbox" />所有使用这些表的电脑均已升级</label>
          <button class="btn danger-text" :disabled="duplicateBusy || !group.confirmed || !group.upgraded || group.selected.length < 2 || !group.signature" @click="confirmMerge = group">备份并合并清理</button>
        </article>
      </section>
    </template>

    <dialog ref="dialog" @cancel.prevent="closeModal">
      <form @submit.prevent="submitModal">
        <header><div><span class="dialog-kicker">签名管理</span><h3>{{ modalTitle }}</h3></div><button type="button" class="icon-btn" aria-label="关闭" :disabled="sending" @click="closeModal"><X :size="19" /></button></header>
        <template v-if="modal === 'create'">
          <div class="form-grid">
            <label><span>姓名</span><input v-model="draft.name" required maxlength="80" autofocus /></label>
            <label><span>楼栋</span><select v-model="draft.building" required><option value="">请选择</option><option v-for="b in buildings" :key="b" :value="buildingLabel(b)">{{ buildingLabel(b) }}</option></select></label>
            <label><span>工号</span><input v-model="draft.employee_no" maxlength="80" /></label>
            <label><span>专业</span><input v-model="draft.specialty" maxlength="80" /></label>
          </div>
        </template>
        <template v-else-if="modal === 'associate'">
          <div class="selected-summary"><span class="avatar">{{ personInitial(selectedPerson || {}) }}</span><div><small>待关联临时人员</small><strong>{{ selectedPerson?.name }}　{{ selectedPerson?.employee_no || '无工号' }}</strong></div></div>
          <label><span>搜索正式人员</span><div class="search-input"><Search :size="17" /><input v-model="staffQuery" type="search" autocomplete="off" placeholder="姓名、工号、岗位或楼栋" /><button v-if="staffQuery" type="button" aria-label="清空正式人员搜索" @click="staffQuery = ''"><X :size="16" /></button></div></label>
          <div class="staff-results" aria-live="polite">
            <div v-if="staffLoading" class="staff-state"><RefreshCw :size="17" class="spin" />正在搜索正式人员…</div>
            <div v-else-if="!staffChoices.length" class="staff-state">没有匹配的正式人员，请调整关键词。</div>
            <button v-for="person in staffChoices" :key="person.record_id" type="button" :class="{ selected: staffId === person.record_id }" @click="staffId = person.record_id">
              <span class="avatar small">{{ personInitial(person) }}</span><span><strong>{{ person.name }}</strong><small>{{ person.employee_no || '无工号' }}　{{ person.building || '楼栋未填写' }}　{{ person.position || '' }}</small></span><CheckCircle2 v-if="staffId === person.record_id" :size="18" />
            </button>
          </div>
          <label class="check-row"><input v-model="samePerson" type="checkbox" required />我已核对姓名、工号和楼栋，确认是同一个人</label>
        </template>
        <template v-else>
          <div class="selected-summary"><span class="avatar">{{ personInitial(selectedPerson || {}) }}</span><div><small>签名人员</small><strong>{{ selectedPerson?.name }}　{{ selectedPerson?.building }}</strong></div></div>
          <p v-if="selectedPerson?.source === 'staff' && !selectedPerson?.can_receive_message" class="hint warning-hint">账号性质为“{{ selectedPerson?.account_nature || '未填写' }}”，不能直接发送飞书消息，请选择代收方式。签名仍保存到该正式人员记录。</p>
          <button v-if="selectedPerson?.source === 'staff' && selectedPerson?.can_receive_message" type="button" class="recipient-direct" :class="{ selected: recipient === 'direct' }" @click="recipient = 'direct'">
            <span><strong>发送给本人</strong><small>使用该人员的 VNET 账号接收</small></span><CheckCircle2 v-if="recipient === 'direct'" :size="18" />
          </button>
          <div class="recipient-section">
            <strong>值班账号代收（置顶）</strong>
            <div class="recipient-pinned" role="group" aria-label="值班账号代收">
              <button v-for="b in buildings" :key="b" type="button" :class="{ selected: recipient === `duty:${b}` }" @click="recipient = `duty:${b}`">{{ buildingLabel(b) }}</button>
            </div>
          </div>
          <label><span>其他 VNET 人员代收</span><div class="search-input"><Search :size="17" /><input v-model="relayQuery" type="search" autocomplete="off" placeholder="搜索姓名、工号、岗位或楼栋" /><button v-if="relayQuery" type="button" aria-label="清空代收人员搜索" @click="relayQuery = ''"><X :size="16" /></button></div></label>
          <div class="staff-results" aria-live="polite">
            <div v-if="relayLoading" class="staff-state"><RefreshCw :size="17" class="spin" />正在读取 VNET 人员…</div>
            <div v-else-if="!relayPeople.length" class="staff-state">没有匹配的可接收人员。</div>
            <button v-for="person in relayPeople" :key="person.record_id" type="button" :class="{ selected: recipient === `staff:${person.record_id}` }" @click="recipient = `staff:${person.record_id}`">
              <span class="avatar small">{{ personInitial(person) }}</span><span><strong>{{ person.name }}</strong><small>{{ person.employee_no || '无工号' }}　{{ person.building || '楼栋未填写' }}　{{ person.position || '' }}</small></span><CheckCircle2 v-if="recipient === `staff:${person.record_id}`" :size="18" />
            </button>
          </div>
          <p class="hint">链接24小时有效，请在局域网内交由本人完成手写签名；重新发送后旧链接失效。</p>
        </template>
        <p v-if="modalError" class="notice error" role="alert"><CircleAlert :size="17" />{{ modalError }}</p>
        <footer><button type="button" class="btn" :disabled="sending" @click="closeModal">取消</button><button class="btn blue" :disabled="modalSubmitDisabled">{{ sending ? '处理中…' : modalSubmitLabel }}</button></footer>
      </form>
    </dialog>

    <ConfirmDialog :open="Boolean(confirmMerge)" title="合并重复人员" tone="danger"
      message="将先备份并验证保留签名，再删除勾选的重复行。只有所有电脑都已升级，旧单据才能通过历史ID继续读取。"
      confirm-label="确认合并" @resolve="resolveMerge" />
  </section>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, ref, watch } from "vue";
import { CheckCircle2, CircleAlert, Fingerprint, Link2, Plus, RefreshCw, Search, ShieldCheck, UserRound, UsersRound, X } from "lucide-vue-next";
import ConfirmDialog from "./ConfirmDialog.vue";
import { requestJson, type Dict } from "../api/client";

const props = defineProps<{ checking: boolean; loggedIn: boolean; loginUrl: string; isAdmin: boolean }>();
const buildings = ["A", "B", "C", "D", "E", "H", "110"];
const people = ref<Dict[]>([]), sources = ref<Dict>({}), counts = ref<Dict>({});
const query = ref(""), source = ref(""), status = ref(""), scope = ref(""), page = ref(1), count = ref(0);
const loading = ref(false), notice = ref(""), noticeError = ref(false);
const sending = ref(false), modal = ref(""), modalError = ref(""), dialog = ref<HTMLDialogElement | null>(null);
const selectedPerson = ref<Dict | null>(null), operationId = ref("");
const recipient = ref("");
const relayQuery = ref(""), relayDirectory = ref<Dict[]>([]), relayLoading = ref(false);
const draft = ref({ name: "", building: "", employee_no: "", specialty: "" });
const staffQuery = ref(""), staffDirectory = ref<Dict[]>([]), staffId = ref(""), samePerson = ref(false), staffLoading = ref(false);
const duplicateBusy = ref(false), showDuplicates = ref(false), duplicateGroups = ref<Dict[]>([]), confirmMerge = ref<Dict | null>(null);
let seq = 0, searchTimer: ReturnType<typeof setTimeout> | undefined, pollTimer: ReturnType<typeof setTimeout> | undefined;
let disposed = false, staffSeq = 0, relaySeq = 0;

const uuid = () => crypto.randomUUID?.() || Array.from(crypto.getRandomValues(new Uint32Array(4)), n => n.toString(16)).join("");
const post = (op: string, payload: Dict) => requestJson("/api/signatures/management/" + op, { method: "POST", body: JSON.stringify(payload) });
const hasPending = computed(() => people.value.some(p => ["sent", "saving", "sending"].includes(p.request?.status)));
const hasFilters = computed(() => Boolean(query.value || source.value || status.value || scope.value));
const totalPages = computed(() => Math.max(1, Math.ceil(count.value / 50)));
const resultRange = computed(() => !count.value ? "0 人" : `${(page.value - 1) * 50 + 1}–${Math.min(page.value * 50, count.value)} / ${count.value} 人`);
const modalTitle = computed(() => modal.value === "create" ? "新增临时人员" : modal.value === "associate" ? "关联正式人员" : "发送签名链接");
const modalSubmitLabel = computed(() => modal.value === "create" ? "新增并继续" : modal.value === "associate" ? "确认关联" : "发送链接");
const modalSubmitDisabled = computed(() => sending.value
  || (modal.value === "associate" && (!staffId.value || !samePerson.value))
  || (modal.value === "send" && !recipient.value));
const filterDirectory = (rows: Dict[], value: string) => {
  const terms = value.trim().toLocaleLowerCase().split(/\s+/).filter(Boolean);
  if (!terms.length) return rows;
  return rows.filter(person => {
    const text = [person.name, person.employee_no, person.building, person.position, person.team, person.specialty]
      .map(value => String(value || "").toLocaleLowerCase()).join(" ");
    return terms.every(term => text.includes(term));
  });
};
const staffChoices = computed(() => filterDirectory(staffDirectory.value, staffQuery.value));
const relayPeople = computed(() => filterDirectory(relayDirectory.value, relayQuery.value));

function buildingLabel(value: string) { return value === "110" ? "110站" : `${value}楼`; }
function dateText(value: number) { return value ? new Date(value * 1000).toLocaleString("zh-CN") : "尚未加载"; }
function stateLabel(value: string) { return ({ signed: "已有可用签名", unsigned: "未签名", resign: "需重新签名" } as Dict)[value] || value; }
function requestLabel(value: string) { return ({ sent: "待本人签名", sending: "消息发送中", saving: "保存待核验", completed: "签名已保存", send_failed: "发送失败", send_unknown: "发送结果待核验", revoked: "链接已撤销", expired: "链接已过期" } as Dict)[value] || value; }
function personInitial(person: Dict) { return String(person.name || person.employee_no || "?").trim().slice(0, 1).toUpperCase() || "?"; }
function clearFilters() { query.value = ""; source.value = ""; status.value = ""; scope.value = ""; }

async function load(refresh = false) {
  if (!props.loggedIn || disposed) return;
  const n = ++seq;
  loading.value = true;
  try {
    const params = new URLSearchParams({ q: query.value, source: source.value, status: status.value, scope: scope.value, page: String(page.value) });
    if (refresh) params.set("refresh", "1");
    const data = await requestJson("/api/signatures/management/people?" + params, { cache: "no-store" });
    if (n !== seq || disposed) return;
    people.value = data.people || []; sources.value = data.sources || {}; counts.value = data.counts || {}; count.value = data.count || 0;
    if (refresh) { noticeError.value = Object.values(sources.value).some((s: any) => !s.ok); notice.value = noticeError.value ? "部分人员表刷新失败，当前保留最近一次成功数据。" : "两张人员表的签名状态已刷新。"; }
  } catch (error: any) { if (n === seq) { noticeError.value = true; notice.value = error.message || "读取失败，已保留原列表。"; } }
  finally {
    if (n === seq) loading.value = false;
    if (pollTimer) clearTimeout(pollTimer);
    if (!disposed && hasPending.value) pollTimer = setTimeout(() => { if (!document.hidden && !modal.value) void load(); }, 6000);
  }
}

async function showModal(kind: string) { modal.value = kind; modalError.value = ""; operationId.value = uuid(); await nextTick(); dialog.value?.showModal(); }
function closeModal() { if (!sending.value) { dialog.value?.close(); modal.value = ""; } }
function openCreate() { draft.value = { name: "", building: "", employee_no: "", specialty: "" }; void showModal("create"); }
function openSend(person: Dict) {
  selectedPerson.value = person;
  recipient.value = person.source === "staff" && person.can_receive_message ? "direct" : "";
  relayQuery.value = ""; relayDirectory.value = [];
  void showModal("send");
  void searchRelayPeople();
}
function openAssociate(person: Dict) { selectedPerson.value = person; staffId.value = ""; samePerson.value = false; staffQuery.value = person.name; staffDirectory.value = []; void showModal("associate"); void searchStaff(); }

async function searchStaff() {
  const n = ++staffSeq;
  staffLoading.value = true;
  modalError.value = "";
  try {
    const data = await requestJson("/api/signatures/management/people?" + new URLSearchParams({ source: "staff", page: "1", page_size: "500" }));
    if (n === staffSeq && !disposed) staffDirectory.value = data.people || [];
  } catch (error: any) { if (n === staffSeq) modalError.value = error.message || "正式人员搜索失败。"; }
  finally { if (n === staffSeq) staffLoading.value = false; }
}

async function searchRelayPeople() {
  const n = ++relaySeq;
  relayLoading.value = true;
  modalError.value = "";
  try {
    const data = await requestJson("/api/signatures/management/people?" + new URLSearchParams({
      source: "staff", message_capable: "1", page: "1", page_size: "500",
    }));
    if (n === relaySeq && !disposed) relayDirectory.value = data.people || [];
  } catch (error: any) { if (n === relaySeq) modalError.value = error.message || "VNET代收人员搜索失败。"; }
  finally { if (n === relaySeq) relayLoading.value = false; }
}

async function submitModal() {
  if (modalSubmitDisabled.value) return;
  sending.value = true; modalError.value = "";
  try {
    if (modal.value === "create") {
      selectedPerson.value = await post("temporary", { ...draft.value, operation_id: operationId.value });
      recipient.value = ""; relayQuery.value = ""; relayDirectory.value = []; void searchRelayPeople();
      modal.value = "send"; operationId.value = uuid(); void load(true); return;
    }
    if (modal.value === "associate") {
      await post("associate", { record_id: selectedPerson.value?.record_id, staff_record_id: staffId.value,
        expected_version: selectedPerson.value?.identity_version, confirmed_same_person: samePerson.value });
      notice.value = "人员关联已保存，目录正在后台刷新。";
    } else {
      const relayRecipient = recipient.value === "direct" ? "" : recipient.value;
      const result = await post("requests", { source: selectedPerson.value?.source, record_id: selectedPerson.value?.record_id, recipient: relayRecipient, operation_id: operationId.value });
      if (result.status !== "sent") {
        modalError.value = result.error || requestLabel(result.status);
        if (result.failure_kind === "bot_unavailable") modalError.value += " 请选择VNET人员或值班账号代收。";
        operationId.value = uuid();
        return;
      }
      notice.value = "签名链接已发送至" + result.recipient_label + "，等待本人签名。";
    }
    noticeError.value = false; dialog.value?.close(); modal.value = ""; void load();
  } catch (error: any) { modalError.value = error.message || "操作失败，请重试。"; }
  finally { sending.value = false; }
}

async function loadDuplicates() {
  duplicateBusy.value = true;
  try {
    const data = await requestJson("/api/signatures/management/duplicates");
    duplicateGroups.value = (data.groups || []).map((g: Dict) => ({ ...g, selected: g.people.map((p: Dict) => p.record_id), keep: g.people[0].record_id,
      signature: g.people.find((p: Dict) => p.has_signature)?.record_id || "", confirmed: false, upgraded: false, operation_id: uuid() }));
    showDuplicates.value = true;
  } catch (error: any) { noticeError.value = true; notice.value = error.message || "重复人员读取失败。"; }
  finally { duplicateBusy.value = false; }
}

async function resolveMerge(confirmed: boolean) {
  const group = confirmMerge.value; confirmMerge.value = null;
  if (!confirmed || !group) return;
  duplicateBusy.value = true;
  try {
    const result = await post("merge", { operation_id: group.operation_id, record_ids: group.selected, keep_record_id: group.keep,
      signature_record_id: group.signature, expected_version: group.version, confirmed_same_person: group.confirmed, all_clients_upgraded: group.upgraded });
    noticeError.value = false; notice.value = "合并完成，已清理 " + result.deleted_ids.length + " 条重复记录。";
    await load(true); await loadDuplicates();
  } catch (error: any) { noticeError.value = true; notice.value = error.message || "合并未完成，可重试。"; }
  finally { duplicateBusy.value = false; }
}

watch([query, source, status, scope], () => { page.value = 1; if (searchTimer) clearTimeout(searchTimer); searchTimer = setTimeout(() => void load(), 250); });
watch(page, () => void load());
watch(recipient, () => { operationId.value = uuid(); });
watch(() => props.loggedIn, value => { if (value) void load(); }, { immediate: true });
onBeforeUnmount(() => { disposed = true; ++seq; ++staffSeq; ++relaySeq; if (searchTimer) clearTimeout(searchTimer); if (pollTimer) clearTimeout(pollTimer); });
</script>

<style scoped>
.signature-management{width:min(1680px,100%);margin:auto;padding:26px 32px 48px;color:#0f2748}.signature-hero{position:relative;overflow:hidden;display:flex;align-items:center;justify-content:space-between;gap:24px;min-height:154px;padding:28px 32px;border:1px solid rgba(255,255,255,.22);border-radius:24px;background:linear-gradient(115deg,#064fc5 0%,#003b9f 55%,#012a7d 100%);box-shadow:0 20px 48px rgba(0,47,135,.22);color:#fff}.signature-hero::after{content:"";position:absolute;right:-40px;bottom:-86px;width:360px;height:220px;border:1px solid rgba(255,255,255,.2);border-radius:50%;box-shadow:0 0 0 34px rgba(255,255,255,.05),0 0 0 72px rgba(255,255,255,.035)}.hero-copy,.hero-actions{position:relative;z-index:1}.eyebrow{display:inline-flex;align-items:center;gap:7px;margin-bottom:10px;color:#cfe4ff;font-size:12px;font-weight:850}.signature-hero h2{display:flex;align-items:center;gap:12px;margin:0;font-size:29px;line-height:1.2}.signature-hero p{margin:10px 0 0;color:rgba(255,255,255,.76);font-size:13px}.hero-actions,.pagination,.pagination>div,.duplicates header,dialog header,dialog footer{display:flex;align-items:center;justify-content:flex-end;gap:10px;flex-wrap:wrap}.btn,.icon-btn{display:inline-flex;align-items:center;justify-content:center;gap:7px;min-height:42px;border:1px solid #d8e5f7;border-radius:14px;padding:8px 14px;background:#fff;color:#174f9b;font:inherit;font-size:13px;font-weight:850;cursor:pointer;text-decoration:none;transition:border-color .18s,background .18s,box-shadow .18s}.btn:hover:not(:disabled),.icon-btn:hover:not(:disabled){border-color:#91bfff;background:#f4f9ff;box-shadow:0 8px 20px rgba(30,99,255,.1)}.btn:disabled,.icon-btn:disabled{opacity:.5;cursor:not-allowed}.btn.blue,.hero-primary{border-color:transparent;background:linear-gradient(135deg,#1e63ff,#1554df);color:#fff;box-shadow:0 10px 22px rgba(30,99,255,.24)}.hero-ghost{border-color:rgba(255,255,255,.34);background:rgba(255,255,255,.12);color:#fff}.hero-ghost:hover:not(:disabled){border-color:rgba(255,255,255,.62);background:rgba(255,255,255,.2)}.danger-text,.error{color:#b42332}.btn:focus-visible,.icon-btn:focus-visible,input:focus-visible,select:focus-visible,.staff-results button:focus-visible{outline:3px solid #8db8ff;outline-offset:2px}.stats{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px;margin:20px 0}.stats article{display:flex;align-items:center;gap:13px;min-height:88px;border:1px solid #d8e5f7;border-radius:18px;background:rgba(255,255,255,.88);padding:16px 18px;box-shadow:0 10px 26px rgba(0,47,135,.07)}.stats article div{display:grid;gap:3px}.stats small{color:#60748e;font-size:12px;font-weight:750}.stats b{color:#102f59;font-size:25px}.stat-icon{display:grid;width:42px;height:42px;place-items:center;border-radius:13px}.stat-icon.success{background:#e8fbf2;color:#087a55}.stat-icon.pending{background:#eef5ff;color:#145fd1}.stat-icon.warning{background:#fff5df;color:#a45e00}.stat-icon.neutral{background:#edf1f7;color:#526981}.notice{display:flex;align-items:flex-start;gap:8px;margin:0 0 14px;border:1px solid #cde0ff;border-radius:14px;background:#edf6ff;padding:11px 14px;color:#17549e;font-size:13px}.notice.error{border-color:#ffd0d5;background:#fff1f2;color:#b42332}.directory-panel,.duplicates{position:relative;border:1px solid #d8e5f7;border-radius:22px;background:rgba(255,255,255,.9);box-shadow:0 14px 34px rgba(0,47,135,.08)}.directory-panel{overflow:hidden}.section-head{display:flex;align-items:flex-start;justify-content:space-between;gap:18px;padding:20px 22px 14px}.section-head h3,.duplicates h3,dialog h3{margin:0;color:#102f59;font-size:18px}.section-head p,.duplicates header p{margin:5px 0 0;color:#61748e;font-size:12px}.result-count{border:1px solid #cfe0ff;border-radius:999px;background:#eff6ff;padding:6px 10px;color:#1559bd;font-size:12px;font-weight:850;white-space:nowrap}.filters{display:grid;grid-template-columns:minmax(280px,1.7fr) repeat(3,minmax(130px,.7fr)) auto;align-items:end;gap:10px;padding:0 22px 16px}.filters label,.form-grid label,dialog form>label,.duplicate-options label{display:grid;gap:6px;color:#425975;font-size:12px;font-weight:750}.filters label>span,.form-grid label>span,dialog form>label>span,.duplicate-options label>span{padding-left:2px}.search-input{display:grid;grid-template-columns:auto minmax(0,1fr) auto;align-items:center;min-height:44px;border:1px solid #cbd8eb;border-radius:14px;background:#fff;padding:0 10px;color:#73869e}.search-input:focus-within{border-color:#4d91ff;box-shadow:0 0 0 3px rgba(30,99,255,.12)}input,select{width:100%;min-width:0;min-height:44px;border:1px solid #cbd8eb;border-radius:14px;background:#fff;padding:9px 11px;color:#102f59;font:inherit}input[type=checkbox]{width:18px;height:18px;min-height:0;accent-color:#1e63ff}.search-input input{min-height:40px;border:0;padding:7px 8px;outline:0;box-shadow:none}.search-input button,.icon-btn{width:38px;min-height:38px;border:0;border-radius:10px;background:transparent;padding:0;color:#60748e}.clear-filter{white-space:nowrap}.source-status{display:flex;gap:14px;flex-wrap:wrap;border-top:1px solid #edf2f8;border-bottom:1px solid #edf2f8;background:#f8fbff;padding:9px 22px;color:#62738a;font-size:11px}.source-status span{display:inline-flex;align-items:center;gap:6px}.source-status i{width:7px;height:7px;border-radius:50%;background:#13a474}.source-status i.bad{background:#d34150}.loading-line{position:absolute;z-index:2;top:0;left:0;width:34%;height:3px;border-radius:999px;background:#1e63ff;animation:loading 1.1s ease-in-out infinite}.people{display:grid;gap:9px;padding:14px}.person-card{display:grid;grid-template-columns:minmax(210px,1.25fr) minmax(150px,.9fr) minmax(190px,1fr) minmax(160px,.8fr) auto;align-items:center;gap:15px;border:1px solid #e0e9f5;border-radius:17px;background:#fff;padding:14px 15px;transition:border-color .18s,box-shadow .18s}.person-card:hover{border-color:#b7d1f5;box-shadow:0 9px 24px rgba(15,86,228,.07)}.person-info,.selected-summary{display:flex;align-items:center;gap:11px;min-width:0}.person-info>div,.selected-summary>div{display:grid;gap:4px;min-width:0}.person-info strong,.selected-summary strong{overflow:hidden;color:#102f59;font-size:15px;text-overflow:ellipsis;white-space:nowrap}.person-info small,.selected-summary small,.signature-state small,.request-state small{color:#61748e;font-size:11px;overflow-wrap:anywhere}.avatar{display:grid;width:42px;height:42px;flex:0 0 auto;place-items:center;border-radius:13px;background:linear-gradient(135deg,#1e63ff,#00a7ce);color:#fff;font-weight:900}.avatar.small{width:34px;height:34px;border-radius:10px}.person-tags{display:flex;gap:5px;flex-wrap:wrap}.person-tags span{border:1px solid #dce7f4;border-radius:999px;background:#f7faff;padding:4px 7px;color:#526981;font-size:10px;font-weight:750}.person-tags .source-staff{border-color:#cfe0ff;background:#eff6ff;color:#1559bd}.person-tags .source-temp{border-color:#d7e1ec;background:#f1f5f9;color:#516277}.signature-state,.request-state,.row-actions{display:grid;gap:5px;min-width:0}.state-title,.request-state>span{display:flex;align-items:center;gap:5px;color:#526981;font-size:12px;font-weight:850}.state-title.good{color:#087a55}.state-title.warning{color:#a45e00}.signature-preview{display:grid;width:150px;height:55px;place-items:center;overflow:hidden;border:1px solid #e1eaf4;border-radius:10px;background:repeating-linear-gradient(45deg,#f8fafc 0 7px,#fff 7px 14px)}.signature-preview img,.duplicates img{max-width:100%;max-height:100%;object-fit:contain;-webkit-user-drag:none;user-select:none}.row-actions{justify-items:stretch}.row-actions .btn{white-space:nowrap}.empty{display:grid;min-height:170px;place-content:center;justify-items:center;gap:7px;padding:30px;color:#60748e;text-align:center}.empty strong{color:#294667}.empty span{font-size:12px}.pagination{justify-content:space-between;border-top:1px solid #edf2f8;padding:14px 18px;color:#60748e;font-size:12px}.pagination>div>span{min-width:90px;text-align:center}.duplicates{display:grid;gap:13px;margin-top:18px;padding:20px}.duplicates article{display:grid;gap:11px;border-top:1px solid #e4ecf6;padding:15px 0}.duplicate-row{display:flex;align-items:center;gap:9px;flex-wrap:wrap}.duplicate-row span{font-size:12px}.duplicate-row img{width:120px;height:48px;margin-left:auto}.duplicate-options,.form-grid{display:grid;grid-template-columns:1fr 1fr;gap:11px}.check-row{display:flex!important;align-items:center;gap:8px;color:#425975;font-size:12px}.state-panel{display:flex;align-items:center;justify-content:center;gap:13px;min-height:220px;margin-top:20px;border:1px solid #d8e5f7;border-radius:22px;background:#fff;color:#294667}.state-panel>div{display:grid;gap:4px}.state-panel p{margin:0;color:#61748e;font-size:12px}dialog{width:min(590px,calc(100vw - 28px));max-height:90dvh;overflow:auto;border:1px solid #d8e5f7;border-radius:22px;padding:0;color:#172c49;box-shadow:0 28px 80px rgba(1,42,125,.3)}dialog::backdrop{background:rgba(5,27,62,.58);backdrop-filter:blur(3px)}dialog form{display:grid;gap:15px;padding:22px}dialog header{justify-content:space-between;border-bottom:1px solid #e7eef8;padding-bottom:13px}.dialog-kicker{display:block;margin-bottom:3px;color:#1e63ff;font-size:10px;font-weight:900}.selected-summary{border:1px solid #d8e5f7;border-radius:15px;background:#f7faff;padding:11px}.hint{margin:0;border-radius:12px;background:#f3f7fc;padding:10px 12px;color:#60748e;font-size:12px;line-height:1.55}.staff-results{display:grid;max-height:290px;overflow:auto;gap:6px;border:1px solid #dce7f4;border-radius:15px;background:#f8fbff;padding:7px}.staff-results>button{display:grid;grid-template-columns:auto minmax(0,1fr) auto;align-items:center;gap:9px;border:1px solid transparent;border-radius:12px;background:#fff;padding:8px;color:#294667;text-align:left;cursor:pointer}.staff-results>button:hover,.staff-results>button.selected{border-color:#77adff;background:#eff6ff}.staff-results>button>span:not(.avatar){display:grid;gap:3px;min-width:0}.staff-results strong,.staff-results small{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.staff-results small{color:#60748e;font-size:11px}.staff-state{display:flex;align-items:center;justify-content:center;gap:7px;min-height:76px;color:#60748e;font-size:12px}dialog footer{position:sticky;bottom:-22px;margin:2px -22px -22px;padding:14px 22px;border-top:1px solid #e2ebf6;background:rgba(255,255,255,.96)}.spin{animation:spin .9s linear infinite}@keyframes spin{to{transform:rotate(360deg)}}@keyframes loading{0%{transform:translateX(-110%)}50%{transform:translateX(190%)}100%{transform:translateX(410%)}}
.person-tags .message-ready{border-color:#bcebd7;background:#eafaf2;color:#087a55}.person-tags .message-relay{border-color:#f4d49b;background:#fff7e8;color:#9a5a08}.warning-hint{border:1px solid #f4d49b!important;background:#fff7e8!important;color:#8a520b!important}.recipient-direct{display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:center;gap:10px;min-height:48px;border:1px solid #d8e5f7;border-radius:13px;background:#fff;padding:9px 12px;color:#294667;text-align:left;cursor:pointer}.recipient-direct span{display:grid;gap:2px}.recipient-direct small{color:#60748e}.recipient-direct:hover,.recipient-direct.selected{border-color:#6ba5ff;background:#edf5ff;color:#1459bb}.recipient-section{display:grid;gap:8px}.recipient-section>strong{color:#425975;font-size:12px}.recipient-pinned{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:7px}.recipient-pinned button{min-height:40px;border:1px solid #d8e5f7;border-radius:11px;background:#fff;color:#49617d;font:inherit;font-size:12px;font-weight:850;cursor:pointer}.recipient-pinned button:hover,.recipient-pinned button.selected{border-color:#6ba5ff;background:#edf5ff;color:#1459bb;box-shadow:0 0 0 2px rgba(30,99,255,.08)}
.protected-signature{display:inline-flex;align-items:center;gap:5px;color:#087a55!important}
@media(max-width:1200px){.stats{grid-template-columns:repeat(2,1fr)}.filters{grid-template-columns:minmax(240px,1fr) repeat(3,minmax(120px,.6fr))}.clear-filter{grid-column:1/-1;justify-self:start}.person-card{grid-template-columns:1.2fr .8fr 1fr auto}.request-state{grid-column:2/4}.row-actions{grid-column:4;grid-row:1/3}}
@media(max-width:760px){.signature-management{padding:14px}.signature-hero{display:grid;padding:22px;border-radius:20px}.signature-hero h2{font-size:24px}.hero-actions{justify-content:flex-start}.hero-actions .btn{flex:1 1 140px}.stats{gap:8px;margin:13px 0}.stats article{min-height:76px;padding:12px}.filters{grid-template-columns:1fr 1fr;padding:0 14px 14px}.search-field{grid-column:1/-1}.section-head{padding:16px 15px 12px}.source-status{padding:9px 14px}.people{padding:9px}.person-card{grid-template-columns:1fr 1fr;padding:13px}.person-info,.person-tags,.signature-state,.request-state,.row-actions{grid-column:1/-1}.row-actions{grid-row:auto;display:flex}.row-actions .btn{flex:1}.pagination{align-items:flex-start}.duplicate-options,.form-grid{grid-template-columns:1fr}dialog form{padding:18px}dialog footer{bottom:-18px;margin:2px -18px -18px;padding:13px 18px}}
@media(max-width:440px){.stats{grid-template-columns:1fr 1fr}.stats b{font-size:21px}.stat-icon{width:36px;height:36px}.filters{grid-template-columns:1fr}.search-field{grid-column:auto}.pagination{display:grid;justify-items:center}.pagination>div{width:100%;justify-content:space-between}.signature-preview{width:100%}.recipient-pinned{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media(prefers-reduced-motion:reduce){.btn,.person-card{transition:none}.spin,.loading-line{animation-duration:1.8s}}
</style>
