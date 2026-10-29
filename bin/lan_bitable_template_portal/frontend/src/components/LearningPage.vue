<template>
  <section class="learning-page" :aria-busy="loading || busy">
    <header class="page-head">
      <div class="heading"><VnetBackButton :to="undefined" :disabled="busy" @click="handleBack" /><BookOpen :size="22" /><h1>画像学练</h1><span v-if="isAdmin" class="badge">管理员</span></div>
      <div class="actions">
        <strong v-if="scopeLabel && tab !== 'sharing'">{{ scopeLabel }}</strong>
        <span v-if="ready && canViewBuildings" class="view-switch" role="group" aria-label="答题/画像视图">
          <button v-if="boot.can_answer" :class="{ active: viewMode === 'practice' }" :aria-pressed="viewMode === 'practice'" :disabled="busy" @click="goPractice"><BookOpen :size="14" />个人学习</button>
          <button v-if="canViewBuildings" :class="{ active: viewMode === 'overview' }" :aria-pressed="viewMode === 'overview'" :disabled="busy" @click="goOverview"><ChartNoAxesCombined :size="14" />学习管理</button>
        </span>
        <span v-if="ready && isAdmin" class="sync-label" :class="{ danger: !!boot.sync?.error, pending: !!boot.sync?.pending }"><Loader2 v-if="syncActive" :size="15" class="spin" />{{ syncLabel }}<template v-if="boot.sync?.pending"> · {{ boot.sync.pending }} 项待同步</template></span>
        <button v-if="tab !== 'home'" class="icon-button" title="刷新当前页面" aria-label="刷新当前页面" :disabled="busy || loading" @click="refreshView"><RefreshCw :size="17" :class="{ spin: loading }" /></button>
      </div>
    </header>

    <div v-if="ready && tab !== 'sharing' && viewMode === 'overview' && !learner && canViewBuildings" class="building-tabs" :class="{ 'with-all': isAdmin }" aria-label="学练楼栋"><button v-for="building in overviewScopes" :key="building.value" :class="{ active: scope === building.value }" :disabled="busy" @click="changeScope(building.label)">{{ building.label }}<span>{{ building.value ? '人员学练' : '全员' }}</span></button></div>
    <div v-if="ready && tab !== 'sharing' && viewMode === 'practice' && !learner && identityIssue" class="alert error" role="alert"><AlertCircle :size="18" /><span>{{ identityIssue }}</span></div>
    <div v-if="ready && tab !== 'sharing' && tab !== 'home'" class="learner-strip"><div><strong>{{ learner ? learner.name : scopeLabel + '学习汇总' }}</strong><span v-if="learner">工号 {{ learner.employee_no || '未填写' }}<template v-if="viewMode === 'practice'"> · 当前答题人</template></span></div><div class="actions"><button v-if="viewMode === 'overview' && canViewBuildings" :disabled="busy" @click="openPeople"><Search :size="16" />{{ learner ? '切换人员' : '选择人员' }}</button></div></div>
    <nav v-if="ready && !isLearningHub" class="tabs" aria-label="学练页面">
      <button v-for="item in tabs" :key="item.id" :class="{ active: tab === item.id }" :aria-current="tab === item.id ? 'page' : undefined" :disabled="busy" @click="switchTab(item.id as Tab)"><component :is="item.icon" :size="17" />{{ item.label }}</button>
    </nav>
    <div v-if="error && !modalKind" class="alert error" role="alert"><AlertCircle :size="18" /><span>{{ error }}</span><button class="icon-button" aria-label="关闭错误提示" @click="error = ''"><X :size="16" /></button></div>
    <div v-if="notice" class="alert success" role="status"><CheckCircle2 :size="18" /><span>{{ notice }}</span></div>
    <div v-if="storageWarning" class="alert warning" role="status"><AlertCircle :size="18" />{{ storageWarning }}</div>
    <div v-if="boot.sync?.error && isAdmin" class="alert warning"><AlertCircle :size="18" /><span>{{ boot.sync.error }}</span><button v-if="isAdmin" :disabled="busy || jobRunning" @click="switchTab('settings')"><Settings :size="15" />前往设置</button></div>
    <div v-if="ready && !boot.settings?.enabled && isAdmin" class="alert warning"><AlertCircle :size="18" /><span>自动发布未启用。{{ isAdmin ? '仍可手动发布今日题单，且不发送通知。' : '请等待管理员发布题单。' }}</span><button v-if="isAdmin" @click="switchTab('settings')"><Settings :size="15" />发布设置</button></div>
    <div v-if="!ready" class="empty"><Loader2 v-if="loading" :size="24" class="spin" /><span>{{ loading ? '正在加载学练数据…' : '学练数据加载失败' }}</span><button v-if="!loading" @click="bootstrap()"><RefreshCw :size="16" />重试</button></div>
    <div v-else-if="tab !== 'sharing' && viewMode === 'practice' && !learner && identityIssue" class="empty"><AlertCircle :size="30" /><strong>尚未关联个人身份</strong><span>{{ identityIssue || '无法进行个人答题，请与管理员核对人员目录。' }}</span></div>

    <template v-else>
      <LearningHome v-if="tab === 'home'" :data="profile" :person="learner" :date="date" :loading="loading || busy" :failed="homeProgressError || !profile.today_summary" @open="openLearning" @refresh="refreshView" />
      <LearningSharing v-else-if="tab === 'sharing'" :is-admin="isAdmin" :user-id="userId" @dirty="sharingDirty = $event" @busy="busy = $event" />
      <LearningCompletion v-else-if="tab === 'completion'" :scope="viewMode === 'overview' ? scope : ''" :person-id="learner?.id" :can-view-buildings="canViewBuildings" :refresh-key="completionRefresh" @open-paper="openCompletionPaper" />
      <template v-else-if="tab === 'today' || (reading && ['review', 'history'].includes(tab))">
        <div class="toolbar">
          <div class="actions">
            <button v-if="completionReturn && reading" :disabled="busy" @click="returnToCompletion"><ChevronLeft :size="16" />返回每日完成</button>
            <button v-if="reading && tab !== 'today'" :disabled="busy" @click="saveDraft(); reading = false; loadView()"><ChevronLeft :size="16" />返回{{ tab === 'review' ? '复习列表' : '学习历史' }}</button>
            <span v-if="tab === 'today'" class="muted">{{ studyMode === 'practice' ? '自主练习' : '每日题单' }} · {{ date }}</span>
            <span v-else>{{ paper?.date }} · {{ paper?.scope }}楼</span>
            <span v-if="paper" class="muted">本题单已答 {{ answered }} / {{ validCount }} 题</span>
            <span v-if="paper?.status" class="badge">{{ paper.status === 'completed' ? '已完成' : paper.status === 'published' ? '已发布' : paper.status === 'pending' ? '待完成' : paper.status }}</span>
          </div>
          <div v-if="tab === 'today' && viewMode === 'practice' && boot.can_answer" class="actions">
            <div class="view-switch" role="group" aria-label="学练方式"><button :class="{ active: studyMode === 'daily' }" :aria-pressed="studyMode === 'daily'" :disabled="busy || loading" @click="switchStudyMode('daily')">每日题单</button><button :class="{ active: studyMode === 'practice' }" :aria-pressed="studyMode === 'practice'" :disabled="busy || loading" @click="switchStudyMode('practice')">自主练习</button></div>
            <button v-if="studyMode === 'practice' || !paper?.questions?.length" class="primary" :disabled="busy || loading" @click="studyMode === 'daily' ? continueLearning() : startSelfPractice()"><Loader2 v-if="busy || loading" :size="16" class="spin" /><BookOpen v-else-if="studyMode === 'daily'" :size="16" /><Shuffle v-else :size="16" />{{ studyMode === 'daily' ? '打开每日题单' : '开始自主练习' }}</button>
          </div>
        </div>
        <div v-if="tab === 'today'" class="actions" role="note"><span class="muted">此处显示本题单进度；每日完成统计包含当班自主练习的有效首答，复习不重复计数。</span><button :disabled="busy || loading" @click="switchTab('completion')">查看每日完成</button></div>
        <div v-if="paper?.legacy" class="muted read-only"><Eye :size="16" />旧楼栋题单 · 只读历史，不计入个人画像</div>
        <div v-if="shortageText(paper?.shortage)" class="alert warning"><AlertCircle :size="18" />{{ shortageText(paper?.shortage) }}</div>
        <div v-if="loading" class="loading-line" role="status"><Loader2 :size="16" class="spin" />正在读取题单…</div>
        <div v-else-if="!paper || !paper.questions?.length" class="empty"><BookOpen :size="30" /><strong>{{ studyMode === 'practice' ? '尚未开始自主练习' : '今日暂无可学习题目' }}</strong><span>{{ syncActive ? '题库正在同步，请稍后刷新。' : studyMode === 'practice' ? '可开始新的自主练习，每轮随机生成15题。' : '每日题单未发布或有效题目不足，可切换到自主练习。' }}</span></div>
        <div v-else-if="current" class="practice-layout">
          <aside class="question-rail" aria-label="题号导航">
            <h2>题目 <span>{{ paper.questions.length }}</span></h2>
            <div class="question-numbers"><button v-for="(q, n) in paper.questions" :key="q.id || q.question_id" :class="{ selected: index === n, done: q.attempt, wrong: q.attempt?.correct === false, invalid: q.invalid }" :aria-label="`第 ${Number(n) + 1} 题，${resultText(q)}`" :aria-current="index === n ? 'step' : undefined" :title="resultText(q)" :disabled="busy" @click="selectQuestion(Number(n))">{{ Number(n) + 1 }}<Star v-if="q.favorite" :size="9" class="number-star" /></button></div>
            <progress :value="answered" :max="validCount || 1" aria-label="题单完成进度"></progress>
            <div class="rail-progress"><strong>{{ answered }} / {{ validCount }}</strong><span>已完成</span></div>
            <div class="rail-legend"><span><CheckCircle2 :size="13" />已答</span><span><AlertCircle :size="13" />错题</span></div>
          </aside>
          <article :key="questionId(current)" class="question-body">
            <div class="question-meta"><div class="actions"><span class="badge">{{ current.type_label || typeLabels[current.type] }}</span><span>{{ bankLabels[current.bank] }}</span><span v-if="current.topic">{{ current.topic }}</span></div><button v-if="canAnswer" class="icon-button" :class="{ starred: current.favorite }" :aria-label="current.favorite ? '取消收藏' : '收藏题目'" :title="current.favorite ? '取消收藏' : '收藏题目'" :disabled="busy" @click="saveNotes({ favorite: !current.favorite })"><Star :size="18" :fill="current.favorite ? 'currentColor' : 'none'" /></button></div>
            <h2 class="stem"><span class="muted">{{ index + 1 }}.</span> {{ current.stem }}</h2>
            <div v-if="current.invalid" class="alert warning"><AlertCircle :size="17" />{{ current.invalid_reason || '此题已失效，已从评价分母剔除，原作答保留。' }}</div>
            <div v-if="current.correction || current.correction_reason" class="alert warning"><RefreshCw :size="17" />{{ current.correction_reason || current.correction }}</div>
            <div v-if="current.needs_review" class="alert warning"><AlertCircle :size="17" />参考内容已更正，请重新核对并练习自评。</div>
            <div v-if="draftRestored && !current.attempt" class="muted draft-status">已恢复本机草稿，尚未正式提交。</div>
            <div v-if="startWarning" class="alert warning" role="status">{{ startWarning }}<button type="button" @click="markStarted">重试同步</button></div>
            <div v-if="current.attachments?.length" class="attachments"><button v-for="a in current.attachments.filter((a: Dict) => a.kind !== 'answer')" :key="a.id" @click="showAttachment(a)"><Eye :size="15" />{{ a.name }}<small>{{ attachmentLabels[a.kind] }}</small></button></div>
            <fieldset v-if="current.type !== 'interview'" class="options" :disabled="locked || busy"><legend class="sr-only">选择答案</legend><label v-for="(option, n) in current.options" :key="option.id" :class="{ checked: draft.option_ids.includes(option.id), correct: current.answer?.correct_option_ids?.includes(option.id), incorrect: !practice && displayedAttempt?.wrong?.includes(option.id) }"><input :type="current.type === 'single' ? 'radio' : 'checkbox'" :name="`answer-${questionId(current)}`" :checked="draft.option_ids.includes(option.id)" :value="option.id" @change="chooseOption(option.id)" /><b>{{ String.fromCharCode(65 + Number(n)) }}</b><span>{{ option.text }}</span><span class="option-result"><template v-if="current.answer?.correct_option_ids?.includes(option.id)"><Check :size="17" aria-hidden="true" /><span class="sr-only">正确选项</span></template><template v-else-if="!practice && displayedAttempt?.wrong?.includes(option.id)"><X :size="17" aria-hidden="true" /><span class="sr-only">错选</span></template></span></label></fieldset>
            <div v-else class="interview-answer"><label>我的回答<textarea v-model="draft.answer_text" rows="6" maxlength="12000" :disabled="locked || busy" @input="draft.operation_id = uid()" /></label><label class="rating">掌握程度<VnetSelect input-id="learning-select-2" :model-value="ratingLabels[draft.self_rating] || ''" :options="Object.values(ratingLabels)" label="掌握程度" placeholder="请选择自评" :disabled="locked || busy" @update:model-value="draft.self_rating = keyFor(ratingLabels, $event); draft.operation_id = uid()" /></label></div>
            <div class="answer-actions actions"><span v-if="displayedAttempt && !practice" role="status" :class="['result', displayedAttempt.correct === false ? 'danger' : 'success-text']"><AlertCircle v-if="displayedAttempt.correct === false" :size="18" /><CheckCircle2 v-else :size="18" />{{ current.practice?.length ? '最近复习' : '首次作答' }}：{{ resultText(current, displayedAttempt) }}<small>{{ timeLabel(displayedAttempt.submitted_at) }}</small></span><button v-if="current.attempt && canAnswer && !current.invalid && !practice" :disabled="busy" @click="startPractice"><RotateCcw :size="16" />{{ current.type === 'interview' ? '重新练习与自评' : '再次练习' }}</button><span v-if="current.hinted || current.attempt?.assisted" class="badge warning-badge">已查看提示或答案</span></div>
            <p v-if="current.practice?.length && !practice" class="first-result">首次作答：{{ resultText(current) }} · {{ timeLabel(current.attempt?.submitted_at) }}</p>
            <div class="answer-tools actions"><button v-if="canAnswer" :disabled="busy" @click="reveal('answer')"><Eye :size="16" />查看答案与解析</button><button v-if="canAnswer && current.has_hint !== false" :disabled="busy" @click="reveal('hint')"><Lightbulb :size="16" />思路提示</button><button v-if="canAnswer" :disabled="busy" @click="openIssue()"><MessageSquare :size="16" />题目有疑问</button></div>
            <p v-if="!practice && (displayedAttempt?.missed?.length || displayedAttempt?.wrong?.length)" class="answer-feedback"><span v-if="displayedAttempt?.missed?.length">漏选：{{ answerLabels(displayedAttempt.missed) }}</span><span v-if="displayedAttempt?.wrong?.length">错选：{{ answerLabels(displayedAttempt.wrong) }}</span></p>
            <details v-if="current.attempt" class="attempt-records"><summary>作答记录<template v-if="attemptRecords(current).length">· {{ attemptRecords(current).length }} 次</template></summary><ol><li v-for="(rec, n) in attemptRecords(current)" :key="n"><strong>{{ rec.label }}</strong><time>{{ timeLabel(rec.submitted_at) }}</time><span :class="rec.correct === false ? 'danger' : 'success-text'">{{ rec.resultText }}</span><span v-if="rec.selectedText" class="record-answer">{{ rec.selectedText }}</span></li></ol></details>
            <section v-if="current.answer" class="answer-reference" aria-label="参考答案">
              <h3>参考答案</h3><p v-if="current.answer.correct_option_ids?.length"><strong>{{ answerLabels(current.answer.correct_option_ids) }}</strong></p><p v-if="current.answer.answer_text" class="prewrap">{{ current.answer.answer_text }}</p>
              <template v-if="current.answer.analysis"><h3>解析</h3><p class="prewrap">{{ current.answer.analysis }}</p></template><template v-if="current.answer.hint"><h3>提示</h3><p class="prewrap">{{ current.answer.hint }}</p></template>
              <div v-if="current.answer.attachments?.length" class="attachments"><button v-for="a in current.answer.attachments" :key="a.id" @click="showAttachment(a)"><Eye :size="15" />{{ a.name }}</button></div>
              <p v-if="!current.answer.answer_text && !current.answer.correct_option_ids?.length && !current.answer.attachments?.length && !current.answer.hint && !current.answer.analysis" class="muted">暂无参考内容，可提交题目质疑。</p>
            </section>
            <details class="notes"><summary>学习笔记<span v-if="noteDirty" class="danger"> · 未保存</span><span v-else-if="noteSaved" class="muted"> · 已记录</span></summary><div class="note-editor"><label class="sr-only" for="learning-note">学习笔记</label><textarea id="learning-note" v-model="note" rows="3" maxlength="5000" :disabled="!canAnswer || busy" /><div class="actions"><button v-if="canAnswer" :disabled="busy || !noteDirty" @click="saveNotes()"><Save :size="16" />保存笔记</button></div></div></details>
            <Teleport to="body"><footer class="question-footer learning-fixed-bar" aria-label="答题操作"><button :disabled="busy || loading || index === 0" @click="selectQuestion(index - 1)"><ChevronLeft :size="16" />上一题</button><button v-if="canAnswer" class="primary" :disabled="busy || loading || locked" @click="submitAnswer"><Loader2 v-if="busy" :size="16" class="spin" /><Check v-else :size="16" />{{ current.invalid ? '题目已失效' : locked ? '已确认作答' : practice ? '提交本次复习' : '确认作答' }}</button><span v-else>{{ index + 1 }} / {{ paper.questions.length }}</span><button :disabled="busy || loading || index === paper.questions.length - 1" @click="selectQuestion(index + 1)">下一题<ChevronRight :size="16" /></button></footer></Teleport>
          </article>
        </div>
      </template>

      <template v-else-if="['review', 'history', 'issues', 'questions'].includes(tab)">
        <form class="toolbar" @submit.prevent="filterChanged">
          <div class="filters">
            <VnetSelect input-id="learning-select-3" v-if="tab === 'review'" :model-value="reviewLabels[filters.review]" :options="Object.values(reviewLabels)" label="复习类别" @update:model-value="filters.review = keyFor(reviewLabels, $event); filterChanged()" /><VnetSelect input-id="learning-select-3b" v-if="tab === 'review'" :model-value="bankLabels[filters.reviewBank] || '全部题库'" :options="['全部题库', ...Object.values(bankLabels)]" label="题库检索" @update:model-value="filters.reviewBank = keyFor(bankLabels, $event); filterChanged()" />
            <template v-if="tab === 'history'"><label v-if="viewMode === 'overview'" class="checkbox-label"><input v-model="legacyHistory" type="checkbox" @change="filterChanged" />旧楼栋只读历史</label><label class="inline-field">开始<input v-model="filters.from" aria-label="开始日期" type="date" /></label><label class="inline-field">结束<input v-model="filters.to" aria-label="结束日期" type="date" /></label></template>
            <template v-if="tab === 'questions'"><VnetSelect input-id="learning-select-4" :model-value="bankLabels[filters.bank] || '全部题库'" :options="['全部题库', ...Object.values(bankLabels)]" label="题库筛选" @update:model-value="filters.bank = keyFor(bankLabels, $event); filterChanged()" /><VnetSelect input-id="learning-select-5" :model-value="statusLabels[filters.status] || '全部状态'" :options="['全部状态', ...Object.values(statusLabels)]" label="题目状态筛选" @update:model-value="filters.status = keyFor(statusLabels, $event); filterChanged()" /><label class="checkbox-label"><input v-model="filters.problems" type="checkbox" @change="filterChanged" />仅问题题目</label></template>
            <VnetSelect input-id="learning-select-6" v-if="tab === 'issues'" :model-value="issueLabels[filters.issue] || '全部状态'" :options="['全部状态', ...Object.values(issueLabels)]" label="质疑状态筛选" @update:model-value="filters.issue = keyFor(issueLabels, $event); filterChanged()" />
            <label v-if="tab !== 'history'" class="search-field"><Search :size="16" /><input v-model="filters.search" aria-label="搜索题目或质疑" placeholder="搜索题目或质疑" type="search" /></label>
            <button type="button" :disabled="loading || busy" @click="clearFilters"><X :size="16" />清空筛选</button><button type="submit" :disabled="loading || busy"><Search :size="16" />查询</button>
          </div>
          <button v-if="isAdmin && tab === 'issues'" type="button" :disabled="busy" @click="filters.problems = true; switchTab('questions')"><AlertCircle :size="16" />题库待核对 {{ boot.question_problem_count ?? '—' }} 题</button><div v-if="tab === 'questions'" class="actions"><button type="button" :disabled="busy" @click="modalKind = 'import'; importRows = []; importName = ''; error = ''"><Upload :size="16" />导入</button><button type="button" :disabled="busy" @click="exportData('questions')"><Download :size="16" />导出</button><button type="button" class="primary" :disabled="busy" @click="editQuestion()"><Plus :size="16" />新增题目</button></div>
          <button v-if="isAdmin && tab === 'history'" type="button" :disabled="busy" @click="exportData('results')"><Download :size="16" />导出学习记录</button>
        </form>
        <div v-if="tab === 'questions' && selected.length" class="batch-bar"><span>已选 {{ selected.length }} 题</span><button :disabled="busy || loading" @click="setQuestionStatus(rows.filter(q => selected.includes(q.id)), 'published')"><CheckCircle2 :size="16" />批量启用</button><button :disabled="busy || loading" @click="setQuestionStatus(rows.filter(q => selected.includes(q.id)), 'disabled')"><X :size="16" />批量停用</button><button :disabled="busy || loading" @click="selected = []">取消选择</button></div>
        <div v-if="loading" class="loading-line" role="status"><Loader2 :size="16" class="spin" />正在读取…</div>
        <div class="table-wrap" :class="{ stale: loading }" :inert="loading || undefined">
          <table v-if="tab === 'history'"><thead><tr><th>日期 / 类型</th><th>楼栋 / 人员</th><th>完成进度</th><th>正确率</th><th>缺题</th><th>状态</th><th>操作</th></tr></thead><tbody><tr v-for="row in rows" :key="row.id"><td>{{ row.date }}<small>{{ row.mode === 'practice' ? '自主练习' : '每日题单' }} · {{ timeLabel(row.created_at) }}</small></td><td>{{ row.scope }}楼<small>{{ row.person?.name || '旧楼栋题单' }}</small></td><td>{{ row.stats?.answered ?? row.answered ?? 0 }} / {{ row.stats?.total ?? row.questions?.length ?? row.assigned ?? '—' }}</td><td>{{ percent(row.stats?.accuracy) }}</td><td>{{ shortageText(row.shortage) || '无' }}</td><td>{{ row.status === 'completed' ? '已完成' : '待完成' }}</td><td><div class="row-actions"><button :disabled="loading || busy" @click="loadPaper(row.id)"><Eye :size="15" />查看题单</button><button v-if="isAdmin" class="icon-button danger" :disabled="loading || busy" :aria-label="`删除${row.scope}楼${row.date}题单`" title="删除题单" @click="deletePaper(row.id)"><Trash2 :size="16" /></button></div></td></tr></tbody></table>
          <table v-else-if="tab === 'review'"><thead><tr><th>题目</th><th>题型 / 知识点</th><th>{{ filters.review === 'mistakes' ? '最近答错' : '题单日期' }}</th><th>结果</th><th>操作</th></tr></thead><tbody><tr v-for="row in rows" :key="`${row.paper_id}:${questionId(row.question)}`"><td class="stem-cell">{{ row.question.stem }}<small v-if="row.question.note" class="note-excerpt">{{ row.question.note }}</small></td><td>{{ typeLabels[row.question.type] }}<small>{{ row.question.topic }}</small></td><td>{{ row.last_wrong_at ? timeLabel(row.last_wrong_at) : row.date }}</td><td :class="{ danger: row.mistake_count || row.question.attempt?.correct === false }"><template v-if="row.mistake_count">累计答错 {{ row.mistake_count }} 次<small>{{ resultText(row.question, row.question.practice?.at(-1) || row.question.attempt) }}</small></template><template v-else>{{ resultText(row.question) }}</template></td><td><button :disabled="loading || busy" @click="loadPaper(row.paper_id, questionId(row.question))"><BookOpen :size="15" />复习</button></td></tr></tbody></table>
          <table v-else-if="tab === 'issues'"><thead><tr><th>楼栋 / 时间</th><th>质疑内容</th><th>类别</th><th>原题版本</th><th>状态</th><th>操作</th></tr></thead><tbody><tr v-for="row in rows" :key="row.id"><td>{{ row.scope }}楼<small>{{ timeLabel(row.created_at) }}</small></td><td class="stem-cell">{{ row.description }}<small>{{ row.stem || row.question?.stem || row.snapshot?.stem }}</small></td><td>{{ categoryLabels[row.category] || row.category }}</td><td>{{ row.question_version }}</td><td><span class="badge" :class="{ 'success-badge': ['resolved', 'no_change'].includes(row.status), 'warning-badge': row.status === 'needs_info' }">{{ issueLabels[row.status] || row.status }}</span></td><td><button :disabled="loading || busy" @click="openIssue({}, row)"><MessageSquare :size="15" />{{ isAdmin ? '处理' : '查看与补充' }}</button></td></tr></tbody></table>
          <table v-else><thead><tr><th class="check-cell"><input type="checkbox" aria-label="选择当前页题目" :disabled="loading || busy" :checked="rows.length > 0 && selected.length === rows.length" @change="selected = ($event.target as HTMLInputElement).checked ? rows.map(q => q.id) : []" /></th><th>题目</th><th>题库 / 题型</th><th>专业 / 知识点</th><th>年度</th><th>状态 / 问题</th><th>操作</th></tr></thead><tbody><tr v-for="row in rows" :key="row.id"><td><input v-model="selected" type="checkbox" :value="row.id" :disabled="loading || busy" :aria-label="`选择题目 ${row.stem}`" /></td><td class="stem-cell"><span class="table-stem" :title="row.stem">{{ row.stem }}</span></td><td>{{ bankLabels[row.bank] }}<small>{{ row.type_label || typeLabels[row.type] }}</small></td><td>{{ row.specialty || '—' }}<small>{{ row.topic || '—' }}</small></td><td>{{ row.year || '—' }}</td><td><span class="badge" :class="{ 'success-badge': row.status === 'published', 'warning-badge': row.status === 'disabled' }">{{ statusLabels[row.status] }}</span><small v-if="row.problems?.length" class="danger">{{ row.problems.join('；') }}</small></td><td><div class="row-actions"><button class="icon-button" aria-label="编辑题目" title="编辑题目" :disabled="busy || loading" @click="editQuestion(row.id)"><Pencil :size="16" /></button><button class="icon-button" aria-label="复制题目" title="复制题目" :disabled="busy || loading" @click="copyQuestion(row)"><Copy :size="16" /></button><button v-if="row.status !== 'deleted'" class="icon-button" :aria-label="row.status === 'published' ? '停用题目' : '发布题目'" :title="row.status === 'published' ? '停用题目' : '发布题目'" :disabled="busy || loading" @click="setQuestionStatus([row], row.status === 'published' ? 'disabled' : 'published')"><X v-if="row.status === 'published'" :size="16" /><Check v-else :size="16" /></button><button v-if="row.status === 'deleted'" class="icon-button" aria-label="恢复为草稿" title="恢复为草稿" :disabled="busy || loading" @click="setQuestionStatus([row], 'draft')"><RotateCcw :size="16" /></button><button v-else class="icon-button danger" aria-label="删除题目" title="移入回收站" :disabled="busy || loading" @click="setQuestionStatus([row], 'deleted')"><Trash2 :size="16" /></button></div></td></tr></tbody></table>
          <div v-if="!rows.length && !loading" class="empty"><FileText :size="28" /><span>{{ error ? '列表读取失败，请重试。' : '暂无符合条件的记录' }}</span></div>
        </div>
        <footer class="pagination list-pagination"><span>共 {{ total }} 条 · 每页 20 条</span><div class="actions"><button class="icon-button" :disabled="loading || pageNumber <= 1" aria-label="第一页" title="第一页" @click="gotoPage(1)"><ChevronsLeft :size="18" /></button><button class="icon-button" :disabled="loading || pageNumber <= 1" aria-label="上一页" title="上一页" @click="turnPage(-1)"><ChevronLeft :size="18" /></button><span>{{ pageNumber }} / {{ pageCount }}</span><button class="icon-button" :disabled="loading || pageNumber >= pageCount" aria-label="下一页" title="下一页" @click="turnPage(1)"><ChevronRight :size="18" /></button><button class="icon-button" :disabled="loading || pageNumber >= pageCount" aria-label="最后一页" title="最后一页" @click="gotoPage(pageCount)"><ChevronsRight :size="18" /></button><label class="page-jump">跳至<input v-model.number="jumpPage" type="number" min="1" :max="pageCount" aria-label="跳转页码" :disabled="loading" @keydown.enter="gotoPage(jumpPage)" /><button type="button" :disabled="loading || !jumpPage" @click="gotoPage(jumpPage)">跳转</button></label></div></footer>
      </template>

      <template v-else-if="tab === 'profile'">
        <LearningHome v-if="viewMode === 'overview' && !learner" management :is-admin="isAdmin" :data="profile" :date="date" :loading="loading || busy" @open="openLearning" />
        <form class="toolbar profile-toolbar" :class="{ 'custom-period': !filters.period }" @submit.prevent="loadView"><div class="filters"><VnetSelect input-id="learning-select-7" :model-value="({ '7': '近7天', '30': '近30天' } as Dict)[filters.period] || '自定义'" :options="['近7天', '近30天', '自定义']" label="统计周期" @update:model-value="setPeriod(keyFor({ '7': '近7天', '30': '近30天' }, $event)); loadView()" /><label class="inline-field">开始<input v-model="filters.from" aria-label="统计开始日期" type="date" /></label><label class="inline-field">结束<input v-model="filters.to" aria-label="统计结束日期" type="date" /></label><button :disabled="loading || busy"><Search :size="16" />查询</button></div><button v-if="isAdmin" type="button" :disabled="busy || loading" @click="exportData('results')"><Download :size="16" />导出报表</button></form>
        <div v-if="loading" class="loading-line" role="status"><Loader2 :size="16" class="spin" />正在读取统计…</div>
        <LearningDashboard :data="profile" :person="learner" :all-scopes="allPortrait" :show-continue="viewMode === 'practice' && boot.can_answer" :disabled="busy || loading" @select="selectLearner" @continue="continueLearning" />
      </template>

      <form v-else-if="tab === 'settings'" class="settings-form" @submit.prevent="saveSettings">
        <details v-if="isAdmin && boot.sync?.history_issue_count" class="history-identity-issues">
          <summary>历史人员待核对 · {{ boot.sync.history_issue_count }} 条</summary>
          <div class="actions"><button type="button" :disabled="busy" @click="exportData('history_identity')"><Download :size="16" />导出完整清单</button><span v-if="boot.sync.history_issue_count > 50">当前显示前 50 条</span></div>
          <ul>
            <li v-for="item in boot.sync.history_issues" :key="item.record_id">
              <strong>{{ item.name || '姓名缺失' }}</strong><span>{{ item.date }} {{ item.scope }}</span>
              <span class="history-identity-reason">{{ item.reason }}</span><small>原记录：{{ item.record_id }}</small>
              <small v-for="candidate in item.candidates" :key="candidate.record_id">候选：{{ candidate.name }} · 工号 {{ candidate.employee_no || '未填写' }} · {{ candidate.scopes.join('、') }} · {{ candidate.record_id }}</small>
            </li>
          </ul>
        </details>
        <div class="settings-section-head"><h2>题库与题单</h2><div class="actions"><button type="button" :disabled="busy || jobRunning" @click="runJob('refresh')"><RefreshCw :size="16" />同步题库与人员</button><button type="button" class="primary" :title="boot.silent_manual_publish ? '生成今日题单，不发送飞书消息' : '请重启主程序后使用静默发布'" :disabled="busy || jobRunning || !boot.silent_manual_publish" @click="runJob('publish')"><Send :size="16" />手动发布题单</button></div></div>
        <div class="settings-section-head"><h2>每日发布</h2><label class="checkbox-label"><input v-model="settingsForm.enabled" type="checkbox" :disabled="busy" />每日自动发布</label></div>
        <div v-for="shift in publishShifts" :key="shift.key" class="shift-publish-row">
          <label class="time-field">{{ shift.label }}<input v-model="settingsForm[shift.field]" type="time" required :disabled="busy || !settingsForm.enabled" /></label>
          <div class="shift-delivery" data-v2-inline-feedback>
            <div class="shift-delivery-status" :class="shiftStatus(shift.key).state" role="status" aria-atomic="true">
              <CheckCircle2 v-if="shiftStatus(shift.key).state === 'success'" :size="18" />
              <AlertCircle v-else-if="['partial','unverified'].includes(shiftStatus(shift.key).state)" :size="18" />
              <span class="shift-result-label">{{ shiftStatus(shift.key).label || '发送结果尚未就绪' }}</span>
              <time>{{ boot.delivery_status?.date || boot.today }}</time>
              <span v-if="shiftStatus(shift.key).expected">已发送 {{ shiftStatus(shift.key).sent }} / {{ shiftStatus(shift.key).expected }} 人</span>
            </div>
            <p v-if="shiftStatus(shift.key).failed" class="shift-failed-names">发送失败：{{ (shiftStatus(shift.key).issues || []).filter((person: Dict) => person.status === 'failed').map((person: Dict) => person.name).join('、') }}</p>
            <details v-if="shiftStatus(shift.key).issues?.length" class="shift-delivery-issues">
              <summary>发送异常 {{ shiftStatus(shift.key).failed || 0 }} 人 · 待发送或核对 {{ shiftStatus(shift.key).pending || 0 }} 人</summary>
              <ul><li v-for="(person, index) in shiftStatus(shift.key).issues" :key="index"><strong>{{ person.name }}<span v-if="person.scope"> · {{ person.scope }}楼</span></strong><span>{{ person.reason }}</span></li></ul>
            </details>
          </div>
        </div>
        <div class="settings-section-head"><h2>未完成提醒</h2><label class="checkbox-label"><input v-model="settingsForm.reminder_enabled" type="checkbox" :disabled="busy" />提醒未完成的楼栋</label>
        <label v-if="settingsForm.reminder_enabled" class="time-field">提醒时间<input v-model="settingsForm.reminder_time" type="time" required :disabled="busy" /></label>
        </div><h2>个人提醒与回顾</h2>
        <label class="checkbox-label"><input v-model="settingsForm.timeout_reminder_enabled" type="checkbox" :disabled="busy" />值班员、值班长开始作答满2小时未完成，提醒本人一次</label>
        <label class="checkbox-label"><input v-model="settingsForm.weekly_report_enabled" type="checkbox" :disabled="busy" />每周结束后（周一00:00）发送：值班员、值班长收到个人回顾；A至E楼主管收到本楼汇总，学练群收到全楼汇总</label>
        <footer class="actions"><button class="primary" :disabled="busy || loading"><Save :size="16" />保存设置</button></footer>
      </form>
    </template>

    <Teleport to="body">
      <UiTransition name="ui-overlay" appear>
      <div v-if="modalKind" class="learning-overlay" @click.self="closeModal">
        <section ref="modalElement" class="learning-modal" :class="{ 'preview-modal': modalKind === 'attachment' }" role="dialog" aria-modal="true" aria-labelledby="learning-modal-title" tabindex="-1" @paste="['question', 'issue'].includes(modalKind) && pasteFiles($event)">
          <header><h2 id="learning-modal-title">{{ modalKind === 'question' ? (editor.id ? '编辑题目' : '新增题目') : modalKind === 'issue' ? (issue.id ? '质疑处理记录' : '题目有疑问') : modalKind === 'import' ? '导入题库' : modalKind === 'people' ? '选择人员' : preview.name }}</h2><button class="icon-button" aria-label="关闭窗口" title="关闭窗口" :disabled="busy" @click="closeModal"><X :size="20" /></button></header>
          <div class="modal-scroll" :inert="busy || undefined">
            <div v-if="error" class="alert error" role="alert"><AlertCircle :size="18" /><span>{{ error }}</span><button v-if="conflicted && ['question', 'issue'].includes(modalKind)" :disabled="busy" @click="reloadConflict"><RefreshCw :size="16" />读取最新版本</button></div>
            <template v-if="modalKind === 'people'"><form class="toolbar" @submit.prevent="peoplePage = 1; loadPeople()"><label class="search-field"><Search :size="16" /><input v-model="peopleSearch" aria-label="按姓名或工号搜索人员" placeholder="姓名 / 工号" type="search" /></label><button type="submit" :disabled="peopleLoading"><Search :size="16" />搜索</button><span>{{ scopeLabel }}</span></form><div v-if="peopleLoading" class="loading-line"><Loader2 :size="16" class="spin" />正在读取人员…</div><div class="people-list"><button v-for="person in peopleRows" :key="person.id" :disabled="peopleLoading || busy" @click="selectLearner(person)"><strong>{{ person.name }}</strong><span>工号 {{ person.employee_no || '未填写' }}</span><small>{{ (person.scopes || []).map((s: string) => s + '楼').join('、') }}</small><ChevronRight :size="16" /></button></div><div v-if="!peopleLoading && !peopleRows.length" class="empty">{{ peopleReady ? '没有匹配人员' : '人员目录尚未准备好，请等待后台同步' }}</div><div v-if="peopleIssues.length && isAdmin" class="alert warning"><AlertCircle :size="16" /><span>{{ peopleIssues.length }} 位人员楼栋或身份待核对：{{ peopleIssues.slice(0, 10).map((p: Dict) => p.name).join('、') }}</span></div><div class="pagination"><span>共 {{ peopleTotal }} 人</span><div class="actions"><button class="icon-button" aria-label="人员选择上一页" :disabled="peopleLoading || peoplePage <= 1" @click="peoplePage--; loadPeople()"><ChevronLeft :size="16" /></button><span>{{ peoplePage }} / {{ Math.max(1, Math.ceil(peopleTotal / 20)) }}</span><button class="icon-button" aria-label="人员选择下一页" :disabled="peopleLoading || peoplePage * 20 >= peopleTotal" @click="peoplePage++; loadPeople()"><ChevronRight :size="16" /></button></div></div></template>
            <form v-else-if="modalKind === 'question'" id="learning-question-form" class="editor-form" @submit.prevent="saveQuestion">
              <div class="form-grid question-basics">
                <label>题库<VnetSelect input-id="learning-select-8" :model-value="bankLabels[editor.bank]" :options="Object.values(bankLabels)" label="所属题库" :disabled="busy || !!editor.id" @update:model-value="editor.bank = keyFor(bankLabels, $event); editor.type = editor.bank === 'written' ? 'single' : 'interview'" /></label>
                <label v-if="['written', 'supplemental'].includes(editor.bank)">题型<VnetSelect input-id="learning-select-9" :model-value="editor.type === 'multiple' ? (editor.type_label === '不定项' ? '不定项' : '多选题') : typeLabels[editor.type]" :options="editor.bank === 'supplemental' ? ['单选题', '多选题', '面试题'] : ['单选题', '多选题', '不定项']" label="题目类型" :disabled="busy" @update:model-value="setEditorType" /></label>
                <label>状态<VnetSelect input-id="learning-select-10" :model-value="statusLabels[editor.status]" :options="Object.values(statusLabels).filter(v => v !== '已删除')" label="编辑状态" :disabled="busy || editor.status === 'deleted'" @update:model-value="editor.status = keyFor(statusLabels, $event)" /></label>
              </div>
              <label><span class="label-text">题干 <span class="required">*</span></span><textarea v-model="editor.stem" rows="4" maxlength="12000" required /></label>
              <section v-if="editor.type !== 'interview'" class="option-editor"><div class="section-heading"><h3>选项与正确答案</h3><button type="button" :disabled="busy || editor.options.length >= 26" @click="editor.options.push({ id: uid(), text: '' })"><Plus :size="16" />添加选项</button></div><div v-for="(option, n) in editor.options" :key="option.id" class="option-edit-row"><input :type="editor.type === 'single' ? 'radio' : 'checkbox'" name="correct-answer" :checked="editor.correct_option_ids.includes(option.id)" :aria-label="`选项 ${String.fromCharCode(65 + Number(n))} 为正确答案`" @change="chooseCorrect(option.id)" /><strong>{{ String.fromCharCode(65 + Number(n)) }}</strong><textarea v-model="option.text" rows="2" :aria-label="`选项 ${String.fromCharCode(65 + Number(n))} 内容`" /><button type="button" class="icon-button" :disabled="Number(n) === 0" aria-label="上移选项" title="上移选项" @click="moveOption(Number(n), -1)"><ArrowUp :size="15" /></button><button type="button" class="icon-button" :disabled="Number(n) === editor.options.length - 1" aria-label="下移选项" title="下移选项" @click="moveOption(Number(n), 1)"><ArrowDown :size="15" /></button><button type="button" class="icon-button danger" aria-label="删除选项" title="删除选项" @click="removeOption(option.id)"><Trash2 :size="15" /></button></div></section>
              <label v-if="editor.type === 'interview'">参考答案<textarea v-model="editor.answer_text" rows="4" maxlength="12000" /></label>
              <details class="question-extra"><summary>解析与提示</summary><div class="editor-form">
                <label>解析<textarea v-model="editor.analysis" rows="3" maxlength="12000" /></label><label>提示<textarea v-model="editor.hint" rows="2" maxlength="5000" /></label>
              </div></details>
              <div v-if="editor.status === 'published' && learningQuestionProblems(editor).length" class="alert warning"><AlertCircle :size="17" /><span>发布前待修正：{{ learningQuestionProblems(editor).join('；') }}</span></div>
              <div v-if="editor.problems?.length" class="alert warning"><AlertCircle :size="17" />{{ editor.problems.join('；') }}</div>
            </form>

            <div v-else-if="modalKind === 'issue'" class="issue-form">
              <div class="question-meta"><span>{{ issue.scope }}楼</span><span>原题版本 {{ issue.question_version }}</span><span v-if="issue.id" class="badge">{{ issueLabels[issue.status] || issue.status }}</span></div>
              <details open><summary>原题与当时作答</summary><div class="issue-snapshot"><h3 class="prewrap">{{ issue.snapshot?.stem || issue.question?.stem || issue.stem || '原题见关联题单' }}</h3><p v-for="(option, n) in issue.snapshot?.options || issue.question?.options || []" :key="option.id">{{ String.fromCharCode(65 + Number(n)) }}. {{ option.text }}</p><p v-if="(issue.attempt || issue.snapshot?.attempt)" class="muted">当时回答：{{ (issue.attempt || issue.snapshot?.attempt).answer_text || answerLabels((issue.attempt || issue.snapshot?.attempt).option_ids, issue.question || issue.snapshot) || '未作答' }}</p><button v-if="isAdmin" :disabled="busy" @click="editIssueQuestion()"><Pencil :size="15" />核对并编辑原题</button></div></details>
              <template v-if="!issue.id"><label>问题类别<VnetSelect input-id="learning-select-12" :model-value="categoryLabels[issue.category]" :options="Object.values(categoryLabels)" label="问题类别" @update:model-value="issue.category = keyFor(categoryLabels, $event)" /></label><label><span class="label-text">问题说明 <span class="required">*</span></span><textarea v-model="issue.description" rows="4" maxlength="6000" /></label><label>建议答案或依据<textarea v-model="issue.suggestion" rows="3" maxlength="6000" /></label></template>
              <template v-else><h3>{{ categoryLabels[issue.category] }}质疑</h3><p class="prewrap">{{ issue.description }}</p><p v-if="issue.suggestion" class="prewrap">建议：{{ issue.suggestion }}</p><section class="comment-list"><h3>处理与补充记录</h3><article v-for="(comment, n) in issue.comments || []" :key="n" class="comment-entry"><div class="actions"><strong>{{ comment.author || comment.actor?.name || comment.name || (comment.is_admin ? '管理员' : issue.scope + '楼') }}</strong><small>{{ timeLabel(comment.created_at || comment.at) }}</small><span v-if="comment.status" class="badge">{{ issueLabels[comment.status] || comment.status }}</span></div><p class="prewrap">{{ comment.text || comment.comment || comment.content }}</p></article><p v-if="!issue.comments?.length" class="muted">暂无回复</p></section><label><span class="label-text">补充说明 / 处理结论 <span class="required">*</span></span><textarea v-model="issueComment" rows="4" maxlength="6000" /></label><label>更新状态<VnetSelect input-id="learning-select-13" :model-value="issueLabels[issueNextStatus] || issueNextStatus" :options="issueStatusOptions" label="更新质疑状态" @update:model-value="issueNextStatus = keyFor(issueLabels, $event)" /></label></template>
            </div>

            <template v-else-if="modalKind === 'import'"><div class="toolbar"><label class="file-button"><Upload :size="16" />选择 JSON / CSV<input type="file" accept=".json,.csv,application/json,text/csv" @change="importFile" /></label><button @click="downloadTemplate"><Download :size="16" />下载模板</button></div><p class="muted">{{ importName || '未选择文件' }}<template v-if="importRows.length"> · {{ importRows.length }} 题 · {{ importErrors }} 题待修正</template></p><div class="table-wrap"><table><thead><tr><th>序号</th><th>题干</th><th>题库 / 题型</th><th>状态</th><th>校验</th></tr></thead><tbody><tr v-for="(row, n) in importVisible" :key="n"><td>{{ (importPage - 1) * 20 + n + 1 }}</td><td class="stem-cell">{{ row.question.stem }}</td><td>{{ bankLabels[row.question.bank] }}<small>{{ typeLabels[row.question.type] }}</small></td><td>{{ statusLabels[row.question.status] }}</td><td :class="row.errors.length ? 'danger' : 'success-text'">{{ row.errors.join('；') || '通过' }}</td></tr></tbody></table></div><div v-if="importRows.length" class="pagination"><span>每页 20 条</span><div class="actions"><button class="icon-button" aria-label="导入预览上一页" :disabled="importPage <= 1" @click="importPage--"><ChevronLeft :size="17" /></button><span>{{ importPage }} / {{ Math.ceil(importRows.length / 20) }}</span><button class="icon-button" aria-label="导入预览下一页" :disabled="importPage * 20 >= importRows.length" @click="importPage++"><ChevronRight :size="17" /></button></div></div></template>

            <template v-else-if="modalKind === 'attachment'"><img v-if="isImage(preview)" class="attachment-image" :src="attachmentUrl(preview)" :alt="preview.name" /><iframe v-else-if="/\.pdf$/i.test(preview.name)" class="attachment-frame" :src="attachmentUrl(preview)" :title="preview.name"></iframe><div v-else class="empty"><FileText :size="40" /><span>{{ preview.name }}</span><a :href="attachmentUrl(preview)" target="_blank" rel="noopener">打开附件</a></div></template>

            <details v-if="['question', 'issue'].includes(modalKind)" class="attachment-editor" :open="modalKind === 'issue' || !!editor.attachments?.length || !!uploadFiles.length" @dragover.prevent @drop.prevent="addFiles(Array.from($event.dataTransfer?.files || []))">
              <summary>附件（选填）</summary><div class="attachments"><div v-for="a in (modalKind === 'question' ? editor.attachments : issue.attachments) || []" :key="a.id" class="attachment-item"><a :href="attachmentUrl(a)" target="_blank" rel="noopener"><Eye :size="15" />{{ a.name }}</a><small>{{ attachmentLabels[a.kind] }}</small><button v-if="isAdmin" class="icon-button danger" aria-label="删除附件" title="删除附件" :disabled="busy" @click="deleteAttachment(a)"><Trash2 :size="15" /></button></div></div>
              <div class="upload-zone"><div class="actions"><VnetSelect input-id="learning-select-14" :model-value="attachmentLabels[attachmentKind]" :options="modalKind === 'issue' ? ['学习资料'] : Object.values(attachmentLabels)" label="附件用途" @update:model-value="attachmentKind = keyFor(attachmentLabels, $event)" /><label class="file-button"><Upload :size="16" />选择附件<input type="file" multiple :disabled="busy" @change="pickFiles" /></label><span class="muted">拖入文件 / 粘贴图片 · 每份 20 MiB，上限 10 份 / 100 MiB</span></div><div v-for="(file, n) in uploadFiles" :key="`${file.name}:${n}`" class="pending-file"><FileText :size="15" /><span>{{ file.name }}</span><small>{{ (file.size / 1024 / 1024).toFixed(2) }} MiB · 待上传</small><button class="icon-button" aria-label="移除待上传附件" title="移除待上传附件" :disabled="busy" @click="uploadFiles.splice(n, 1)"><X :size="15" /></button></div><button v-if="uploadFiles.length && (modalKind === 'question' ? editor.id : issue.id)" :disabled="busy" @click="perform(() => uploadAttachments(modalKind === 'question' ? 'question' : 'issue'), '附件已上传。')"><Upload :size="16" />上传所选附件</button></div>
            </details>
          </div>
          <footer><span v-if="busy" class="muted actions"><Loader2 :size="16" class="spin" />正在保存…</span><span v-else-if="modalDirty" class="muted">有未保存内容</span><div class="actions"><button :disabled="busy" @click="closeModal">关闭</button><button v-if="modalKind === 'question'" class="primary" type="submit" form="learning-question-form" :disabled="busy || editor.status === 'deleted'"><Save :size="16" />保存题目</button><button v-else-if="modalKind === 'issue'" class="primary" :disabled="busy" @click="submitIssue"><Send :size="16" />{{ issue.id ? '提交补充与处理' : '提交质疑' }}</button><button v-else-if="modalKind === 'import'" class="primary" :disabled="busy || !importRows.length || !!importErrors" @click="commitImport"><Upload :size="16" />确认导入 {{ importRows.length || '' }}</button><a v-else-if="modalKind === 'attachment'" :href="attachmentUrl(preview)" target="_blank" rel="noopener" download><Download :size="16" />下载附件</a></div></footer>
        </section>
      </div>
      </UiTransition>
    </Teleport>
    <ConfirmDialog :open="confirm.open" :title="confirm.title" :message="confirm.message" :tone="confirm.tone" @resolve="confirmed" />
  </section>
</template>

<script lang="ts">
import type { Dict } from "../api/client";

const learningYearPattern = /^\s*$|^(19\d{2}|20\d{2}|2100)年?$/;

export function learningQuestionProblems(q: Dict): string[] {
  const errors: string[] = [];
  if (q.year !== undefined && q.year !== null && String(q.year).trim() !== "" && !learningYearPattern.test(String(q.year))) errors.push("年度须为 1900 至 2100 的四位年份，可带“年”后缀（如 2026 或 2026年）");
  if (!String(q.stem || "").trim()) errors.push("题干不能为空");
  if (!["written", "duty", "professional", "supplemental"].includes(q.bank)) errors.push("题库无效");
  if (!["single", "multiple", "interview"].includes(q.type)) errors.push("题型无效");
  if (q.bank !== "supplemental" && (q.bank === "written") === (q.type === "interview")) errors.push("题型与题库不一致");
  if (q.status && !["draft", "published", "disabled", "deleted"].includes(q.status)) errors.push("题目状态无效");
  if (q.type !== "interview") {
    const options = Array.isArray(q.options) ? q.options : [];
    const ids = options.map((o: Dict) => o.id);
    const correct = Array.isArray(q.correct_option_ids) ? q.correct_option_ids : [];
    if (options.length < 2 || options.length > 26 || options.some((o: Dict) => !o.id || !String(o.text || "").trim()) || new Set(ids).size !== ids.length) errors.push("须有 2 至 26 个非空选项，选项标识不能重复");
    if (!correct.length || correct.some((id: string) => !ids.includes(id)) || new Set(correct).size !== correct.length) errors.push("请重新选择有效的正确答案");
    if (q.type === "single" && correct.length !== 1) errors.push("单选题须有一个正确答案");
  } else if (!String(q.answer_text || "").trim() && !(q.attachments || []).some((a: Dict) => a.kind === "answer")) errors.push("面试题须有参考答案或答案附件");
  return errors;
}

export function parseLearningImport(text: string, csv = false): Array<{ question: Dict; errors: string[] }> {
  text = text.replace(/^\uFEFF/, "");
  let rows: Dict[];
  if (!csv) {
    const parsed = JSON.parse(text);
    rows = Array.isArray(parsed) ? parsed : parsed.questions;
  } else {
    const records: string[][] = []; let row: string[] = [], field = "", quoted = false;
    for (let i = 0; i < text.length; i++) {
      const char = text[i];
      if (char === '"') {
        if (quoted && text[i + 1] === '"') { field += '"'; i++; }
        else if (quoted || !field) quoted = !quoted;
        else throw new Error("CSV 引号位置不正确");
      } else if (!quoted && (char === "," || char === "\n" || char === "\r")) {
        row.push(field); field = "";
        if (char !== ",") { records.push(row); row = []; if (char === "\r" && text[i + 1] === "\n") i++; }
      } else field += char;
    }
    if (quoted) throw new Error("CSV 引号未闭合");
    if (field || row.length) { row.push(field); records.push(row); }
    const headers = records.shift()?.map(h => h.trim()) || [];
    if (!headers.includes("stem")) throw new Error("CSV 缺少 stem 列；可下载导入模板");
    if (new Set(headers).size !== headers.length) throw new Error("CSV 列名重复");
    rows = records.filter(r => r.some(Boolean)).map((r, i) => {
      if (r.length !== headers.length) throw new Error(`CSV 第 ${i + 2} 行列数不匹配`);
      const item: Dict = Object.fromEntries(headers.map((key, index) => [key, r[index]]));
      for (const key of ["options", "correct_option_ids", "attachments"]) if (item[key]) {
        try { item[key] = JSON.parse(item[key]); } catch { throw new Error(`CSV 第 ${i + 2} 行 ${key} 不是有效 JSON`); }
      }
      return item;
    });
  }
  if (!Array.isArray(rows) || !rows.length) throw new Error("导入内容须为非空 questions 数组");
  if (rows.length > 500) throw new Error("单次最多导入 500 题");
  return rows.map((row, i) => {
    if (!row || typeof row !== "object" || Array.isArray(row)) throw new Error(`第 ${i + 1} 题格式无效`);
    const question = { ...row, options: row.options || [], correct_option_ids: row.correct_option_ids || [], attachments: row.attachments || [], status: row.status || "draft" };
    return { question, errors: learningQuestionProblems(question) };
  });
}
</script>

<style scoped>
.building-tabs { display: grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap: 12px; margin: 14px 0; }.building-tabs button { display: flex; justify-content: space-between; padding: 15px 18px; font-size: 18px; }.building-tabs span { color: #657b95; font-size: 12px; }.building-tabs .active { background: #eaf2ff; border-color: #5890e8; color: #174fab; }.learner-strip { display: flex; justify-content: space-between; align-items: center; padding: 14px 0; border-bottom: 1px solid #dce5ef; }.learner-strip strong { font-size: 18px; }.learner-strip span { margin-left: 14px; color: #64758b; }.people-list { display: grid; gap: 0; }.people-list button { display: grid; grid-template-columns: 1fr 1fr 1fr 20px; padding: 13px 16px; border-radius: 0; border-width: 0 0 1px; text-align: left; color: #25425f; }.people-list small { color: #64758b; }
.building-tabs.with-all { grid-template-columns: repeat(7, minmax(0, 1fr)); }.building-tabs.with-all button { padding-inline: 10px; font-size: 16px; }
@media (min-width: 701px) and (max-width: 1050px) { .building-tabs.with-all span { display: none; } }
.learning-page { max-width: 1800px; margin: 0 auto; padding: 22px 30px 40px; color: #17263b; font-size: 14px; letter-spacing: 0; }
.learning-page *, .learning-modal * { box-sizing: border-box; }
.page-head, .heading, .actions, .filters, .row-actions, .question-meta, .toolbar, .section-heading, .pagination, .question-footer, .learning-modal > header, .learning-modal > footer { display: flex; align-items: center; gap: 10px; }
.page-head, .toolbar, .question-meta, .section-heading, .pagination, .question-footer, .learning-modal > header, .learning-modal > footer { justify-content: space-between; }
.page-head, .actions, .filters, .toolbar, .question-meta { flex-wrap: wrap; }
.page-head { margin-bottom: 18px; }
h1, h2, h3, p { margin: 0; }
h1 { font-size: 23px; font-weight: 650; }
h2 { font-size: 17px; font-weight: 650; }
h3 { font-size: 14px; font-weight: 650; }
button, .file-button, a[download] { display: inline-flex; align-items: center; justify-content: center; gap: 6px; min-height: 36px; padding: 7px 12px; border: 1px solid #d8e5f7; border-radius: 6px; background: #fff; color: #2357a0; font: inherit; font-weight: 500; cursor: pointer; text-decoration: none; line-height: 1.4; transition: background-color .18s ease, border-color .18s ease, color .18s ease, box-shadow .18s ease; }
button:hover:not(:disabled), .file-button:hover { border-color: #8db4ef; background: #f1f6ff; }
button:disabled { opacity: .55; cursor: not-allowed; }
button.primary { background: #1e63ff; border-color: #1e63ff; color: #fff; }
button.primary:hover:not(:disabled) { background: #1554df; }
button.icon-button { width: 34px; height: 34px; min-height: 34px; padding: 6px; flex: 0 0 34px; }
button:focus-visible, a:focus-visible, input:focus-visible, textarea:focus-visible, summary:focus-visible { outline: 2px solid #1e63ff; outline-offset: 2px; }
svg { flex-shrink: 0; }
.tabs { display: flex; gap: 8px; border-bottom: 1px solid #d8e5f7; overflow-x: auto; margin-bottom: 16px; }
.tabs button { flex: 0 0 auto; border: 0; border-bottom: 3px solid transparent; border-radius: 0; padding: 12px 16px; min-height: 46px; color: #52647b; background: transparent; }
.tabs button.active { border-bottom-color: #1e63ff; color: #1456c7; background: #f0f5ff; }
.toolbar { gap: 12px; margin: 16px 0; }
.today-admin-bar { display: flex; align-items: center; flex-wrap: wrap; gap: 8px; margin: -4px 0 12px; padding: 8px 10px; background: #f3f7fc; border: 1px solid #d8e5f7; border-radius: 6px; }.today-admin-bar button { min-height: 32px; padding: 5px 10px; font-size: 12px; }
.filters { gap: 8px; }
.muted, small, .sync-label { color: #65758a; font-size: 12px; }
.sync-label, .read-only { display: inline-flex; align-items: center; gap: 6px; }
.read-only { margin: 0 0 12px; }
.badge { display: inline-flex; align-items: center; gap: 4px; padding: 3px 7px; border-radius: 4px; background: #edf3ff; color: #285caf; font-size: 12px; line-height: 1.5; white-space: nowrap; }
.success-badge { background: #ecfdf5; color: #087c57; }
.warning-badge { background: #fff6df; color: #98630d; }
.alert { display: flex; align-items: center; gap: 9px; padding: 10px 12px; margin: 12px 0; border: 1px solid; border-radius: 6px; line-height: 1.65; overflow-wrap: anywhere; }
.alert > span { flex: 1; min-width: 0; }
.alert.error { background: #fff1f2; border-color: #fecdd3; color: #ad2345; }
.alert.warning { background: #fffbeb; border-color: #f2dfac; color: #926018; }
.alert.success { background: #ecfdf5; border-color: #b6e7d1; color: #087c57; }
.danger { color: #b62946 !important; }
.success-text { color: #087c57; }
.required { color: #b62946; }
.loading-line { display: flex; align-items: center; gap: 8px; padding: 9px 0; color: #275aae; }
.empty { display: grid; min-height: 200px; padding: 32px 16px; justify-items: center; align-content: center; gap: 12px; color: #6a7c92; text-align: center; background: #fff; }
.practice-layout { display: grid; grid-template-columns: 180px minmax(0, 1fr); background: #fff; border-top: 1px solid #d8e5f7; border-bottom: 1px solid #d8e5f7; }
.question-rail { align-self: start; padding: 22px 18px; position: sticky; top: 10px; }
.question-rail h2 { display: flex; justify-content: space-between; font-size: 15px; }
.question-rail h2 span { font-size: 12px; color: #7a899c; }
.question-numbers { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 8px; margin: 18px 0; }
.question-numbers button { position: relative; aspect-ratio: 1; min-height: 0; padding: 0; color: #5a6f88; }
.question-numbers button.done { background: #effaf5; color: #087c57; border-color: #c3e9d8; }
.question-numbers button.wrong { background: #fff1f2; color: #b62946; border-color: #f3c4ce; }
.question-numbers button.invalid { color: #8894a6; background: #f1f3f5; text-decoration: line-through; }
.question-numbers button.selected { outline: 2px solid #1e63ff; outline-offset: 1px; }
.number-star { position: absolute; right: 2px; top: 2px; color: #9d6812; }
progress { width: 100%; height: 6px; accent-color: #168963; margin: 0 0 8px; }
.question-body { min-width: 0; border-left: 1px solid #e2eaf5; padding: 24px 30px; animation: learning-question-enter .22s ease-out; }
.question-meta { font-size: 12px; color: #65758a; margin-bottom: 18px; }
.stem { font-size: 18px; line-height: 1.8; white-space: pre-wrap; overflow-wrap: anywhere; margin: 14px 0 22px; font-weight: 550; }
.options { display: grid; gap: 12px; border: 0; padding: 0; margin: 16px 0; min-width: 0; }
.options label { display: flex; align-items: flex-start; gap: 12px; padding: 15px 16px; border: 1px solid #dde5f0; border-radius: 6px; color: #34465c; cursor: pointer; line-height: 1.7; transition: background-color .18s ease, border-color .18s ease, box-shadow .18s ease; }
.options input { flex: 0 0 16px; margin-top: 5px; }.options label b { color: #536986; }.options label span { flex: 1; min-width: 0; white-space: pre-wrap; overflow-wrap: anywhere; }
.options label.checked { border-color: #6098ef; background: #f1f6ff; }.options label.correct { border-color: #85ccad; background: #f0fbf5; }.options label.incorrect { border-color: #eea4b2; background: #fff6f6; }
.options:disabled label { cursor: default; }
.options:not(:disabled) label:hover { border-color: #8cafe1; box-shadow: 0 2px 7px #29558b0b; }.options label:focus-within { outline: 2px solid #1e63ff; outline-offset: 2px; }
.options label .option-result { display: inline-flex; align-items: center; justify-content: center; flex: 0 0 20px; width: 20px; height: 26px; color: #27805d; }.options label.incorrect .option-result { color: #b62946; }
label { display: grid; gap: 7px; color: #4f6177; }
input, textarea { min-width: 0; width: 100%; border: 1px solid #cedced; background: #fff; color: #22364f; border-radius: 6px; padding: 8px 10px; font: inherit; line-height: 1.6; transition: border-color .18s ease, box-shadow .18s ease; }
input:disabled, textarea:disabled { background: #f5f7fa; color: #52647b; }
input[type='checkbox'], input[type='radio'] { width: 16px; height: 16px; min-height: 0; padding: 0; accent-color: #1e63ff; flex-shrink: 0; }
textarea { resize: vertical; }
.checkbox-label, .inline-field { display: inline-flex; align-items: center; gap: 8px; }
.inline-field input { width: 158px; }
.checkbox-label { white-space: nowrap; }.checkbox-label input { margin: 0; }
.interview-answer { display: grid; gap: 16px; }.rating { width: 210px; }
.answer-actions { margin: 18px 0; }.answer-tools { padding: 14px 0; border-top: 1px solid #e5ecf5; }
.answer-reference { background: #f6faf8; border-left: 3px solid #7cc7a6; padding: 16px 18px; margin-bottom: 20px; animation: learning-question-enter .22s ease-out; }
.answer-reference h3 { color: #226748; margin: 8px 0; }.answer-reference p { line-height: 1.8; }
.prewrap { white-space: pre-wrap; overflow-wrap: anywhere; line-height: 1.8; }.draft-status { margin: 10px 0; }
.answer-feedback { display: flex; gap: 20px; margin: 10px 0; color: #b62946; }
.first-result { margin: 8px 0; color: #64758b; font-size: 12px; line-height: 1.6; }
.result { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; padding: 9px 12px; border-radius: 6px; background: #f0faf5; animation: learning-question-enter .22s ease-out; }.result.danger { background: #fff2f3; }.result small { font-weight: 400; }
.notes { border-top: 1px solid #e5ecf5; padding-top: 14px; }.note-editor { display: grid; gap: 10px; padding: 8px 0 14px; }.question-footer { padding-top: 22px; margin-top: 22px; border-top: 1px solid #e5ecf5; font-size: 12px; color: #667b95; }
.starred { color: #aa730b; background: #fff9e9; }
.attachments { display: flex; flex-wrap: wrap; gap: 8px; margin: 10px 0; }.attachments button, .attachments a { min-width: 0; max-width: 100%; overflow-wrap: anywhere; text-align: left; }
.attachments small { white-space: nowrap; }.attachment-item { display: flex; align-items: center; gap: 6px; min-width: 0; border: 1px solid #d8e5f7; border-radius: 6px; padding: 5px 8px; }
.attachment-item a { display: inline-flex; gap: 5px; align-items: center; color: #2357a0; text-decoration: none; }
.search-field { display: flex; align-items: center; gap: 5px; background: #fff; border: 1px solid #cedced; padding: 0 10px; border-radius: 6px; }.search-field input { border: 0; width: 210px; padding-left: 4px; }
.table-wrap { overflow-x: auto; background: #fff; border-top: 1px solid #d8e5f7; border-bottom: 1px solid #d8e5f7; }.table-wrap.stale { opacity: .55; pointer-events: none; }
table { width: 100%; border-collapse: collapse; text-align: left; font-size: 13px; }
th { background: #f3f7fc; color: #60738b; font-weight: 550; font-size: 12px; white-space: nowrap; }
td, th { padding: 13px 14px; border-bottom: 1px solid #e6edf6; vertical-align: middle; }
tbody tr:last-child td { border-bottom: 0; }tbody tr:hover { background: #fafcff; }
td { line-height: 1.65; }td small { display: block; margin-top: 4px; }.stem-cell { min-width: 220px; max-width: 480px; white-space: pre-wrap; overflow-wrap: anywhere; }
.note-excerpt { max-height: 3.4em; overflow: hidden; }.check-cell { width: 42px; }.row-actions { gap: 4px; }.row-actions button { border-color: transparent; }
.table-stem { display: -webkit-box; -webkit-line-clamp: 3; -webkit-box-orient: vertical; overflow: hidden; }
.pagination { padding: 14px 2px; font-size: 12px; color: #667b95; }.list-pagination { position: sticky; bottom: 0; background: #fff; z-index: 6; border-top: 1px solid #e5ecf5; padding: 12px; }.list-pagination .actions { flex-wrap: wrap; }.batch-bar { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; padding: 10px; background: #eef5ff; margin-bottom: 12px; }
.profile-overview { display: grid; grid-template-columns: minmax(270px, 1fr) minmax(0, 2fr); gap: 24px; align-items: center; background: #fff; border-block: 1px solid #d8e5f7; padding: 18px 24px; margin: 18px 0; }
.completion-graphic { display: flex; align-items: center; gap: 20px; min-width: 0; }.completion-ring { flex: 0 0 108px; width: 108px; aspect-ratio: 1; display: grid; place-items: center; position: relative; border-radius: 50%; background: conic-gradient(#168963 var(--chart-fill), #e6edf5 0); }.completion-ring.empty { background: #e6edf5; }.completion-ring::before { content: ''; position: absolute; inset: 13px; border-radius: 50%; background: #fff; }.completion-ring strong { position: relative; font-size: 16px; color: #183d52; text-align: center; }.completion-copy { display: grid; gap: 6px; color: #667b95; font-size: 12px; }.completion-copy strong { color: #20344e; font-size: 15px; }
.profile-kpis { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); }.profile-kpis > div { padding: 12px 18px; border-left: 1px solid #e6edf6; min-width: 0; }.profile-kpis span { display: block; color: #6b7c91; font-size: 12px; }.profile-kpis strong { display: block; font-size: 20px; margin-top: 8px; font-weight: 600; overflow-wrap: anywhere; }
.rate-cell { display: flex; align-items: center; gap: 12px; }.rate-cell strong { min-width: 64px; font-size: 12px; font-weight: 600; white-space: nowrap; }.chart-bar { flex: 1; height: 9px; min-width: 80px; max-width: 260px; border-radius: 5px; background: #e6edf5; overflow: hidden; }.chart-bar span { display: block; height: 100%; border-radius: inherit; background: #168963; }.topic-bar span { background: #2e77bd; }
.subtabs { display: flex; gap: 6px; }.subtabs button { border: 0; background: transparent; border-radius: 0; }.subtabs button.active { color: #1554df; box-shadow: inset 0 -2px #1e63ff; }
.inventory { margin-top: 24px; }.inventory dl { display: flex; flex-wrap: wrap; align-items: center; gap: 12px; }.inventory dd { margin: 0 30px 0 0; font-size: 18px; font-weight: 600; }
.inventory dd { display: flex; flex-wrap: wrap; gap: 14px; font-size: 13px; font-weight: 400; }.inventory dd strong { font-size: 18px; font-weight: 600; }
.settings-form { background: #fff; padding: 24px 28px; display: grid; gap: 18px; border-top: 1px solid #d8e5f7; }.settings-form h2:not(:first-child) { border-top: 1px solid #e5ecf5; padding-top: 22px; }.time-field { width: 170px; }.page-jump { display: inline-flex; align-items: center; gap: 5px; }.page-jump input { width: 64px; }
.settings-form { min-width:0; grid-template-columns:repeat(auto-fit,minmax(min(100%,400px),1fr)); gap:12px 20px; padding:14px 16px; }.settings-form>h2,.settings-form>.settings-section-head,.settings-form>footer,.settings-form>.history-identity-issues{grid-column:1/-1}.settings-form h2:not(:first-child){padding-top:12px}
.settings-section-head{display:flex;align-items:center;flex-wrap:wrap;gap:8px 18px;padding-top:8px}.settings-section-head .time-field{display:flex;align-items:center;gap:8px;width:auto}.settings-section-head .time-field input{width:140px}.settings-form .checkbox-label { min-width: 0; white-space: normal; align-items: flex-start; }
.settings-form .checkbox-label input { flex-shrink: 0; }
.shift-publish-row { display: flex; flex-wrap: wrap; align-items: start; gap: 12px 20px; min-width: 0; }
.shift-publish-row .time-field { flex:0 0 150px; }
.shift-delivery { flex: 1 1 180px; min-width: 0; padding-top: 24px; }
.shift-delivery-status { display: flex; align-items: center; flex-wrap: wrap; gap: 6px 10px; font-size: 13px; color: var(--text-color-3, #64758a); }
.shift-result-label, .shift-delivery-status time { white-space: nowrap; }.shift-delivery-status svg { flex-shrink: 0; }
.shift-delivery-status.success { color: #18794e; }.shift-delivery-status.partial, .shift-delivery-status.unverified { color: #a05216; }
.shift-failed-names { margin: 6px 0 0; color: #a05216; font-size: 13px; overflow-wrap: anywhere; }
.shift-delivery-issues { margin-top: 8px; font-size: 13px; }.shift-delivery-issues summary { cursor: pointer; padding: 6px 0; }
.shift-delivery-issues ul { list-style: none; padding: 0; margin: 4px 0; }.shift-delivery-issues li { display: flex; flex-wrap: wrap; gap: 4px 12px; padding: 8px 0; border-top: 1px solid #e5ecf5; overflow-wrap: anywhere; }
@media(max-width:600px) { .shift-delivery { flex-basis: 150px; padding-top: 0; }.shift-publish-row .time-field { flex: 1 1 150px; width: auto; } }
.history-identity-issues { min-width: 0; border-block: 1px solid #d8e5f7; padding: 10px 0; }
.history-identity-issues summary { cursor: pointer; font-weight: 600; padding: 8px 0; }
.history-identity-issues ul { list-style: none; margin: 8px 0 0; padding: 0; }
.history-identity-issues li { display: flex; flex-wrap: wrap; gap: 6px 12px; padding: 12px 0; border-top: 1px solid #e5ecf5; overflow-wrap: anywhere; }
.history-identity-issues .history-identity-reason, .history-identity-issues small { flex-basis: 100%; }
.learning-overlay { position: fixed; inset: 0; z-index: 800; display: grid; place-items: center; background: rgba(15, 35, 65, .4); padding: 24px; }
.learning-modal { display: flex; flex-direction: column; width: min(1040px, 100%); max-height: calc(100dvh - 48px); background: #fff; border: 1px solid #d8e5f7; border-radius: 8px; box-shadow: 0 22px 80px #12356235; font: 14px 'Microsoft YaHei', system-ui, sans-serif; color: #20344e; outline: none; }
.learning-modal > header { padding: 18px 22px; border-bottom: 1px solid #d8e5f7; flex: 0 0 auto; }.learning-modal > header h2 { min-width: 0; overflow-wrap: anywhere; }
.learning-modal > footer { padding: 14px 22px; border-top: 1px solid #d8e5f7; background: #f8fbff; flex: 0 0 auto; flex-wrap: wrap; gap: 12px; }.learning-modal > footer > .actions { margin-left: auto; }
.modal-scroll { padding: 20px 24px; overflow-y: auto; min-height: 0; overscroll-behavior: contain; }
.editor-form, .issue-form { display:grid; gap:12px; }.form-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap:10px; }
.form-grid.question-basics { grid-template-columns: repeat(3, minmax(0, 1fr)); }.question-extra > .editor-form { margin-top: 10px; }
@media (max-width: 700px) { .form-grid.question-basics { grid-template-columns: minmax(0, 1fr); } }
.option-editor { display: grid; gap: 12px; }.option-edit-row { display: grid; grid-template-columns: 18px 18px minmax(0, 1fr) repeat(3, 34px); gap: 8px; align-items: center; }.option-edit-row textarea { min-height: 60px; }
.attachment-editor { margin-top: 24px; border-top: 1px solid #e5ecf5; padding-top: 18px; }.upload-zone { padding: 16px; border: 1px dashed #b6ccef; border-radius: 6px; margin-top: 12px; background: #f8fbff; }.file-button { position: relative; overflow: hidden; }.file-button input { position: absolute; inset: 0; opacity: 0; cursor: pointer; }.file-button:focus-within { outline: 2px solid #1e63ff; }.pending-file { display: flex; align-items: center; gap: 8px; margin-top: 10px; }.pending-file > span { flex: 1; min-width: 0; overflow-wrap: anywhere; }
.comment-list { border-block: 1px solid #e5ecf5; padding: 18px 0; }.comment-entry { padding: 12px 0; border-bottom: 1px solid #edf1f7; }.comment-entry small { margin: 0 10px; }.comment-entry p { margin-top: 8px; }.comment-entry:last-child { border-bottom: 0; }
summary { cursor: pointer; color: #2357a0; padding: 8px 0; }.issue-snapshot { background: #f7f9fc; padding: 14px; }.issue-snapshot p { line-height: 1.7; margin: 8px 0; }
.attachment-image { display: block; max-width: 100%; max-height: 65vh; object-fit: contain; margin: auto; }.attachment-frame { width: 100%; height: 65vh; border: 0; }
.sr-only { position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px; overflow: hidden; clip: rect(0, 0, 0, 0); white-space: nowrap; border: 0; }
.spin { animation: learning-spin 1s linear infinite; }@keyframes learning-spin { to { transform: rotate(360deg); } }
:deep(.vnet-select) { min-width: 145px; max-width: 100%; }:deep(.vnet-select-trigger), :deep(.vnet-select-input) { border-radius: 6px; min-height: 36px; }
@media (max-width: 1050px) { .learning-page { padding: 18px 20px 30px; }.practice-layout { grid-template-columns: 175px minmax(0, 1fr); }.question-body { padding: 22px; }.question-rail { padding: 22px 12px; }.profile-overview { grid-template-columns: minmax(0, 1fr); }.form-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
@media (max-width: 700px) { .learning-page { padding: 16px 12px; }.heading { gap: 7px; flex-wrap: wrap; }h1 { font-size: 20px; }.page-head > .actions { width: 100%; }.tabs { gap: 0; }.tabs button { padding: 10px; }.practice-layout { grid-template-columns: minmax(0, 1fr); }.question-rail { position: static; border-bottom: 1px solid #e5ecf5; padding: 14px; }.question-numbers { grid-template-columns: repeat(10, minmax(0, 1fr)); gap: 5px; margin: 12px 0; }.question-rail progress { display: none; }.question-body { border-left: 0; padding: 18px 14px; }.stem { font-size: 16px; }.options label { padding: 12px; gap: 8px; }.answer-tools { gap: 7px; }.answer-tools button { font-size: 12px; padding: 7px 9px; }.profile-overview { padding: 16px; gap: 14px; }.profile-kpis > div { padding: 10px; }.profile-kpis strong { font-size: 16px; }.settings-form { padding: 18px 14px; }.learning-overlay { padding: 10px; }.learning-modal { max-height: calc(100dvh - 20px); }.learning-modal > header, .learning-modal > footer { padding: 12px; }.modal-scroll { padding: 14px; }.form-grid { grid-template-columns: minmax(0, 1fr); }.option-edit-row { grid-template-columns: 16px 16px minmax(0, 1fr) repeat(3, 30px); gap: 4px; }.option-edit-row .icon-button { width: 30px; flex-basis: 30px; }.search-field { max-width: 100%; }.search-field input { width: 180px; }.inline-field input { width: 145px; }.toolbar { align-items: flex-start; }.pending-file { flex-wrap: wrap; }.alert { align-items: flex-start; }.table-wrap { max-width: 100%; }.pagination { flex-wrap: wrap; } }
.rail-progress { display: flex; justify-content: space-between; align-items: center; gap: 8px; color: #64758b; font-size: 12px; }.rail-progress strong { font-size: 15px; font-weight: 600; color: #294866; font-variant-numeric: tabular-nums; }
.rail-legend { display: flex; gap: 16px; flex-wrap: wrap; margin-top: 20px; color: #547367; font-size: 12px; }.rail-legend span { display: inline-flex; align-items: center; gap: 5px; }.rail-legend span:last-child { color: #a35666; }
@media (min-width: 701px) {
  .building-tabs button { min-height: 60px; border-radius: 8px; }.building-tabs button.active { box-shadow: inset 0 -3px #4481db; }
  .learner-strip { padding: 18px 0; }.learner-strip > div:first-child { border-left: 3px solid #3979d5; padding-left: 12px; }.learner-strip span { font-size: 13px; }
  .practice-layout { grid-template-columns: 204px minmax(0, 1fr); margin-top: 16px; }
  .question-rail { padding: 24px 20px; background: #fafcff; }.question-numbers { gap: 9px; margin: 22px 0; }.question-numbers button { min-height: 36px; border-radius: 7px; font-variant-numeric: tabular-nums; }
  .question-numbers button.selected { outline-offset: 2px; font-weight: 650; }
  .question-body { padding: 28px 34px; }.stem { margin-top: 20px; margin-bottom: 26px; line-height: 1.8; }
  .options label { min-height: 64px; align-items: center; padding: 13px 18px; font-size: 15px; border-radius: 8px; }.options input { margin: 0; }.options label b { display: grid; place-items: center; flex: 0 0 32px; height: 32px; border-radius: 6px; font-size: 14px; background: #eef2f7; font-weight: 600; }
  .options label.checked b { background: #dfeaff; color: #205ba8; }.options label.correct b { background: #dcefe4; color: #247651; }.options label.incorrect b { background: #f9e2e7; color: #ad3a53; }
  .answer-actions { min-height: 44px; margin: 22px 0 18px; }.answer-actions .primary { min-width: 128px; min-height: 42px; box-shadow: 0 3px 8px #2164cf20; }
  .answer-tools { gap: 8px; }.answer-tools button { border-color: transparent; background: #f4f7fc; color: #4b6484; }.answer-tools button:hover:not(:disabled) { background: #eaf2ff; color: #225fb5; }
  .interview-answer textarea { padding: 14px 16px; line-height: 1.9; min-height: 180px; }.interview-answer textarea:focus { border-color: #6a9ee2; box-shadow: 0 0 0 3px #eaf2ff; }
  .question-footer button { min-width: 105px; min-height: 40px; }.question-footer > span { font-variant-numeric: tabular-nums; }
  .people-list button { min-height: 52px; }.people-list button:hover:not(:disabled) { box-shadow: inset 3px 0 #4481db; }
}
@keyframes learning-question-enter { from { opacity: .35; transform: translateY(5px); } to { opacity: 1; transform: translateY(0); } }
.view-switch { display: inline-flex; align-items: center; gap: 6px; padding: 3px; background: #eef2f7; border-radius: 8px; }.view-switch button { display: inline-flex; align-items: center; gap: 5px; padding: 6px 12px; border-radius: 6px; border-color: transparent; background: transparent; color: #4b6484; font-size: 13px; font-weight: 600; }.view-switch button.active { background: #fff; color: #174fab; box-shadow: 0 1px 3px #0d3f8f22; }
.attempt-records { margin: 12px 0; border: 1px solid #dce5ef; border-radius: 8px; padding: 10px 12px; background: #fbfcfe; }.attempt-records summary { cursor: pointer; font-weight: 600; color: #25425f; }.attempt-records ol { margin: 8px 0 0; padding-left: 18px; display: grid; gap: 6px; }.attempt-records li { display: flex; flex-wrap: wrap; align-items: baseline; gap: 8px; line-height: 1.6; }.attempt-records li strong { min-width: 64px; }.attempt-records li time { color: #64758b; font-size: 12px; }.attempt-records .record-answer { color: #25425f; }.attempt-records .danger { color: #ad3a53; font-weight: 600; }.attempt-records .success-text { color: #247651; font-weight: 600; }
@media (prefers-reduced-motion: reduce) { .spin, .question-body, .answer-reference, .result { animation: none; }button, .file-button, a[download], input, textarea, .options label { transition: none; } }
.filters>.search-field{flex:0 1 240px}.filters>.inline-field{flex:0 1 auto}
@media(min-width:701px){.building-tabs button{min-height:44px;padding:9px 12px}.learner-strip{padding:10px 0}.question-body{padding:18px 20px}.stem{margin:12px 0 16px}.options{grid-template-columns:repeat(auto-fit,minmax(min(100%,300px),1fr));gap:8px}.options label{min-height:48px;padding:9px 12px}.question-footer{padding-top:12px;margin-top:12px}.toolbar{margin:10px 0}.tabs{margin-bottom:10px}}
</style>
<style scoped src="../learningMobile.css"></style>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, reactive, ref, watch } from "vue";
import { AlertCircle, ArrowDown, ArrowUp, BookOpen, Check, CheckCircle2, ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight, Copy, Download, Eye, FileText, History, Lightbulb, Loader2, MessageSquare, Pencil, Plus, RefreshCw, RotateCcw, Save, Search, Send, Settings, Shuffle, Star, Trash2, Upload, Users, X, ChartNoAxesCombined } from "lucide-vue-next";
import { ApiError, downloadFile, requestJson } from "../api/client";
import { usePageReadRefresh } from '../api/usePageReadRefresh';
import { requestLearning, type LearningApiOptions } from "../api/learning";
import { beijingDate } from "../workspace-v2/eventPeriod";
import { navigate, registerNavigationGuard } from "../navigation";
import { acquireModal } from "../modalState";
import VnetBackButton from "./VnetBackButton.vue";
import VnetSelect from "./VnetSelect.vue";
import ConfirmDialog from "./ConfirmDialog.vue";
import LearningDashboard from "./LearningDashboard.vue";
import LearningHome from "./LearningHome.vue";
import LearningSharing from "./LearningSharing.vue";
import LearningCompletion from "./LearningCompletion.vue";

const props = defineProps<{ scope?: string; personId?: string; userId: string }>();
type Tab = "home" | "today" | "review" | "history" | "issues" | "profile" | "questions" | "settings" | "sharing" | "completion";
const completionRefresh = ref(0);
const completionReturn = ref<{ person:Dict|null }|null>(null);
const sharingDirty = ref(false);
const bankLabels: Dict = { written: "笔试题库", duty: "值班面试", professional: "专业面试", supplemental: "专项题库" };
const typeLabels: Dict = { single: "单选题", multiple: "多选 / 不定项", interview: "面试题" };
const statusLabels: Dict = { draft: "草稿", published: "已发布", disabled: "已停用", deleted: "已删除" };
const issueLabels: Dict = { pending: "待处理", processing: "处理中", needs_info: "待补充", resolved: "已解决", no_change: "无需修改", withdrawn: "已撤回" };
const categoryLabels: Dict = Object.fromEntries(["题目", "题干", "选项", "答案", "解析", "资料", "适用条件", "其他"].map(v => [v, v]));
const attachmentLabels: Dict = { question: "题面附件", answer: "答案附件", material: "学习资料" };
const ratingLabels: Dict = Object.fromEntries(["需复习", "部分掌握"].map(v => [v, v]));
const reviewLabels: Dict = { mistakes: "个人错题集", wrong: "错题与待复习", favorites: "收藏", notes: "笔记", all: "全部复习" };
const tabs = computed(() => {
    if (viewMode.value === "overview") {
      return [
        { id: "profile", label: learner.value ? "个人画像" : "管理总览", icon: ChartNoAxesCombined },
        { id: "history", label: "学习历史", icon: History }, { id: "issues", label: isAdmin.value ? "问题中心" : "人员质疑", icon: MessageSquare },
        { id: "sharing", label: "分享会练习", icon: Users },
        { id: "completion", label: "每日完成", icon: CheckCircle2 },
        ...(isAdmin.value ? [{ id: "questions", label: "题库管理", icon: FileText }, { id: "settings", label: "发布设置", icon: Settings }] : []),
      ];
    }
    return [
      { id: 'home', label: '学习首页', icon: BookOpen },
      { id: "profile", label: learner.value ? "个人画像" : "楼栋汇总", icon: ChartNoAxesCombined },
      ...(learner.value ? [{ id: "today", label: "今日学练", icon: BookOpen }, { id: "review", label: "错题与复习", icon: Star }] : []),
      { id: "history", label: "学习历史", icon: History }, { id: "issues", label: isAdmin.value ? "问题中心" : "人员质疑", icon: MessageSquare },
      { id: "sharing", label: "分享会练习", icon: Users },
      { id: "completion", label: "每日完成", icon: CheckCircle2 },
      ...(isAdmin.value ? [{ id: "questions", label: "题库管理", icon: FileText }, { id: "settings", label: "发布设置", icon: Settings }] : []),
    ];
  });
const boot = ref<Dict>({});
const ready = ref(false), loading = ref(true), busy = ref(false);
const error = ref(""), notice = ref(""), storageWarning = ref("");
const scope = ref(props.scope || ""), date = ref(""), tab = ref<Tab>("profile");
const isLearningHub = computed(() => tab.value === 'home' || (viewMode.value === 'overview' && tab.value === 'profile' && !learner.value));
const viewParam = ref(new URLSearchParams(window.location.search).get("view") || "");
const learner = ref<Dict | null>(null), legacyHistory = ref(false);
const peopleRows = ref<Dict[]>([]), peopleIssues = ref<Dict[]>([]), peopleSearch = ref(""), peoplePage = ref(1), peopleTotal = ref(0), peopleReady = ref(false), peopleLoading = ref(false);
let readRequest: AbortController | undefined, peopleRequest: AbortController | undefined;
const isAdmin = computed(() => boot.value.is_admin === true);
const scopes = computed<Array<{ value: string; label: string }>>(() => (boot.value.scopes || []).filter((s: Dict) => s.value));
const overviewScopes = computed(() => isAdmin.value ? [{ value: '', label: '全部画像' }, ...scopes.value] : scopes.value);
const allPortrait = computed(() => viewMode.value === 'overview' && isAdmin.value && !scope.value && !learner.value);
const scopeLabel = computed(() => viewMode.value === 'overview' && isAdmin.value && !scope.value ? '全部楼栋' : scopes.value.find(s => s.value === scope.value)?.label || (scope.value ? `${scope.value}楼` : ''));
const canAnswer = computed(() => viewMode.value === "practice" && boot.value.can_answer === true && !!learner.value && !paper.value?.legacy && paper.value?.person_id === boot.value.self_person?.id && learner.value.id === boot.value.self_person?.id);
const canViewBuildings = computed(() => boot.value.can_view_buildings === true);
const identityIssue = computed(() => String(boot.value.identity_issue || ""));
const viewMode = computed<"practice" | "overview">(() => {
  if (boot.value.can_view_buildings === false) return "practice";
  if (boot.value.can_answer === false && boot.value.can_view_buildings === true) return "overview";
  if (viewParam.value === "overview") return "overview";
  if (viewParam.value === "practice") return "practice";
  return boot.value.can_answer === false ? "overview" : "practice";
});
const paper = ref<Dict | null>(null), reading = ref(false), index = ref(0), practice = ref(false);
const studyMode = ref<'daily' | 'practice'>('daily'), selfPracticeOperation = ref(uid());
const current = computed<Dict | null>(() => paper.value?.questions?.[index.value] || null);
const displayedAttempt = computed<Dict | null>(() => current.value?.practice?.at(-1) || current.value?.attempt || null);
const answered = computed(() => (paper.value?.questions || []).filter((q: Dict) => q.attempt && !q.invalid && !q.needs_review).length);
const validCount = computed(() => (paper.value?.questions || []).filter((q: Dict) => !q.invalid).length);
const locked = computed(() => !canAnswer.value || !!current.value?.invalid || (!!current.value?.attempt && !practice.value));
const draft = reactive({ option_ids: [] as string[], answer_text: "", self_rating: "", operation_id: "" });
const note = ref(""), noteSaved = ref(""), noteDirty = computed(() => note.value !== noteSaved.value);
const draftRestored = ref(false);
let draftLoading = false;
const pages = reactive<Record<string, number>>({ review: 1, history: 1, issues: 1, profile: 1, questions: 1 });
const rows = ref<Dict[]>([]), total = ref(0), profile = ref<Dict>({});
const homeProgressError = ref(false);
const profileSection = ref("topics");
const profileSections = computed(() => [...(isAdmin.value ? [{ value: "buildings", label: "六楼进度" }] : []), { value: "topics", label: "知识点薄弱项" }, ...(isAdmin.value ? [{ value: "questions", label: "题目统计" }] : [])]);
const profileRows = computed<Dict[]>(() => profileSection.value === "buildings" ? (boot.value.scopes || []).filter((s: Dict) => s.value && (!scope.value || s.value === scope.value)).map((s: Dict) => (profile.value.buildings || []).find((b: Dict) => b.scope === s.value) || { scope: s.value, assigned: 0, answered: 0 }) : profile.value[profileSection.value] || []);
const visibleProfileRows = computed(() => profileRows.value.slice((pages.profile - 1) * 20, pages.profile * 20));
const filters = reactive({ search: "", status: "", bank: "", problems: false, review: "mistakes", reviewBank: "", issue: "", from: "", to: "", period: "7" });
const selected = ref<string[]>([]);
const pageNumber = computed(() => pages[tab.value] || 1);
const pageCount = computed(() => Math.max(1, Math.ceil(total.value / 20)));
const jumpPage = ref<number>(1);
const settingsForm = reactive<Dict>({ enabled: false, publish_time: "13:00", night_publish_time: "20:00", reminder_enabled: false, reminder_time: "17:00", portal_url: "", timeout_reminder_enabled: true, weekly_report_enabled: true });
let settingsSnapshot = "";
const syncActive = computed(() => boot.value.sync?.active ?? ["syncing", "publishing"].includes(boot.value.sync?.status));
const jobRunning = computed(() => ["syncing", "publishing"].includes(boot.value.sync?.status));
const syncLabel = computed(() => {
  if (!syncActive.value && Number(boot.value.sync?.pending || 0) > 0) return boot.value.sync?.error || boot.value.sync?.waiting ? '等待重试' : '等待同步';
  return ({ idle: "未初始化", syncing: "同步中", publishing: "发布中", ready: "已同步", error: "同步失败", pending: '等待同步', retry_waiting: '等待重试' }[String(boot.value.sync?.status)] || "等待同步");
});
const publishShifts = [{ key: '白', field: 'publish_time', label: '白班题单发布时间' }, { key: '夜', field: 'night_publish_time', label: '夜班题单发布时间' }];
function shiftStatus(shift: string): Dict { return boot.value.delivery_status?.shifts?.find((row: Dict) => row.shift === shift) || {}; }
let epoch = 0, disposed = false, pollTimer: number | undefined, todayTimer: number | undefined, pollFailures = 0;
let onPopstate: (() => void) | undefined;
const lifetime = new AbortController();
const modalKind = ref<"" | "question" | "issue" | "import" | "attachment" | "people">("");
const modalElement = ref<HTMLElement | null>(null), editor = ref<Dict>({}), issue = ref<Dict>({});
const editorSnapshot = ref(""), issueSnapshot = ref("");
const conflicted = ref(false);
const issueComment = ref(""), issueNextStatus = ref("");
const issueStatusOptions = computed(() => isAdmin.value ? Object.values(issueLabels).filter(v => v !== '已撤回') : [...new Set([issueLabels[issue.value.status] || '待处理', ...(issue.value.status === 'pending' ? ['已撤回'] : []), ...(['resolved', 'no_change'].includes(issue.value.status) ? ['待处理'] : [])])]);
const attachmentKind = ref("question"), uploadFiles = ref<File[]>([]), preview = ref<Dict>({});
const importRows = ref<Array<{ question: Dict; errors: string[] }>>([]), importName = ref(""), importPage = ref(1);
const importErrors = computed(() => importRows.value.filter(r => r.errors.length).length);
const importVisible = computed(() => importRows.value.slice((importPage.value - 1) * 20, importPage.value * 20));
const confirm = reactive({ open: false, title: "", message: "", tone: "primary" as "primary" | "danger" | "warning" });
let resolveConfirm: ((value: boolean) => void) | undefined;
let modalOwner: ReturnType<typeof acquireModal> | undefined, returnFocus: HTMLElement | null = null;
let releaseGuard: (() => void) | undefined;

function uid(): string { return globalThis.crypto?.randomUUID?.() || `${Date.now()}-${Math.random().toString(36).slice(2)}-${Math.random().toString(36).slice(2)}`; }
function copy<T>(value: T): T { return JSON.parse(JSON.stringify(value)); }
const QUESTION_META_KEYS = ["year", "topic", "specialty", "difficulty", "reason"];
function withoutQuestionMetadata(q: Dict): Dict {
  const clean = copy(q);
  for (const key of QUESTION_META_KEYS) delete clean[key];
  return clean;
}
function keyFor(labels: Dict, label: string): string { return Object.keys(labels).find(k => labels[k] === label) || ""; }
function percent(value: unknown): string { return value === undefined || value === null ? "数据不足" : `${Number(value).toFixed(1)}%`; }
function chartPercent(value: unknown): number { const n = Number(value); return value == null || !Number.isFinite(n) ? 0 : Math.min(100, Math.max(0, n)); }
function completionRate(row: Dict): number | null { const value = row.summary || row; return value.completion_rate ?? (Number(value.assigned) > 0 ? Number(value.answered || 0) * 100 / Number(value.assigned) : null); }
function timeLabel(value: unknown): string {
  if (!value) return "";
  const d = new Date(typeof value === "number" && value < 1e12 ? value * 1000 : value as string);
  return Number.isNaN(d.getTime()) ? String(value) : d.toLocaleString("zh-CN", { hour12: false });
}
function questionId(q: Dict): string { return String(q.question_id || q.id); }
function answerLabels(ids: string[] = [], q: Dict = current.value || {}): string {
  return ids.map(id => { const position = (q.options || []).findIndex((o: Dict) => o.id === id); return position < 0 ? id : String.fromCharCode(65 + position); }).join("、");
}
function resultText(q: Dict, attempt: Dict | null = q.attempt): string {
  if (q.invalid) return "已失效，不计入评价";
  if (!attempt) return "未作答";
  if (q.type === "interview") return ratingLabels[attempt.self_rating] || "已提交";
  return attempt.correct === true ? "回答正确" : "回答错误";
}
function attemptRecords(q: Dict): Array<{ label: string; submitted_at: unknown; correct: boolean | null | undefined; resultText: string; selectedText: string }> {
  const records: Array<{ label: string; submitted_at: unknown; correct: boolean | null | undefined; resultText: string; selectedText: string }> = [];
  const selectedText = (a: Dict): string => {
    if (q.type === "interview") return String(a.answer_text || "");
    return answerLabels(a.option_ids || [], q);
  };
  if (q.attempt) {
    records.push({
      label: "首次作答",
      submitted_at: q.attempt.submitted_at,
      correct: q.attempt.correct,
      resultText: resultText(q),
      selectedText: selectedText(q.attempt),
    });
  }
  const practice = Array.isArray(q.practice) ? q.practice : [];
  practice.forEach((a: Dict, n: number) => {
    records.push({
      label: `复习 ${n + 1}`,
      submitted_at: a.submitted_at,
      correct: a.correct,
      resultText: q.type === "interview" ? ratingLabels[a.self_rating] || "已提交" : (a.correct === true ? "回答正确" : "回答错误"),
      selectedText: selectedText(a),
    });
  });
  return records;
}
function shortageText(value: unknown): string {
  if (!value) return "";
  if (typeof value === "number") return value > 0 ? `题库不足，缺 ${value} 题` : "";
  if (typeof value === "object") return Object.entries(value).filter(([, n]) => Number(n) > 0).map(([k, n]) => `${bankLabels[k] || typeLabels[k] || k}缺 ${n} 题`).join("；");
  return String(value);
}
const learningOptions: LearningApiOptions = {
  signal: lifetime.signal,
  onSyncPending: () => {
    boot.value.sync = { ...boot.value.sync, pending: Math.max(1, Number(boot.value.sync?.pending || 0)) };
    schedulePoll();
  },
};
async function perform(action: () => Promise<void>, message = ""): Promise<void> {
  if (busy.value) return;
  busy.value = true; error.value = ""; notice.value = "";
  try { await action(); if (message) notice.value = message; void refreshSync(); }
  catch (e) { if (!disposed) { error.value = e instanceof Error ? e.message : "操作失败，请重试"; if (e instanceof ApiError && e.status === 409) conflicted.value = true; } }
  finally { busy.value = false; }
}
function ask(title: string, message: string, tone: "primary" | "danger" | "warning" = "warning"): Promise<boolean> {
  Object.assign(confirm, { open: true, title, message, tone });
  return new Promise(resolve => { resolveConfirm = resolve; });
}
function confirmed(value: boolean): void { confirm.open = false; resolveConfirm?.(value); resolveConfirm = undefined; }
function draftKey(q = current.value, p = paper.value): string {
  const user = props.userId || boot.value.user_id;
  return user && q && p ? `learning:draft:${encodeURIComponent(user)}:${encodeURIComponent(p.person_id || "legacy")}:${encodeURIComponent(p.id)}:${encodeURIComponent(questionId(q))}` : "";
}
function localDraft(action: "read" | "write" | "remove", key: string, value?: Dict): Dict | null {
  if (!key) return null;
  try {
    if (action === "remove") localStorage.removeItem(key);
    if (action === "write") localStorage.setItem(key, JSON.stringify(value));
    return action === "read" ? JSON.parse(localStorage.getItem(key) || "null") : null;
  } catch { storageWarning.value = "浏览器无法保存草稿，请保持页面打开并及时提交。"; return null; }
}
function saveDraft(): void {
  if (draftLoading || !current.value || !canAnswer.value) return;
  localDraft("write", draftKey(), { version: current.value.version, ...((!current.value.attempt || practice.value) && !current.value.invalid ? copy(draft) : {}), practice: practice.value, attempt_submitted_at: current.value.attempt?.submitted_at || null, note: note.value, saved_note: noteSaved.value });
}
const startRequests = new Set<string>();
const startWarning = ref('');
async function markStarted(): Promise<void> {
  const target = paper.value;
  if (disposed || !canAnswer.value || !target || target.started_at || target.status === 'completed' || startRequests.has(target.id)) return;
  startRequests.add(target.id);
  try {
    const result = await requestLearning(learningOptions, `/papers/${encodeURIComponent(target.id)}/start`, 'POST', { person_id: target.person_id });
    if (!disposed && paper.value && paper.value.id === target.id) { paper.value.started_at = result.started_at; startWarning.value = ''; }
  } catch {
    if (!disposed && paper.value?.id === target.id) startWarning.value = '作答开始时间尚未同步，提醒暂未计时；本机草稿已保留。';
  } finally { startRequests.delete(target.id); }
}
watch([() => [...draft.option_ids], () => draft.answer_text], () => {
  if (!draftLoading && (draft.option_ids.length || draft.answer_text.trim())) void markStarted();
}, { flush: 'sync' });
function hydrateQuestion(): void {
  startWarning.value = '';
  draftLoading = true; practice.value = false; draftRestored.value = false;
  const q = current.value;
  const attempt = displayedAttempt.value;
  Object.assign(draft, { option_ids: [...(attempt?.option_ids || [])], answer_text: attempt?.answer_text || "", self_rating: attempt?.self_rating || "", operation_id: uid() });
  note.value = typeof q?.note === "string" ? q.note : q?.notes?.note || ""; noteSaved.value = note.value;
  const saved = localDraft("read", draftKey());
  if (saved && q && !q.invalid && String(saved.version) === String(q.version)) {
    const savedPractice = saved.practice === true && q.attempt && saved.attempt_submitted_at === q.attempt.submitted_at;
    if (!q.attempt || savedPractice) {
      practice.value = !!savedPractice;
      draft.option_ids = Array.isArray(saved.option_ids) ? saved.option_ids.filter((id: string) => q.options?.some((o: Dict) => o.id === id)) : [];
      draft.answer_text = String(saved.answer_text || ""); draft.self_rating = String(saved.self_rating || ""); draft.operation_id = saved.operation_id || uid();
      draftRestored.value = !!(draft.option_ids.length || draft.answer_text);
    }
    if (saved.saved_note === noteSaved.value && typeof saved.note === "string") note.value = saved.note;
  } else if (saved && q) localDraft("remove", draftKey());
  draftLoading = false;
}
watch([() => [...draft.option_ids], () => draft.answer_text, () => draft.self_rating, () => draft.operation_id, note], saveDraft, { flush: "sync" });
function selectQuestion(position: number): void {
  if (busy.value || position < 0 || position >= (paper.value?.questions.length || 0)) return;
  saveDraft(); index.value = position; hydrateQuestion();
  void nextTick(() => {
    const rail = document.querySelector<HTMLElement>('.learning-page:not([inert]) .question-numbers');
    const button = rail?.querySelector<HTMLElement>('[aria-current="step"]');
    if (!rail || !button || rail.scrollWidth <= rail.clientWidth) return;
    const area = rail.getBoundingClientRect(), target = button.getBoundingClientRect();
    const left = target.right > area.right ? target.right - area.right + 4 : target.left < area.left ? target.left - area.left - 4 : 0;
    rail.scrollBy({ left, behavior: matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth' });
  });
}
function chooseOption(id: string): void {
  if (locked.value || busy.value) return;
  draft.option_ids = current.value?.type === "single" ? [id] : draft.option_ids.includes(id) ? draft.option_ids.filter(v => v !== id) : [...draft.option_ids, id];
  draft.operation_id = uid();
}
function startPractice(): void {
  practice.value = true;
  Object.assign(draft, { option_ids: [], answer_text: "", self_rating: "", operation_id: uid() });
  notice.value = "本次复习单独记录，首次作答保留。";
}
async function submitAnswer(): Promise<void> {
  if (!current.value || !paper.value || locked.value) return;
  if (current.value.type === "interview" ? !draft.answer_text.trim() || !draft.self_rating : !draft.option_ids.length) { error.value = current.value.type === "interview" ? "请填写面试回答并选择自评。" : "请选择答案。"; return; }
  const qid = questionId(current.value), key = draftKey();
  const pendingNote = noteDirty.value ? note.value : null;
  await perform(async () => {
    try {
      const updated = await requestLearning(learningOptions,`/papers/${encodeURIComponent(paper.value!.id)}/answer`, "POST", { person_id: learner.value!.id, question_id: qid, ...copy(draft), version: paper.value!.version, practice: practice.value });
      paper.value = updated; localDraft("remove", key); hydrateQuestion();
      if (pendingNote !== null) note.value = pendingNote;
      notice.value = updated.sync_pending ? "作答已确认，等待云端同步。" : "作答已确认。";
    } catch (e) {
      if (e instanceof ApiError && e.status === 409) {
        const updated = await requestLearning(learningOptions,`/papers/${encodeURIComponent(paper.value!.id)}`);
        paper.value = updated;
        if (current.value?.attempt) { hydrateQuestion(); notice.value = "该题已有已确认作答，已显示最新记录。"; }
        else if (current.value?.invalid) { hydrateQuestion(); notice.value = "此题已失效，原草稿已停止使用。"; }
        else { notice.value = "题单已更新，输入保留，请核对后重试。"; }
        if (pendingNote !== null) note.value = pendingNote;
      } else throw e;
    }
  });
}
async function reveal(kind: string): Promise<void> {
  if (!paper.value || !current.value || !canAnswer.value) return;
  await perform(async () => {
    const hadAttempt = !!current.value?.attempt;
    paper.value = await requestLearning(learningOptions,`/papers/${encodeURIComponent(paper.value!.id)}/reveal`, "POST", { person_id: learner.value!.id, question_id: questionId(current.value!), kind });
    if (!hadAttempt && current.value?.attempt) { hydrateQuestion(); notice.value = "该题已有已确认作答，已显示最新记录。"; }
  });
}
async function saveNotes(extra: Dict = {}): Promise<void> {
  if (!canAnswer.value || !paper.value || !current.value) return;
  await perform(async () => {
    paper.value = await requestLearning(learningOptions,`/papers/${encodeURIComponent(paper.value!.id)}/notes`, "POST", { person_id: learner.value!.id, version: paper.value!.version, question_id: questionId(current.value!), note: note.value, favorite: !!current.value!.favorite, ...extra });
    noteSaved.value = note.value; saveDraft();
  }, "学习记录已保存。");
}
async function loadPaper(id: string, qid = ""): Promise<void> {
  const token = ++epoch; loading.value = true; error.value = ""; saveDraft();
  try {
    const data = await requestLearning(learningOptions,`/papers/${encodeURIComponent(id)}`);
    if (token !== epoch || disposed) return;
    paper.value = data; if (data.person_id) learner.value = data.person; reading.value = true;
    index.value = Math.max(0, (data.questions || []).findIndex((q: Dict) => questionId(q) === qid)); hydrateQuestion();
  } catch (e) { if (token === epoch) error.value = String((e as Error).message); }
  finally { if (token === epoch) loading.value = false; }
}
async function openCompletionPaper(value:{paperId:string;person:Dict;date:string}):Promise<void>{
  if(busy.value)return;
  completionReturn.value={person:learner.value};
  saveDraft();tab.value='history';reading.value=false;notice.value='';
  filters.from=value.date;filters.to=value.date;
  await loadPaper(value.paperId);
}
function returnToCompletion():void{
  saveDraft();learner.value=completionReturn.value?.person||null;completionReturn.value=null;
  reading.value=false;tab.value='completion';
}
async function deletePaper(id: string): Promise<void> {
  if (!isAdmin.value || !await ask("删除已发布题单", "删除后该人员不再显示或统计这份题单，已有作答保留审计；当天不会自动重新出题。确认删除？", "danger")) return;
  await perform(async () => {
    await requestLearning(learningOptions, `/papers/${encodeURIComponent(id)}`, "DELETE", {});
    if (paper.value?.id === id) { saveDraft(); paper.value = null; reading.value = false; }
    await loadView();
  }, "题单已删除。该人员将从次日新题单继续学习。");
}
function query(extra: Dict = {}): string {
  return new URLSearchParams(Object.entries({ scope: scope.value, person_id: learner.value?.id, page: pageNumber.value, page_size: 20, ...extra }).filter(([, v]) => v !== "" && v !== undefined).map(([k, v]) => [k, String(v)])).toString();
}
async function loadView(): Promise<void> {
  if (tab.value === 'sharing' || tab.value === 'completion') { ++epoch; readRequest?.abort(); loading.value = false; return; }
  const token = ++epoch; readRequest?.abort(); readRequest = new AbortController(); const readOptions = { signal: readRequest.signal }; loading.value = true; error.value = ""; selected.value = [];
  try {
    if (tab.value === "today") {
      if (!learner.value) { paper.value = null; return; }
      saveDraft(); paper.value = null;
      const data = await requestLearning(readOptions,`/papers?${query({ today: 1, mode: studyMode.value })}`);
      if (token !== epoch) return;
      date.value = data.today || date.value;
      const p = data.items?.[0];
      const nextPaper = p ? (p.questions ? p : await requestLearning(readOptions,`/papers/${encodeURIComponent(p.id)}`)) : null;
      if (token !== epoch) return;
      saveDraft(); paper.value = nextPaper; index.value = 0; hydrateQuestion();
    } else if (tab.value === "settings") {
      const data = await requestLearning(readOptions,"/settings");
      if (token === epoch) { Object.assign(settingsForm, data); settingsSnapshot = JSON.stringify(settingsForm); }
    } else {
      const extra = tab.value === "questions" ? { search: filters.search, status: filters.status, bank: filters.bank, problems: filters.problems ? 1 : "" }
        : tab.value === "review" ? { kind: filters.review, bank: filters.reviewBank, search: filters.search }
        : tab.value === "issues" ? { status: filters.issue, search: filters.search }
        : { from: filters.from, to: filters.to, period: filters.period, ...(tab.value === 'profile' && allPortrait.value ? { all_people: 1 } : {}), ...(tab.value === "history" && legacyHistory.value ? { legacy: 1, person_id: "" } : {}) };
      const data = await requestLearning(readOptions,`/${tab.value === 'home' ? 'profile' : tab.value}?${query(tab.value === 'home' ? {period:'7'} : extra)}`);
      if (token !== epoch) return;
      if (tab.value === "profile" || tab.value === 'home') { profile.value = data; if (learner.value && data.person) learner.value = data.person; }
      else { rows.value = data.items || []; total.value = Number(data.total ?? rows.value.length); if (data.page) pages[tab.value] = data.page; }
      if (tab.value === 'home') {
        homeProgressError.value = false;
        if (/^\d{4}-\d{2}-\d{2}$/.test(String(data.to || ''))) date.value = data.to;
      }
    }
  } catch (e) { if (token === epoch && !disposed) { error.value = String((e as Error).message); if (tab.value === 'home') homeProgressError.value = true; } }
  finally { if (token === epoch) loading.value = false; }
}
async function switchTab(value: Tab): Promise<void> {
  if (busy.value || value === tab.value) return;
  if (tab.value === 'sharing' && sharingDirty.value && !await ask('离开分享会练习', '题目修改尚未保存，确认放弃？')) return;
  sharingDirty.value = false;
  if (tab.value === "settings" && JSON.stringify(settingsForm) !== settingsSnapshot && !await ask("离开发布设置", "尚有未保存的设置，确认放弃？")) return;
  saveDraft(); rows.value = []; total.value = 0; tab.value = value; reading.value = false; notice.value = "";
  if (value === "today" && !scope.value) scope.value = boot.value.scopes?.find((s: Dict) => s.value)?.value || "";
  if (value === 'today' && viewMode.value === 'practice') await loadTodayPaper();
  else await loadView();
  schedulePoll();
}
async function openLearning(value:string): Promise<void> {
  if (busy.value || loading.value) return;
  if (value === 'practice' || value === 'today') {
    saveDraft(); studyMode.value = value === 'practice' ? 'practice' : 'daily';
    await loadTodayPaper();
  } else if (tabs.value.some(item=>item.id===value)) await switchTab(value as Tab);
}
async function changeScope(label: string): Promise<void> {
  const next = overviewScopes.value.find(s => s.label === label)?.value;
  if (next === undefined || busy.value) return;
  saveDraft(); scope.value = next; reading.value = false; paper.value = null; learner.value = null; tab.value = "profile"; profile.value = {};
  for (const key of Object.keys(pages)) pages[key] = 1;
  const params = new URLSearchParams({ scope: next });
  if (viewMode.value === "overview") params.set("view", "overview");
  navigate("/learning?" + params);
}
async function openPeople(): Promise<void> {
  if (busy.value) return;
  peopleSearch.value = ""; peoplePage.value = 1; modalKind.value = "people"; await loadPeople();
}
async function loadPeople(): Promise<void> {
  peopleRequest?.abort(); const request = new AbortController(); peopleRequest = request; peopleLoading.value = true;
  try { const data = await requestLearning({ signal: request.signal }, "/people?" + new URLSearchParams({ scope: scope.value, q: peopleSearch.value, page: String(peoplePage.value), page_size: "20" }));
    if (request.signal.aborted) return;
    peopleRows.value = data.items || []; peopleTotal.value = data.total; peopleReady.value = data.ready; peopleIssues.value = data.issues || [];
  } catch (e) { if (!request.signal.aborted && !disposed) error.value = (e as Error).message; }
  finally { if (peopleRequest === request) peopleLoading.value = false; }
}
async function selectLearner(person: Dict): Promise<void> {
  if (busy.value) return;
  saveDraft(); modalKind.value = "";
  const params = new URLSearchParams({ scope: scope.value, person_id: person.id });
  if (viewMode.value === "overview") params.set("view", "overview");
  navigate(`/learning?${params}`);
}
// Profile back returns to its building, not the previous person's browser-history entry.
function handleBack(): void {
  saveDraft();
  if (viewMode.value === "overview" && learner.value?.id) {
    navigate(`/learning?view=overview&scope=${encodeURIComponent(scope.value)}`);
  } else {
    navigate("/?entry=special");
  }
}
function goPractice(): void {
  if (busy.value || viewMode.value === "practice") return;
  navigate("/learning?view=practice&tab=home");
}
function goOverview(): void {
  if (busy.value || viewMode.value === "overview") return;
  navigate(`/learning?view=overview${scope.value ? `&scope=${encodeURIComponent(scope.value)}` : ""}`);
}
async function continueLearning(): Promise<void> {
  if (!learner.value || busy.value || loading.value || viewMode.value !== "practice") return;
  saveDraft();
  await loadTodayPaper();
}
async function enterPracticeToday(): Promise<void> {
  if (viewMode.value !== "practice" || !boot.value.self_person || boot.value.can_answer !== true) return;
  await loadTodayPaper();
}
async function switchStudyMode(mode: 'daily' | 'practice'): Promise<void> {
  if (busy.value || loading.value || studyMode.value === mode) return;
  saveDraft(); studyMode.value = mode; paper.value = null;
  await loadTodayPaper();
}
async function startSelfPractice(): Promise<void> {
  if (busy.value || loading.value || viewMode.value !== 'practice' || !boot.value.can_answer || !learner.value) return;
  saveDraft();
  await perform(async () => {
    const data = await requestLearning(learningOptions, '/papers/claim', 'POST', { mode: 'practice', scope: scope.value, person_id: learner.value!.id, operation_id: selfPracticeOperation.value });
    epoch++; paper.value = data; date.value = data.date; studyMode.value = 'practice'; tab.value = 'today'; reading.value = true; index.value = 0;
    selfPracticeOperation.value = uid(); hydrateQuestion();
  }, '自主练习已生成，开始作答。');
}
async function loadTodayPaper(): Promise<void> {
  if (!learner.value || viewMode.value !== "practice" || busy.value) return;
  const token = ++epoch;
  loading.value = true; error.value = "";
  try {
    const data = await requestLearning(learningOptions, `/papers?` + query({ today: 1, mode: studyMode.value }));
    if (token !== epoch) return;
    date.value = data.today || date.value;
    let p = data.items?.[0] || null;
    if (!p && studyMode.value === 'daily' && data.published && boot.value.can_answer === true) {
      p = await requestLearning(learningOptions, "/papers/claim", "POST", { scope: scope.value, person_id: learner.value.id });
    }
    if (token !== epoch) return;
    if (p && !p.questions) p = await requestLearning(learningOptions, `/papers/${encodeURIComponent(p.id)}`);
    if (token !== epoch) return;
    paper.value = p; tab.value = "today"; reading.value = true; index.value = Math.max(0,(p?.questions || []).findIndex((q:Dict)=>!q.invalid && (!q.attempt || q.needs_review))); hydrateQuestion();
  } catch (e) { if (token === epoch && !disposed) error.value = String((e as Error).message); }
  finally { if (token === epoch) loading.value = false; }
}
function filterChanged(): void { pages[tab.value] = 1; void loadView(); }
function clearFilters(): void {
  if (tab.value === "review") { filters.review = "mistakes"; filters.reviewBank = ""; filters.search = ""; }
  else if (tab.value === "history") { filters.from = ""; filters.to = ""; }
  else if (tab.value === "issues") { filters.issue = ""; filters.search = ""; }
  else if (tab.value === "questions") { filters.search = ""; filters.status = ""; filters.bank = ""; filters.problems = false; }
  filterChanged();
}
async function refreshView(): Promise<void> {
  if (!ready.value) { await bootstrap(); return; }
  if (tab.value === 'completion') { completionRefresh.value++; return; }
  if (tab.value === "settings" && JSON.stringify(settingsForm) !== settingsSnapshot && !await ask("刷新设置", "尚有未保存的设置，确认放弃并重新读取？")) return;
  if (tab.value === 'today' && viewMode.value === 'practice' && !paper.value) await loadTodayPaper();
  else if (reading.value && paper.value && current.value) await loadPaper(paper.value.id, questionId(current.value));
  else await loadView();
  void refreshSync();
}
function setPeriod(value: string): void { filters.period = value; filters.from = ""; filters.to = ""; pages.profile = 1; }
async function refreshSync(): Promise<void> {
  if (!ready.value || disposed) return;
  try {
    const data = await requestJson(`/api/learning/bootstrap?scope=${encodeURIComponent(scope.value)}`, { signal: lifetime.signal, timeoutMs: 10000, cache: "no-store" });
    if (disposed) return;
    boot.value.sync = data.sync; boot.value.summary = data.summary; boot.value.question_problem_count = data.question_problem_count; pollFailures = 0; schedulePoll();
  } catch { if (!disposed) notice.value = "操作已保存，暂时无法读取云端同步状态，请稍后刷新。"; }
}
function turnPage(change: number): void { if (busy.value || loading.value) return; gotoPage(pageNumber.value + change); }
function gotoPage(target: number): void {
  if (busy.value || loading.value) return;
  const next = Math.min(pageCount.value, Math.max(1, Math.floor(Number(target) || 1)));
  jumpPage.value = next;
  if (Number.isNaN(next)) return;
  if (next === pageNumber.value) return;
  pages[tab.value] = next;
  void loadView();
}
async function bootstrap(): Promise<void> {
    loading.value = true; error.value = "";
    try {
      boot.value = await requestLearning(learningOptions,`/bootstrap${scope.value ? `?scope=${encodeURIComponent(scope.value)}` : ""}`);
      viewParam.value = new URLSearchParams(window.location.search).get("view") || viewParam.value;
      if (viewMode.value === "practice") {
        learner.value = boot.value.self_person || null;
        scope.value = boot.value.self_scope || scope.value || "";
      } else {
        scope.value = overviewScopes.value.some(s => s.value === scope.value) ? scope.value : boot.value.scope || scopes.value[0]?.value || "";
        learner.value = props.personId ? { id: props.personId } : null;
        if (learner.value && boot.value.self_person?.id === learner.value.id) learner.value = boot.value.self_person;
      }
      date.value = typeof boot.value.today === "string" ? boot.value.today : boot.value.today?.date || beijingDate();
      Object.assign(settingsForm, boot.value.settings || {}); settingsSnapshot = JSON.stringify(settingsForm);
      ready.value = true;
      if (['sharing', 'completion'].includes(new URLSearchParams(window.location.search).get('tab') || '')) {
        tab.value = new URLSearchParams(window.location.search).get('tab') as Tab; loading.value = false;
      } else if (viewMode.value === "practice" && boot.value.can_answer === true && learner.value?.id && boot.value.self_scope) {
        const entry = new URLSearchParams(window.location.search);
        if (entry.get('tab') === 'review') { tab.value = 'review'; await loadView(); }
        else if (entry.get('paper_id')) { tab.value = 'history'; await loadPaper(entry.get('paper_id')!); }
        else if (entry.has('date') || entry.has('shift')) {
          const claimed = await requestLearning(learningOptions, '/papers/claim', 'POST', {
            scope: scope.value, person_id: learner.value.id, date: entry.get('date'), shift: entry.get('shift'),
          });
          tab.value = 'history'; await loadPaper(claimed.id);
        }
        else if (entry.get('tab') === 'home' || !entry.get('view')) { tab.value = 'home'; await loadView(); }
        else await enterPracticeToday();
      } else if (viewMode.value === 'overview') {
        const entry = new URLSearchParams(window.location.search);
        const from = entry.get('from'), to = entry.get('to');
        if (from && to && /^\d{4}-\d{2}-\d{2}$/.test(from) && /^\d{4}-\d{2}-\d{2}$/.test(to)) {
          Object.assign(filters, { from, to, period: '' });
        }
        await loadView();
      }
      schedulePoll();
    } catch (e) { error.value = String((e as Error).message); }
    finally { loading.value = false; }
  }
function schedulePoll(): void {
  window.clearTimeout(pollTimer);
  if (disposed || (!syncActive.value && !Number(boot.value.sync?.pending || 0) && tab.value !== 'settings') || pollFailures >= 3) return;
  pollTimer = window.setTimeout(async () => {
    if (document.hidden) { schedulePoll(); return; }
    try {
      const data = await requestLearning(learningOptions,`/bootstrap?scope=${encodeURIComponent(scope.value)}&refresh=1`);
      boot.value.sync = data.sync; boot.value.delivery_status = data.delivery_status; boot.value.summary = data.summary; boot.value.question_problem_count = data.question_problem_count; pollFailures = 0;
      if (!syncActive.value && !paper.value && tab.value === "today") await loadView();
    } catch (e) { pollFailures++; if (pollFailures >= 3) error.value = `同步状态读取失败，请手动刷新。${(e as Error).message}`; }
    schedulePoll();
  }, (syncActive.value ? 4000 : 30000) + pollFailures * 3000);
}
async function checkToday(): Promise<void> {
  if (disposed || document.hidden || !learner.value || busy.value || loading.value) return;
  if (tab.value === 'home') {
    if (date.value !== beijingDate()) await loadView();
    return;
  }
  if (tab.value !== 'today') return;
  const token = epoch;
  try {
    const data = await requestLearning(learningOptions, `/papers?${query({ today: 1, mode: studyMode.value, refresh: 1 })}`);
    if (disposed || token !== epoch || tab.value !== "today" || busy.value || loading.value) return;
    const day = data.today || date.value;
    const changed = day !== date.value || data.items?.[0]?.id !== paper.value?.id;
    if (changed) { saveDraft(); date.value = day; paper.value = data.items?.[0] || null; index.value = 0; hydrateQuestion(); }
  } catch { return; }
}
usePageReadRefresh(url => url.pathname === '/api/learning/papers', checkToday, () => !disposed && tab.value === 'today' && !loading.value && !busy.value);
async function runJob(kind: "refresh" | "publish"): Promise<void> {
  if (kind === "publish" && !boot.value.silent_manual_publish) { error.value = "请重启主程序后使用静默发布，当前后端仍为旧版本。"; return; }
  if (kind === "publish" && !await ask("手动发布今日题单", `${boot.value.today}，为六楼生成今日题单，不发送飞书消息；已发布题单保持不变。`)) return;
  await perform(async () => {
    const result = await requestLearning(learningOptions,`/${kind}`, "POST", kind === "publish" ? { notify: false } : {});
    boot.value.sync = result.sync || result;
    boot.value = await requestLearning(learningOptions,`/bootstrap?scope=${encodeURIComponent(scope.value)}`);
    pollFailures = 0; schedulePoll(); if (tab.value !== "settings") await loadView();
    notice.value = kind === "refresh" ? "题库同步请求已提交。" : result.already_published ? "今日题单已发布，本次未重新生成或发送通知。" : "手动发布请求已提交，不发送飞书消息。";
  });
}
async function saveSettings(): Promise<void> {
  if (![settingsForm.publish_time, settingsForm.night_publish_time, settingsForm.reminder_time].every(value => /^(?:[01]\d|2[0-3]):[0-5]\d$/.test(value))) { error.value = "请填写有效时间。"; return; }
  if (settingsForm.enabled && !boot.value.settings?.enabled && !await ask("启用每日自动发布", "启用后将按设置定时发布题单并发送通知。确认启用？")) return;
  await perform(async () => {
    const payload = { enabled: settingsForm.enabled, publish_time: settingsForm.publish_time, night_publish_time: settingsForm.night_publish_time, reminder_enabled: settingsForm.reminder_enabled, reminder_time: settingsForm.reminder_time, timeout_reminder_enabled: settingsForm.timeout_reminder_enabled, weekly_report_enabled: settingsForm.weekly_report_enabled };
    const result = await requestLearning(learningOptions,"/settings", "PUT", payload);
    Object.assign(settingsForm, result); settingsSnapshot = JSON.stringify(settingsForm);
    boot.value.settings = copy(settingsForm); boot.value = await requestLearning(learningOptions,`/bootstrap?scope=${encodeURIComponent(scope.value)}`); schedulePoll();
  }, "设置已保存。");
}
async function exportData(kind: string): Promise<void> {
  await perform(async () => {
    if (kind === "questions") downloadJson(await requestLearning(learningOptions,`/export?${query({ kind, bank: filters.bank, status: filters.status, search: filters.search, problems: filters.problems ? 1 : "" })}`), "学练题库.json");
    else {
      const reportQuery: Dict = { kind, from: filters.from, to: filters.to };
      if (tab.value === "profile") reportQuery.period = filters.period;
      await downloadFile(`/api/learning/export?${query(reportQuery)}`);
    }
  });
}

const modalDirty = computed(() => uploadFiles.value.length > 0 || (modalKind.value === "question" && JSON.stringify(editor.value) !== editorSnapshot.value) || (modalKind.value === "issue" && (issueComment.value.trim() || (!issue.value.id && JSON.stringify(issue.value) !== issueSnapshot.value))));
async function closeModal(): Promise<void> {
  if (busy.value) return;
  if (modalDirty.value && !await ask("关闭窗口", "尚有未保存内容或未上传附件，确认放弃？")) return;
  modalKind.value = ""; uploadFiles.value = []; error.value = "";
}
async function scrollModalError(): Promise<void> {
  await nextTick();
  const scrollEl = modalElement.value?.querySelector<HTMLElement>(".modal-scroll");
  scrollEl?.scrollTo({ top: 0, behavior: "auto" });
}
async function editQuestion(id = ""): Promise<void> {
  await perform(async () => {
    editor.value = withoutQuestionMetadata(id ? await requestLearning(learningOptions,`/questions/${encodeURIComponent(id)}`) : { new_id: "q_" + uid(), bank: "written", type: "single", stem: "", options: [{ id: uid(), text: "" }, { id: uid(), text: "" }], correct_option_ids: [], answer_text: "", analysis: "", hint: "", status: "draft", attachments: [] });
    editorSnapshot.value = JSON.stringify(editor.value); uploadFiles.value = []; attachmentKind.value = "question"; modalKind.value = "question";
    conflicted.value = false;
  });
}
async function editIssueQuestion(): Promise<void> {
  if (modalDirty.value && !await ask("编辑原题", "质疑中尚有未提交内容，确认放弃并打开原题？")) return;
  await editQuestion(issue.value.question_id);
}
async function reloadConflict(): Promise<void> {
  if (modalKind.value === "question") {
    if (await ask("读取最新题目", "当前题目修改将被最新版本替换，确认后重新编辑。")) await editQuestion(editor.value.id || editor.value.new_id);
  } else await perform(async () => {
    const params = new URLSearchParams({ scope: issue.value.scope, search: issue.value.description, page_size: "20", page: "1" });
    let latest: Dict | undefined;
    for (let page = 1; !disposed; page++) {
      params.set("page", String(page));
      const data = await requestLearning(learningOptions,`/issues?${params}`);
      latest = (data.items || []).find((i: Dict) => i.id === issue.value.id);
      if (latest || page * 20 >= data.total) break;
    }
    if (!latest) throw new Error("未找到最新质疑，请返回列表刷新。");
    issue.value = latest; issueNextStatus.value = latest.status; conflicted.value = false;
    notice.value = "已读取最新处理记录，补充说明仍保留，请核对后提交。";
  });
}
function moveOption(position: number, delta: number): void {
  const options = editor.value.options; if (!options[position + delta]) return;
  [options[position], options[position + delta]] = [options[position + delta], options[position]];
}
function removeOption(id: string): void {
  editor.value.options = editor.value.options.filter((o: Dict) => o.id !== id);
  editor.value.correct_option_ids = editor.value.correct_option_ids.filter((v: string) => v !== id);
}
function chooseCorrect(id: string): void {
  editor.value.correct_option_ids = editor.value.type === "single" ? [id] : editor.value.correct_option_ids.includes(id) ? editor.value.correct_option_ids.filter((v: string) => v !== id) : [...editor.value.correct_option_ids, id];
}
function setEditorType(label: string): void {
  editor.value.type = label === "单选题" ? "single" : label === "面试题" ? "interview" : "multiple";
  editor.value.type_label = label === "不定项" ? "不定项" : label === "多选题" ? "多选" : label === "单选题" ? "单选" : "面试";
}
async function saveQuestion(): Promise<void> {
  const problems = learningQuestionProblems(editor.value);
  if (!String(editor.value.stem || "").trim() || (editor.value.status === "published" && problems.length)) { error.value = problems.join("；"); void scrollModalError(); return; }
  await perform(async () => {
    const id = editor.value.id;
    const payload = withoutQuestionMetadata(editor.value); delete payload.audit;
    editor.value = withoutQuestionMetadata(await requestLearning(learningOptions,id ? `/questions/${encodeURIComponent(id)}` : "/questions", id ? "PUT" : "POST", payload));
    editorSnapshot.value = JSON.stringify(editor.value);
    conflicted.value = false;
    if (uploadFiles.value.length) await uploadAttachments("question");
    await loadView(); notice.value = "题目已保存。";
  });
}
async function setQuestionStatus(items: Dict[], status: string): Promise<void> {
  if (!items.length || !await ask(status === "deleted" ? "移入回收站" : "修改题目状态", `${items.length} 题将设为“${statusLabels[status]}”。${status === "deleted" ? "历史记录保留，可从回收站恢复。" : ""}`, status === "deleted" ? "danger" : "warning")) return;
  await perform(async () => {
    let completed = 0;
    try { for (const q of items) { await requestLearning(learningOptions,`/questions/${encodeURIComponent(q.id)}/status`, "POST", { status, version: q.version }); completed++; } }
    finally { await loadView(); notice.value = `已处理 ${completed} / ${items.length} 题。`; }
  });
}
async function copyQuestion(q: Dict): Promise<void> {
  await perform(async () => { editor.value = withoutQuestionMetadata(await requestLearning(learningOptions,`/questions/${encodeURIComponent(q.id)}/copy`, "POST", { version: q.version })); editorSnapshot.value = JSON.stringify(editor.value); modalKind.value = "question"; uploadFiles.value = []; await loadView(); }, "已复制为草稿。");
}
function openIssue(q: Dict = current.value || {}, existing?: Dict): void {
  issue.value = existing ? copy(existing) : { person_id: paper.value?.person_id || learner.value?.id, scope: paper.value?.scope || scope.value, paper_id: paper.value?.id, question_id: questionId(q), question_version: q.version, category: "答案", description: "", suggestion: "", snapshot: copy(q), attachments: [] };
  issueSnapshot.value = JSON.stringify(issue.value); issueComment.value = ""; issueNextStatus.value = issue.value.status || "pending"; uploadFiles.value = []; attachmentKind.value = "material"; modalKind.value = "issue";
  conflicted.value = false;
}
async function submitIssue(): Promise<void> {
  if (!issue.value.id && !String(issue.value.description).trim()) { error.value = "请填写问题说明。"; return; }
  if (issue.value.id && !issueComment.value.trim()) { error.value = "请填写补充说明或处理结论。"; return; }
  await perform(async () => {
    if (issue.value.id) {
      issue.value = await requestLearning(learningOptions,`/issues/${encodeURIComponent(issue.value.id)}`, "PATCH", { version: issue.value.version, status: issueNextStatus.value, remark: issueComment.value });
    } else issue.value = await requestLearning(learningOptions,"/issues", "POST", copy(issue.value));
    issueComment.value = ""; issueNextStatus.value = issue.value.status; issueSnapshot.value = JSON.stringify(issue.value);
    if (uploadFiles.value.length) await uploadAttachments("issue");
    if (tab.value === "issues") await loadView();
  }, "质疑记录已保存。");
}
function addFiles(files: File[]): void {
  const currentFiles = modalKind.value === "question" ? editor.value.attachments || [] : issue.value.attachments || [];
  const incoming = files.filter(f => !uploadFiles.value.some(v => v.name === f.name && v.size === f.size && v.lastModified === f.lastModified));
  if (incoming.some(f => f.size > 20 * 1024 * 1024)) { error.value = "单个附件不能超过 20 MiB。"; return; }
  if (currentFiles.length + uploadFiles.value.length + incoming.length > 10) { error.value = "每题或质疑最多 10 份附件。"; return; }
  const size = [...currentFiles, ...uploadFiles.value, ...incoming].reduce((sum, f) => sum + Number(f.size || 0), 0);
  if (size > 100 * 1024 * 1024) { error.value = "附件合计不能超过 100 MiB。"; return; }
  uploadFiles.value.push(...incoming); error.value = "";
}
function pickFiles(event: Event): void { const input = event.target as HTMLInputElement; addFiles(Array.from(input.files || [])); input.value = ""; }
function pasteFiles(event: ClipboardEvent): void {
  const files = Array.from(event.clipboardData?.files || []).filter(f => f.type.startsWith("image/"));
  if (files.length) { event.preventDefault(); addFiles(files); }
}
async function uploadAttachments(target: "question" | "issue"): Promise<void> {
  const item = target === "question" ? editor.value : issue.value;
  if (!item.id || !uploadFiles.value.length) return;
  const body = new FormData();
  body.append("version", String(item.version));
  for (const file of uploadFiles.value) { body.append("files", file); body.append("kind", attachmentKind.value); }
  const result = await requestLearning(learningOptions,`/attachments?${target}_id=${encodeURIComponent(item.id)}&kind=${attachmentKind.value}&version=${encodeURIComponent(item.version)}`, "POST", body);
  const attachments = Array.isArray(result) ? result : result.items || result.attachments;
  if (!Array.isArray(attachments)) throw new Error("附件响应格式异常，请刷新记录核对后重试。");
  item.attachments = result.attachments || [...(item.attachments || []), ...attachments]; item.version = result.version ?? item.version; uploadFiles.value = [];
  if (target === "question") { const saved = JSON.parse(editorSnapshot.value); saved.attachments = item.attachments; saved.version = item.version; editorSnapshot.value = JSON.stringify(saved); }
  else issueSnapshot.value = JSON.stringify(issue.value);
}
async function deleteAttachment(attachment: Dict): Promise<void> {
  if (!await ask("删除附件", `确认删除 ${attachment.name}？`, "danger")) return;
  await perform(async () => {
    const item = modalKind.value === "question" ? editor.value : issue.value;
    const result = await requestLearning(learningOptions,`/attachments/${encodeURIComponent(attachment.id)}?version=${encodeURIComponent(item.version)}`, "DELETE");
    item.attachments = (item.attachments || []).filter((a: Dict) => a.id !== attachment.id);
    item.version = result.version ?? item.version;
    if (modalKind.value === "question") { const saved = JSON.parse(editorSnapshot.value); saved.attachments = item.attachments; saved.version = item.version; editorSnapshot.value = JSON.stringify(saved); }
  }, "附件已删除。");
}
function attachmentUrl(a: Dict): string {
  const fallback = `/api/learning/attachments/${encodeURIComponent(a.id || "")}`;
  try { const url = new URL(a.url || fallback, window.location.origin); return url.origin === window.location.origin && url.pathname.startsWith("/api/learning/attachments/") ? url.href : fallback; } catch { return fallback; }
}
function isImage(a: Dict): boolean { return /\.(png|jpe?g|gif|webp|bmp)$/i.test(a.name || "") || String(a.mime || a.content_type || "").startsWith("image/"); }
function showAttachment(a: Dict): void { preview.value = a; modalKind.value = "attachment"; }
async function importFile(event: Event): Promise<void> {
  const input = event.target as HTMLInputElement, file = input.files?.[0]; input.value = "";
  if (!file) return;
  error.value = ""; importRows.value = [];
  try {
    if (file.size > 2 * 1024 * 1024) throw new Error("导入文件不能超过 2 MiB");
    importRows.value = parseLearningImport(await file.text(), /\.csv$/i.test(file.name)); importName.value = file.name; importPage.value = 1;
    for (const row of importRows.value) row.question.status = "draft";
    const result = await requestLearning(learningOptions,"/import", "POST", { questions: importRows.value.map(r => r.question), preview: true });
    for (const item of result.errors || []) if (importRows.value[item.row - 1]) importRows.value[item.row - 1].errors.push(item.error);
  } catch (e) { error.value = String((e as Error).message); }
}
function downloadTemplate(): void {
  downloadJson({ questions: [{ bank: "written", type: "single", stem: "示例题目", options: [{ id: "a", text: "选项一" }, { id: "b", text: "选项二" }], correct_option_ids: ["a"], answer_text: "", analysis: "", topic: "", specialty: "", difficulty: "中等", status: "draft" }] }, "学练导入模板.json");
}
function downloadJson(value: Dict, name: string): void {
  const blob = new Blob([JSON.stringify(value, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob), link = document.createElement("a"); link.href = url; link.download = name; link.click(); window.setTimeout(() => URL.revokeObjectURL(url), 1000);
}
async function commitImport(): Promise<void> {
  if (!importRows.value.length || importErrors.value || !await ask("确认导入题库", `导入 ${importRows.value.length} 道题并保存为草稿，审核后可发布。`)) return;
  await perform(async () => {
    const result = await requestLearning(learningOptions,"/import", "POST", { questions: importRows.value.map(r => r.question) });
    if (result.errors?.length) { error.value = result.errors.map((e: unknown) => typeof e === "string" ? e : JSON.stringify(e)).join("；"); return; }
    modalKind.value = ""; importRows.value = []; await loadView(); notice.value = "导入完成。";
  });
}
function modalKeydown(event: KeyboardEvent): void {
  if (!modalOwner?.isTop(event, modalElement.value) || !modalElement.value || confirm.open) return;
  if (event.key === "Escape") { event.preventDefault(); event.stopImmediatePropagation(); void closeModal(); }
  if (event.key === "Tab") {
    const nodes = Array.from(modalElement.value.querySelectorAll<HTMLElement>('button:not(:disabled),input:not(:disabled),textarea:not(:disabled),a[href],[tabindex="0"]')).filter(n => n.getClientRects().length);
    const first = nodes[0], last = nodes[nodes.length - 1];
    if (!modalElement.value.contains(document.activeElement) || (event.shiftKey ? document.activeElement === first : document.activeElement === last)) { event.preventDefault(); (event.shiftKey ? last : first)?.focus(); }
  }
}
watch(modalKind, async (value, previous) => {
  if (value && !previous) { returnFocus = document.activeElement as HTMLElement; modalOwner = acquireModal(); window.addEventListener("keydown", modalKeydown, true); }
  if (!value) { modalOwner?.release(); modalOwner = undefined; window.removeEventListener("keydown", modalKeydown, true); await nextTick(); returnFocus?.focus(); }
  else { await nextTick(); modalElement.value?.focus(); }
});
watch(error, (value) => { if (value && modalKind.value) void scrollModalError(); });
watch(pageNumber, (value) => { jumpPage.value = value; });
function beforeUnload(event: BeforeUnloadEvent): void {
  saveDraft(); if (busy.value || sharingDirty.value || modalDirty.value || (tab.value === "settings" && JSON.stringify(settingsForm) !== settingsSnapshot) || (storageWarning.value && (draft.answer_text || draft.option_ids.length || noteDirty.value))) { event.preventDefault(); event.returnValue = ""; }
}
onMounted(() => {
  releaseGuard = registerNavigationGuard((_target, proceed) => {
    saveDraft();
    if (busy.value) { notice.value = "正在保存，请稍候再离开。"; return false; }
    if (sharingDirty.value || modalDirty.value || (tab.value === "settings" && JSON.stringify(settingsForm) !== settingsSnapshot)) { void ask("离开画像学练", "尚有未保存内容，确认离开？").then(ok => { if (ok) proceed(); }); return false; }
    return true;
  });
  window.addEventListener("beforeunload", beforeUnload);
  document.addEventListener("visibilitychange", checkToday);
  todayTimer = window.setInterval(checkToday, 60_000);
  onPopstate = () => {
    const next = new URLSearchParams(window.location.search);
    const v = next.get("view") || "";
    if (v !== viewParam.value) { viewParam.value = v; epoch++; void bootstrap(); }
  };
  window.addEventListener("popstate", onPopstate);
  void bootstrap();
});
onBeforeUnmount(() => { saveDraft(); disposed = true; epoch++; lifetime.abort(); readRequest?.abort(); peopleRequest?.abort(); window.clearTimeout(pollTimer); window.clearInterval(todayTimer); releaseGuard?.(); modalOwner?.release(); window.removeEventListener("keydown", modalKeydown, true); window.removeEventListener("beforeunload", beforeUnload); document.removeEventListener("visibilitychange", checkToday); if (onPopstate) window.removeEventListener("popstate", onPopstate); resolveConfirm?.(false); });
</script>

<style scoped>
.learning-page:has(.practice-layout){padding-bottom:110px}.question-body{scroll-margin-bottom:110px}
.learning-fixed-bar.question-footer{position:fixed;z-index:100;left:16px;right:16px;bottom:calc(12px + env(safe-area-inset-bottom));max-width:780px;margin:0 auto;padding:10px;display:grid;grid-template-columns:1fr 1.3fr 1fr;gap:10px;border:1px solid #d5e2f3;border-radius:12px;background:var(--card-background,#fff);box-shadow:0 -3px 18px #1c4b771a;box-sizing:border-box}
.learning-fixed-bar button{display:flex;align-items:center;justify-content:center;gap:6px;min-height:44px;min-width:0;padding:8px;border:1px solid #ccdcef;border-radius:8px;background:#fff;color:#31547b;font:inherit;font-size:14px;cursor:pointer}
.learning-fixed-bar button.primary{background:#1769cf;color:#fff;border-color:#1769cf}.learning-fixed-bar button:disabled{cursor:default;opacity:.65}.learning-fixed-bar button:focus-visible{outline:2px solid #1769cf;outline-offset:2px}.learning-fixed-bar>span{text-align:center}
@media(max-width:700px){.learning-fixed-bar.question-footer{left:8px;right:8px;bottom:calc(8px + env(safe-area-inset-bottom));gap:6px;padding:8px}.learning-fixed-bar button{font-size:13px}.learning-page:has(.practice-layout){padding-bottom:105px}}
</style>
