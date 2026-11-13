<template>
  <section class="page" data-module="ingest">
    <header class="page-head">
      <div>
        <h2>多源接入管理</h2>
        <p class="page-desc">
          接入通道进入运行中之前必须通过准入校验：源端类型、连接参数与目标范围均须符合规则，
          且关联原始表处于可接收状态。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记接入通道</button>
        <button class="btn" type="button" @click="exportRows">导出多源接入清单</button>
        <button class="btn" type="button" @click="showRules = !showRules">
          {{ showRules ? '收起准入规则' : '查看准入规则' }}
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <section v-if="showRules" class="rule-panel">
      <header class="rule-panel-head">
        <strong>可复核准入规则</strong>
        <span>规则版本 {{ ruleVersion }} · {{ conflictPolicy }}</span>
      </header>
      <table class="data-table">
        <thead>
          <tr>
            <th>规则编号</th>
            <th>规则名称</th>
            <th>源端类型</th>
            <th>严格度</th>
            <th>必填连接参数</th>
            <th>允许目标范围</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="rule in rules" :key="rule.规则编号">
            <td>{{ rule.规则编号 }}</td>
            <td>{{ rule.规则名称 }}</td>
            <td>{{ rule.源端类型 }}</td>
            <td>{{ rule.严格度 }}</td>
            <td>{{ rule.必填连接参数.join('、') || rule.必填字段?.join('、') || '—' }}</td>
            <td>{{ rule.允许目标范围.join('、') || '—' }}</td>
          </tr>
        </tbody>
      </table>
    </section>

    <form class="filter-bar" @submit.prevent="reload">
      <label class="filter-item">
        <span>通道编号</span>
        <input v-model="filters.keyword" placeholder="按通道编号检索" />
      </label>
      <label class="filter-item">
        <span>源端类型</span>
        <select v-model="filters.source_type">
          <option value="">全部</option>
          <option v-for="type in sourceTypes" :key="type" :value="type">{{ type }}</option>
        </select>
      </label>
      <label class="filter-item">
        <span>通道状态</span>
        <select v-model="filters.status">
          <option value="">全部</option>
          <option v-for="status in statuses" :key="status" :value="status">{{ status }}</option>
        </select>
      </label>
      <label class="filter-item checkbox-item">
        <input v-model="filters.acceptance_lane" type="checkbox" true-value="待校验" false-value="" />
        <span>只看当前批次待校验通道</span>
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <div v-if="feedback" class="notice" :class="feedback.ok ? 'notice-ok' : 'notice-error'">
      <strong>{{ feedback.ok ? '操作成功' : '准入未通过' }}</strong>
      <p>{{ feedback.message }}</p>
      <p v-if="feedback.fields.length" class="notice-fields">
        阻塞字段：<span v-for="field in feedback.fields" :key="field" class="field-chip">{{ field }}</span>
      </p>
      <p v-if="feedback.detail" class="notice-detail">{{ feedback.detail }}</p>
      <p v-if="feedback.entry && feedback.fields.length">
        通道当前状态：{{ feedback.entry.status }}；原始表：{{ feedback.entry['关联原始表状态'] }}
      </p>
    </div>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column">
            <span v-if="column === '准入结论'" :class="acceptanceClass(row)">
              {{ row[column] ?? '—' }}
            </span>
            <span v-else>{{ row[column] ?? '—' }}</span>
          </td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button class="link" type="button" @click="openEditor(row)">改参数</button>
            <button class="link" type="button" @click="openDetail(row)">校验记录</button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">
            暂无符合条件的多源接入通道，可调整源端类型或准入批次
          </td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条多源接入记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>

    <dialog v-if="editor.open" class="dialog" open>
      <h3>修改接入参数 · {{ editor.entry?.['通道编号'] }}</h3>
      <p class="page-desc">修改连接参数或目标范围后，旧校验结果会作废并回到待校验。</p>
      <label v-for="field in editorFields" :key="field" class="dialog-field">
        <span>{{ field }}</span>
        <input v-model="editor.values[field]" :placeholder="`请输入${field}`" />
      </label>
      <div class="dialog-actions">
        <button class="btn primary" type="button" @click="saveEditor">保存并作废旧校验</button>
        <button class="btn ghost" type="button" @click="editor.open = false">取消</button>
      </div>
      <p v-if="editor.message" class="error-text">{{ editor.message }}</p>
    </dialog>

    <dialog v-if="detail.open" class="dialog" open>
      <h3>准入校验记录 · {{ detail.entry?.['通道编号'] }}</h3>
      <p v-if="detail.entry?.['准入校验']">
        <strong>当前结论：{{ detail.entry['准入校验']['校验结果'] }}</strong>
        · 有效规则 {{ detail.entry['准入校验']['有效规则编号'] }}（{{ detail.entry['准入校验']['有效规则名称'] }}）
        · 参数指纹 {{ detail.entry['准入校验']['参数指纹'] }}
      </p>
      <p v-if="detail.entry?.['准入校验']?.['冲突说明']?.length" class="notice-detail">
        {{ detail.entry['准入校验']['冲突说明'].join('；') }}
      </p>
      <table class="data-table">
        <thead>
          <tr>
            <th>校验编号</th>
            <th>校验结果</th>
            <th>失败字段</th>
            <th>失败原因</th>
            <th>规则版本</th>
            <th>提交时间</th>
            <th>状态</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="record in detail.records" :key="record.校验编号">
            <td>{{ record.校验编号 }}</td>
            <td>{{ record.校验结果 }}</td>
            <td>{{ (record.失败字段 || []).join('、') || '—' }}</td>
            <td>{{ record.失败原因摘要 || '—' }}</td>
            <td>{{ record.规则版本 }}</td>
            <td>{{ record.提交时间 }}</td>
            <td>{{ record.已作废 ? '已作废' : record.已放行 ? '已放行' : '有效' }}</td>
          </tr>
          <tr v-if="!detail.records.length">
            <td colspan="7" class="empty-state">暂无校验记录，请先提交准入校验</td>
          </tr>
        </tbody>
      </table>
      <div class="dialog-actions">
        <button class="btn ghost" type="button" @click="detail.open = false">关闭</button>
      </div>
    </dialog>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, any>

const ENDPOINT = '/api/ingest'
const columns = [
  '通道编号', '源端类型', '连接参数', '目标范围', '关联原始表', '关联原始表状态',
  '准入结论', '失败字段', '有效规则编号', '待同步状态', '通道负责人',
]
const actions = ['提交校验', '启动接入', '停止接入']
const statuses = ['草稿', '待校验', '运行中', '已停止']
const editorFields = ['连接参数', '目标范围', '目标路径', '关联原始表', '通道负责人']
const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const showRules = ref(false)
const rules = ref<Row[]>([])
const ruleVersion = ref('—')
const conflictPolicy = ref('源端类型与连接参数冲突时按更严格的规则为准')
const sourceTypes = ref<string[]>(['JDBC', 'Kafka', 'API', 'FILE'])
const filters = reactive<Record<string, string>>({
  keyword: '',
  source_type: '',
  status: '',
  acceptance_lane: '',
})
const feedback = ref<{ ok: boolean; message: string; fields: string[]; detail: string; entry: Row | null } | null>(null)
const editor = reactive<{ open: boolean; entry: Row | null; values: Record<string, string>; message: string }>({
  open: false,
  entry: null,
  values: {},
  message: '',
})
const detail = reactive<{ open: boolean; entry: Row | null; records: Row[] }>({
  open: false,
  entry: null,
  records: [],
})

const stats = computed(() => {
  return [
    { label: '接入通道总数', value: total.value },
    { label: '运行中通道', value: rows.value.filter((row) => row.status === '运行中').length },
    {
      label: '当前批次待校验',
      value: rows.value.filter((row) => row['准入结论'] !== '通过' && ['草稿', '待校验'].includes(String(row.status))).length,
    },
  ]
})

function acceptanceClass(row: Row) {
  if (String(row['准入结论']).startsWith('通过')) return 'pill pill-ok'
  if (String(row['准入结论']).includes('失效')) return 'pill pill-warn'
  return 'pill pill-bad'
}

function resetFilters() {
  filters.keyword = ''
  filters.source_type = ''
  filters.status = ''
  filters.acceptance_lane = ''
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  feedback.value = {
    ok: false,
    message: '接入通道登记请使用后端 POST /api/ingest 接口，页面暂未开放表单',
    fields: [],
    detail: '',
    entry: null,
  }
}

function openEditor(row: Row) {
  editor.open = true
  editor.entry = row
  editor.message = ''
  editor.values = {}
  for (const field of editorFields) {
    editor.values[field] = String(row[field] ?? '')
  }
}

async function saveEditor() {
  if (!editor.entry) return
  editor.message = ''
  const response = await request(`${ENDPOINT}/${editor.entry.id}`, {
    method: 'PUT',
    body: JSON.stringify({ values: editor.values }),
  })
  const payload = await response.json()
  if (!response.ok || !payload.ok) {
    editor.message = payload.message ?? '参数修改失败'
    return
  }
  editor.open = false
  await reload()
}

async function openDetail(row: Row) {
  detail.open = true
  detail.entry = null
  detail.records = []
  const response = await request(`${ENDPOINT}/${row.id}`)
  if (!response.ok) {
    detail.records = []
    return
  }
  const payload = await response.json()
  detail.entry = payload
  detail.records = payload['校验记录'] ?? []
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  feedback.value = null
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ values: { action } }),
    })
    const payload = await response.json()
    if (!response.ok) {
      throw new Error(payload.detail ?? '多源接入动作未生效，请稍后重试')
    }
    const admission = payload.entry?.['准入校验'] ?? {}
    const fields: string[] = payload.failed_fields ?? admission['失败字段'] ?? []
    const reasons: Record<string, string> = admission['失败原因'] ?? {}
    const detailText = fields.map((field: string) => `${field}：${reasons[field] ?? '未满足准入规则'}`).join('；')
    feedback.value = {
      ok: Boolean(payload.ok),
      message: payload.message ?? '',
      fields,
      detail: detailText,
      entry: payload.entry ?? null,
    }
    if (!payload.ok) {
      errorMessage.value = payload.message ?? '准入未通过'
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '多源接入操作失败'
  }
}

async function loadRules() {
  const response = await request(`${ENDPOINT}/rules`)
  if (!response.ok) return
  const payload = await response.json()
  rules.value = payload.items ?? []
  ruleVersion.value = payload.rule_version ?? '—'
  conflictPolicy.value = payload.conflict_policy ?? conflictPolicy.value
  if (payload.source_types?.length) sourceTypes.value = payload.source_types
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams()
  for (const [key, value] of Object.entries(filters)) {
    if (value) query.set(key, value)
  }
  try {
    const response = await request(`${ENDPOINT}?${query.toString()}`)
    if (!response.ok) {
      throw new Error('接入通道列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '多源接入列表读取失败'
  }
}

onMounted(() => {
  void loadRules()
  void reload()
})
</script>

<style scoped>
.rule-panel {
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 12px;
  margin-bottom: 12px;
}
.rule-panel-head {
  display: flex;
  justify-content: space-between;
  font-size: 12px;
  color: var(--muted);
  margin-bottom: 8px;
}
.checkbox-item {
  flex-direction: row;
  display: flex;
  gap: 6px;
  align-items: center;
  font-size: 13px;
}
.notice {
  border-radius: 8px;
  padding: 10px 12px;
  margin-bottom: 12px;
  font-size: 13px;
}
.notice-ok {
  background: #ecfdf3;
  border: 1px solid #a6f4c5;
  color: #027a48;
}
.notice-error {
  background: #fef3f2;
  border: 1px solid #fecdca;
  color: #b42318;
}
.notice-fields {
  margin: 4px 0 0;
}
.field-chip {
  display: inline-block;
  border: 1px solid currentColor;
  border-radius: 999px;
  padding: 0 8px;
  margin-right: 6px;
  font-size: 12px;
}
.notice-detail {
  margin: 4px 0 0;
  color: #475467;
}
.pill {
  border-radius: 999px;
  padding: 1px 8px;
  font-size: 12px;
}
.pill-ok {
  background: #ecfdf3;
  color: #027a48;
}
.pill-bad {
  background: #fef3f2;
  color: #b42318;
}
.pill-warn {
  background: #fffaeb;
  color: #b54708;
}
.dialog {
  position: fixed;
  inset: 10vh auto auto 50%;
  transform: translateX(-50%);
  width: min(760px, 92vw);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 16px;
  background: #fff;
  z-index: 20;
}
.dialog-field {
  display: block;
  margin-bottom: 8px;
  font-size: 13px;
}
.dialog-field span {
  display: block;
  color: var(--muted);
  margin-bottom: 4px;
}
.dialog-field input {
  width: 100%;
  padding: 6px 8px;
  border: 1px solid var(--border);
  border-radius: 6px;
}
.dialog-actions {
  display: flex;
  gap: 8px;
  margin-top: 12px;
}
</style>
