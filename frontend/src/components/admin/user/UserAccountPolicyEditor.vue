<template>
  <section class="space-y-3 rounded-xl border border-gray-200 p-4 dark:border-dark-600">
    <h4 class="font-semibold">{{ t('admin.users.accountPolicy.title') }}</h4>
    <p class="text-xs text-gray-500">{{ t('admin.users.accountPolicy.hint') }}</p>
    <select v-model="groupId" :disabled="saving" class="input w-full" :aria-label="t('admin.users.accountPolicy.group')">
      <option :value="0">{{ t('admin.users.accountPolicy.group') }}</option>
      <option v-for="group in groups" :key="group.id" :value="group.id">{{ group.name }}</option>
    </select>
    <p v-if="loading" class="text-sm">{{ t('common.loading') }}</p>
    <template v-else-if="loaded">
      <select v-model="policy.mode" :disabled="saving" class="input w-full" :aria-label="t('admin.users.accountPolicy.title')">
        <option value="all">{{ t('admin.users.accountPolicy.all') }}</option>
        <option value="allowlist">{{ t('admin.users.accountPolicy.allowlist') }}</option>
      </select>
      <template v-if="policy.mode === 'allowlist'">
        <div class="max-h-64 space-y-2 overflow-y-auto">
          <label v-for="account in accounts" :key="account.id" class="flex items-center gap-2 text-sm">
            <input v-model="policy.account_ids" type="checkbox" :value="account.id" :disabled="saving" />
            {{ account.name }} (#{{ account.id }})
            <span v-if="account.status !== 'active' || !account.schedulable" class="text-amber-600">{{ t('admin.users.accountPolicy.unavailable') }}</span>
          </label>
          <label v-for="id in missingIds" :key="id" class="flex items-center gap-2 text-sm text-amber-600">
            <input v-model="policy.account_ids" type="checkbox" :value="id" :disabled="saving" />
            #{{ id }} — {{ t('admin.users.accountPolicy.removed') }}
          </label>
        </div>
        <p v-if="policy.account_ids.length === 0" class="text-sm text-amber-600">{{ t('admin.users.accountPolicy.empty') }}</p>
      </template>
      <button type="button" class="btn btn-secondary" :disabled="saving" @click="save">{{ t('admin.users.accountPolicy.save') }}</button>
    </template>
    <p v-if="error" role="alert" class="text-sm text-red-600">{{ error }}</p>
  </section>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { adminAPI } from '@/api/admin'
import type { UserAccountPolicy } from '@/api/admin/users'
import type { AccountListItem, Group } from '@/types'
import { useAppStore } from '@/stores/app'

const props = defineProps<{ userId: number; groups: Group[] }>()
const { t } = useI18n()
const app = useAppStore()
const groupId = ref(0)
const loading = ref(false)
const loaded = ref(false)
const saving = ref(false)
const error = ref('')
const policy = ref<UserAccountPolicy>({ mode: 'all', account_ids: [] })
const accounts = ref<AccountListItem[]>([])
const missingIds = computed(() => policy.value.account_ids.filter(id => !accounts.value.some(a => a.id === id)))
let generation = 0
watch(() => [props.userId, groupId.value], async () => {
  const request = ++generation
  loaded.value = false
  loading.value = false
  error.value = ''
  if (!groupId.value) return
  loading.value = true
  const user = props.userId
  const group = groupId.value
  try {
    const value = await adminAPI.users.getAccountPolicy(user, group)
    const items: AccountListItem[] = []
    for (let page = 1; ; page++) {
      const result = await adminAPI.accounts.list(page, 100, { group: String(group), lite: '1' })
      if (request !== generation) return
      items.push(...result.items)
      if (items.length >= result.total || result.items.length === 0) break
    }
    if (request !== generation) return
    policy.value = { mode: value.mode, account_ids: [...value.account_ids] }
    accounts.value = items
    loaded.value = true
  } catch {
    if (request === generation) error.value = t('admin.users.accountPolicy.loadFailed')
  } finally {
    if (request === generation) loading.value = false
  }
})
async function save() {
  if (!loaded.value || saving.value) return
  saving.value = true
  error.value = ''
  const request = generation
  const user = props.userId
  const group = groupId.value
  try {
    const saved = await adminAPI.users.setAccountPolicy(user, group, {
      mode: policy.value.mode,
      account_ids: policy.value.mode === 'all' ? [] : [...policy.value.account_ids],
    })
    if (request !== generation) return
    policy.value = { mode: saved.mode, account_ids: [...saved.account_ids] }
    app.showSuccess(t('admin.users.accountPolicy.saved'))
  } catch {
    if (request === generation) error.value = t('admin.users.accountPolicy.saveFailed')
  } finally { saving.value = false }
}
</script>
