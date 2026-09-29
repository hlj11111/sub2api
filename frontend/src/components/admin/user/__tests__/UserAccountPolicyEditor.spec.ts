import { beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import UserAccountPolicyEditor from '../UserAccountPolicyEditor.vue'
import type { Group } from '@/types'
const mocks = vi.hoisted(() => ({ get: vi.fn(), set: vi.fn(), list: vi.fn(), success: vi.fn() }))
vi.mock('@/api/admin', () => ({ adminAPI: { accounts: { list: mocks.list }, users: { getAccountPolicy: mocks.get, setAccountPolicy: mocks.set } } }))
vi.mock('@/stores/app', () => ({ useAppStore: () => ({ showSuccess: mocks.success }) }))
vi.mock('vue-i18n', () => ({ useI18n: () => ({ t: (key: string) => key }) }))
beforeEach(() => {
  vi.clearAllMocks()
  mocks.get.mockResolvedValue({ mode: 'allowlist', account_ids: [10] })
  mocks.list.mockResolvedValue({ items: [{ id: 10, name: 'A', status: 'active', schedulable: true }], total: 1 })
  mocks.set.mockImplementation((_u, _g, p) => Promise.resolve(p))
})
const create = () => mount(UserAccountPolicyEditor, { props: { userId: 7, groups: [{ id: 1, name: 'G' }, { id: 2, name: 'H' }] as Group[] } })
describe('user account policy editor', () => {
  it('loads existing restrictions and preserves empty deny lists', async () => {
    const w = create()
    await w.get('select').setValue(1); await flushPromises()
    expect((w.get('input').element as HTMLInputElement).checked).toBe(true)
    await w.get('input').setValue(false)
    expect(w.text()).toContain('admin.users.accountPolicy.empty')
    await w.get('button').trigger('click'); await flushPromises()
    expect(mocks.set).toHaveBeenCalledWith(7, 1, { mode: 'allowlist', account_ids: [] })
  })
  it('clears account ids only when explicitly choosing unrestricted', async () => {
    const w = create()
    await w.get('select').setValue(1); await flushPromises()
    await w.findAll('select')[1].setValue('all')
    await w.get('button').trigger('click'); await flushPromises()
    expect(mocks.set).toHaveBeenCalledWith(7, 1, { mode: 'all', account_ids: [] })
  })
  it('does not expose a save button after a failed load', async () => {
    mocks.get.mockRejectedValueOnce(new Error('offline'))
    const w = create()
    await w.get('select').setValue(1); await flushPromises()
    expect(w.find('button').exists()).toBe(false)
    expect(w.get('[role="alert"]').text()).toContain('loadFailed')
  })
  it('ignores stale responses after switching groups', async () => {
    let resolve!: (value: unknown) => void
    mocks.get.mockReturnValueOnce(new Promise(r => { resolve = r }))
    const w = create()
    await w.get('select').setValue(1)
    await w.get('select').setValue(2); await flushPromises()
    resolve({ mode: 'allowlist', account_ids: [99] }); await flushPromises()
    await w.get('button').trigger('click'); await flushPromises()
    expect(mocks.set).toHaveBeenCalledWith(7, 2, { mode: 'allowlist', account_ids: [10] })
  })
  it('ignores a late save response after switching users', async () => {
    let resolve!: (value: unknown) => void
    mocks.set.mockReturnValueOnce(new Promise(r => { resolve = r }))
    const w = create()
    await w.get('select').setValue(1); await flushPromises()
    await w.findAll('select')[1].setValue('all')
    await w.get('button').trigger('click')
    await w.setProps({ userId: 8 }); await flushPromises()
    resolve({ mode: 'all', account_ids: [] }); await flushPromises()
    expect((w.findAll('select')[1].element as HTMLSelectElement).value).toBe('allowlist')
    await w.get('button').trigger('click'); await flushPromises()
    expect(mocks.set).toHaveBeenLastCalledWith(8, 1, { mode: 'allowlist', account_ids: [10] })
  })

})
