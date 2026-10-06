<script setup lang="ts">
import { computed, onBeforeUnmount, ref, shallowRef, watch, watchEffect } from 'vue'
import { formatTime, type DebugLog, type DebugCommit } from './lib/debug'
import CommitInspector from './components/CommitInspector.vue'
import { commitName, matchesCommit } from './lib/commits'

const log = shallowRef<DebugLog | null>(null)
const search = ref('')
const domain = ref('')
const selectedKey = ref('')
const commitList = ref<HTMLElement | null>(null)
const darkTheme = ref(window.matchMedia('(prefers-color-scheme: dark)').matches)
watchEffect(() => document.documentElement.classList.toggle('dark', darkTheme.value))
const isDragging = ref(false)
const isLoading = ref(false)
const progress = ref(0)
const error = ref('')
const fileInput = ref<HTMLInputElement | null>(null)
const searchInput = ref<HTMLInputElement | null>(null)
let worker: Worker | null = null

// Filtering selects whole commits; it never removes a domain/event from an
// atomic operation just because another event matched the search.
const filteredCommits = computed(() => log.value?.commits.filter((commit) => matchesCommit(commit, domain.value, search.value)) ?? [])
function commitKey(commit: DebugCommit) {
  return commit.id ?? JSON.stringify([commit.sessionId, commit.lineIndex, commit.seq, commit.commitId])
}
const selectedCommit = computed(() => filteredCommits.value.find(commit => commitKey(commit) === selectedKey.value) ?? null)
watch(filteredCommits, commits => {
  if (!commits.some(commit => commitKey(commit) === selectedKey.value)) selectedKey.value = commits[0] ? commitKey(commits[0]) : ''
})
function navigateCommits(event: KeyboardEvent) {
  const keys = ['ArrowDown', 'ArrowUp', 'Home', 'End']
  if (!keys.includes(event.key) || !filteredCommits.value.length) return
  event.preventDefault()
  const current = filteredCommits.value.findIndex(commit => commitKey(commit) === selectedKey.value)
  const last = filteredCommits.value.length - 1
  const index = event.key === 'Home' ? 0 : event.key === 'End' ? last : Math.max(0, Math.min(last, current + (event.key === 'ArrowDown' ? 1 : -1)))
  selectedKey.value = commitKey(filteredCommits.value[index]!)
  commitList.value?.querySelectorAll<HTMLButtonElement>('.commit-row')[index]?.focus()
}

function stopWorker() {
  worker?.terminate()
  worker = null
  isLoading.value = false
}
function openFile() { fileInput.value?.click() }
function loadFile(file?: File) {
  if (!file) return
  stopWorker()
  const current = new Worker(new URL('./parse.worker.ts', import.meta.url), { type: 'module' })
  worker = current
  isLoading.value = true
  error.value = ''
  progress.value = 0
  current.onmessage = (event: MessageEvent) => {
    if (worker !== current) return
    const message = event.data as { type: string; loaded?: number; total?: number; log?: DebugLog; message?: string }
    if (message.type === 'progress') {
      progress.value = Math.round((message.loaded ?? 0) / Math.max(message.total ?? 0, 1) * 100)
    } else if (message.type === 'result' && message.log) {
      log.value = message.log
      search.value = ''
      domain.value = ''
      selectedKey.value = ''
      stopWorker()
    } else if (message.type === 'error') {
      error.value = message.message ?? 'Unable to read file'
      stopWorker()
    }
  }
  current.onerror = () => {
    if (worker !== current) return
    error.value = 'Unable to read file'
    stopWorker()
  }
  current.postMessage({ type: 'parse', file, fileName: file.name })
}
function onFileChange(event: Event) {
  const input = event.target as HTMLInputElement
  loadFile(input.files?.[0])
  input.value = ''
}
function onDrop(event: DragEvent) {
  isDragging.value = false
  loadFile(event.dataTransfer?.files[0])
}
function onKeydown(event: KeyboardEvent) {
  if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'k') {
    event.preventDefault()
    searchInput.value?.focus()
  }
}
// The window listener also works when keyboard focus is outside the toolbar.
window.addEventListener('keydown', onKeydown)
onBeforeUnmount(() => {
  stopWorker()
  window.removeEventListener('keydown', onKeydown)
})
</script>

<template>
  <div class="monitor" :class="{ dragging: isDragging }" @dragover.prevent="isDragging = true" @dragleave.prevent="isDragging = false" @drop.prevent="onDrop">
    <header class="toolbar">
      <button type="button" class="open-button" @click="openFile">Open</button>
      <input ref="fileInput" class="visually-hidden" type="file" accept=".jsonl,.json,.txt,application/json" @change="onFileChange" />
      <span v-if="log" class="file-name">{{ log.fileName }}</span>
      <template v-if="log">
        <input ref="searchInput" v-model="search" class="search" type="search" aria-label="Filter commits" placeholder="Filter" />
        <select v-model="domain" class="domain-filter" aria-label="Domain">
          <option value="">All domains</option>
          <option v-for="name in log.domains" :key="name" :value="name">{{ name }}</option>
        </select>
      </template>
      <span v-if="isLoading" class="loading" role="status">Reading {{ progress }}%</span>
      <button v-if="isLoading" type="button" @click="stopWorker">Cancel</button>
      <button type="button" class="theme-button" :aria-label="darkTheme ? 'Switch to light theme' : 'Switch to dark theme'" @click="darkTheme = !darkTheme">{{ darkTheme ? 'Light' : 'Dark' }}</button>
    </header>
    <div v-if="error" class="error" role="alert">{{ error }}</div>
    <details v-if="log?.errors.length" class="warnings">
      <summary>{{ log.errors.length }} parsing warnings</summary>
      <ul><li v-for="(warning, index) in log.errors" :key="index">{{ warning }}</li></ul>
    </details>

    <div v-if="!log" class="placeholder">Drop a JSONL file here</div>
    <main v-else class="monitor-panes">
      <section class="list-pane" aria-label="Commit list">
        <div class="list-heading"><span>Commit</span><span>Time</span></div>
        <div ref="commitList" class="commit-list" role="listbox" aria-label="Commits" @keydown="navigateCommits">
          <div v-if="!filteredCommits.length" class="placeholder">No matching commits</div>
          <button v-for="commit in filteredCommits" :key="commitKey(commit)" type="button" class="commit-row" role="option" :aria-selected="commitKey(commit) === selectedKey" :tabindex="commitKey(commit) === selectedKey ? 0 : -1" aria-controls="commit-inspector" @click="selectedKey = commitKey(commit)">
            <span class="commit-name">{{ commitName(commit) }}</span>
            <time>{{ formatTime(commit.events.find(event => event.recordedAtUnixMilli !== undefined)?.recordedAtUnixMilli) }}</time>
          </button>
        </div>
      </section>
      <CommitInspector v-if="selectedCommit" :key="selectedKey" :commit="selectedCommit" />
      <section v-else id="commit-inspector" class="inspector"><div class="placeholder">Select a commit</div></section>
    </main>
  </div>
</template>
