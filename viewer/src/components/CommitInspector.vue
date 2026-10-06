<script setup lang="ts">
import { computed, ref } from 'vue'
import type { DebugCommit } from '../lib/debug'
import { formatJson } from '../lib/debug'
import { commitDomains, commitName } from '../lib/commits'
import DomainInspector from './DomainInspector.vue'

const props = defineProps<{ commit: DebugCommit }>()
const groups = computed(() => commitDomains(props.commit))
const mode = ref<'Details' | 'Raw'>('Details')
// Domain-local choices survive a round trip through the commit's Raw view.
const eventIndices = ref<Record<string, number>>({})
const thinkingDomains = ref(new Set<string>())
const raw = computed(() => formatJson({
  sessionId: props.commit.sessionId,
  seq: props.commit.seq,
  commitId: props.commit.commitId,
  events: props.commit.events.map(event => ({
    domain: event.domainKey ?? event.domain,
    type: event.type,
    recordedAtUnixMilli: event.recordedAtUnixMilli,
    payload: event.payload,
    bodies: event.bodies,
  })),
}))
function selectEvent(name: string, index: number) {
  eventIndices.value[name] = index
  const next = new Set(thinkingDomains.value)
  next.delete(name)
  thinkingDomains.value = next
}
function toggleThinking(name: string) {
  const next = new Set(thinkingDomains.value)
  if (next.has(name)) next.delete(name)
  else next.add(name)
  thinkingDomains.value = next
}
function navigateViews(event: KeyboardEvent) {
  if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return
  const buttons = [...(event.currentTarget as HTMLElement).querySelectorAll<HTMLButtonElement>('[role=tab]')]
  const current = buttons.indexOf(event.target as HTMLButtonElement)
  if (current < 0 || !buttons.length) return
  event.preventDefault()
  const index = event.key === 'Home' ? 0 : event.key === 'End' ? buttons.length - 1 : (current + (event.key === 'ArrowRight' ? 1 : -1) + buttons.length) % buttons.length
  buttons[index]?.click()
  buttons[index]?.focus()
}
</script>

<template>
  <section id="commit-inspector" class="inspector" :aria-label="`${commitName(commit)} inspector`">
    <nav class="inspector-tabs" role="tablist" aria-label="Views" @keydown="navigateViews">
      <button v-for="item in (['Details', 'Raw'] as const)" :id="`view-tab-${item}`" :key="item" type="button" role="tab" :aria-selected="mode === item" :tabindex="mode === item ? 0 : -1" aria-controls="inspector-view" @click="mode = item">{{ item }}</button>
    </nav>
    <div id="inspector-view" class="inspector-content" role="tabpanel" :aria-labelledby="`view-tab-${mode}`">
      <pre v-if="mode === 'Raw'" class="content raw-content">{{ raw }}</pre>
      <template v-else>
        <DomainInspector v-for="(group, index) in groups" :key="group.name" :name="group.name" :events="group.events" :heading-id="`domain-heading-${index}`" :event-index="eventIndices[group.name] ?? 0" :thinking="thinkingDomains.has(group.name)" @select="selectEvent(group.name, $event)" @toggle-thinking="toggleThinking(group.name)" />
        <div v-if="!groups.length" class="placeholder">No events in this commit</div>
      </template>
    </div>
  </section>
</template>
