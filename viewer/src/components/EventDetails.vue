<script setup lang="ts">
import { computed } from 'vue'
import { formatJson, type JsonValue } from '../lib/debug'

const props = defineProps<{ value: JsonValue }>()
const fields = computed(() => {
  let entries: [string, JsonValue][] = props.value !== null && typeof props.value === 'object' && !Array.isArray(props.value)
    ? Object.entries(props.value)
    : [['value', props.value]]
  // A resolved frozen envelope is not another navigation level. Inspect its
  // fields directly; deeper structures are read-only values, never drawers.
  const single = entries.length === 1 ? entries[0] : undefined
  if (single && ['request', 'result', 'output', 'response'].includes(single[0]) && single[1] !== null && typeof single[1] === 'object' && !Array.isArray(single[1])) {
    entries = Object.entries(single[1])
  }
  const contentKeys = new Set(['text', 'messages', 'content', 'output'])
  return [...entries.filter(([key]) => contentKeys.has(key)), ...entries.filter(([key]) => !contentKeys.has(key))]
})
function display(value: JsonValue) { return typeof value === 'string' ? value : formatJson(value) }
</script>

<template>
  <dl v-if="fields.length" class="event-fields">
    <div v-for="[key, value] in fields" :key="key" class="property-row">
      <dt>{{ key }}</dt>
      <dd><pre class="content">{{ display(value) }}</pre></dd>
    </div>
  </dl>
  <div v-else class="empty-content">No content fields</div>
</template>
