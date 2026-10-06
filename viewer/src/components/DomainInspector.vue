<script setup lang="ts">
import { computed } from 'vue'
import type { DebugEvent } from '../lib/debug'
import { shortType } from '../lib/debug'
import { eventPresentation } from '../lib/details'
import EventDetails from './EventDetails.vue'

const props = defineProps<{
  name: string
  events: DebugEvent[]
  headingId: string
  eventIndex: number
  thinking: boolean
}>()
const emit = defineEmits<{ select: [index: number]; toggleThinking: [] }>()
const presentation = computed(() => {
  const event = props.events[props.eventIndex]
  return event ? eventPresentation(event) : null
})
function chooseEvent(event: Event) {
  emit('select', Number((event.target as HTMLSelectElement).value))
}
</script>

<template>
  <section class="domain-section" :aria-labelledby="headingId">
    <header class="domain-header">
      <h2 :id="headingId">{{ name }}</h2>
      <select v-if="events.length > 1" class="event-selector" :aria-label="`${name} event`" :value="eventIndex" @change="chooseEvent">
        <option v-for="(event, index) in events" :key="event.id" :value="index">{{ shortType(event.type) }}</option>
      </select>
      <button v-if="presentation?.thinking" class="thinking-toggle" type="button" :aria-pressed="thinking" :title="thinking ? 'Show event details' : 'Show returned Thinking'" @click="emit('toggleThinking')">Thinking</button>
    </header>
    <pre v-if="thinking && presentation?.thinking" class="content thinking-content">{{ presentation.thinking }}</pre>
    <EventDetails v-else-if="presentation" :value="presentation.details" />
    <div v-else class="empty-content">No event content</div>
  </section>
</template>
