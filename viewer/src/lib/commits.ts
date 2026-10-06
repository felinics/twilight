import { shortType, type DebugCommit, type DebugEvent } from './debug'

// View labels, not persisted facts. Prefer the lifecycle event that explains
// the operation; all accompanying events stay inside the same commit.
const labels: [string, string][] = [
  ['twilight/turn/started', 'Start turn'],
  ['twilight/turn/ended', 'End turn'],
  ['twilight/run/model_step_completed', 'Model response'],
  ['twilight/run/model_step_prepared', 'Model request'],
  ['twilight/run/tool_call_completed', 'Tool result'],
  ['twilight/run/tool_call_answered', 'Tool response'],
  ['twilight/chatlog/input_submitted', 'Submit input'],
  ['twilight/chatlog/input_delivered', 'Deliver input'],
  ['agent/workspace/bound', 'Bind workspace'],
  ['agent/workspace/unbound', 'Unbind workspace'],
]

export function commitName(commit: DebugCommit): string {
  for (const [type, label] of labels) {
    if (commit.events.some((event) => event.type === type)) return label
  }
  const names = [...new Set(commit.events.map((event) => shortType(event.type)))]
  return names.length ? names.join(' + ') : 'Empty commit'
}

export function commitDomains(commit: DebugCommit): { name: string; events: DebugEvent[] }[] {
  const groups = new Map<string, DebugEvent[]>()
  for (const event of commit.events) {
    const events = groups.get(event.domain) ?? []
    events.push(event)
    groups.set(event.domain, events)
  }
  return [...groups].map(([name, events]) => ({ name, events }))
}

export function matchesCommit(commit: DebugCommit, domain: string, query: string): boolean {
  if (domain && !commit.events.some((event) => event.domain === domain)) return false
  const needle = query.trim().toLowerCase()
  if (!needle || commitName(commit).toLowerCase().includes(needle)) return true
  return commit.events.some((event) =>
    `${event.type} ${event.domain} ${JSON.stringify(event.payload)} ${JSON.stringify(event.bodies)}`
      .toLowerCase().includes(needle),
  )
}
