import type { DebugEvent, JsonValue } from './debug'

const bodyFields: Record<string, string> = {
  requestDigest: 'request', resultDigest: 'result',
  outputDigest: 'output', responseDigest: 'response',
}

// These fields identify ledger machinery, not the content being inspected.
// They remain available in the mutually exclusive Raw view.
const internalFields = new Set([
  'v', 'runId', 'stepId', 'effect', 'causationId', 'scope',
  'turnId', 'inputId', 'inputIds', 'requestDigest', 'resultDigest',
  'outputDigest', 'responseDigest',
])

export function eventDetails(event: DebugEvent): JsonValue {
  const used = new Set<string>()
  const source = event.payload
  const requestDigest = source && typeof source === 'object' && !Array.isArray(source) ? source.requestDigest : null
  const request = typeof requestDigest === 'string' ? event.bodies[requestDigest] : null
  const resultDigest = source && typeof source === 'object' && !Array.isArray(source) ? source.resultDigest : null
  const result = typeof resultDigest === 'string' ? event.bodies[resultDigest] : null
  function expand(value: JsonValue, root = false): JsonValue {
    if (Array.isArray(value)) return value.map((child) => expand(child))
    if (value === null || typeof value !== 'object') return value
    const fields: [string, JsonValue][] = []
    for (const [key, child] of Object.entries(value)) {
      // Model/tool content already lives inside the expanded request. The
      // outer copies are ledger metadata, kept only in Raw.
      if (root && (key === 'model' || key === 'tools') && request && typeof request === 'object' && !Array.isArray(request) && Object.prototype.hasOwnProperty.call(request, key)) continue
      // Settlement facts copy finish reason/usage from the frozen result.
      // Display exact mirrors only inside that result, never in both places.
      if (root && result && typeof result === 'object' && !Array.isArray(result) && Object.prototype.hasOwnProperty.call(result, key) && JSON.stringify(child) === JSON.stringify(result[key])) continue
      const label = bodyFields[key]
      if (label && typeof child === 'string') {
        if (Object.prototype.hasOwnProperty.call(event.bodies, child)) {
          // A frozen body has exactly one content location in the detail view.
          if (!used.has(child)) {
            used.add(child)
            fields.push([label, event.bodies[child]!])
          }
        } else {
          fields.push([label, 'Not included in this file'])
        }
      } else if (!root || !internalFields.has(key)) {
        fields.push([key, expand(child)])
      }
    }
    return Object.fromEntries(fields)
  }
  const payload = expand(event.payload, true)
  const extraBodies = Object.entries(event.bodies).filter(([digest]) => !used.has(digest))
  if (!extraBodies.length) return payload
  return { payload, bodies: extraBodies.map(([, body]) => body) }
}

// Only model result facts own thinking. Do not confuse reasoning embedded in
// request history with a new model response, or manufacture missing text.
export function eventPresentation(event: DebugEvent): { thinking: string; details: JsonValue } {
  const details = eventDetails(event)
  if (event.type !== 'twilight/run/model_step_completed' || !details || typeof details !== 'object' || Array.isArray(details)) {
    return { thinking: '', details }
  }
  const result = details.result
  if (!result || typeof result !== 'object' || Array.isArray(result)) return { thinking: '', details }
  const parts = Array.isArray(result.reasoningParts) ? result.reasoningParts : []
  const texts = parts.flatMap(part => part && typeof part === 'object' && !Array.isArray(part) && typeof part.text === 'string' ? [part.text] : [])
  const thinking = typeof result.reasoning === 'string' && result.reasoning ? result.reasoning : texts.join('')
  if (!thinking) return { thinking: '', details }
  const remaining = { ...result }
  delete remaining.reasoning
  // Consumed plaintext belongs only in Thinking. Opaque/non-text parts remain
  // visible as data; original IDs, signatures and blocks are always in Raw.
  const opaque = thinking === texts.join('') ? parts.filter(part => !part || typeof part !== 'object' || Array.isArray(part) || typeof part.text !== 'string' || !part.text) : parts
  if (opaque.length) remaining.reasoningParts = opaque
  else delete remaining.reasoningParts
  return { thinking, details: { ...details, result: remaining } }
}
