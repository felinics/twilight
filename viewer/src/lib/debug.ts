export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue }

export interface DebugEvent {
  id: string
  seq: number
  batchIndex: number
  eventIndex: number
  /** Base domain name used for filtering (e.g. run, not run/r7). */
  domain: string
  domainId?: string
  domainKey?: string
  sessionId?: string
  lineIndex?: number
  type: string
  recordedAtUnixMilli?: number
  payload: JsonValue
  bodies: Record<string, JsonValue>
}

export interface DebugCommit {
  seq: number
  commitId: string
  events: DebugEvent[]
  id?: string
  sessionId?: string
  lineIndex?: number
  schema?: 'twilight.debug.v1'
}

export interface DebugLog {
  sessionId: string
  fileName: string
  commits: DebugCommit[]
  events: DebugEvent[]
  domains: string[]
  lineCount: number
  errors: string[]
}

function isObject(value: unknown): value is Record<string, JsonValue> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
}

function hasOwn(object: object, key: string): boolean {
  return Object.prototype.hasOwnProperty.call(object, key)
}

function fileSessionId(fileName: string): string {
  const base = fileName.split(/[\\/]/).pop() ?? fileName
  return base.replace(/\.jsonl?$/i, '') || 'session'
}

// Compare decimal values without expanding exponents or rounding through JS.
function decimalIdentity(token: string): string {
  const [mantissa = '', exponent = '0'] = token.toLowerCase().split('e')
  const negative = mantissa.startsWith('-')
  const unsigned = negative ? mantissa.slice(1) : mantissa
  const dot = unsigned.indexOf('.')
  const fractionLength = dot < 0 ? 0 : unsigned.length - dot - 1
  let digits = unsigned.replace('.', '').replace(/^0+/, '')
  if (!digits) return '0'
  const trailingZeros = digits.length - digits.replace(/0+$/, '').length
  digits = digits.slice(0, digits.length - trailingZeros)
  return `${negative ? '-' : ''}${digits}e${Number(exponent) - fractionLength + trailingZeros}`
}

/**
 * JSON.parse rounds large integers and long decimals. Preserve those tokens as
 * exact decimal strings (still JsonValue), rather than silently corrupting them.
 * Strings/escapes are skipped lexically; ordinary representable numbers retain
 * their original number type. No payload stringify/reparse round-trip is used.
 */
function parseJson(source: string): JsonValue {
  const pieces: string[] = []
  let copiedUntil = 0
  let index = 0
  while (index < source.length) {
    const char = source[index]
    if (char === '"') {
      index++
      while (index < source.length) {
        if (source[index] === '\\') index += 2
        else if (source[index++] === '"') break
      }
    } else if (char === '-' || (char !== undefined && char >= '0' && char <= '9')) {
      const start = index++
      while (index < source.length && /[0-9.eE+\-]/.test(source[index]!)) index++
      const token = source.slice(start, index)
      // Leave malformed tokens untouched so JSON.parse reports their syntax.
      if (!/^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$/.test(token)) continue
      const number = Number(token)
      if (!Number.isFinite(number) || decimalIdentity(token) !== decimalIdentity(String(number))) {
        pieces.push(source.slice(copiedUntil, start), `"${token}"`)
        copiedUntil = index
      }
    } else {
      index++
    }
  }
  if (!pieces.length) return JSON.parse(source) as JsonValue
  pieces.push(source.slice(copiedUntil))
  return JSON.parse(pieces.join('')) as JsonValue
}

// Incremental parser for the streaming worker.
export class DebugLogParser {
  private readonly log: DebugLog
  private readonly domains = new Set<string>()
  private readonly sessions = new Set<string>()
  private readonly fallbackSession: string
  private finished = false

  constructor(fileName = 'debug.jsonl') {
    this.fallbackSession = fileSessionId(fileName)
    this.log = {
      sessionId: this.fallbackSession, fileName, commits: [], events: [],
      domains: [], lineCount: 0, errors: [],
    }
  }

  private diagnostic(message: string, batchIndex?: number, eventIndex?: number): void {
    const line = this.log.lineCount
    const location = `${batchIndex === undefined ? '' : ` batch ${batchIndex}`}${eventIndex === undefined ? '' : ` event ${eventIndex}`}`
    this.log.errors.push(`line ${line}${location}: ${message}`)
  }

  pushLine(line: string): void {
    if (this.finished) throw new Error('Cannot append to a finished debug log')
    const lineIndex = this.log.lineCount++
    const source = (lineIndex === 0 ? line.replace(/^\uFEFF/, '') : line).trim()
    if (!source) return
    let raw: JsonValue
    try {
      raw = parseJson(source)
    } catch (error) {
      this.diagnostic(error instanceof Error ? error.message : 'invalid JSON')
      return
    }
    if (!isObject(raw)) {
      this.diagnostic('commit must be a JSON object')
      return
    }
    const versioned = hasOwn(raw, 'schema')
    if (versioned && raw.schema !== 'twilight.debug.v1') {
      this.diagnostic('unsupported schema (expected twilight.debug.v1)')
      return
    }
    if ((versioned || hasOwn(raw, 'kind')) && raw.kind !== 'commit') {
      this.diagnostic('unsupported record kind (expected commit)')
      return
    }
    if (versioned && (typeof raw.sessionId !== 'string' || !raw.sessionId.trim())) {
      this.diagnostic('versioned commit requires a non-empty sessionId')
      return
    }
    if (!Array.isArray(raw.batches)) {
      this.diagnostic('missing or invalid batches (expected array)')
      return
    }
    let sessionId = this.fallbackSession
    if (typeof raw.sessionId === 'string' && raw.sessionId.trim()) sessionId = raw.sessionId
    else if (hasOwn(raw, 'sessionId')) this.diagnostic('invalid sessionId; using file name')
    if (!this.sessions.size) this.log.sessionId = sessionId
    this.sessions.add(sessionId)
    let seq = this.log.commits.length
    if (typeof raw.seq === 'number' && Number.isSafeInteger(raw.seq) && raw.seq >= 0) seq = raw.seq
    else if (hasOwn(raw, 'seq')) this.diagnostic('invalid seq (expected non-negative safe integer); using commit index')
    let commitId = 'unknown'
    if (typeof raw.commitId === 'string' && raw.commitId) commitId = raw.commitId
    else if (hasOwn(raw, 'commitId')) this.diagnostic('invalid commitId (expected non-empty string)')
    // JSON-encoded tuple avoids collisions even when session names contain ':' etc.
    const id = JSON.stringify([sessionId, lineIndex, seq])
    const commit: DebugCommit = {
      id, seq, commitId, sessionId, lineIndex, events: [],
      ...(versioned ? { schema: 'twilight.debug.v1' as const } : {}),
    }
    raw.batches.forEach((batch, batchIndex) => {
      if (!isObject(batch)) {
        this.diagnostic('batch must be an object', batchIndex)
        return
      }
      let domain = 'ledger'
      let domainId: string | undefined
      if (typeof batch.domain === 'string') {
        const slash = batch.domain.indexOf('/')
        domain = (slash < 0 ? batch.domain : batch.domain.slice(0, slash)) || 'ledger'
        domainId = slash < 0 ? undefined : batch.domain.slice(slash + 1) || undefined
      } else if (isObject(batch.domain)) {
        if (typeof batch.domain.name === 'string') domain = batch.domain.name || 'ledger'
        else if (hasOwn(batch.domain, 'name')) this.diagnostic('invalid domain.name', batchIndex)
        if (typeof batch.domain.id === 'string') domainId = batch.domain.id || undefined
        else if (hasOwn(batch.domain, 'id')) this.diagnostic('invalid domain.id', batchIndex)
      } else if (hasOwn(batch, 'domain')) {
        this.diagnostic('invalid domain (expected string or object); using ledger', batchIndex)
      }
      if (!Array.isArray(batch.events)) {
        this.diagnostic('missing or invalid events (expected array)', batchIndex)
        return
      }
      batch.events.forEach((event, eventIndex) => {
        if (!isObject(event)) {
          this.diagnostic('event must be an object', batchIndex, eventIndex)
          return
        }
        let type = 'unknown'
        if (typeof event.type === 'string' && event.type) type = event.type
        else if (hasOwn(event, 'type')) this.diagnostic('invalid event type; using unknown', batchIndex, eventIndex)
        let recordedAtUnixMilli: number | undefined
        if (typeof event.recordedAtUnixMilli === 'number' && Number.isSafeInteger(event.recordedAtUnixMilli)) {
          recordedAtUnixMilli = event.recordedAtUnixMilli
        } else if (hasOwn(event, 'recordedAtUnixMilli')) {
          this.diagnostic('invalid recordedAtUnixMilli (expected safe integer)', batchIndex, eventIndex)
        }
        const payload = event.payload ?? null
        let bodies: Record<string, JsonValue> = {}
        if (isObject(event.bodies)) bodies = event.bodies
        else if (hasOwn(event, 'bodies')) this.diagnostic('invalid bodies (expected object)', batchIndex, eventIndex)
        const item: DebugEvent = {
          id: JSON.stringify([sessionId, lineIndex, seq, batchIndex, eventIndex]),
          seq, batchIndex, eventIndex, sessionId, lineIndex, domain, domainId,
          domainKey: domainId ? `${domain}/${domainId}` : domain,
          type, recordedAtUnixMilli, payload, bodies,
        }
        commit.events.push(item)
        this.log.events.push(item)
        this.domains.add(domain)
      })
    })
    this.log.commits.push(commit)
  }

  finish(): DebugLog {
    if (!this.finished) {
      this.finished = true
      // Preserve the old seq ordering for a single session. Across sessions,
      // seq is not comparable; retain file order instead.
      if (this.sessions.size <= 1) this.log.commits.sort((a, b) => a.seq - b.seq)
      this.log.domains = [...this.domains].sort()
    }
    return this.log
  }
}

export function shortType(type: string): string {
  const parts = type.split('/')
  return parts[parts.length - 1] || type
}

export function formatJson(value: unknown): string {
  return JSON.stringify(value, null, 2) ?? 'null'
}

export function formatTime(timestamp?: number): string {
  if (!timestamp || !Number.isFinite(timestamp)) return '—'
  const date = new Date(timestamp)
  return Number.isNaN(date.getTime()) ? '—' : date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false })
}
