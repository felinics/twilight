import { DebugLogParser } from './lib/debug'

const worker = self as unknown as {
  postMessage: (message: unknown) => void
  onmessage: ((event: MessageEvent<{ file: File; fileName: string }>) => void) | null
}
worker.onmessage = async ({ data }) => {
  try {
    const { file, fileName } = data
    if (!(file instanceof Blob)) throw new Error('No file supplied')
    const parser = new DebugLogParser(fileName)
    const decoder = new TextDecoder()
    const reader = file.stream().getReader()
    let parts: string[] = []
    let loaded = 0
    let chunks = 0
    while (true) {
      const { value, done } = await reader.read()
      if (done) break
      loaded += value.byteLength
      const decoded = decoder.decode(value, { stream: true })
      parts.push(decoded)
      if (decoded.includes('\n')) {
        const text = parts.join('')
        let start = 0
        for (let end = text.indexOf('\n'); end >= 0; end = text.indexOf('\n', start)) {
          parser.pushLine(text.slice(start, end))
          start = end + 1
        }
        parts = start < text.length ? [text.slice(start)] : []
      }
      if (++chunks % 8 === 0) worker.postMessage({ type: 'progress', loaded, total: file.size })
    }
    parser.pushLine(parts.join('') + decoder.decode())
    worker.postMessage({ type: 'result', log: parser.finish() })
  } catch (error) {
    worker.postMessage({ type: 'error', message: error instanceof Error ? error.message : 'Unable to parse file' })
  }
}
