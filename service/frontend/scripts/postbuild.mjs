import { chmodSync, readdirSync } from 'node:fs'
import { join } from 'node:path'

// Linux nginx needs readable files; Windows ACLs do not use POSIX mode bits.
if (process.platform !== 'win32') {
  const visit = (path) => {
    chmodSync(path, 0o755)
    for (const item of readdirSync(path, { withFileTypes: true })) {
      const child = join(path, item.name)
      if (item.isDirectory()) visit(child)
      else chmodSync(child, 0o644)
    }
  }
  visit('dist')
}
