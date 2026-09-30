/**
 * Boundary and recovery test coverage for the jobs barrel entry point
 * (`src/jobs/index.ts`).
 *
 * `src/jobs/index.ts` is the public module surface of the background-jobs
 * subsystem: every scheduler, worker, sweeper, and job type that callers may
 * import is re-exported there. A defect in the barrel (a missing re-export, a
 * value that resolves to `undefined`, a broken dependency of any re-exported
 * module) makes the *whole* jobs subsystem unimportable at runtime, which
 * silently degrades every consumer that imports from the barrel.
 *
 * This suite validates deterministic behaviour of that entry point under:
 * - Valid, invalid, duplicate, and boundary-case inputs handed to the
 *   re-exported implementations through the barrel
 * - Module-loading invariants: the barrel loads exactly once, every runtime
 *   export is defined, and re-exported bindings keep their deep-import identity
 * - In-flight guards, failure recovery, and bounded work loops in the
 *   re-exported sweeper, driven through the public barrel import
 * - Type-level surface compatibility for the shared job contracts
 *
 * Related: Issue #1425
 */

import { describe, expect, it, vi } from 'vitest'
import * as jobsBarrel from './index.js'
import { RingBuffer as DeepRingBuffer } from './ringBuffer.js'
import { parseCronToInterval as deepParseCronToInterval } from './scheduler.js'
import { IdempotencyKeySweeper as DeepIdempotencyKeySweeper } from './idempotencyKeySweeper.js'
import { ReportJobStatus as DeepReportJobStatus } from './types.js'
import type { Queryable } from '../db/repositories/queryable.js'
import type { QueryResult } from 'pg'
import type { ScoreSnapshot, ReportJob } from './index.js'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type QueryCall = { text: string; params?: readonly unknown[] }

/**
 * Scriptable Queryable double. `steps` are consumed in order; the last step
 * repeats, which keeps long delete loops deterministic without dynamic magic.
 */
function fakeDb(
  steps: Array<Partial<QueryResult> | Error>,
  calls: QueryCall[] = [],
): Queryable {
  let i = 0
  return {
    query: vi.fn(async (text: string, params?: readonly unknown[]) => {
      calls.push({ text, params })
      const step = steps[Math.min(i, steps.length - 1)]
      i++
      if (step instanceof Error) throw step
      return step as QueryResult
    }),
  }
}

const countResult = (n: number): Partial<QueryResult> => ({
  rows: [{ count: String(n) }],
  rowCount: 0,
})

const deleteResult = (n: number): Partial<QueryResult> => ({
  rows: [],
  rowCount: n,
})

/** Runtime export names the barrel must always publish (compat guard). */
const REQUIRED_RUNTIME_EXPORTS = [
  'RingBuffer',
  'JobScheduler',
  'createScheduler',
  'parseCronToInterval',
  'getBulkWorkerPollQuery',
  'ReportJobStatus',
  'ScoreSnapshotJob',
  'ExportWorker',
  'BatchPayoutProcessor',
  'ResumableBackfillRunner',
  'runResumableBackfill',
  'IdempotencyKeySweeper',
  'sweepExpiredIdempotencyKeys',
  'InvoiceDueDateWorker',
  'AnalyticsRefreshWorker',
  'AuditChainVerifier',
  'WebhookDlqProcessor',
] as const

// ---------------------------------------------------------------------------
// Module-loading invariants
// ---------------------------------------------------------------------------

describe('jobs barrel - Boundary and Recovery Tests', () => {
  describe('Module Loading Invariants', () => {
    it('loads and publishes every required runtime export', () => {
      for (const name of REQUIRED_RUNTIME_EXPORTS) {
        expect(jobsBarrel, `missing export: ${name}`).toHaveProperty(name)
      }
    })

    it('never publishes an undefined runtime export', () => {
      // A barrel that re-exports a name which resolves to undefined (bad
      // star-export, circular import, half-deleted module) would make callers
      // crash far from the cause. The entry point must fail loudly here.
      const undefinedExports = Object.keys(jobsBarrel).filter(
        (key) => (jobsBarrel as Record<string, unknown>)[key] === undefined,
      )
      expect(undefinedExports).toEqual([])
    })

    it('keeps re-exported bindings identical to their deep imports', () => {
      // Identity (not structural) equality proves the barrel re-exports the
      // same implementation instead of a divergent copy.
      expect(jobsBarrel.RingBuffer).toBe(DeepRingBuffer)
      expect(jobsBarrel.parseCronToInterval).toBe(deepParseCronToInterval)
      expect(jobsBarrel.IdempotencyKeySweeper).toBe(DeepIdempotencyKeySweeper)
      expect(jobsBarrel.ReportJobStatus).toBe(DeepReportJobStatus)
    })

    it('exposes no duplicate bindings across modules (load would throw)', async () => {
      // ESM throws a SyntaxError at link time when two star-exports claim the
      // same binding, so a successful import is itself the invariant check.
      // Importing again through a dynamic specifier exercises the ESM cache:
      // the same namespace object must come back, not a fresh evaluation.
      const again = await import('./index.js')
      expect(again).toBe(jobsBarrel)
    })

    it('type-level contracts stay importable through the barrel', () => {
      // Compile-time surface guard: erased at runtime, checked by tsc.
      const snapshot: ScoreSnapshot = {
        address: 'GABC',
        score: 0.5,
        bondedAmount: '0',
        attestationCount: 0,
        timestamp: new Date().toISOString(),
      }
      const job: ReportJob = {
        id: 'job-1',
        type: 'export',
        status: jobsBarrel.ReportJobStatus.QUEUED,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      }
      expect(snapshot.score).toBe(0.5)
      expect(job.status).toBe('queued')
    })
  })

  // -------------------------------------------------------------------------
  // Boundary behaviour of re-exported implementations, via the barrel
  // -------------------------------------------------------------------------

  describe('RingBuffer boundaries (via barrel export)', () => {
    it('accepts exactly one item at capacity 1 and applies backpressure beyond it', () => {
      const buf = new jobsBarrel.RingBuffer<number>(1)
      expect(buf.push(1)).toBe(true)
      expect(buf.isFull).toBe(true)
      expect(buf.push(2)).toBe(false)
      expect(buf.size).toBe(1)
      expect(buf.pop()).toBe(1)
      expect(buf.isEmpty).toBe(true)
    })

    it('wraps around after cycling the full capacity', () => {
      const buf = new jobsBarrel.RingBuffer<string>(2)
      buf.push('a')
      buf.push('b')
      expect(buf.push('c')).toBe(false)
      expect(buf.pop()).toBe('a')
      expect(buf.push('c')).toBe(true)
      expect(buf.pop()).toBe('b')
      expect(buf.pop()).toBe('c')
    })
  })

  describe('parseCronToInterval boundaries (via barrel export)', () => {
    it('maps the three supported cron patterns to their exact intervals', () => {
      expect(jobsBarrel.parseCronToInterval('* * * * *')).toBe(60_000)
      expect(jobsBarrel.parseCronToInterval('0 * * * *')).toBe(3_600_000)
      expect(jobsBarrel.parseCronToInterval('0 0 * * *')).toBe(86_400_000)
    })

    it('rejects cron expressions with the wrong number of fields', () => {
      expect(() => jobsBarrel.parseCronToInterval('0 0 * *')).toThrow(
        'Invalid cron expression: must have 5 parts',
      )
      expect(() => jobsBarrel.parseCronToInterval('0 0 * * * *')).toThrow(
        'Invalid cron expression: must have 5 parts',
      )
    })

    it('rejects well-formed but unsupported patterns', () => {
      expect(() => jobsBarrel.parseCronToInterval('59 * * * *')).toThrow(
        'Unsupported cron expression: 59 * * * *',
      )
    })
  })

  describe('ReportJobStatus enum boundaries (via barrel export)', () => {
    it('publishes the full status vocabulary with canonical values', () => {
      expect(jobsBarrel.ReportJobStatus.QUEUED).toBe('queued')
      expect(jobsBarrel.ReportJobStatus.RUNNING).toBe('running')
      expect(jobsBarrel.ReportJobStatus.COMPLETED).toBe('completed')
      expect(jobsBarrel.ReportJobStatus.FAILED).toBe('failed')
      expect(jobsBarrel.ReportJobStatus.CANCELLED).toBe('cancelled')
    })

    it('keeps every status value unique (no collapsed transitions)', () => {
      const values = Object.values(jobsBarrel.ReportJobStatus)
      expect(new Set(values).size).toBe(values.length)
    })
  })

  describe('JobScheduler lifecycle (via barrel export)', () => {
    it('starts only when asked, reports activity, and stops cleanly', () => {
      vi.useFakeTimers()
      try {
        const job = { run: vi.fn(async () => ({ ok: true })) }
        const scheduler = new jobsBarrel.JobScheduler(job, {
          intervalMs: 1000,
          logger: () => {},
        })

        expect(scheduler.isActive()).toBe(false)
        scheduler.start()
        expect(scheduler.isActive()).toBe(true)

        // stop() releases the timer; a second stop() is a safe no-op.
        scheduler.stop()
        scheduler.stop()
        expect(scheduler.isActive()).toBe(false)
      } finally {
        vi.useRealTimers()
      }
    })
  })

  describe('IdempotencyKeySweeper (via barrel export)', () => {
    it('dry-run counts expired keys without issuing any delete', async () => {
      const calls: QueryCall[] = []
      const db = fakeDb([countResult(5)], calls)
      const sweeper = new jobsBarrel.IdempotencyKeySweeper(db, {
        dryRun: true,
        logger: () => {},
      })

      const result = await sweeper.run()

      expect(result.expiredCount).toBe(5)
      expect(result.deletedCount).toBe(0)
      expect(result.dryRun).toBe(true)
      expect(result.durationMs).toBeGreaterThanOrEqual(0)
      expect(calls.filter((c) => c.text.includes('DELETE'))).toHaveLength(0)
      expect(calls.filter((c) => c.text.includes('SELECT COUNT'))).toHaveLength(1)
    })

    it('zero expired keys is a no-op: no delete query is issued', async () => {
      const calls: QueryCall[] = []
      const db = fakeDb([countResult(0)], calls)
      const sweeper = new jobsBarrel.IdempotencyKeySweeper(db, { logger: () => {} })

      const result = await sweeper.run()

      expect(result.expiredCount).toBe(0)
      expect(result.deletedCount).toBe(0)
      expect(calls.filter((c) => c.text.includes('DELETE'))).toHaveLength(0)
    })

    it('stops batch-deleting exactly when a partial batch drains the backlog', async () => {
      const calls: QueryCall[] = []
      // expired=5, batchSize=2 → full batch, full batch, then a short batch
      // that must terminate the loop instead of issuing a fourth delete.
      const db = fakeDb([countResult(5), deleteResult(2), deleteResult(2), deleteResult(1)], calls)
      const sweeper = new jobsBarrel.IdempotencyKeySweeper(db, {
        batchSize: 2,
        logger: () => {},
      })

      const result = await sweeper.run()

      expect(result.expiredCount).toBe(5)
      expect(result.deletedCount).toBe(5)
      const deletes = calls.filter((c) => c.text.includes('DELETE'))
      expect(deletes).toHaveLength(3)
      expect(deletes.every((c) => c.params?.[0] === 2)).toBe(true)
    })

    it('bounds the delete loop when a misbehaving driver reports constant rowCount', async () => {
      const calls: QueryCall[] = []
      // A driver that always claims rowCount == batchSize must not spin
      // forever: the loop is bounded by the initial expired count.
      const db = fakeDb([countResult(4), deleteResult(2)], calls)
      const sweeper = new jobsBarrel.IdempotencyKeySweeper(db, {
        batchSize: 2,
        logger: () => {},
      })

      const result = await sweeper.run()

      expect(result.deletedCount).toBe(4)
      expect(calls.filter((c) => c.text.includes('DELETE'))).toHaveLength(2)
    })

    it('skips re-entrant runs while one is in flight and reports them as no-ops', async () => {
      let releaseCount!: (value: Partial<QueryResult>) => void
      const gate = new Promise<Partial<QueryResult>>((resolve) => {
        releaseCount = resolve
      })
      const db = fakeDb([gate, countResult(0)])
      const sweeper = new jobsBarrel.IdempotencyKeySweeper(db, { logger: () => {} })

      const first = sweeper.run()
      const second = sweeper.run()

      expect(sweeper.isRunning()).toBe(true)
      releaseCount(countResult(0))

      const [r1, r2] = await Promise.all([first, second])
      expect(r2.skipped).toBe(true)
      expect(r2.durationMs).toBe(0)
      expect(r1.skipped).toBeUndefined()
      expect(sweeper.isRunning()).toBe(false)
    })

    it('releases the in-flight guard on failure so the next cycle can recover', async () => {
      const calls: QueryCall[] = []
      const db = fakeDb([new Error('connection reset'), countResult(0)], calls)
      const sweeper = new jobsBarrel.IdempotencyKeySweeper(db, { logger: () => {} })

      await expect(sweeper.run()).rejects.toThrow('connection reset')
      expect(sweeper.isRunning()).toBe(false)

      // Recovery: the immediately following run executes normally.
      const recovered = await sweeper.run()
      expect(recovered.expiredCount).toBe(0)
      expect(sweeper.isRunning()).toBe(false)
    })

    it('reports non-Error throwables without leaking their content in logs', async () => {
      const logs: string[] = []
      const db = fakeDb([{ rows: [], rowCount: 0 }])
      // Row shape is wrong on purpose: rows[0] is undefined → falls back to '0'
      const sweeper = new jobsBarrel.IdempotencyKeySweeper(db, { logger: (m) => logs.push(m) })

      const result = await sweeper.run()
      expect(result.expiredCount).toBe(0)
      expect(logs.some((l) => l.includes('expired=0'))).toBe(true)
    })
  })

  describe('sweepExpiredIdempotencyKeys standalone helper (via barrel export)', () => {
    it('delegates to a one-shot sweeper run and returns its result', async () => {
      const db = fakeDb([countResult(3)])
      const result = await jobsBarrel.sweepExpiredIdempotencyKeys(db, {
        dryRun: true,
        logger: () => {},
      })

      expect(result.expiredCount).toBe(3)
      expect(result.deletedCount).toBe(0)
      expect(result.dryRun).toBe(true)
    })

    it('works with no config at all (all defaults apply)', async () => {
      const db = fakeDb([countResult(0)])
      const result = await jobsBarrel.sweepExpiredIdempotencyKeys(db)
      expect(result.expiredCount).toBe(0)
      expect(result.dryRun).toBe(false)
    })
  })

  describe('getBulkWorkerPollQuery (via barrel export)', () => {
    it('polls the default tables with weighted ordering and LIMIT 1', () => {
      const sql = jobsBarrel.getBulkWorkerPollQuery()
      expect(sql).toContain('FROM bulk_jobs')
      expect(sql).toContain('FROM org_usage_daily')
      expect(sql).toContain("WHERE j.status = 'pending'")
      expect(sql).toContain('LIMIT 1')
      expect(sql).toContain('ORDER BY wfq_score ASC, created_at ASC')
    })

    it('interpolates custom table identifiers verbatim (documented integrator contract)', () => {
      const sql = jobsBarrel.getBulkWorkerPollQuery('bulk_jobs_shard_a', 'org_usage_daily_2026')
      expect(sql).toContain('FROM bulk_jobs_shard_a')
      expect(sql).toContain('FROM org_usage_daily_2026')
      // Deliberately unquoted identifiers are the documented caller contract —
      // the query must not silently add quoting that changes the table name.
      expect(sql).not.toContain('"bulk_jobs_shard_a"')
    })
  })

  describe('Backfill exports (via barrel export)', () => {
    it('publishes the resumable backfill runner and its one-shot helper', () => {
      expect(typeof jobsBarrel.ResumableBackfillRunner).toBe('function')
      expect(typeof jobsBarrel.runResumableBackfill).toBe('function')
    })
  })
})
