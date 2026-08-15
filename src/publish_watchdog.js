// Publish-path watchdog.
//
// content_pipeline items are approved by a human and then handed to the
// publisher. When the publisher stops being invoked at all, nothing in the
// system notices: content_publish_attempts simply stops gaining rows, and a
// daily "did the check run" probe keeps reporting success because the check
// itself ran fine. Approved work then sits in status=ready indefinitely.
//
// This watchdog closes that gap. It finds approved items that are past due,
// records a real failed attempt for each one so the drop is visible in
// content_publish_attempts, raises a severity-routed notification, and reports
// unhealthy so the daily check fails instead of passing.
//
// It deliberately does not publish. Publishing lives in the channel workers;
// this worker owns the alerting path, and the failure it exists to catch is
// "the publisher was never invoked at all".

import { supabaseQuery } from './lib/supabase.js';
import { dispatchNotification } from './notify.js';

const READY_STATUS = 'ready';
const PENDING_ATTEMPT_STATUS = 'pending';

// An approved item with no scheduled_at is considered due this many hours
// after approval. An item with a scheduled_at is due at that instant.
const DEFAULT_GRACE_HOURS = 24;

// Don't re-alert more often than this, no matter how often the cron fires.
const ALERT_DEDUPE_MS = 6 * 60 * 60 * 1000;

const MAX_ITEMS = 200;
const MAX_ATTEMPT_ROWS = 500;
const ALERT_SEVERITY = 'P1';
const ALERT_TRIGGER_KEY = 'content.publish_stalled';
const ALERT_DEDUPE_KEY = 'content-publish-stalled';
const DEFAULT_TENANT_ID = '00000000-0000-0000-0000-000000000000';

const ATTEMPT_FAILURE_STATUS = 'failed';
const ATTEMPT_FAILURE_REASON = 'publish_never_invoked: item was past due with no publish attempt on record';

function graceMs(env) {
  const hours = Number(env.PUBLISH_GRACE_HOURS);
  return (Number.isFinite(hours) && hours >= 0 ? hours : DEFAULT_GRACE_HOURS) * 60 * 60 * 1000;
}

function parseTs(value) {
  if (!value) return null;
  const ms = Date.parse(value);
  return Number.isFinite(ms) ? ms : null;
}

// When an approved item should have been published, in epoch ms.
// Returns null for items that were never approved -- those are not overdue,
// they are simply not ready, and flagging them would bury the real signal.
export function dueAt(row, grace) {
  const scheduled = parseTs(row.scheduled_at);
  if (scheduled !== null) return scheduled;
  const approved = parseTs(row.approved_at);
  if (approved !== null) return approved + grace;
  return null;
}

export function overdueItems(rows, nowMs, grace) {
  return rows
    .map((row) => ({ row, due: dueAt(row, grace) }))
    .filter(({ due }) => due !== null && due <= nowMs)
    .sort((a, b) => a.due - b.due);
}

function pick(columns, candidates) {
  return candidates.find((c) => columns.includes(c)) || candidates[0];
}

// content_publish_attempts belongs to another service's schema, so learn its
// column names from a row that service already wrote rather than hard-coding a
// guess that would make every insert 400.
async function describeAttempts(env) {
  const res = await supabaseQuery(env, 'content_publish_attempts?select=*&limit=1');
  const sample = res.data?.[0] || null;
  const columns = sample ? Object.keys(sample) : [];
  return {
    columns,
    sampled: Boolean(sample),
    contentKey: pick(columns, ['content_id', 'content_pipeline_id', 'pipeline_id', 'item_id']),
    statusKey: pick(columns, ['status', 'state', 'result']),
    errorKey: pick(columns, ['error', 'error_message', 'failure_reason', 'message', 'detail']),
    createdKey: pick(columns, ['attempted_at', 'created_at', 'inserted_at', 'ts']),
  };
}

async function latestAttemptAt(env, shape) {
  const res = await supabaseQuery(env,
    `content_publish_attempts?select=*&order=${shape.createdKey}.desc&limit=1`
  );
  const row = res.data?.[0];
  return row ? row[shape.createdKey] ?? null : null;
}

// Attempts recorded since `sinceIso`, keyed by the content id they belong to.
async function attemptsSince(env, shape, sinceIso) {
  const res = await supabaseQuery(env,
    `content_publish_attempts?select=*&${shape.createdKey}=gte.${encodeURIComponent(sinceIso)}&limit=${MAX_ATTEMPT_ROWS}`
  );
  const byContent = new Map();
  for (const row of res.data || []) {
    const key = row[shape.contentKey];
    if (key == null) continue;
    const at = parseTs(row[shape.createdKey]);
    const prev = byContent.get(key);
    if (prev == null || (at !== null && at > prev)) byContent.set(key, at);
  }
  return byContent;
}

// Attempts that started and never finished. The publisher crashing mid-flight
// is a different failure from never being invoked, but it strands the item
// just as completely.
async function stuckPendingAttempts(env, shape, nowMs, grace) {
  const res = await supabaseQuery(env,
    `content_publish_attempts?select=*&${shape.statusKey}=eq.${PENDING_ATTEMPT_STATUS}&limit=${MAX_ATTEMPT_ROWS}`
  );
  return (res.data || []).filter((row) => {
    const at = parseTs(row[shape.createdKey]);
    return at !== null && nowMs - at > grace;
  });
}

function buildAttemptRow(shape, item, now, reason) {
  const row = {};
  row[shape.contentKey] = item.id;
  row[shape.statusKey] = ATTEMPT_FAILURE_STATUS;
  row[shape.errorKey] = reason;
  row[shape.createdKey] = now.toISOString();

  // Carry over any correlating columns the table and the item share.
  for (const key of ['tenant_id', 'channel_id', 'platform', 'brand_project_id', 'project_id']) {
    if (shape.columns.includes(key) && item[key] != null) row[key] = item[key];
  }
  return row;
}

async function recordFailedAttempt(env, shape, item, now, reason) {
  const res = await supabaseQuery(env, 'content_publish_attempts', {
    method: 'POST',
    body: buildAttemptRow(shape, item, now, reason),
    prefer: 'return=minimal',
  });
  if (!res.ok) {
    console.error('content_publish_attempts insert failed', item.id, res.status, res.error);
  }
  return res;
}

function alertBody(overdue, stuckPending, lastAttemptAt, nowMs) {
  const lines = [];
  if (overdue.length) {
    lines.push(`${overdue.length} approved item(s) are past due with no publish attempt on record.`);
  } else {
    lines.push('No approved item is past due, but the publish path is not clearing its work.');
  }
  if (lastAttemptAt) {
    const ageDays = Math.floor((nowMs - (parseTs(lastAttemptAt) ?? nowMs)) / 86400000);
    lines.push(`Last publish attempt of any kind: ${lastAttemptAt} (${ageDays}d ago).`);
  } else {
    lines.push('No publish attempt has ever been recorded.');
  }
  if (stuckPending.length) {
    lines.push(`${stuckPending.length} attempt(s) stuck in ${PENDING_ATTEMPT_STATUS}.`);
  }
  lines.push('');
  for (const { row, due } of overdue.slice(0, 5)) {
    const title = row.title || row.id;
    lines.push(`- ${title} (due ${new Date(due).toISOString()})`);
  }
  if (overdue.length > 5) lines.push(`- ...and ${overdue.length - 5} more`);
  return lines.join('\n');
}

async function raiseStalledAlert(env, overdue, stuckPending, lastAttemptAt, now) {
  const cutoff = new Date(now.getTime() - ALERT_DEDUPE_MS).toISOString();
  const recent = await supabaseQuery(env,
    `notification_log?dedupe_key=eq.${encodeURIComponent(ALERT_DEDUPE_KEY)}&created_at=gt.${cutoff}&limit=1&select=id`
  );
  if (recent.ok && recent.data?.length) return { status: 'deduped' };

  const tenantId = overdue[0]?.row?.tenant_id || env.CHANNELS_DEFAULT_TENANT_ID || DEFAULT_TENANT_ID;

  const dispatched = await dispatchNotification(env, {
    tenant_id: tenantId,
    agent_id: env.CHANNELS_DEFAULT_AGENT_ID || 'sovereign',
    trigger_key: ALERT_TRIGGER_KEY,
    severity: ALERT_SEVERITY,
    title: 'Content publish path stalled',
    body: alertBody(overdue, stuckPending, lastAttemptAt, now.getTime()),
    dedupe_key: ALERT_DEDUPE_KEY,
  });

  if (dispatched.status === 'no_route') {
    console.error(`${ALERT_TRIGGER_KEY} has no enabled notification_routing rule; alert was logged only`);
  }
  return dispatched;
}

// dryRun reads the same state and reaches the same verdict without writing
// attempt rows or sending anything -- that is what the daily health check uses.
export async function runPublishWatchdog(env, { now = new Date(), dryRun = false } = {}) {
  const nowMs = now.getTime();
  const grace = graceMs(env);
  const checkedAt = now.toISOString();

  const readyRes = await supabaseQuery(env,
    `content_pipeline?status=eq.${READY_STATUS}&select=*&limit=${MAX_ITEMS}`
  );
  if (!readyRes.ok) {
    // Cannot tell healthy from stalled, so report unhealthy rather than pass.
    return {
      ok: false,
      status: 'error',
      checked_at: checkedAt,
      error: `content_pipeline query failed (${readyRes.status})`,
    };
  }

  const ready = readyRes.data || [];
  const shape = await describeAttempts(env);
  const lastAttemptAt = await latestAttemptAt(env, shape);
  const stuckPending = await stuckPendingAttempts(env, shape, nowMs, grace);

  const overdueAll = overdueItems(ready, nowMs, grace);

  // An item is only "silently dropped" if nothing tried to publish it since it
  // came due. An item with a recent failed attempt is already visible.
  const oldestDue = overdueAll.length ? overdueAll[0].due : nowMs;
  const attempts = overdueAll.length
    ? await attemptsSince(env, shape, new Date(oldestDue).toISOString())
    : new Map();
  const overdue = overdueAll.filter(({ row, due }) => {
    const at = attempts.get(row.id);
    return at == null || at < due;
  });

  const base = {
    checked_at: checkedAt,
    ready_count: ready.length,
    overdue_count: overdue.length,
    stuck_pending_count: stuckPending.length,
    last_attempt_at: lastAttemptAt,
    grace_hours: grace / 3600000,
    attempts_schema_sampled: shape.sampled,
  };

  if (!overdue.length && !stuckPending.length) {
    return { ok: true, status: 'healthy', ...base };
  }

  const items = overdue.slice(0, 20).map(({ row, due }) => ({
    id: row.id,
    title: row.title ?? null,
    due_at: new Date(due).toISOString(),
    scheduled_at: row.scheduled_at ?? null,
    approved_at: row.approved_at ?? null,
  }));

  // 'stalled' means approved work is past due and nothing tried to publish it.
  // 'degraded' means only that attempts are stuck part-way -- a different
  // failure, still not something to report as a healthy run.
  const status = overdue.length ? 'stalled' : 'degraded';

  if (dryRun) {
    return { ok: false, status, ...base, items, attempts_recorded: 0, alert: null };
  }

  let recorded = 0;
  const writeErrors = [];
  for (const { row } of overdue) {
    const res = await recordFailedAttempt(env, shape, row, now, ATTEMPT_FAILURE_REASON);
    if (res.ok) recorded += 1;
    else writeErrors.push({ id: row.id, status: res.status, error: res.error });
  }

  // The alert goes out even if recording the attempts failed -- a schema
  // mismatch must not turn back into a silent drop.
  const alert = await raiseStalledAlert(env, overdue, stuckPending, lastAttemptAt, now);

  return {
    ok: false,
    status,
    ...base,
    items,
    attempts_recorded: recorded,
    ...(writeErrors.length ? { attempt_write_errors: writeErrors } : {}),
    alert,
  };
}

export function readPublishHealth(env, options = {}) {
  return runPublishWatchdog(env, { ...options, dryRun: true });
}
