// centramind-channels Worker
// Severity-routed notification outbox + publish-path watchdog

import { supabaseQuery } from './lib/supabase.js';
import { deliver, dispatchNotification } from './notify.js';
import { runPublishWatchdog, readPublishHealth } from './publish_watchdog.js';

// The watchdog is cheap when healthy, but the cron fires every minute for the
// digest flush and there is no reason to sweep the pipeline that often.
const WATCHDOG_INTERVAL_MINUTES = 5;

function jsonOk(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
  });
}

function jsonError(status, error) {
  return new Response(JSON.stringify({ ok: false, error }), {
    status,
    headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
  });
}

function authorized(request, env) {
  const auth = request.headers.get('Authorization') || '';
  const token = auth.startsWith('Bearer ') ? auth.slice(7).trim() : '';
  return Boolean(token) && token === env.CHANNELS_WORKER_TOKEN;
}

// POST /send
async function handleSend(request, env) {
  if (!authorized(request, env)) return jsonError(401, 'Invalid token');

  const body = await request.json().catch(() => null);
  if (!body) return jsonError(400, 'Invalid JSON');

  const { tenant_id, trigger_key, severity, body: msgBody } = body;
  if (!tenant_id || !trigger_key || !severity || !msgBody) {
    return jsonError(400, 'Missing required fields: tenant_id, trigger_key, severity, body');
  }

  const result = await dispatchNotification(env, body);
  return jsonOk(result);
}

// POST /digest-flush (cron-triggered)
async function handleDigestFlush(env) {
  const routeRes = await supabaseQuery(env,
    `notification_routing?mode=eq.digest&enabled=eq.true&select=*`
  );

  if (!routeRes.ok || !routeRes.data?.length) return { flushed: 0 };

  let totalFlushed = 0;
  const now = new Date();

  for (const rule of routeRes.data) {
    // Check if the digest_cron matches "in the last 60s"
    if (!rule.digest_cron) continue;
    if (!cronMatchesNow(rule.digest_cron, now)) continue;

    const channelId = rule.channel_id;
    const triggerKey = rule.trigger_key;
    const tenantId = rule.tenant_id;
    const agentId = rule.agent_id;

    const queueRes = await supabaseQuery(env,
      `notification_log?status=eq.queued&trigger_key=eq.${encodeURIComponent(triggerKey)}&channel_id=eq.${channelId}&tenant_id=eq.${tenantId}&agent_id=eq.${agentId}&select=id,title,body,severity,created_at&order=created_at.asc&limit=100`
    );

    if (!queueRes.ok || !queueRes.data?.length) continue;

    const items = queueRes.data;
    const batchId = crypto.randomUUID();

    const lines = items.map(item => {
      const prefix = item.severity ? `[${item.severity}] ` : '';
      const titlePart = item.title ? `<b>${item.title}</b>: ` : '';
      return `${prefix}${titlePart}${item.body || ''}`;
    });
    const digestText = `Digest (${items.length} items):\n\n${lines.map(l => `- ${l}`).join('\n')}`;

    const chanRes = await supabaseQuery(env,
      `agent_channels?id=eq.${channelId}&select=*&limit=1`
    );
    const failure = await deliver(env, chanRes.data?.[0], channelId, digestText);
    if (failure) {
      // Leave the items queued so a later flush can still deliver them, and do
      // not let one broken channel abort the flush for every other rule.
      console.error('digest flush delivery failed', channelId, failure);
      continue;
    }

    const ids = items.map(i => i.id);
    for (const id of ids) {
      await supabaseQuery(env, `notification_log?id=eq.${id}`, {
        method: 'PATCH',
        body: { status: 'digested', digest_batch_id: batchId, delivered_at: new Date().toISOString() },
        prefer: 'return=minimal',
      });
    }

    totalFlushed += items.length;
  }

  return { flushed: totalFlushed };
}

function cronMatchesNow(cronExpr, now) {
  // Parse 5-field cron: minute hour day month weekday
  const parts = cronExpr.trim().split(/\s+/);
  if (parts.length < 5) return false;

  const [minField, hourField, dayField, monthField, dowField] = parts;
  const minute = now.getUTCMinutes();
  const hour = now.getUTCHours();
  const day = now.getUTCDate();
  const month = now.getUTCMonth() + 1;
  const dow = now.getUTCDay();

  return fieldMatches(minField, minute)
    && fieldMatches(hourField, hour)
    && fieldMatches(dayField, day)
    && fieldMatches(monthField, month)
    && fieldMatches(dowField, dow);
}

function fieldMatches(field, value) {
  if (field === '*') return true;
  const parts = field.split(',');
  for (const p of parts) {
    // Handle step values like */5
    if (p.includes('/')) {
      const [range, step] = p.split('/');
      const stepNum = parseInt(step, 10);
      if (range === '*' && value % stepNum === 0) return true;
      continue;
    }
    // Handle ranges like 1-5
    if (p.includes('-')) {
      const [lo, hi] = p.split('-').map(Number);
      if (value >= lo && value <= hi) return true;
      continue;
    }
    if (parseInt(p, 10) === value) return true;
  }
  return false;
}

// Main Worker
export default {
  async fetch(request, env) {
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        status: 204,
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Telegram-Bot-Api-Secret-Token',
          'Access-Control-Max-Age': '86400',
        },
      });
    }

    const url = new URL(request.url);
    const path = url.pathname;

    // Health (liveness only -- says nothing about the publish path)
    if (path === '/health' || path === '/') {
      return jsonOk({ ok: true, service: 'centramind-channels', ts: new Date().toISOString() });
    }

    // Telegram webhook: retired 2026-07-07
    const tgMatch = path.match(/^\/webhooks\/telegram\/([a-zA-Z0-9_-]+)$/);
    if (tgMatch && request.method === 'POST') {
      return jsonError(410, 'Telegram webhook retired 2026-07-07');
    }

    // Internal send: POST /send
    if (path === '/send' && request.method === 'POST') {
      return handleSend(request, env);
    }

    // Digest flush: POST /digest-flush
    if (path === '/digest-flush' && request.method === 'POST') {
      if (!authorized(request, env)) return jsonError(401, 'Invalid token');
      const result = await handleDigestFlush(env);
      return jsonOk({ ok: true, ...result });
    }

    // Publish health: GET /publish-health
    // Read-only verdict for the daily publish check. Returns 503 when approved
    // work is past due, so a caller that only looks at the status code still
    // fails instead of reporting a green run against a dead publisher.
    if (path === '/publish-health' && request.method === 'GET') {
      if (!authorized(request, env)) return jsonError(401, 'Invalid token');
      const result = await readPublishHealth(env);
      return jsonOk(result, result.ok ? 200 : 503);
    }

    // Publish watchdog: POST /publish-watchdog
    // Same verdict, but records the failed attempts and raises the alert.
    if (path === '/publish-watchdog' && request.method === 'POST') {
      if (!authorized(request, env)) return jsonError(401, 'Invalid token');
      const result = await runPublishWatchdog(env);
      return jsonOk(result, result.ok ? 200 : 503);
    }

    return jsonError(404, `Unknown route: ${request.method} ${path}`);
  },

  async scheduled(event, env, ctx) {
    // Cron trigger: flush digests every minute, sweep the publish path every
    // WATCHDOG_INTERVAL_MINUTES.
    ctx.waitUntil(
      handleDigestFlush(env).catch(err => console.error('digest flush failed', err))
    );

    const now = new Date(event.scheduledTime ?? Date.now());
    if (now.getUTCMinutes() % WATCHDOG_INTERVAL_MINUTES === 0) {
      ctx.waitUntil(
        runPublishWatchdog(env, { now })
          .then(result => {
            if (!result.ok) console.error('publish watchdog unhealthy', JSON.stringify(result));
          })
          .catch(err => console.error('publish watchdog failed', err))
      );
    }
  },
};
