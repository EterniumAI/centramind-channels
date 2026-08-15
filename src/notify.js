// Severity routing + delivery for the notification outbox.
// Shared by the HTTP /send handler, the digest flush, and the publish watchdog.

import { sendInbox } from './senders/inbox.js';
import { supabaseQuery } from './lib/supabase.js';

const SEVERITY_ORDER = { P0: 0, P1: 1, P2: 2, P3: 3 };

// Channel types that used to deliver and no longer do. Looking one up is not
// an error: it resolves to "cannot deliver", and the caller records a failed
// delivery. Throwing here used to abort the whole request, so one stale
// telegram route killed delivery for every other route on the same message
// and left no notification_log row behind to say so.
const RETIRED_CHANNEL_TYPES = {
  telegram: 'telegram channel retired 2026-07-07',
};

// notification_log statuses older than this worker's 'failed' status. If the
// column rejects 'failed', re-file under one of these rather than lose the row.
const FALLBACK_LOG_STATUS = 'dropped';

export function severityAtOrAbove(severity, floor) {
  const s = SEVERITY_ORDER[severity] ?? 3;
  const f = SEVERITY_ORDER[floor] ?? 3;
  return s <= f;
}

// Returns { sender, reason }. A null sender always comes with a reason, so no
// caller has to guess why nothing was delivered.
export function resolveSender(channelType) {
  const retired = RETIRED_CHANNEL_TYPES[channelType];
  if (retired) return { sender: null, reason: retired };
  if (channelType === 'inbox') return { sender: sendInbox, reason: null };
  return { sender: null, reason: `no sender for channel_type "${channelType}"` };
}

export async function insertNotificationLog(env, row) {
  const res = await supabaseQuery(env, 'notification_log', {
    method: 'POST',
    body: row,
    prefer: 'return=minimal',
  });
  if (res.ok) return res;

  if (row.status !== FALLBACK_LOG_STATUS) {
    const retry = {
      ...row,
      status: FALLBACK_LOG_STATUS,
      body: `[${row.status}] ${row.body || ''}`,
    };
    const second = await supabaseQuery(env, 'notification_log', {
      method: 'POST',
      body: retry,
      prefer: 'return=minimal',
    });
    if (second.ok) return second;
  }

  console.error('notification_log insert failed', res.status, res.error);
  return res;
}

// Attempts delivery of one message through every enabled route that matches.
// Never throws for a per-channel problem: a channel that cannot deliver is
// logged as 'failed' and the remaining channels still get their shot.
export async function dispatchNotification(env, payload) {
  const {
    tenant_id,
    agent_id,
    trigger_key,
    severity,
    title,
    body: msgBody,
    dedupe_key,
  } = payload;

  const agentIdResolved = agent_id || 'sovereign';

  const routeRes = await supabaseQuery(env,
    `notification_routing?tenant_id=eq.${tenant_id}&agent_id=eq.${agentIdResolved}&trigger_key=eq.${encodeURIComponent(trigger_key)}&enabled=eq.true&select=*`
  );

  if (!routeRes.ok || !routeRes.data?.length) {
    // No routing rules: record the miss so an unrouted alert is still visible.
    await insertNotificationLog(env, {
      tenant_id, agent_id: agentIdResolved, trigger_key, severity,
      channel_id: null, title, body: msgBody, dedupe_key,
      status: 'no_route', delivered_at: null, digest_batch_id: null,
    });
    return { ok: true, status: 'no_route', routes_evaluated: 0, results: [] };
  }

  const results = [];

  for (const rule of routeRes.data) {
    const channelId = rule.channel_id;

    if (!severityAtOrAbove(severity, rule.severity_floor || 'P3')) {
      await insertNotificationLog(env, {
        tenant_id, agent_id: agentIdResolved, trigger_key, severity,
        channel_id: channelId, title, body: msgBody, dedupe_key,
        status: 'dropped', delivered_at: null, digest_batch_id: null,
      });
      results.push({ channel_id: channelId, status: 'dropped' });
      continue;
    }

    if (rule.dedupe_window_sec > 0 && dedupe_key) {
      const cutoff = new Date(Date.now() - rule.dedupe_window_sec * 1000).toISOString();
      const dedupeRes = await supabaseQuery(env,
        `notification_log?dedupe_key=eq.${encodeURIComponent(dedupe_key)}&tenant_id=eq.${tenant_id}&agent_id=eq.${agentIdResolved}&trigger_key=eq.${encodeURIComponent(trigger_key)}&channel_id=eq.${channelId}&created_at=gt.${cutoff}&limit=1&select=id`
      );
      if (dedupeRes.ok && dedupeRes.data?.length > 0) {
        await insertNotificationLog(env, {
          tenant_id, agent_id: agentIdResolved, trigger_key, severity,
          channel_id: channelId, title, body: msgBody, dedupe_key,
          status: 'deduped', delivered_at: null, digest_batch_id: null,
        });
        results.push({ channel_id: channelId, status: 'deduped' });
        continue;
      }
    }

    const mode = rule.mode || 'instant';

    if (mode === 'instant') {
      const chanRes = await supabaseQuery(env,
        `agent_channels?id=eq.${channelId}&select=*&limit=1`
      );
      const channel = chanRes.data?.[0];
      const messageText = title ? `<b>${title}</b>\n\n${msgBody}` : msgBody;

      const failure = await deliver(env, channel, channelId, messageText);
      const status = failure ? 'failed' : 'sent';

      await insertNotificationLog(env, {
        tenant_id, agent_id: agentIdResolved, trigger_key, severity,
        channel_id: channelId, title,
        body: failure ? `${msgBody}\n\n[delivery failed: ${failure}]` : msgBody,
        dedupe_key,
        status,
        delivered_at: failure ? null : new Date().toISOString(),
        digest_batch_id: null,
      });
      results.push({ channel_id: channelId, status, ...(failure ? { error: failure } : {}) });
    } else if (mode === 'digest') {
      await insertNotificationLog(env, {
        tenant_id, agent_id: agentIdResolved, trigger_key, severity,
        channel_id: channelId, title, body: msgBody, dedupe_key,
        status: 'queued', delivered_at: null, digest_batch_id: null,
      });
      results.push({ channel_id: channelId, status: 'queued' });
    } else if (mode === 'silent') {
      await insertNotificationLog(env, {
        tenant_id, agent_id: agentIdResolved, trigger_key, severity,
        channel_id: channelId, title, body: msgBody, dedupe_key,
        status: 'silent', delivered_at: null, digest_batch_id: null,
      });
      results.push({ channel_id: channelId, status: 'silent' });
    }
  }

  return { ok: true, results };
}

// Runs one sender. Returns null on success, or a human-readable failure reason.
// A message is only ever recorded as 'sent' when a sender actually ran and did
// not report an error -- a missing channel or a retired channel type used to
// be logged as 'sent', which is how the outbox reported success while
// delivering nothing.
export async function deliver(env, channel, channelId, messageText) {
  if (!channel) return `channel ${channelId} not found`;

  const { sender, reason } = resolveSender(channel.channel_type);
  if (!sender) return reason;

  try {
    const sendRes = await sender(env, channel, messageText);
    if (sendRes && sendRes.ok === false) {
      return `sender returned ${sendRes.status ?? 'error'}`;
    }
    return null;
  } catch (err) {
    return `sender threw: ${err.message}`;
  }
}
