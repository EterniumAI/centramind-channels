// centramind-channels Worker
// Telegram webhook ingress + severity-routed notification outbox

import { sendTelegram } from './senders/telegram.js';
import { sendInbox } from './senders/inbox.js';

const SEVERITY_ORDER = { P0: 0, P1: 1, P2: 2, P3: 3 };

function severityAtOrAbove(severity, floor) {
  const s = SEVERITY_ORDER[severity] ?? 3;
  const f = SEVERITY_ORDER[floor] ?? 3;
  return s <= f;
}

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

async function supabaseQuery(env, path, options = {}) {
  const res = await fetch(`${env.SUPABASE_URL}/rest/v1/${path}`, {
    method: options.method || 'GET',
    headers: {
      apikey: env.SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
      'Content-Type': 'application/json',
      ...(options.prefer ? { Prefer: options.prefer } : {}),
    },
    body: options.body ? JSON.stringify(options.body) : undefined,
  });
  if (options.prefer === 'return=minimal') return { ok: res.ok, status: res.status };
  const data = await res.json().catch(() => null);
  return { ok: res.ok, status: res.status, data };
}

function getSender(channelType) {
  if (channelType === 'telegram') return sendTelegram;
  if (channelType === 'inbox') return sendInbox;
  return null;
}

// POST /webhooks/telegram/:agent_id
async function handleTelegramWebhook(request, env, agentId) {
  // Verify webhook secret
  const secretHeader = request.headers.get('X-Telegram-Bot-Api-Secret-Token') || '';
  const expectedSecret = env[`TELEGRAM_WEBHOOK_SECRET_${agentId.toUpperCase()}`] || env.TELEGRAM_WEBHOOK_SECRET || '';
  if (!expectedSecret || secretHeader !== expectedSecret) {
    return jsonError(403, 'Invalid webhook secret');
  }

  const update = await request.json().catch(() => null);
  if (!update?.message?.text) return jsonOk({ ok: true, skipped: true });

  const text = update.message.text;
  const chatId = String(update.message.chat.id);

  // Verify sender is authorized
  const chanRes = await supabaseQuery(env,
    `agent_channels?agent_id=eq.${agentId}&channel_type=eq.telegram&select=*`
  );
  if (!chanRes.ok || !chanRes.data?.length) {
    return jsonError(403, 'No authorized channel found');
  }

  // Find channel matching this chat_id
  const channel = chanRes.data.find(c => String(c.config?.chat_id) === chatId);
  if (!channel) {
    return jsonError(403, 'Chat not authorized for this agent');
  }

  // Forward to centramind-agent /chat
  const agentRes = await fetch('https://centramind-agent.ty-823.workers.dev/chat', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${env.CHANNELS_WORKER_TOKEN}`,
    },
    body: JSON.stringify({
      tenant_id: channel.tenant_id,
      agent_id: agentId,
      conversation_id: `tg-${chatId}`,
      message: text,
    }),
  });

  let reply = 'Sorry, I could not process your message.';
  if (agentRes.ok) {
    const agentData = await agentRes.json().catch(() => ({}));
    reply = agentData.reply || reply;
  }

  // Send reply back via Telegram
  const botToken = env[`TELEGRAM_BOT_TOKEN_${agentId.toUpperCase()}`];
  if (botToken) {
    await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: chatId,
        text: reply,
        parse_mode: 'HTML',
      }),
    });
  }

  return jsonOk({ ok: true });
}

// POST /send
async function handleSend(request, env) {
  // Auth
  const auth = request.headers.get('Authorization') || '';
  const token = auth.startsWith('Bearer ') ? auth.slice(7).trim() : '';
  if (!token || token !== env.CHANNELS_WORKER_TOKEN) {
    return jsonError(401, 'Invalid token');
  }

  const body = await request.json().catch(() => null);
  if (!body) return jsonError(400, 'Invalid JSON');

  const { tenant_id, agent_id, trigger_key, severity, title, body: msgBody, dedupe_key } = body;
  if (!tenant_id || !trigger_key || !severity || !msgBody) {
    return jsonError(400, 'Missing required fields: tenant_id, trigger_key, severity, body');
  }

  const agentIdResolved = agent_id || 'sovereign';

  // Load routing rules
  const routeRes = await supabaseQuery(env,
    `notification_routing?tenant_id=eq.${tenant_id}&agent_id=eq.${agentIdResolved}&trigger_key=eq.${encodeURIComponent(trigger_key)}&enabled=eq.true&select=*`
  );

  if (!routeRes.ok || !routeRes.data?.length) {
    // No routing rules, log and return
    await insertNotificationLog(env, {
      tenant_id, agent_id: agentIdResolved, trigger_key, severity,
      channel_id: null, title, body: msgBody, dedupe_key,
      status: 'no_route', delivered_at: null, digest_batch_id: null,
    });
    return jsonOk({ ok: true, status: 'no_route', routes_evaluated: 0 });
  }

  const results = [];

  for (const rule of routeRes.data) {
    const channelId = rule.channel_id;

    // Severity floor check
    if (!severityAtOrAbove(severity, rule.severity_floor || 'P3')) {
      await insertNotificationLog(env, {
        tenant_id, agent_id: agentIdResolved, trigger_key, severity,
        channel_id: channelId, title, body: msgBody, dedupe_key,
        status: 'dropped', delivered_at: null, digest_batch_id: null,
      });
      results.push({ channel_id: channelId, status: 'dropped' });
      continue;
    }

    // Dedupe check
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
      // Load channel config
      const chanRes = await supabaseQuery(env,
        `agent_channels?id=eq.${channelId}&select=*&limit=1`
      );
      const channel = chanRes.data?.[0];
      const messageText = title ? `<b>${title}</b>\n\n${msgBody}` : msgBody;

      if (channel) {
        const sender = getSender(channel.channel_type);
        if (sender) {
          await sender(env, channel, messageText);
        }
      }

      await insertNotificationLog(env, {
        tenant_id, agent_id: agentIdResolved, trigger_key, severity,
        channel_id: channelId, title, body: msgBody, dedupe_key,
        status: 'sent', delivered_at: new Date().toISOString(), digest_batch_id: null,
      });
      results.push({ channel_id: channelId, status: 'sent' });
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

  return jsonOk({ ok: true, results });
}

async function insertNotificationLog(env, row) {
  await supabaseQuery(env, 'notification_log', {
    method: 'POST',
    body: row,
    prefer: 'return=minimal',
  });
}

// POST /digest-flush (cron-triggered)
async function handleDigestFlush(env) {
  // Find digest routing rules
  const routeRes = await supabaseQuery(env,
    `notification_routing?mode=eq.digest&enabled=eq.true&select=*`
  );

  if (!routeRes.ok || !routeRes.data?.length) return { flushed: 0 };

  let totalFlushed = 0;
  const now = new Date();

  for (const rule of routeRes.data) {
    // Check if the digest_cron matches "in the last 60s"
    // Simple approach: parse cron and check if minute/hour match
    if (!rule.digest_cron) continue;
    if (!cronMatchesNow(rule.digest_cron, now)) continue;

    const channelId = rule.channel_id;
    const triggerKey = rule.trigger_key;
    const tenantId = rule.tenant_id;
    const agentId = rule.agent_id;

    // Fetch queued items
    const queueRes = await supabaseQuery(env,
      `notification_log?status=eq.queued&trigger_key=eq.${encodeURIComponent(triggerKey)}&channel_id=eq.${channelId}&tenant_id=eq.${tenantId}&agent_id=eq.${agentId}&select=id,title,body,severity,created_at&order=created_at.asc&limit=100`
    );

    if (!queueRes.ok || !queueRes.data?.length) continue;

    const items = queueRes.data;
    const batchId = crypto.randomUUID();

    // Build digest message
    const lines = items.map(item => {
      const prefix = item.severity ? `[${item.severity}] ` : '';
      const titlePart = item.title ? `<b>${item.title}</b>: ` : '';
      return `${prefix}${titlePart}${item.body || ''}`;
    });
    const digestText = `Digest (${items.length} items):\n\n${lines.map(l => `- ${l}`).join('\n')}`;

    // Load channel and send
    const chanRes = await supabaseQuery(env,
      `agent_channels?id=eq.${channelId}&select=*&limit=1`
    );
    const channel = chanRes.data?.[0];
    if (channel) {
      const sender = getSender(channel.channel_type);
      if (sender) {
        await sender(env, channel, digestText);
      }
    }

    // Mark items as digested
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
  // Handle comma-separated values
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

    // Health
    if (path === '/health' || path === '/') {
      return jsonOk({ ok: true, service: 'centramind-channels', ts: new Date().toISOString() });
    }

    // Telegram webhook: POST /webhooks/telegram/:agent_id
    const tgMatch = path.match(/^\/webhooks\/telegram\/([a-zA-Z0-9_-]+)$/);
    if (tgMatch && request.method === 'POST') {
      return handleTelegramWebhook(request, env, tgMatch[1]);
    }

    // Internal send: POST /send
    if (path === '/send' && request.method === 'POST') {
      return handleSend(request, env);
    }

    // Digest flush: POST /digest-flush
    if (path === '/digest-flush' && request.method === 'POST') {
      const auth = request.headers.get('Authorization') || '';
      const token = auth.startsWith('Bearer ') ? auth.slice(7).trim() : '';
      if (!token || token !== env.CHANNELS_WORKER_TOKEN) {
        return jsonError(401, 'Invalid token');
      }
      const result = await handleDigestFlush(env);
      return jsonOk({ ok: true, ...result });
    }

    return jsonError(404, `Unknown route: ${request.method} ${path}`);
  },

  async scheduled(event, env, ctx) {
    // Cron trigger: flush digests every minute
    ctx.waitUntil(handleDigestFlush(env));
  },
};
