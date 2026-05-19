// Telegram channel sender.
// Sends messages via the Telegram Bot API using per-agent bot tokens.

export async function sendTelegram(env, channelConfig, text) {
  const agentId = channelConfig.agent_id || 'sovereign';
  const secretName = `TELEGRAM_BOT_TOKEN_${agentId.toUpperCase()}`;
  const botToken = env[secretName];
  if (!botToken) {
    return { ok: false, error: `Missing secret ${secretName}` };
  }

  const chatId = channelConfig.config?.chat_id;
  if (!chatId) {
    return { ok: false, error: 'No chat_id in channel config' };
  }

  const res = await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      chat_id: chatId,
      text,
      parse_mode: 'HTML',
    }),
  });

  const data = await res.json().catch(() => ({}));
  return { ok: res.ok, status: res.status, data };
}
