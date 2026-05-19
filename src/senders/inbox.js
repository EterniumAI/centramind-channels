// Inbox channel sender.
// No-op outbound -- inbox is a read-only feed via notification_log query.
// The insert into notification_log is already done by the /send handler.

export async function sendInbox(_env, _channelConfig, _text) {
  return { ok: true, status: 200, data: { inbox: true } };
}
