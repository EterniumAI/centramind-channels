// Thin PostgREST client for the Supabase project behind centramind-channels.
// Extracted from index.js so the outbox, the digest flush, and the publish
// watchdog can share it without importing each other.

export async function supabaseQuery(env, path, options = {}) {
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

  if (options.prefer === 'return=minimal') {
    // Keep the response body on failure -- PostgREST names the offending
    // column or constraint there, and a write that fails for an unreadable
    // reason is how rows go missing without anyone noticing.
    const error = res.ok ? null : await res.text().catch(() => null);
    return { ok: res.ok, status: res.status, error };
  }

  const data = await res.json().catch(() => null);
  return { ok: res.ok, status: res.status, data };
}
