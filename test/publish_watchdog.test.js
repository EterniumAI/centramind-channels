// Pure-logic coverage for the publish watchdog and the delivery path.
// No network: only the exported functions that decide overdue-ness and
// whether a channel can deliver.
//
// Run with: npm test

import test from 'node:test';
import assert from 'node:assert/strict';

import { dueAt, overdueItems } from '../src/publish_watchdog.js';
import { deliver, resolveSender, severityAtOrAbove } from '../src/notify.js';

const HOUR = 3600000;
const GRACE = 24 * HOUR;
const NOW = Date.parse('2026-08-14T00:00:00Z');

test('scheduled_at wins over approved_at when deciding due time', () => {
  const row = { scheduled_at: '2026-07-11T00:22:00Z', approved_at: '2026-07-08T00:00:00Z' };
  assert.equal(dueAt(row, GRACE), Date.parse('2026-07-11T00:22:00Z'));
});

test('an approved item with no schedule is due after the grace window', () => {
  const row = { approved_at: '2026-07-08T00:00:00Z', scheduled_at: null };
  assert.equal(dueAt(row, GRACE), Date.parse('2026-07-08T00:00:00Z') + GRACE);
});

test('an unapproved item is never due', () => {
  assert.equal(dueAt({ approved_at: null, scheduled_at: null }, GRACE), null);
});

test('overdueItems returns only past-due items, oldest first', () => {
  const rows = [
    { id: 'b', approved_at: '2026-07-08T00:00:00Z' },
    { id: 'a', scheduled_at: '2026-07-11T00:22:00Z' },
    { id: 'future', scheduled_at: '2026-09-01T00:00:00Z' },
    { id: 'unapproved' },
  ];
  const overdue = overdueItems(rows, NOW, GRACE);
  assert.deepEqual(overdue.map(o => o.row.id), ['b', 'a']);
});

test('the stranded Salesforce item is overdue', () => {
  const row = { id: 'sf', title: 'The Salesforce Tax', scheduled_at: '2026-07-11T00:22:00Z' };
  assert.equal(overdueItems([row], NOW, GRACE).length, 1);
});

test('a retired channel type resolves to no sender instead of throwing', () => {
  const { sender, reason } = resolveSender('telegram');
  assert.equal(sender, null);
  assert.match(reason, /retired/);
});

test('deliver reports the retired channel as a failure and does not throw', async () => {
  const failure = await deliver({}, { channel_type: 'telegram' }, 'chan-1', 'hello');
  assert.match(failure, /retired/);
});

test('deliver reports a missing channel rather than claiming success', async () => {
  const failure = await deliver({}, undefined, 'chan-1', 'hello');
  assert.match(failure, /not found/);
});

test('deliver returns null when a real sender succeeds', async () => {
  const failure = await deliver({}, { channel_type: 'inbox' }, 'chan-1', 'hello');
  assert.equal(failure, null);
});

test('severity floor still gates', () => {
  assert.equal(severityAtOrAbove('P1', 'P2'), true);
  assert.equal(severityAtOrAbove('P3', 'P1'), false);
});
