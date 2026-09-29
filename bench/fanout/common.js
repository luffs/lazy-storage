// common.js - What the fan-out bench's processes share: a clock they agree on,
// a latency histogram that sums across processes, CPU time, and each child's
// control server (a port it prints as its first line)
import { availableParallelism, cpus } from 'node:os';

/**
 * Milliseconds since the epoch at sub-millisecond resolution; processes on
 * one machine agree on it, and a server on another has its offset measured
 * (see run.js's --remote)
 */
export const clock = () => performance.timeOrigin + performance.now();

// Latency in microseconds, in buckets 5% apart: a few hundred counts, however many messages
const STEP = Math.log(1.05);
const BUCKETS = 400;
export const newHistogram = () => new Array(BUCKETS).fill(0);
export function record(histogram, ms) {
  const us = Math.max(1, ms * 1000);
  histogram[Math.min(BUCKETS - 1, Math.floor(Math.log(us) / STEP))]++;
}
export function addInto(total, histogram) {
  for (let i = 0; i < BUCKETS; i++) total[i] += histogram[i];
  return total;
}
/** The percentile's upper bound in ms (within 5%), or null for an empty histogram */
export function percentile(histogram, p) {
  const count = histogram.reduce((a, b) => a + b, 0);
  if (!count) return null;
  let seen = 0;
  for (let i = 0; i < BUCKETS; i++) {
    seen += histogram[i];
    if (seen >= p * count) return Math.exp((i + 1) * STEP) / 1000;
  }
  return null;
}

// CPU seconds this process has used. On Windows process.cpuUsage() moves in
// steps of the 15.6 ms clock tick and undercounts short bursts, so there the
// process's cycle count is read instead (QueryProcessCycleTime, every thread
// of the process), at the rate run.js measured once (CYCLE_HZ)
const cycles = (() => {
  if (process.platform !== 'win32' || !process.env.CYCLE_HZ) return null;
  try {
    const { dlopen, FFIType, ptr } = require('bun:ffi');
    const k32 = dlopen('kernel32.dll', {
      QueryProcessCycleTime: { args: [FFIType.ptr, FFIType.ptr], returns: FFIType.i32 },
      GetCurrentProcess: { args: [], returns: FFIType.ptr }
    }).symbols;
    const buf = new BigUint64Array(1);
    return () => {
      k32.QueryProcessCycleTime(k32.GetCurrentProcess(), ptr(buf));
      return Number(buf[0]);
    };
  } catch {
    return null;
  }
})();
export const cpuSeconds = cycles
  ? () => cycles() / Number(process.env.CYCLE_HZ)
  : () => {
    const { user, system } = process.cpuUsage();
    return (user + system) / 1e6;
  };

/** The cycle counter's rate, for CYCLE_HZ: cycles over a spin of 300 ms (null where there is none) */
export function cycleRate() {
  if (process.platform !== 'win32') return null;
  const { dlopen, FFIType, ptr } = require('bun:ffi');
  const k32 = dlopen('kernel32.dll', {
    QueryProcessCycleTime: { args: [FFIType.ptr, FFIType.ptr], returns: FFIType.i32 },
    GetCurrentProcess: { args: [], returns: FFIType.ptr }
  }).symbols;
  const buf = new BigUint64Array(1);
  const read = () => (k32.QueryProcessCycleTime(k32.GetCurrentProcess(), ptr(buf)), Number(buf[0]));
  const c0 = read();
  const t0 = performance.now();
  while (performance.now() - t0 < 300);
  return (read() - c0) / ((performance.now() - t0) / 1000);
}

// A pad that compresses as readings do (numbers, about 4 to 6 times under deflate), in a string, so the
// clients' search for "at", "sent" and "v" never lands in it
export const padOf = size => {
  let pad = '';
  while (pad.length < size) pad += `${(Math.random() * 1000).toFixed(2)},`;
  return pad.slice(0, size);
};

/** `--name value` pairs; a bare `--flag` is true */
export function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i++) {
    if (!argv[i].startsWith('--')) continue;
    const next = argv[i + 1];
    args[argv[i].slice(2)] = next === undefined || next.startsWith('--') ? true : (i++, next);
  }
  return args;
}

/**
 * A control server, on 127.0.0.1 unless told otherwise (the server's, run
 * by agent.js for a run.js on another machine): GET <path> answers with
 * routes[path](url) as JSON. Prints its port first. The process ends when
 * its parent's pipe to it closes, so no child outlives a run that died
 */
export function control(routes, extra = {}, { hostname = '127.0.0.1', port = 0 } = {}) {
  const server = Bun.serve({
    hostname,
    port,
    async fetch(req) {
      const url = new URL(req.url);
      const route = routes[url.pathname];
      if (!route) return new Response('Not found', { status: 404 });
      try {
        return Response.json(await route(url));
      } catch (err) {
        return Response.json({ error: String(err?.stack || err) }, { status: 500 });
      }
    }
  });
  console.log(JSON.stringify({ control: server.port, pid: process.pid, ...extra }));
  (async () => {
    try {
      for await (const chunk of Bun.stdin.stream()) void chunk;
    } catch { /* the pipe broke: the parent is gone either way */ }
    process.exit(0);
  })();
  return server;
}

/**
 * What a child process says of its control server (see control): the
 * first line it prints that starts so, parsed. What it prints after is
 * read and dropped, so it never blocks on a full pipe
 */
export async function controlLine(child, file) {
  const reader = child.stdout.getReader();
  const decoder = new TextDecoder();
  let buffered = '';
  for (;;) {
    const { value, done } = await reader.read();
    if (done) throw new Error(`${file} ended before it said its port`);
    buffered += decoder.decode(value, { stream: true });
    const lines = buffered.split('\n');
    buffered = lines.pop();
    const line = lines.find(l => l.startsWith('{"control"'));
    if (!line) continue;
    (async () => { for (;;) { const { done: over } = await reader.read(); if (over) return; } })();
    return JSON.parse(line);
  }
}

/** The logical CPUs of this machine */
export const logicalCpus = () => (typeof availableParallelism === 'function' ? availableParallelism() : cpus().length);
