#!/usr/bin/env node
// relay.mjs - runs on ANY host that has `gh` authed as the relay account.
// Client side of the outbound-only relay to the Allianz MacBook.
//
//   node relay.mjs run 'uname -a'            # one command
//   node relay.mjs run 'ls -la' --cwd ~/x    # in a directory
//   node relay.mjs sh                        # interactive shell loop
//   node relay.mjs watch                     # print poller traffic (tail the issue)
//
// Transport: comments on one private issue of a private repo. Nothing is
// encrypted in transit beyond TLS - the repo is private.

import { execFile } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import readline from "node:readline";
import { slackConfig, slackApi, slackChannelId, slackPost, slackHistory, socketMode, parseAnswer, PREFIXES } from "./slack.mjs";

const REPO = process.env.RELAY_REPO || "bikboy/mbp-relay";
const ISSUE = process.env.RELAY_ISSUE || "1";
const HOSTNAME = process.env.RELAY_HOST || "github.com";
const POLL = Number(process.env.RELAY_POLL || 0.7) * 1000;
const NTFY = process.env.NTFY_SERVER || "https://ntfy.sh";

// ---------------------------------------------------------------------------
// ntfy transport (fast path)
//
// ntfy.sh is reachable from BOTH machines, and a publish + cached-subscribe
// round trip costs ~300 ms instead of the ~4 s a GitHub mailbox poll costs.
// Topic names are effectively public on ntfy.sh, so every payload is sealed
// with AES-256-GCM; the key lives in ntfy.env next to this script and never
// leaves the two machines (it was shipped over the private GitHub relay).
//
// Message bodies are capped at 4 KB on ntfy.sh, so large payloads in both
// directions are chunked; commands too big for a reasonable number of
// chunks fall back to the GitHub transport.
// ---------------------------------------------------------------------------

function ntfyConfig() {
	if (process.env.RELAY_NO_NTFY) return null;
	try {
		const text = fs.readFileSync(new URL("./ntfy.env", import.meta.url), "utf8");
		const out = {};
		for (const line of text.split("\n")) {
			const m = line.match(/^([A-Z_]+)=(.*)$/);
			if (m) out[m[1]] = m[2].trim();
		}
		return out.NTFY_TOPIC && out.NTFY_KEY ? out : null;
	} catch {
		return null;
	}
}

function seal(key, obj) {
	const iv = crypto.randomBytes(12);
	const c = crypto.createCipheriv("aes-256-gcm", Buffer.from(key, "hex"), iv);
	const ct = Buffer.concat([c.update(JSON.stringify(obj), "utf8"), c.final()]);
	return Buffer.concat([iv, c.getAuthTag(), ct]).toString("base64");
}

function unseal(key, b64) {
	const buf = Buffer.from(b64, "base64");
	const d = crypto.createDecipheriv("aes-256-gcm", Buffer.from(key, "hex"), buf.subarray(0, 12));
	d.setAuthTag(buf.subarray(12, 28));
	return JSON.parse(Buffer.concat([d.update(buf.subarray(28)), d.final()]).toString("utf8"));
}

const CHUNK = 3000; // sealed chunk budget, safely under ntfy's 4096-byte body cap

async function ntfyPublish(cfg, kind, payload, id = "") {
	const sealed = seal(cfg.NTFY_KEY, payload);
	if (sealed.length <= CHUNK) {
		await ntfyPost(cfg, { title: kind }, sealed);
		return 1;
	}
	// Too big for one message: split, marked so the receiver reassembles.
	const parts = [];
	for (let i = 0; i < sealed.length; i += CHUNK) parts.push(sealed.slice(i, i + CHUNK));
	if (parts.length > 16) throw new Error("payload too large for ntfy (use GitHub transport)");
	for (let i = 0; i < parts.length; i++) {
		await ntfyPost(cfg, { title: `${kind}/${i}/${parts.length}/${id}` }, parts[i]);
	}
	return parts.length;
}

/**
 * Subscribe to the topic and wait for one complete answer with this id.
 *
 * `since` reaches a few seconds back, so an OUT that was published while we
 * were still connecting is replayed from the ntfy cache instead of lost.
 */
/** POST one ntfy message, retrying rate limits (429) as requested. */
async function ntfyPost(cfg, headers, body) {
	for (let attempt = 1; ; attempt++) {
		const res = await fetch(`${NTFY}/${cfg.NTFY_TOPIC}`, { method: "POST", headers, body });
		if (res.ok) return;
		// ntfy.sh rate limits anonymous visitors; Retry-After says when the
		// budget refills.
		if (res.status === 429 && attempt < 8) {
			const wait = Number(res.headers.get("retry-after") ?? 5) * 1000 + 250;
			res.body?.cancel().catch(() => {});
			await new Promise((r) => setTimeout(r, wait));
			continue;
		}
		throw new Error(`ntfy publish ${res.status}`);
	}
}

async function ntfyWaitAnswer(cfg, id, waitMs, lookbackSec = 30) {
	// Look back far enough that an answer published while this stream was
	// still opening is replayed instead of lost. The MacBook answers in
	// ~40 ms, so a 3 s window lost answers whenever the stream lagged.
	const since = Math.floor(Date.now() / 1000) - lookbackSec;
	const ctrl = new AbortController();
	const timer = setTimeout(() => ctrl.abort(), waitMs);
	try {
		const res = await fetch(`${NTFY}/${cfg.NTFY_TOPIC}/json?since=${since}`, { signal: ctrl.signal });
		if (!res.ok || !res.body) throw new Error(`ntfy subscribe ${res.status}`);
		// Chunked answers: id -> { seen parts, total }
		const partBuffers = new Map();
		let carry = "";
		const dec = new TextDecoder();
		// The reader must be created exactly once; getReader() on a locked
		// stream throws.
		const reader = res.body.getReader();
		for (;;) {
			const { done, value } = await reader.read();
			if (done) throw new Error("ntfy stream closed");
			carry += dec.decode(value, { stream: true });
			let nl;
			while ((nl = carry.indexOf("\n")) >= 0) {
				const line = carry.slice(0, nl).trim();
				carry = carry.slice(nl + 1);
				if (!line) continue; // keepalive
				let ev;
				try {
					ev = JSON.parse(line);
				} catch {
					continue;
				}
				if (ev.event === "keepalive" || ev.event === "open") continue;
				const body = String(ev.message ?? "");
				if (!body) continue;
				const title = String(ev.title ?? "");
				try {
					if (title.startsWith("out/")) {
						// out/<i>/<total>/<id>: one part of a chunked answer
						const m = title.match(/^out\/(\d+)\/(\d+)\/([0-9a-f]+)$/);
						if (!m || m[3] !== id) continue;
						const rec = partBuffers.get(id) ?? { seen: new Map(), total: Number(m[2]) };
						rec.seen.set(Number(m[1]), body);
						partBuffers.set(id, rec);
						if (rec.seen.size === rec.total) {
							let sealed = "";
							for (let i = 0; i < rec.total; i++) sealed += rec.seen.get(i);
							const o = unseal(cfg.NTFY_KEY, sealed);
							if (o.id === id) return o;
						}
						continue;
					}
					if (title !== "out") continue;
					const o = unseal(cfg.NTFY_KEY, body);
					if (o.id === id) return o;
				} catch {
					/* not ours or corrupt - ignore */
				}
			}
		}
	} finally {
		clearTimeout(timer);
		ctrl.abort();
	}
}

/**
 * Ask the MacBook to run a command and wait for its answer.
 *
 * The transport is asymmetric because of the network the MacBook sits behind:
 * Zscaler blocks incoming ntfy streams there (the websocket opens but never
 * carries a frame, and the HTTP stream hangs), while plain ntfy POSTs work.
 * So the *command* travels over the GitHub mailbox - which the MacBook polls
 * anyway - and the *answer* comes back over ntfy, which arrives in ~300 ms
 * instead of the ~4 s a GitHub round trip needs. The mailbox is still watched
 * as a fallback, so a blocked ntfy only costs latency, never correctness.
 */
async function runViaMailbox(cmd, opts = {}) {
  const cfg = ntfyConfig();
  const id = crypto.randomBytes(6).toString("hex");
  const t0 = Date.now();
  const seen = new Set((await comments()).map((c) => c.id));
  const posted = JSON.parse(
    await gh(
      ["api", "--hostname", HOSTNAME, "-X", "POST", `/repos/${REPO}/issues/${ISSUE}/comments`, "--input", "-"],
      JSON.stringify({ body: "CMD " + JSON.stringify({ id, cmd, cwd: opts.cwd, timeout: opts.timeout }) }),
    ),
  );
  process.stderr.write(`[${id}] queued\n`);

  // Listen for the answer on ntfy (fast) while watching the mailbox (safe).
  let ntfyAnswer = null;
  if (cfg) {
    ntfyWaitAnswer(cfg, id, opts.wait || 180000, 60)
      .then((a) => {
        ntfyAnswer = a;
      })
      .catch(() => {});
  }

  const deadline = Date.now() + (opts.wait || 180000);
  while (Date.now() < deadline) {
    if (ntfyAnswer) {
      const o = ntfyAnswer;
      o.ms = (o.ms ?? 0) + (Date.now() - t0 - (o.ms ?? 0));
      await removeComment(posted.id);
      return o;
    }
    for (const c of await comments()) {
      if (seen.has(c.id)) continue;
      seen.add(c.id);
      const body = String(c.body || "");
      if (!body.startsWith("OUT ")) continue;
      let o;
      try {
        o = JSON.parse(body.slice(4));
      } catch {
        continue;
      }
      if (o.id !== id) continue;
      // The mailbox stays small: drop the request and its answer.
      await removeComment(posted.id);
      await removeComment(c.id);
      return o;
    }
    await sleep(POLL);
  }
  await removeComment(posted.id);
  throw new Error("timeout waiting for the MacBook");
}

function gh(args, input) {
  return new Promise((resolve, reject) => {
    // Zscaler can black-hole a request and never answer; without a timeout
    // the whole relay hangs forever instead of falling back.
    const child = execFile("gh", args, { maxBuffer: 128 * 1024 * 1024, timeout: 60000 }, (err, stdout, stderr) => {
      if (err) return reject(new Error(`gh ${args[0]}: ${stderr || err.message}`));
      resolve(stdout);
    });
    if (input !== undefined) {
      child.stdin.write(input);
      child.stdin.end();
    }
  });
}

async function post(body) {
  await gh(["api", "--hostname", HOSTNAME, "-X", "POST", `/repos/${REPO}/issues/${ISSUE}/comments`, "--input", "-"], JSON.stringify({ body }));
}

async function comments() {
  // The issue can hold more than 100 comments; the API only returns the oldest
  // page by default, so a long-running mailbox would go blind. Fetch the last
  // pages explicitly.
  const first = JSON.parse(
    await gh(["api", "--hostname", HOSTNAME, `/repos/${REPO}/issues/${ISSUE}/comments?per_page=100`]),
  );
  if (first.length < 100) return first;
  const issue = JSON.parse(await gh(["api", "--hostname", HOSTNAME, `/repos/${REPO}/issues/${ISSUE}`]));
  const lastPage = Math.max(1, Math.ceil(Number(issue.comments || 0) / 100));
  const out = [];
  for (let p = lastPage; p >= Math.max(1, lastPage - 1); p--) {
    const arr = JSON.parse(
      await gh(["api", "--hostname", HOSTNAME, `/repos/${REPO}/issues/${ISSUE}/comments?per_page=100&page=${p}`]),
    );
    out.unshift(...arr);
  }
  return out;
}

/** Keep the mailbox small: both sides delete their comments once consumed. */
async function removeComment(id) {
  try {
    await gh(["api", "--hostname", HOSTNAME, "-X", "DELETE", `/repos/${REPO}/issues/comments/${id}`]);
  } catch {
    /* already gone */
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function run(cmd, opts = {}) {
  if (transport() === "slack") {
    try {
      return await runViaSlack(cmd, opts);
    } catch (e) {
      // A Slack hiccup must not break the relay: fall back to the GitHub
      // mailbox, which is always available.
      process.stderr.write(`[slack] failed (${e.message}); falling back to github\n`);
    }
  }
  // Command over the GitHub mailbox, answer over ntfy (or the mailbox).
  return await runViaMailbox(cmd, opts);
}

// ---------------------------------------------------------------------------
// Slack transport (fast path, preferred when a Slack app is configured)
//
// Slack is the central relay: both sides are pure API/WebSocket clients, so
// no inbound connectivity is needed anywhere. Commands go out with
// chat.postMessage, the answer comes back as another message in the same
// channel - delivered over Socket Mode (~300 ms) or found by polling
// conversations.history (recovery path, ~2 s).
// ---------------------------------------------------------------------------

const SLACK_POLL_MS = Number(process.env.RELAY_SLACK_POLL || 2000);
const SLACK_FAST_POLL_MS = Number(process.env.RELAY_SLACK_FAST_POLL || 100);

/** Which transport to use: "slack", "github" or "auto". */
function transport() {
  const want = (process.env.RELAY_TRANSPORT || "auto").toLowerCase();
  if (want === "github") return "github";
  if (want === "slack") return "slack";
  return slackConfig() ? "slack" : "github";
}

async function runViaSlack(cmd, opts = {}) {
  const cfg = slackConfig();
  if (!cfg) throw new Error("slack: not configured (no bot token)");
  const id = crypto.randomBytes(6).toString("hex");
  const t0 = Date.now();
  const channel = await slackChannelId(cfg.SLACK_CHANNEL);
  const req = { id, cmd, cwd: opts.cwd, timeout: opts.timeout };
  await slackPost(PREFIXES.CMD_PREFIX + JSON.stringify(req), channel);
  process.stderr.write(`[${id}] queued (slack)\n`);

  let answer = null;
  let done = false;
  const finish = (a) => {
    if (!done && a && a.id === id) {
      done = true;
      answer = a;
    }
  };

  // NOTE: deliberately NO client-side Socket Mode connection. With a single
  // shared bot, opening one here races the MacBook's persistent socket: Slack
  // picks one connection at random to deliver the command, so the MacBook can
  // miss it by up to its own poll interval. Keeping the client socket-free
  // makes the MacBook's the only connection => the command always reaches it,
  // and the adaptive history poll below catches the answer in ~100-300 ms.
  const parts = new Map();
  const seenTs = new Set();
  const deadline = Date.now() + (opts.wait || 180000);
  // Adaptive poll: most commands answer in well under a second, so poll fast
  // right after the command goes out, then back off for long-running ones.
  // This keeps the no-socket-event fallback snappy without hammering the API.
  try {
    while (!done && Date.now() < deadline) {
      for (const m of await slackHistory(channel)) {
        if (seenTs.has(m.ts)) continue;
        seenTs.add(m.ts);
        const a = parseAnswer(m.text, parts);
        if (a) finish(a);
      }
      if (done) break;
      const elapsed = Date.now() - t0;
      // 100 ms for the first 1.5 s, ramp to SLACK_POLL_MS by ~8 s.
      const fast = SLACK_FAST_POLL_MS;
      const interval = elapsed < 1500
        ? fast
        : Math.min(SLACK_POLL_MS, fast + (elapsed - 1500) * (SLACK_POLL_MS - fast) / 6500);
      await sleep(Math.min(interval, Math.max(50, deadline - Date.now())));
    }
  } finally {
    // no client-side socket to close (kept socket-free on purpose)
  }
  if (!answer) throw new Error("timeout waiting for the MacBook (slack)");
  answer.ms = (answer.ms ?? 0) + (Date.now() - t0 - (answer.ms ?? 0));
  return answer;
}

const argv = process.argv.slice(2);
const cmd = argv[0];
const arg = (name, dflt) => {
  const i = argv.indexOf(`--${name}`);
  return i >= 0 ? argv[i + 1] : dflt;
};

if (cmd === "run") {
  let command = arg("cmd") || argv[argv.indexOf("run") + 1];
  // "relay.mjs run < file" pipes the command in on stdin (multi-line scripts).
  if (!command && !process.stdin.isTTY) {
    const chunks = [];
    for await (const chunk of process.stdin) chunks.push(chunk);
    command = Buffer.concat(chunks).toString("utf8");
  }
  const o = await run(command, { cwd: arg("cwd"), timeout: arg("timeout") && Number(arg("timeout")), wait: Number(arg("wait") || 180000) });
  if (o.stdout) process.stdout.write(o.stdout);
  if (o.stderr) process.stderr.write(o.stderr);
  process.stderr.write(`[${o.host || "macbook"}] exit=${o.code} ${o.ms}ms\n`);
  process.exit(o.code ?? 0);
} else if (cmd === "pi") {
  // Talk to the persistent pi session agent on the MacBook.
  const line = argv[argv.indexOf("pi") + 1] || "state";
  const o = await run(`pi: ${line}`, {
    timeout: arg("timeout") && Number(arg("timeout")),
    wait: Number(arg("wait") || 900000),
  });
  if (o.stdout) process.stdout.write(o.stdout);
  if (o.stderr) process.stderr.write(o.stderr);
  if (o.trace?.length) process.stderr.write(`[trace] ${o.trace.join(" | ")}\n`);
  process.stderr.write(`[${o.host || "macbook"}] exit=${o.code} ${o.ms}ms\n`);
  process.exit(o.code ?? 0);
} else if (cmd === "sh") {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  let cwd;
  for await (const line of rl) {
    const command = line.trim();
    if (!command) continue;
    if (command === "exit" || command === "quit") break;
    const m = command.match(/^cd\s+(.+)$/);
    if (m) {
      const o = await run(`cd ${m[1]} && pwd`);
      cwd = (o.stdout || "").trim() || cwd;
      console.log(cwd);
      continue;
    }
    const o = await run(command, { cwd });
    if (o.stdout) process.stdout.write(o.stdout);
    if (o.stderr) process.stderr.write(o.stderr);
    process.stderr.write(`[exit=${o.code} ${o.ms}ms]\n`);
  }
} else if (cmd === "watch") {
  let last = 0;
  for (;;) {
    for (const c of (await comments()).reverse()) {
      if (c.id <= last) continue;
      last = c.id;
      console.log(c.id, String(c.body || "").slice(0, 300));
    }
    await sleep(3000);
  }
} else {
  console.error("usage: relay.mjs run '<cmd>' | pi '<pisess line>' | sh | watch");
  process.exit(2);
}
