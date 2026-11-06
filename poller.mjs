#!/usr/bin/env node
// poller.mjs - runs ON the Allianz MacBook. Outbound-only: polls a private GitHub
// issue for `CMD {...}` comments, runs them, posts `OUT {...}` results back.
//
// No inbound ports, no tunnels - only api.github.com over HTTPS (allowed by Zscaler).
//
//   node poller.mjs                  # foreground
//   nohup node poller.mjs >/dev/null 2>&1 &
//   RELAY_INTERVAL=2 node poller.mjs # poll every 2s (default 3)

import { execFile } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";

const REPO = process.env.RELAY_REPO || "bikboy/mbp-relay";
const ISSUE = process.env.RELAY_ISSUE || "1";
const HOSTNAME = process.env.RELAY_HOST || "github.com";
const INTERVAL = Number(process.env.RELAY_INTERVAL || 3) * 1000;
const MAX_OUT = 40000;
const STATE_DIR = path.join(os.homedir(), ".mbp-relay");
const STATE_FILE = path.join(STATE_DIR, "state.json");
// Unix socket of the persistent pi session agent (pisess.mjs) on this Mac.
const PI_SOCKET = process.env.RELAY_PI_SOCKET || path.join(os.homedir(), "mbp-relay", "pi.sock");

function gh(args, input) {
  return new Promise((resolve, reject) => {
    // Zscaler occasionally black-holes a request and never answers it. Without
    // a timeout the poll loop blocks forever and the relay goes silent while
    // the process is still alive.
    const child = execFile("gh", args, { maxBuffer: 64 * 1024 * 1024, timeout: 45000 }, (err, stdout, stderr) => {
      if (err) return reject(new Error(`gh ${args[0]}: ${stderr || err.message}`));
      resolve(stdout);
    });
    if (input !== undefined) {
      child.stdin.write(input);
      child.stdin.end();
    }
  });
}

async function comments() {
  // The mailbox can exceed 100 comments; the default API page returns the
  // OLDEST comments only, so a busy poller would stop seeing new commands.
  // Read the last pages explicitly.
  const first = JSON.parse(
    await gh([
      "api",
      "--hostname",
      HOSTNAME,
      `/repos/${REPO}/issues/${ISSUE}/comments?per_page=100`,
    ]),
  );
  if (first.length < 100) return first;
  const issue = JSON.parse(await gh(["api", "--hostname", HOSTNAME, `/repos/${REPO}/issues/${ISSUE}`]));
  const lastPage = Math.max(1, Math.ceil(Number(issue.comments || 0) / 100));
  const out = [];
  for (let p = lastPage; p >= Math.max(1, lastPage - 1); p--) {
    const arr = JSON.parse(
      await gh([
        "api",
        "--hostname",
        HOSTNAME,
        `/repos/${REPO}/issues/${ISSUE}/comments?per_page=100&page=${p}`,
      ]),
    );
    out.unshift(...arr);
  }
  return out;
}

/** Keep the mailbox small: delete a request comment once it has been answered. */
async function removeComment(id) {
  try {
    await gh(["api", "--hostname", HOSTNAME, "-X", "DELETE", `/repos/${REPO}/issues/comments/${id}`]);
  } catch {
    /* already gone */
  }
}

async function post(body) {
  await gh(
    ["api", "--hostname", HOSTNAME, "-X", "POST", `/repos/${REPO}/issues/${ISSUE}/comments`, "--input", "-"],
    JSON.stringify({ body }),
  );
}

function state() {
  try {
    return JSON.parse(fs.readFileSync(STATE_FILE, "utf8"));
  } catch {
    return { lastId: 0 };
  }
}

function saveState(s) {
  fs.mkdirSync(STATE_DIR, { recursive: true });
  fs.writeFileSync(STATE_FILE, JSON.stringify(s));
}

function run(cmd, cwd, timeout) {
  return new Promise((resolve) => {
    const started = Date.now();
    execFile(
      "/bin/bash",
      ["-lc", cmd],
      { cwd: cwd || os.homedir(), timeout: timeout || 120000, maxBuffer: 16 * 1024 * 1024, env: { ...process.env, TERM: "dumb" } },
      (err, stdout, stderr) => {
        resolve({
          code: err?.code ?? 0,
          stdout: String(stdout || "").slice(0, MAX_OUT),
          stderr: String(stderr || "").slice(0, MAX_OUT),
          ms: Date.now() - started,
        });
      },
    );
  });
}

/**
 * Forward a command to the persistent pi session agent.
 * Commands arriving here start with "pi:" and the rest is a pisess line.
 */
function piAgent(line, timeout) {
  return new Promise((resolve) => {
    const started = Date.now();
    const sock = net.createConnection(PI_SOCKET);
    let buf = "";
    let done = false;
    const finish = (res) => {
      if (done) return;
      done = true;
      try {
        sock.destroy();
      } catch {
        /* ignore */
      }
      resolve(res);
    };
    sock.setTimeout(timeout || 15 * 60 * 1000, () =>
      finish({ code: 124, stdout: "", stderr: "pi agent timeout", ms: Date.now() - started }),
    );
    sock.on("error", (e) =>
      finish({ code: 1, stdout: "", stderr: `pi agent error: ${e.message}`, ms: Date.now() - started }),
    );
    sock.on("connect", () => sock.write(`${JSON.stringify({ line })}\n`));
    sock.on("data", (chunk) => {
      buf += chunk.toString();
      const idx = buf.indexOf("\n");
      if (idx < 0) return;
      let reply;
      try {
        reply = JSON.parse(buf.slice(0, idx));
      } catch {
        reply = { ok: false, text: buf.slice(0, idx) };
      }
      finish({
        code: reply.ok ? 0 : 1,
        stdout: reply.text || "",
        stderr: reply.error || "",
        ms: Date.now() - started,
        trace: reply.trace,
        sessionId: reply.sessionId,
      });
    });
  });
}


// ---------------------------------------------------------------------------
// ntfy transport (fast path)
//
// ntfy.sh is reachable from this machine and answers in ~300 ms where the
// GitHub mailbox costs ~4 s of polling. Payloads are sealed with AES-256-GCM
// (the topic name alone is not a secret on ntfy.sh); the key lives in
// ntfy.env. Commands older than CMD_TTL_MS are dropped, so a cached command
// replayed after a reconnect cannot fire twice alongside the GitHub fallback.
// ---------------------------------------------------------------------------

const NTFY = process.env.NTFY_SERVER || "https://ntfy.sh";
const NTFY_CHUNK = 3000;
const CMD_TTL_MS = 30000;

function ntfyConfig() {
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

/** POST one ntfy message, retrying rate limits (429) as requested. */
async function ntfyPost(headers, body, signal) {
	for (let attempt = 1; ; attempt++) {
		const res = await fetch(`${NTFY}/${cfg_topic}`, {
			method: "POST",
			headers,
			body,
			// Never hang forever on a black-holed connection.
			signal: signal ?? AbortSignal.timeout(20000),
		});
		if (res.ok) return;
		// ntfy.sh rate limits anonymous visitors; Retry-After says when the
		// budget refills. Without the retry a chunked answer is simply lost
		// and the client falls back to the slow GitHub transport.
		//
		// CAP THE WAIT: ntfy.sh has been observed to answer "retry-after:
		// 82013" (= 22 hours). Honouring that literally blocks the poller for a
		// whole day. The GitHub transport carries the answer anyway, so a short
		// backoff and then giving up is strictly better than stalling.
		if (res.status === 429 && attempt < 3) {
			const raw = Number(res.headers.get("retry-after") ?? 5);
			const wait = Math.min(Math.max(raw, 1), 30) * 1000 + 250;
			res.body?.cancel().catch(() => {});
			console.error(new Date().toISOString(), `ntfy: 429 (retry-after ${raw}s), retry in ${wait}ms`);
			await new Promise((r) => setTimeout(r, wait));
			if (signal?.aborted) throw new Error("ntfy publish aborted");
			continue;
		}
		throw new Error(`ntfy publish ${res.status}`);
	}
}

let cfg_topic = "";
function setCfgTopic(cfg) {
	cfg_topic = cfg.NTFY_TOPIC;
}

async function ntfyPublish(cfg, title, payload, id = "", signal) {
	setCfgTopic(cfg);
	const sealed = seal(cfg.NTFY_KEY, payload);
	if (sealed.length <= NTFY_CHUNK) {
		await ntfyPost({ title }, sealed, signal);
		return;
	}
	// Big answers are chunked; the id rides in every part title.
	const parts = [];
	for (let i = 0; i < sealed.length; i += NTFY_CHUNK) parts.push(sealed.slice(i, i + NTFY_CHUNK));
	for (let i = 0; i < parts.length; i++) {
		await ntfyPost({ title: `${title}/${i}/${parts.length}/${id}` }, parts[i], signal);
	}
}

/**
 * Execute one request (either transport) and return the result record.
 * Shared with the GitHub tick so both transports behave identically.
 */
async function handleRequest(req) {
	if (typeof req.cmd === "string" && req.cmd.startsWith("pi:")) {
		return await piAgent(req.cmd.slice(3).trim(), req.timeout);
	}
	return await run(req.cmd, req.cwd, req.timeout);
}

/**
 * Stream commands from ntfy and answer them.
 *
 * On this machine Zscaler black-holes ntfy.sh's long-lived HTTP streams (the
 * request hangs forever with no bytes), but the WebSocket endpoint and plain
 * POSTs go through, so the listener uses wss:// and answers over HTTP.
 */
async function ntfyLoop() {
	const cfg = ntfyConfig();
	if (!cfg) return; // no key file: stay on the GitHub transport
	if (typeof WebSocket === "undefined") {
		console.error(new Date().toISOString(), "ntfy: no WebSocket support, staying on GitHub");
		return;
	}
	for (;;) {
		const ws = new WebSocket(`wss://ntfy.sh/${cfg.NTFY_TOPIC}/ws`);
		let alive = false;
		const giveUp = setTimeout(() => {
			if (!alive) {
				try {
				ws.close();
			} catch {
				/* ignore */
			}
			}
		}, 20000);
		await new Promise((resolveOpened) => {
			let settled = false;
			ws.onopen = () => {
				alive = true;
				settled = true;
				clearTimeout(giveUp);
				console.error(new Date().toISOString(), "ntfy: subscribed (ws)");
				resolveOpened();
			};
			ws.onerror = () => {
				if (!settled) {
					settled = true;
				clearTimeout(giveUp);
					resolveOpened();
				}
			};
			ws.onclose = () => {
				if (!settled) {
					settled = true;
					clearTimeout(giveUp);
					resolveOpened();
				}
				};
		});
		if (!alive) {
			console.error(new Date().toISOString(), "ntfy: ws connect failed; retry in 10s");
			await new Promise((r) => setTimeout(r, 10000));
			continue;
		}
		// Reassembly for chunked commands: id -> { seen, total }
		const cmdParts = new Map();
		// Zscaler silently kills the websocket after a while: the socket stays
		// "open" but no more frames arrive, so the poller would sit there
		// forever without ever processing a command. Drop the connection if
		// nothing is heard for a while and let the loop reconnect.
		const IDLE_MS = Number(process.env.RELAY_WS_IDLE_MS || 90000);
		let lastFrame = Date.now();
		const idleWatch = setInterval(() => {
			if (Date.now() - lastFrame < IDLE_MS) return;
			console.error(new Date().toISOString(), "ntfy: ws idle, reconnecting");
			try {
				ws.close();
			} catch {
				/* ignore */
			}
		}, 15000);
		await new Promise((resolveClosed) => {
			ws.onmessage = async (ev) => {
				lastFrame = Date.now();
				// The ws channel wraps payloads in a JSON event object.
				let frame;
				try {
					frame = JSON.parse(String(ev.data ?? ""));
				} catch {
					return;
				}
				if (frame.event === "keepalive" || frame.event === "open") return;
				const body = String(frame.message ?? "");
				if (!body) return;
				const title = String(frame.title ?? "");
				let payload = null;
				if (title.startsWith("cmd/")) {
					// cmd/<i>/<total>/<id>: one part of a chunked command
					const m = title.match(/^cmd\/(\d+)\/(\d+)\/([0-9a-f]+)$/);
					if (!m) return;
					const rec = cmdParts.get(m[3]) ?? { seen: new Map(), total: Number(m[2]) };
					rec.seen.set(Number(m[1]), body);
					cmdParts.set(m[3], rec);
					if (rec.seen.size !== rec.total) return;
					let sealed = "";
					for (let i = 0; i < rec.total; i++) sealed += rec.seen.get(i);
					cmdParts.delete(m[3]);
					try {
						payload = unseal(cfg.NTFY_KEY, sealed);
					} catch {
						return;
					}
				} else if (title === "cmd") {
					try {
						payload = unseal(cfg.NTFY_KEY, body);
					} catch {
						return; // not addressed to us
					}
				}
				if (!payload || payload.kind !== "cmd" || !payload.id) return;
				// Drop stale commands: they were probably also sent over the
				// GitHub fallback and would run twice.
				if (Date.now() - Number(payload.ts || 0) > CMD_TTL_MS) {
					console.error(new Date().toISOString(), "ntfy: dropped stale command", payload.id);
					return;
				}
				console.error(new Date().toISOString(), "ntfy: command", payload.id);
				try {
					const res2 = await handleRequest(payload);
					await ntfyPublish(cfg, "out", { kind: "out", id: payload.id, host: os.hostname(), ...res2 }, payload.id);
				} catch (e) {
					console.error(new Date().toISOString(), "ntfy: answer failed:", e.message);
				}
			};
			ws.onerror = () => {};
			ws.onclose = () => {
				clearInterval(idleWatch);
				resolveClosed();
			};
		});
		clearInterval(idleWatch);
		console.error(new Date().toISOString(), "ntfy: ws closed; reconnecting in 5s");
		await new Promise((r) => setTimeout(r, 5000));
	}
}

/**
 * Keep the mailbox small.
 *
 * Every answer is posted as an OUT comment as well as over ntfy, and only the
 * GitHub-based client deletes it again. A long-running mailbox then grows to
 * thousands of comments: the client has to page through them, the poller has
 * to crawl them every tick, and everything gets slow. Keep only the newest
 * few OUT comments.
 */
async function pruneOut() {
  const keep = Number(process.env.RELAY_OUT_KEEP || 40);
  const list = await comments(); // newest pages first
  const outs = list
    .filter((c) => String(c.body || "").startsWith("OUT "))
    .sort((a, b) => b.id - a.id)
    .slice(keep);
  for (const c of outs) {
    try {
      await removeComment(c.id);
    } catch {
      /* ignore */
    }
  }
  if (outs.length) {
    console.error(new Date().toISOString(), `mailbox: pruned ${outs.length} old answers`);
  }
}

let lastPrune = 0;

/** Timestamp of the last ntfy publish, for throttling. */
let lastNtfy = 0;

/**
 * Minimum gap between two ntfy publishes. ntfy.sh rate limits anonymous
 * visitors hard; a burst of answers (e.g. the pi-sessiond "pi: list" refresh
 * every 60s plus interactive commands) burns the budget and the next answers
 * get 429. GitHub carries every answer anyway, so skipping the fast channel
 * occasionally is harmless.
 */
const NTFY_MIN_GAP_MS = 3000;


/**
 * Publish one answer over ntfy (best effort - the mailbox is the fallback).
 *
 * NEVER let ntfy block the command loop. The GitHub transport always carries
 * the answer, so ntfy is only an accelerator. A rate-limited (429) or hanging
 * ntfy must not stall the poller, otherwise no further command is ever
 * processed.
 */
async function ntfyPublishAnswer(answer) {
  const cfg = ntfyConfig();
  if (!cfg) return;

  // ntfy.sh allows only 250 messages per 24h for an anonymous topic
  // (ratelimit-policy: "messages";q=250;w=86400). Chunked answers cost one
  // message PER CHUNK, so a single "pi: list" (8 chunks) eats 3% of the daily
  // budget; ~30 list refreshes exhaust it and every later publish gets
  // 429 retry-after=~80000, which used to block the poller for a day.
  //
  // Therefore: only send SMALL answers (one chunk) over ntfy and skip the
  // session-list flood entirely. GitHub always carries the answer, so nothing
  // is lost - the fast channel is simply reserved for interactive commands.
  const sealedLen = seal(cfg.NTFY_KEY, answer).length;
  if (sealedLen > NTFY_CHUNK) {
    console.error(new Date().toISOString(), `ntfy: skipped (${sealedLen}B needs chunks; saves the 250/day budget)`);
    return;
  }
  const gap = Date.now() - lastNtfy;
  if (gap < NTFY_MIN_GAP_MS) {
    console.error(new Date().toISOString(), `ntfy: skipped (throttled, ${gap}ms since last)`);
    return;
  }
  lastNtfy = Date.now();
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(new Error("ntfy publish timed out (8s)")), 8000);
  try {
    await ntfyPublish(cfg, "out", answer, answer.id, ac.signal);
  } finally {
    clearTimeout(timer);
  }
}

async function tick() {
  const list = (await comments()).reverse(); // oldest first
  const st = state();
  const pending = list.filter((c) => c.id > st.lastId && !c.user?.login?.includes("[bot]"));
  if (pending.length) {
    console.error(new Date().toISOString(), `mailbox: ${pending.length} pending, lastId ${st.lastId}`);
  }
  for (const c of pending) {
    st.lastId = c.id;
    saveState(st);
    const body = String(c.body || "");
    if (!body.startsWith("CMD ")) continue;
    let req;
    try {
      req = JSON.parse(body.slice(4));
    } catch {
      await post("OUT " + JSON.stringify({ id: "?", error: "bad CMD payload" }));
      continue;
    }
    const res = await handleRequest(req);
    const answer = { id: req.id, host: os.hostname(), ...res };
    console.error(new Date().toISOString(), `answer for ${req.id}: ${String(res.stdout || res.error || "").slice(0, 120)}`);
    // Answer over BOTH channels at once: the mailbox (always available) and
    // ntfy (fast). The MacBook cannot read ntfy, but it can write to it, so
    // the client gets the answer in ~300 ms instead of a mailbox round trip.
    await Promise.all([
      post("OUT " + JSON.stringify(answer)).catch((e) => {
        console.error(new Date().toISOString(), "github answer failed:", e.message);
      }),
      ntfyPublishAnswer(answer).catch((e) => {
        console.error(new Date().toISOString(), "ntfy answer failed:", e.message);
      }),
    ]);
    await removeComment(c.id);
  }
  if (Date.now() - lastPrune > 60000) {
    lastPrune = Date.now();
    try {
      await pruneOut();
    } catch (e) {
      console.error(new Date().toISOString(), "mailbox: prune failed:", e.message);
    }
  }
}

// Fast transport runs alongside the GitHub loop, which stays as fallback.
ntfyLoop();

let warned = false;
for (;;) {
  try {
    await tick();
    if (warned) {
      console.error(new Date().toISOString(), "reconnected");
      warned = false;
    }
  } catch (e) {
    if (!warned) {
      console.error(new Date().toISOString(), "poll error:", e.message);
      warned = true;
    }
  }
  await new Promise((r) => setTimeout(r, INTERVAL));
}
