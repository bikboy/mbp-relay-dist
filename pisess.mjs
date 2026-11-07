#!/usr/bin/env node
/**
 * pisess.mjs - persistent pi session agent for the Allianz MacBook.
 *
 * Runs `pi --mode rpc` as a long-lived child and exposes it over a unix
 * socket with a line based JSON protocol. The relay poller forwards
 * commands prefixed with "pi:" to this socket, which turns GitHub issues
 * into a remote control channel for pi sessions on the MacBook.
 *
 * Protocol (one JSON object per line, both directions):
 *   -> {"line":"prompt what is 2+2"}
 *   <- {"ok":true,"text":"4","trace":[...],"sessionId":"..."}
 *
 * Supported "line" commands:
 *   ping                       liveness check
 *   state                      get_state of the live RPC session
 *   stats                      get_session_stats
 *   prompt <text>              send a prompt, wait for the run to settle
 *   new                        start a fresh session
 *   switch <sessionPath>       switch to an existing session file
 *   abort                      abort the running turn
 *   list                       list stored sessions (id, cwd, time, name)
 *                               plus herdr pane info (#herdr / #pane lines)
 *   exec <shell command>       run a shell command on the MacBook
 *   raw <json record>          send a raw RPC record (escape hatch)
 *   cwd                        print the working directory of the agent
 */

import { createServer } from "node:net";
import { spawn, exec } from "node:child_process";
import { existsSync, readdirSync, readFileSync, statSync, unlinkSync, mkdirSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const HOME = homedir();
const SOCKET = process.env.PISESS_SOCKET || join(HOME, "mbp-relay", "pi.sock");
const DEFAULT_CWD = process.env.PISESS_CWD || HOME;
const SESSION_ROOT = join(HOME, ".pi", "agent", "sessions");
const PROMPT_TIMEOUT_MS = Number(process.env.PISESS_TIMEOUT_MS || 15 * 60 * 1000);

// ---------------------------------------------------------------------------
// Minimal pi RPC client
// ---------------------------------------------------------------------------

class PiRpc {
	constructor(cwd) {
		this.cwd = cwd;
		this.pending = new Map();
		this.waiters = [];
		this.extra = [];
		this.buf = "";
		this.seq = 0;
		this.stderr = "";
		this.exited = false;
	}

	start() {
		this.proc = spawn("pi", ["--mode", "rpc"], {
			cwd: this.cwd,
			env: process.env,
			stdio: ["pipe", "pipe", "pipe"],
		});
		this.proc.stdout.setEncoding("utf8");
		this.proc.stdout.on("data", (chunk) => this.onData(chunk));
		this.proc.stderr.setEncoding("utf8");
		this.proc.stderr.on("data", (chunk) => {
			this.stderr = (this.stderr + chunk).slice(-4000);
		});
		this.proc.on("exit", (code) => {
			this.exited = true;
			for (const [, p] of this.pending) p.reject(new Error(`pi exited with code ${code}`));
			this.pending.clear();
		});
		return this;
	}

	stop() {
		try {
			this.proc?.stdin?.end();
		} catch {
			/* already gone */
		}
		try {
			this.proc?.kill("SIGTERM");
		} catch {
			/* already gone */
		}
	}

	onData(chunk) {
		this.buf += chunk;
		let idx;
		while ((idx = this.buf.indexOf("\n")) >= 0) {
			const line = this.buf.slice(0, idx);
			this.buf = this.buf.slice(idx + 1);
			if (!line.trim()) continue;
			let rec;
			try {
				rec = JSON.parse(line);
			} catch {
				continue;
			}
			if (rec.type === "response" && rec.id && this.pending.has(rec.id)) {
				const { resolve, reject } = this.pending.get(rec.id);
				this.pending.delete(rec.id);
				if (rec.success) resolve(rec.data);
				else reject(new Error(rec.error || "rpc error"));
				continue;
			}
			this.emit(rec);
		}
	}

	emit(rec) {
		for (const fn of this.extra) {
			try {
				fn(rec);
			} catch {
				/* listener errors must not break the stream */
			}
		}
		this.waiters = this.waiters.filter((w) => {
			if (w.type === rec.type) {
				w.resolve(rec);
				return false;
			}
			return true;
		});
	}

	/** Resolve with the next event of the given type. */
	waitFor(type, timeoutMs) {
		return new Promise((resolve, reject) => {
			const timer = setTimeout(() => {
				this.waiters = this.waiters.filter((w) => w.timer !== timer);
				reject(new Error(`timeout waiting for ${type}`));
			}, timeoutMs);
			this.waiters.push({ type, resolve, timer });
		});
	}

	send(record) {
		if (this.exited) return Promise.reject(new Error("pi process is not running"));
		const id = `r${++this.seq}-${Date.now().toString(36)}`;
		const payload = { ...record, id };
		return new Promise((resolve, reject) => {
			this.pending.set(id, { resolve, reject });
			try {
				this.proc.stdin.write(`${JSON.stringify(payload)}\n`);
			} catch (err) {
				this.pending.delete(id);
				reject(err);
			}
		});
	}
}

// ---------------------------------------------------------------------------
// Session helpers (read pi's own session files)
// ---------------------------------------------------------------------------

/**
 * Read every stored session file, newest first.
 *
 * Cached for a short while: this parses EVERY transcript on disk (the MacBook
 * has ~100 of them), and both the session list and the context command need
 * it. Without the cache a context request took ~9 s.
 */
let sessionCache = { at: 0, sessions: [] };

function listSessions(force = false) {
	if (!force && sessionCache.sessions.length && Date.now() - sessionCache.at < 20_000) {
		return sessionCache.sessions;
	}
	const out = [];
	let dirs = [];
	try {
		dirs = readdirSync(SESSION_ROOT, { withFileTypes: true })
			.filter((d) => d.isDirectory())
			.map((d) => join(SESSION_ROOT, d.name));
	} catch {
		return out;
	}
	for (const dir of dirs) {
		let files = [];
		try {
			files = readdirSync(dir).filter((f) => f.endsWith(".jsonl"));
		} catch {
			continue;
		}
		for (const file of files) {
			const path = join(dir, file);
			let header = null;
			let name = null;
			let messages = 0;
			let firstUser = null;
			try {
				const text = readFileSync(path, "utf8");
				// Parsing every line of every transcript with JSON.parse took
				// minutes once the machine had a hundred sessions, and the
				// relay processes commands one after another - a slow list
				// blocked every other command for minutes. The records we
				// need (header, name, first user message) live in the first
				// lines, so parse only those and count the messages with a
				// plain text scan over the rest.
				let pos = 0;
				let seen = 0;
				while (pos < text.length && seen < 200) {
					const nl = text.indexOf("\n", pos);
					const line = text.slice(pos, nl === -1 ? text.length : nl);
					pos = nl === -1 ? text.length : nl + 1;
					seen++;
					if (!line.trim()) continue;
					let rec;
					try {
						rec = JSON.parse(line);
					} catch {
						continue;
					}
					if (rec.type === "session" && !header) {
						header = rec;
					} else if (rec.type === "session_info" || rec.type === "session_name") {
						name = rec.name || name;
					} else if (rec.type === "message" && !firstUser && rec.message?.role === "user") {
						// content is a string or an array of content blocks
						const c = rec.message.content;
						if (typeof c === "string") firstUser = c;
						else if (Array.isArray(c)) {
							firstUser = c
								.map((b) => (b && typeof b === "object" && typeof b.text === "string" ? b.text : ""))
								.join(" ")
								.trim();
						}
					}
				}
				messages = (text.match(/"type"\s*:\s*"message"/g) || []).length;
			} catch {
				continue;
			}
			if (!header) continue;
			let mtime = 0;
			try {
				mtime = statSync(path).mtimeMs;
			} catch {
				/* ignore */
			}
			out.push({
				id: header.id,
				path,
				cwd: header.cwd,
				created: header.timestamp,
				modified: new Date(mtime).toISOString(),
				messages,
				name: name || undefined,
				firstUser: (firstUser || "").slice(0, 120) || undefined,
			});
		}
	}
	out.sort((a, b) => String(b.modified).localeCompare(String(a.modified)));
	sessionCache = { at: Date.now(), sessions: out };
	return out;
}

/**
 * Read the tail of a session transcript and project it into compact turns.
 *
 * This is the laptop-side twin of the daemon's src/context.ts: the transcript
 * file lives HERE, on the MacBook, so this is the only place that can read it.
 * The daemon pulls the result over the relay when the phone asks for context.
 */
const MAX_TAIL_BYTES = 512 * 1024;
const MAX_TURNS = 12;
const MAX_TEXT = 400;

function clipText(s, max = MAX_TEXT) {
	const t = String(s).replace(/\r/g, "").replace(/\n{3,}/g, "\n\n").trim();
	return t.length <= max ? t : t.slice(0, max - 1) + "…";
}

function contentToText(content) {
	if (typeof content === "string") return content;
	if (Array.isArray(content)) {
		return content
			.map((c) => (c && typeof c === "object" && typeof c.text === "string" ? c.text : ""))
			.join("\n");
	}
	return "";
}

function readSessionContext(path) {
	let text = "";
	try {
		text = readFileSync(path, "utf8");
	} catch {
		return { turns: [], totalEntries: 0, truncated: false };
	}
	const lines = text.split("\n").filter((l) => l.trim());
	const turns = [];
	let totalEntries = 0;
	for (const line of lines) {
		totalEntries++;
		let e;
		try {
			e = JSON.parse(line);
		} catch {
			continue;
		}
		if (e.type !== "message" || !e.message) continue;
		const role = e.message.role;
		if (role === "user" || role === "assistant") {
			const body = clipText(contentToText(e.message.content));
			if (body) turns.push({ role, text: body });
		} else if (role === "tool" || role === "tool_result") {
			const name = e.message.toolName ?? e.message.tool_name ?? "tool";
			turns.push({ role: "tool", text: "", toolName: String(name) });
		}
	}
	// Collapse consecutive tool turns into one readable line.
	const compact = [];
	for (const t of turns) {
		const prev = compact[compact.length - 1];
		if (t.role === "tool" && prev?.role === "tool") {
			prev.text = prev.text ? `${prev.text}, ${t.toolName}` : String(t.toolName);
			continue;
		}
		compact.push({ ...t, text: t.role === "tool" ? String(t.toolName) : t.text });
	}
	return { turns: compact.slice(-MAX_TURNS), totalEntries, truncated: false };
}

// ---------------------------------------------------------------------------
// Agent
// ---------------------------------------------------------------------------

class Agent {
	constructor() {
		this.rpc = null;
		this.cwd = DEFAULT_CWD;
		this.busy = false;
	}

	ensure() {
		if (this.rpc && !this.rpc.exited) return this.rpc;
		this.rpc = new PiRpc(this.cwd).start();
		return this.rpc;
	}

	async setCwd(cwd) {
		if (!cwd || cwd === this.cwd) return this.cwd;
		this.rpc?.stop();
		this.rpc = null;
		this.cwd = cwd;
		return this.cwd;
	}

	async prompt(text) {
		const rpc = this.ensure();
		const trace = [];
		const listener = (rec) => {
			if (rec.type === "tool_execution_start") {
				const args = rec.args || {};
				const hint = args.command || args.path || args.file_path || args.pattern || "";
				trace.push(`tool ${rec.toolName}${hint ? `: ${String(hint).slice(0, 120)}` : ""}`);
			} else if (rec.type === "tool_execution_end" && rec.isError) {
				trace.push("tool error");
			}
		};
		rpc.extra.push(listener);
		try {
			const settled = rpc.waitFor("agent_settled", PROMPT_TIMEOUT_MS);
			const res = await rpc.send({ type: "prompt", message: text });
			const disposition = res?.disposition;
			if (disposition !== "handled") {
				await settled.catch(() => {});
			}
			const last = await rpc.send({ type: "get_last_assistant_text" });
			// Prefer the full message: it carries provider errors (quota, auth)
			// that get_last_assistant_text swallows into an empty string.
			let answer = last?.text ?? "";
			let error;
			try {
				const msgs = await rpc.send({ type: "get_messages" });
				const assistant = [...(msgs?.messages || [])].reverse().find((m) => m.role === "assistant");
				if (assistant) {
					const blocks = Array.isArray(assistant.content) ? assistant.content : [];
					const joined = blocks
						.filter((b) => b && (b.type === "text" || typeof b.text === "string"))
						.map((b) => b.text || "")
						.join("")
						.trim();
					if (joined) answer = joined;
					if (assistant.errorMessage) error = assistant.errorMessage;
				}
			} catch {
				/* keep the text we have */
			}
			let sessionId = null;
			try {
				const state = await rpc.send({ type: "get_state" });
				sessionId = state?.sessionId || state?.sessionPath || null;
			} catch {
				/* state is optional */
			}
			return {
				ok: !error,
				text: error ? `${answer}\n[error] ${error}`.trim() : answer,
				trace,
				disposition: disposition ?? null,
				sessionId,
			};
		} finally {
			rpc.extra = rpc.extra.filter((fn) => fn !== listener);
		}
	}
}

// ---------------------------------------------------------------------------
// Socket server
// ---------------------------------------------------------------------------

const agent = new Agent();

function runShell(command, timeout = 120000) {
	return new Promise((resolve) => {
		exec(command, { cwd: agent.cwd, timeout, maxBuffer: 4 * 1024 * 1024 }, (err, stdout, stderr) => {
			resolve({
				ok: !err,
				text: [stdout, stderr].filter(Boolean).join("\n"),
				error: err ? String(err.message).split("\n")[0] : undefined,
			});
		});
	});
}

/**
 * Read the local herdr panes and map them to pi session ids.
 *
 * The sessions on this machine run inside herdr, so the pane and the agent
 * status are worth relaying: without them a remote viewer cannot tell an
 * open session from a stale file on disk.
 */
/**
 * Run a herdr command on this machine.
 *
 * The launch agent has a minimal PATH, so the known install location is
 * tried first and the bare name second.
 *
 * herdr is the consumer of pi's OSC 7501 program-status channel: it turns
 * the reported state (working/idle/done/error/blocked) into `agent_status`
 * and into the `--until` conditions of `herdr agent wait`.
 */
async function herdr(args) {
	let res = await runShell(`${HOME}/.local/bin/herdr ${args} 2>&1`, 20000);
	if (!res.text.trim()) res = await runShell(`herdr ${args} 2>&1`, 20000);
	return res;
}

/**
 * Workspace id -> display name, as shown on the herdr dial.
 *
 * `herdr workspace list` reports the label the user set for each workspace
 * ("PR review", "custom sso role for secrets", ...). Those names are what the
 * phone should show; the pi session's first message is only a fallback.
 */
async function herdrWorkspaceLabels() {
	const map = new Map();
	for (const cmd of [`${HOME}/.local/bin/herdr workspace list`, "herdr workspace list"]) {
		const res = await runShell(`${cmd} 2>/dev/null`, 20000);
		if (!res.ok || !res.text.trim()) continue;
		try {
			const json = JSON.parse(res.text);
			const list = json?.result?.workspaces;
			if (!Array.isArray(list)) continue;
			for (const w of list) {
				const id = String(w?.workspace_id ?? "");
				const label = String(w?.label ?? "").trim();
				if (id && label) map.set(id, label);
			}
			if (map.size) return map;
		} catch {
			/* try the next command */
		}
	}
	return map;
}

async function herdrPanes() {
	// The launch agent has a minimal PATH, so the known install location is
	// tried first and the bare name second.
	//
	// `herdr agent list` is preferred over `herdr pane list`: it reports the
	// agent status herdr derived from pi's OSC 7501 channel, including the
	// finer states (done, blocked, error) that the plain pane list flattens.
	let res = await runShell(`${HOME}/.local/bin/herdr agent list 2>/dev/null`, 20000);
	let agents = [];
	try {
		const json = JSON.parse(res.text || "");
		if (Array.isArray(json?.result?.agents)) agents = json.result.agents;
	} catch {
		agents = [];
	}
	if (!agents.length) {
		res = await runShell(`${HOME}/.local/bin/herdr pane list 2>/dev/null`, 20000);
		if (!res.ok || !res.text.trim()) {
			res = await runShell("herdr pane list 2>/dev/null", 20000);
		}
		try {
			const json = JSON.parse(res.text || "");
			if (Array.isArray(json?.result?.panes)) agents = json.result.panes;
		} catch {
			return { available: false, panes: new Map() };
		}
	}
	if (!agents.length) return { available: false, panes: new Map() };
	// Workspace id -> display name, so each pane can carry the name the user
	// actually sees on the herdr dial.
	const wsLabels = await herdrWorkspaceLabels();
	const map = new Map();
	for (const p of agents) {
		const value = p?.agent_session?.value;
		if (typeof value !== "string") continue;
		const file = value.split(/[\\/]/).pop() ?? "";
		const stem = file.replace(/\.jsonl$/i, "");
		const u = stem.lastIndexOf("_");
		const id = u >= 0 ? stem.slice(u + 1) : stem;
		if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id)) continue;
		map.set(id, {
			paneId: String(p.pane_id ?? ""),
			status: String(p.agent_status ?? ""),
			// herdr names a workspace (that is what the user sees on the dial),
			// so relay it: without it the phone has to guess the title from the
			// first user message, which is not the same thing at all.
			workspaceId: String(p.workspace_id ?? ""),
			workspace: wsLabels.get(String(p.workspace_id ?? "")) ?? "",
		});
	}
	return { available: true, panes: map };
}

async function handleLine(line) {
	const trimmed = (line || "").trim();
	if (!trimmed) return { ok: true, text: "" };
	const space = trimmed.indexOf(" ");
	const verb = (space < 0 ? trimmed : trimmed.slice(0, space)).toLowerCase();
	const rest = space < 0 ? "" : trimmed.slice(space + 1).trim();
	switch (verb) {
		case "ping":
			return { ok: true, text: "pong", cwd: agent.cwd };
		case "cwd":
			return { ok: true, text: agent.cwd, cwd: agent.cwd };
		case "state": {
			const state = await agent.ensure().send({ type: "get_state" });
			return { ok: true, text: JSON.stringify(state, null, 2), data: state };
		}
		case "stats": {
			const stats = await agent.ensure().send({ type: "get_session_stats" });
			return { ok: true, text: JSON.stringify(stats, null, 2), data: stats };
		}
		case "prompt":
			return agent.prompt(rest);
		case "new": {
			const res = await agent.ensure().send({ type: "new_session" });
			return { ok: true, text: "new session started", data: res };
		}
		case "switch": {
			const sessions = listSessions();
			const target =
				sessions.find((s) => s.path === rest) ||
				sessions.find((s) => s.id === rest) ||
				sessions.find((s) => s.id.startsWith(rest));
			if (!target) return { ok: false, text: `session not found: ${rest}` };
			const res = await agent.ensure().send({ type: "switch_session", sessionPath: target.path });
			return { ok: true, text: `switched to ${target.id}`, data: res };
		}
		case "abort": {
			await agent.ensure().send({ type: "abort" });
			return { ok: true, text: "aborted" };
		}
		case "list": {
			const sessions = listSessions();
			const lines = sessions.map(
				(s) => `${s.id.slice(0, 8)}  ${s.modified}  ${s.messages}m  ${s.cwd}  ${s.name || s.firstUser || ""}`.trimEnd(),
			);
			const herdr = await herdrPanes();
			lines.unshift(herdr.available ? "#herdr 1" : "#herdr 0");
			for (const s of sessions) {
				const info = herdr.panes.get(s.id);
				if (info) {
					// #pane <session> <pane> <status> [workspace label]
					// The label is last and may contain spaces, so it runs to EOL.
					const ws = info.workspace ? ` ${info.workspace}` : "";
					lines.push(`#pane ${s.id} ${info.paneId} ${info.status}${ws}`.trimEnd());
				}
			}
			return { ok: true, text: lines.join("\n") || "(no sessions)", data: sessions };
		}
		case "context": {
			// Use the cached session list (scanning every transcript takes
			// seconds); only re-scan if the session is not in it yet.
			let sessions = listSessions();
			let target =
				sessions.find((s) => s.id === rest) ||
				sessions.find((s) => s.id.startsWith(rest)) ||
				sessions.find((s) => s.path === rest);
			if (!target) {
				sessions = listSessions(true);
				target =
					sessions.find((s) => s.id === rest) ||
					sessions.find((s) => s.id.startsWith(rest)) ||
					sessions.find((s) => s.path === rest);
			}
			if (!target) return { ok: false, text: `session not found: ${rest}` };
			const ctx = readSessionContext(target.path);
			return {
				ok: true,
				text: JSON.stringify(ctx),
				data: { ...ctx, sessionId: target.id, label: target.firstUser, cwd: target.cwd },
			};
		}
		case "herdr": {
			// Pass-through for the pane-level operations the Telegram bridge
			// needs: read what the pane shows, interrupt it, wait for it.
			const parts = rest.split(/\s+/).filter(Boolean);
			const sub = (parts.shift() || "").toLowerCase();
			switch (sub) {
				case "read": {
					const pane = parts.shift();
					if (!pane) return { ok: false, text: "pane required" };
					const lines = parts.shift() || "40";
					const res = await herdr(`agent read ${pane} --lines ${lines} --format text`);
					return { ok: res.ok, text: res.text || "(no output)" };
				}
				case "keys": {
					const pane = parts.shift();
					if (!pane || !parts.length) return { ok: false, text: "pane and keys required" };
					const res = await herdr(`agent send-keys ${pane} ${parts.join(" ")}`);
					return { ok: res.ok, text: res.text || "keys sent" };
				}
				case "wait": {
					const pane = parts.shift();
					const status = parts.shift() || "idle";
					const ms = parts.shift() || "120000";
					if (!pane) return { ok: false, text: "pane required" };
					const res = await herdr(`agent wait ${pane} --until ${status} --timeout ${ms}`);
					return { ok: res.ok, text: res.text || `reached ${status}` };
				}
				case "get": {
					const pane = parts.shift();
					if (!pane) return { ok: false, text: "pane required" };
					const res = await herdr(`agent get ${pane}`);
					return { ok: res.ok, text: res.text };
				}
				default:
					return { ok: false, text: `unknown herdr subcommand: ${sub}` };
			}
		}
		case "exec":
			return runShell(rest);
		case "raw": {
			let record;
			try {
				record = JSON.parse(rest);
			} catch (err) {
				return { ok: false, text: `invalid json: ${err.message}` };
			}
			const data = await agent.ensure().send(record);
			return { ok: true, text: JSON.stringify(data, null, 2), data };
		}
		default:
			return { ok: false, text: `unknown command: ${verb}` };
	}
}

const server = createServer((socket) => {
	let buf = "";
	socket.setEncoding("utf8");
	socket.on("data", async (chunk) => {
		buf += chunk;
		let idx;
		while ((idx = buf.indexOf("\n")) >= 0) {
			const line = buf.slice(0, idx);
			buf = buf.slice(idx + 1);
			let reply;
			try {
				const parsed = JSON.parse(line || "{}");
				reply = await handleLine(parsed.line ?? parsed.cmd ?? "");
			} catch (err) {
				reply = { ok: false, text: String(err.message || err) };
			}
			try {
				socket.write(`${JSON.stringify(reply)}\n`);
			} catch {
				/* client gone */
			}
		}
	});
	socket.on("error", () => {});
});

try {
	mkdirSync(join(SOCKET, ".."), { recursive: true });
	if (existsSync(SOCKET)) unlinkSync(SOCKET);
} catch {
	/* ignore */
}
server.listen(SOCKET, () => {
	console.log(`pisess listening on ${SOCKET} (cwd ${agent.cwd})`);
});
server.on("error", (err) => {
	console.error("socket error", err.message);
	process.exit(1);
});
