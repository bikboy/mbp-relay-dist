// Slack transport for the mbp-relay.
//
// Both sides of the relay are pure clients of Slack's API - nothing on our
// machines ever has to accept an inbound connection:
//
//   command : chat.postMessage -> "MBP-CMD <json>"  in the relay channel
//   answer  : chat.postMessage -> "MBP-OUT <json>"  in the relay channel
//
// The fast path is Socket Mode: an outbound WebSocket to wss-primary.slack.com
// that delivers message events in ~200-500 ms. Because Slack may deliver a
// payload to *any* open connection of the app ("each payload may be sent to
// any of the connections"), conversations.history polling stays enabled as
// the recovery path - an event that landed on the wrong connection is picked
// up within a few seconds.
//
// Config (file ~/.mbp-relay/slack.env or the environment):
//   SLACK_BOT_TOKEN=xoxb-...      bot token (chat:write, channels/groups:history)
//   SLACK_APP_TOKEN=xapp-...      app-level token (connections:write) for Socket Mode
//   SLACK_CHANNEL=#mbp-relay      channel name or ID

import fs from "node:fs";

const API = "https://slack.com/api";
const CMD_PREFIX = "MBP-CMD ";
const OUT_PREFIX = "MBP-OUT ";
const OUT_PART_PREFIX = "MBP-OUT2 ";

/** Load slack.env (KEY=VALUE lines) next to the other relay config. */
export function slackConfig() {
  const out = {};
  for (const f of [process.env.SLACK_ENV_FILE, `${process.env.HOME}/.mbp-relay/slack.env`]) {
    if (!f) continue;
    let txt;
    try {
      txt = fs.readFileSync(f, "utf8");
    } catch {
      continue;
    }
    for (const line of txt.split("\n")) {
      const m = line.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$/);
      if (m) out[m[1]] = m[2].replace(/^["']|["']$/g, "");
    }
  }
  for (const k of ["SLACK_BOT_TOKEN", "SLACK_APP_TOKEN", "SLACK_CHANNEL"]) {
    if (process.env[k]) out[k] = process.env[k];
  }
  if (!out.SLACK_BOT_TOKEN) return null;
  out.SLACK_CHANNEL = out.SLACK_CHANNEL || "#mbp-relay";
  return out;
}

/** POST a Slack Web API method. Throws on `ok: false`. */
export async function slackApi(method, params = {}, token = null) {
  const cfg = token ? { SLACK_BOT_TOKEN: token } : slackConfig();
  const tok = token || cfg?.SLACK_BOT_TOKEN;
  if (!tok) throw new Error("slack: no bot token");
  const res = await fetch(`${API}/${method}`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${tok}`,
      "Content-Type": "application/json; charset=utf-8",
      // Ask for identity encoding: some intermediaries return a Brotli body
      // without the content-encoding header, which makes res.json() fail.
      "accept-encoding": "identity",
    },
    body: JSON.stringify(params),
    signal: AbortSignal.timeout(20000),
  });
  const j = await res.json().catch(() => ({}));
  if (!j.ok) throw new Error(`slack ${method}: ${j.error || res.status}`);
  return j;
}

/** Our own user id (so we can ignore the bot's own messages). */
export async function slackBotUserId() {
  const j = await slackApi("auth.test");
  return j.user_id;
}

/**
 * Resolve the relay target to a channel ID.
 *
 *   "#mbp-relay"  -> the private/public channel with that name
 *   "C0123..."     -> used as-is
 *   "U0123..."     -> the direct message with that user (no channel setup,
 *                     no invite needed - the bot may open the DM itself)
 */
export async function slackChannelId(name = null) {
  const cfg = slackConfig();
  const want = name || cfg?.SLACK_CHANNEL;
  if (!want) throw new Error("slack: no channel configured");
  if (want.startsWith("C") || want.startsWith("D") || want.startsWith("G")) return want;
  if (want.startsWith("U")) {
    // Direct message: find the IM channel with that user.
    let cursor = "";
    for (;;) {
      const j = await slackApi("conversations.list", {
        types: "im",
        limit: 200,
        cursor: cursor || undefined,
      });
      const im = (j.channels || []).find((c) => c.user === want || c.id === want);
      if (im) return im.id;
      cursor = j.response_metadata?.next_cursor;
      if (!cursor) break;
    }
    return want; // chat.postMessage resolves a user id to the DM itself
  }
  let cursor = "";
  for (;;) {
    const j = await slackApi("conversations.list", {
      types: "public_channel,private_channel",
      limit: 200,
      cursor: cursor || undefined,
    });
    const ch = (j.channels || []).find((c) => c.name === String(want).replace(/^#/, ""));
    if (ch) return ch.id;
    cursor = j.response_metadata?.next_cursor;
    if (!cursor) break;
  }
  throw new Error(`slack: channel ${want} not found (is the bot a member?)`);
}

/** Post one text into the relay channel. */
export async function slackPost(text, channel = null) {
  const cfg = slackConfig();
  const ch = channel || (await slackChannelId(cfg?.SLACK_CHANNEL));
  await slackApi("chat.postMessage", { channel: ch, text, unfurl_links: false, unfurl_media: false });
  return ch;
}

/** Post an answer, chunked if it is too big for one Slack message. */
export async function slackPostAnswer(obj) {
  const json = JSON.stringify(obj);
  if (json.length <= 30000) return await slackPost(OUT_PREFIX + json);
  const parts = [];
  for (let i = 0; i < json.length; i += 30000) parts.push(json.slice(i, i + 30000));
  for (let i = 0; i < parts.length; i++) {
    await slackPost(`${OUT_PART_PREFIX}${i + 1}/${parts.length} ${parts[i]}`);
  }
}

/**
 * Read the channel history (newest last) as [{ts, text}].
 * Used as the recovery path when a Socket Mode event went to the other
 * connection of the app.
 */
export async function slackHistory(channel = null, oldest = null) {
  const cfg = slackConfig();
  const ch = channel || (await slackChannelId(cfg?.SLACK_CHANNEL));
  const j = await slackApi("conversations.history", {
    channel: ch,
    limit: 100,
    oldest: oldest || undefined,
  });
  return (j.messages || [])
    // No bot_id filter: the relay uses a single bot for both directions, so
    // every message is bot-authored. Each side discriminates by text prefix
    // (client parses OUT, remote parses CMD), ignoring its own posts.
    .filter((m) => m && m.type === "message" && typeof m.text === "string")
    .map((m) => ({ ts: m.ts, text: m.text }));
}

/**
 * Open a Socket Mode connection and dispatch message events.
 *
 * The returned function closes the connection. Reconnects are automatic:
 * Slack sends a `disconnect` envelope before dropping the socket, and the
 * wss URL expires after ~30 s of use - so we re-open it in a loop.
 */
export function socketMode({ onMessage, onStatus }) {
  let ws = null;
  let closed = false;

  async function connect() {
    while (!closed) {
      try {
        const cfg = slackConfig();
        if (!cfg?.SLACK_APP_TOKEN) throw new Error("slack: no app-level token (SLACK_APP_TOKEN)");
        const open = await slackApi("apps.connections.open", {}, cfg.SLACK_APP_TOKEN);
        if (!open.url) throw new Error("slack: apps.connections.open returned no url");
        await slackBotUserId().catch(() => null); // sanity: token validity
        const sock = new WebSocket(open.url);
        ws = sock;
        sock.onopen = () => onStatus?.("socket open");
        sock.onmessage = async (ev) => {
          let msg;
          try {
            msg = JSON.parse(String(ev.data));
          } catch {
            return;
          }
          if (msg.type === "hello") onStatus?.("hello");
          else if (msg.type === "disconnect") {
            onStatus?.(`disconnect: ${msg.reason}`);
            try {
              sock.close();
            } catch {
              /* ignore */
            }
          } else if (msg.type === "events_api") {
            const e = msg.payload?.event;
            if (!e || e.type !== "message") return;
            // Direction is encoded in the text prefix (CMD vs OUT); each
            // side only parses what it consumes, so its own posts are ignored.
            if (typeof e.text !== "string") return;
            await onMessage({ ts: e.ts, text: e.text, channel: e.channel });
          }
        };
        sock.onerror = () => onStatus?.("socket error");
        sock.onclose = () => {
          if (!closed) onStatus?.("socket closed; reconnecting");
        };
        await new Promise((resolve) => {
          sock.onclose = resolve;
          sock.onerror = resolve;
        });
      } catch (e) {
        onStatus?.(`socket retry: ${e.message}`);
      }
      if (!closed) await new Promise((r) => setTimeout(r, 3000));
    }
  }

  const done = connect();
  return {
    close() {
      closed = true;
      try {
        ws?.close();
      } catch {
        /* ignore */
      }
    },
    done,
  };
}

/** Parse an answer out of a channel message (handles the chunked form). */
export function parseAnswer(text, parts = new Map()) {
  if (text.startsWith(OUT_PREFIX)) {
    try {
      return JSON.parse(text.slice(OUT_PREFIX.length));
    } catch {
      return null;
    }
  }
  if (text.startsWith(OUT_PART_PREFIX)) {
    const m = text.slice(OUT_PART_PREFIX.length).match(/^(\d+)\/(\d+)\s+([\s\S]*)$/);
    if (!m) return null;
    parts.set(m[1], m[3]);
    if (parts.size < Number(m[2])) return null;
    let joined = "";
    for (let i = 1; i <= Number(m[2]); i++) joined += parts.get(String(i)) || "";
    try {
      return JSON.parse(joined);
    } catch {
      return null;
    }
  }
  return null;
}

/** Parse a command out of a channel message. */
export function parseCommand(text) {
  if (!text.startsWith(CMD_PREFIX)) return null;
  try {
    return JSON.parse(text.slice(CMD_PREFIX.length));
  } catch {
    return null;
  }
}

export const PREFIXES = { CMD_PREFIX, OUT_PREFIX, OUT_PART_PREFIX };
