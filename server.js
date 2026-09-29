import express from "express";
import { createServer } from "http";
import { WebSocketServer } from "ws";
import crypto from "crypto";
import { AccessToken } from "livekit-server-sdk";

const app = express();
const server = createServer(app);
const wss = new WebSocketServer({ server });

const rooms = new Map();
const TTL = 15 * 60 * 1000;
const MAX = 12;

app.use(express.json());
app.use(express.static("public"));

app.get("/api/health", (req, res) => {
  res.json({
    ok: true,
    livekitConfigured: !!(
      process.env.LIVEKIT_URL &&
      process.env.LIVEKIT_API_KEY &&
      process.env.LIVEKIT_API_SECRET
    )
  });
});

app.post("/api/livekit-token", async (req, res) => {
  try {
    const { roomName, identity, name } = req.body || {};

    if (
      !process.env.LIVEKIT_URL ||
      !process.env.LIVEKIT_API_KEY ||
      !process.env.LIVEKIT_API_SECRET
    ) {
      return res
        .status(500)
        .json({ error: "LiveKit is not configured on the server." });
    }

    const room = String(roomName || "")
      .trim()
      .toUpperCase()
      .slice(0, 32);

    const id = String(identity || "")
      .trim()
      .slice(0, 64);

    const displayName = String(name || "Гравець")
      .trim()
      .slice(0, 40);

    if (!room || !id) {
      return res
        .status(400)
        .json({ error: "roomName and identity are required." });
    }

    const token = new AccessToken(
      process.env.LIVEKIT_API_KEY,
      process.env.LIVEKIT_API_SECRET,
      {
        identity: id,
        name: displayName,
        ttl: "6h"
      }
    );

    token.addGrant({
      roomJoin: true,
      room,
      canPublish: true,
      canSubscribe: true,
      canPublishData: true
    });

    res.json({
      serverUrl: process.env.LIVEKIT_URL,
      participantToken: await token.toJwt()
    });
  } catch (e) {
    console.error(e);
    res
      .status(500)
      .json({ error: "Could not create LiveKit token." });
  }
});

const makeId = () =>
  crypto.randomBytes(5).toString("hex").toUpperCase();

const send = (ws, msg) =>
  ws?.readyState === 1 &&
  ws.send(JSON.stringify(msg));

const snapshot = (r) => ({
  type: "room-state",
  room: r.code,
  hostId: r.hostId,
  players: [...r.players.values()].map((p) => ({
    id: p.id,
    name: p.name,
    dead: p.dead,
    nominated: p.nominated,
    selfMuted: p.selfMuted,
    hostMuted: p.hostMuted,
    connected: !!p.ws
  }))
});

const broadcast = (r, msg) =>
  r.players.forEach(
    (p) => p.ws && send(p.ws, msg)
  );

function getRoom(code) {
  if (!rooms.has(code)) {
    rooms.set(code, {
      code,
      hostId: null,
      players: new Map()
    });
  }

  return rooms.get(code);
}

wss.on("connection", (ws) => {
  ws.on("message", (raw) => {
    let m;

    try {
      m = JSON.parse(raw);
    } catch {
      return;
    }

    if (m.type === "join") {
      const code = String(m.room || "")
        .toUpperCase()
        .slice(0, 12);

      if (!code) {
        return send(ws, {
          type: "error",
          message: "Немає коду кімнати."
        });
      }

      const r = getRoom(code);

      const requestedId = String(m.playerId || "");

      let p =
        requestedId &&
        r.players.get(requestedId);

      if (p) {
        if (p.ws && p.ws !== ws) {
          p.ws.close();
        }

        p.ws = ws;
        p.lastSeen = Date.now();

        p.name = String(
          m.name || p.name || "Гравець"
        ).slice(0, 20);
      } else {
        if (
          [...r.players.values()].filter(
            (x) => x.ws
          ).length >= MAX
        ) {
          return send(ws, {
            type: "error",
            message:
              "У кімнаті вже 12 активних гравців."
          });
        }

        p = {
          id: makeId(),
          name: String(
            m.name || "Гравець"
          ).slice(0, 20),
          ws,
          dead: false,
          nominated: false,
          selfMuted: false,
          hostMuted: false,
          lastSeen: Date.now()
        };

        r.players.set(p.id, p);

        if (!r.hostId) {
          r.hostId = p.id;
        }
      }

      ws.playerId = p.id;
      ws.roomId = code;

      send(ws, {
        type: "joined",
        id: p.id,
        room: code,
        hostId: r.hostId
      });

      broadcast(r, snapshot(r));

      return;
    }

    const r = rooms.get(ws.roomId);
    const p = r?.players.get(ws.playerId);

    if (!r || !p) return;

    if (m.type === "rename") {
      p.name = String(
        m.name || p.name
      ).slice(0, 20);

      broadcast(r, snapshot(r));
    }

    else if (m.type === "media-state") {
      if (typeof m.selfMuted === "boolean") {
        p.selfMuted = m.selfMuted;
      }

      broadcast(r, snapshot(r));
    }

    else if (m.type === "host-action") {
      if (r.hostId !== p.id) return;

      const target =
        r.players.get(
          String(m.target || "")
        );

      if (m.action === "vote" && target) {
        target.nominated =
          !target.nominated;
      }

      if (m.action === "kill" && target) {
        target.dead = !target.dead;
        target.nominated = false;
      }

      if (m.action === "clear" && target) {
        target.dead = false;
        target.nominated = false;
      }

      if (
        m.action === "mute" &&
        target &&
        target.id !== r.hostId
      ) {
        target.hostMuted =
          !target.hostMuted;
      }

      if (m.action === "mute-all") {
        const list = [
          ...r.players.values()
        ].filter(
          (x) => x.id !== r.hostId
        );

        const mute = list.some(
          (x) => !x.hostMuted
        );

        list.forEach(
          (x) => (x.hostMuted = mute)
        );
      }

      if (
        m.action === "transfer-host" &&
        target
      ) {
        r.hostId = target.id;
        target.hostMuted = false;
      }

      broadcast(r, snapshot(r));
    }

    else if (
      m.type === "order" &&
      r.hostId === p.id &&
      Array.isArray(m.ids)
    ) {
      const ids = m.ids
        .map(String)
        .filter((x) => r.players.has(x));

      const index = (id) => {
        const i = ids.indexOf(id);
        return i < 0 ? 999 : i;
      };

      r.players = new Map(
        [...r.players.values()]
          .sort(
            (a, b) =>
              index(a.id) - index(b.id)
          )
          .map((x) => [x.id, x])
      );

      broadcast(r, snapshot(r));
    }

    else if (m.type === "ping") {
      p.lastSeen = Date.now();

      send(ws, {
        type: "pong"
      });
    }
  });

  ws.on("close", () => {
    const r = rooms.get(ws.roomId);
    const p = r?.players.get(ws.playerId);

    if (!r || !p || p.ws !== ws) return;

    p.ws = null;
    p.lastSeen = Date.now();

    broadcast(r, snapshot(r));

    setTimeout(() => {
      const x = r.players.get(p.id);

      if (
        x &&
        !x.ws &&
        Date.now() - x.lastSeen >= TTL
      ) {
        r.players.delete(p.id);

        if (r.hostId === p.id) {
          r.hostId =
            [...r.players.values()][0]?.id ||
            null;
        }

        broadcast(r, snapshot(r));

        if (!r.players.size) {
          rooms.delete(r.code);
        }
      }
    }, TTL + 1000);
  });
});

setInterval(() => {
  const now = Date.now();

  for (const r of rooms.values()) {
    for (const p of r.players.values()) {
      if (
        !p.ws &&
        now - p.lastSeen > TTL
      ) {
        r.players.delete(p.id);
      }
    }

    if (
      r.hostId &&
      !r.players.has(r.hostId)
    ) {
      r.hostId =
        [...r.players.values()][0]?.id ||
        null;
    }

    if (!r.players.size) {
      rooms.delete(r.code);
    }
  }
}, 60000);

server.listen(
  process.env.PORT || 3000,
  () => {
    console.log(
      "Mafia Room 3.0 + LiveKit running"
    );
  }
);
