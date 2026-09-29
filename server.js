import express from "express";
import { createServer } from "http";
import { WebSocketServer } from "ws";
import crypto from "crypto";

const app = express();
const server = createServer(app);
const wss = new WebSocketServer({ server });

const rooms = new Map();
const RECONNECT_TTL = 15 * 60 * 1000;

app.use(express.static("public"));

app.get("/{*splat}", (req, res) => {
  res.sendFile(process.cwd() + "/public/index.html");
});

function id() {
  return crypto.randomBytes(5).toString("hex").toUpperCase();
}

function send(ws, msg) {
  if (ws?.readyState === 1) {
    ws.send(JSON.stringify(msg));
  }
}

function broadcast(room, msg, exceptId = null) {
  for (const p of room.players.values()) {
    if (p.ws && p.id !== exceptId) {
      send(p.ws, msg);
    }
  }
}

function snapshot(room) {
  return [...room.players.values()].map(p => ({
    id: p.id,
    name: p.name,
    dead: !!p.dead,
    nominated: !!p.nominated,
    muted: !!p.muted,
    connected: !!p.ws
  }));
}

function roomState(room) {
  return {
    type: "room-state",
    room: room.code,
    hostId: room.hostId,
    players: snapshot(room)
  };
}

function getOrCreate(code) {
  let room = rooms.get(code);

  if (!room) {
    room = {
      code,
      hostId: null,
      players: new Map()
    };

    rooms.set(code, room);
  }

  return room;
}

function cleanupRoom(room) {
  if (!room.players.size) {
    rooms.delete(room.code);
  }
}

wss.on("connection", ws => {

  ws.on("message", raw => {

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

      const room = getOrCreate(code);

      const requestedId = String(m.playerId || "");

      let player = requestedId
        ? room.players.get(requestedId)
        : null;

      if (player) {

        if (player.ws && player.ws !== ws) {
          player.ws.close();
        }

        player.ws = ws;
        player.lastSeen = Date.now();

        player.name = String(
          m.name || player.name || "Гравець"
        ).slice(0, 20);

      } else {

        const connected = [
          ...room.players.values()
        ].filter(p => p.ws).length;

        if (connected >= 12) {

          return send(ws, {
            type: "error",
            message: "У лобі вже 12 учасників."
          });

        }

        player = {
          id: id(),
          name: String(
            m.name || "Гравець"
          ).slice(0, 20),
          ws,
          dead: false,
          nominated: false,
          muted: false,
          lastSeen: Date.now()
        };

        room.players.set(player.id, player);

        if (!room.hostId) {
          room.hostId = player.id;
        }
      }

      ws.playerId = player.id;
      ws.roomId = room.code;

      send(ws, {
        type: "joined",
        id: player.id,
        room: room.code,
        hostId: room.hostId
      });

      broadcast(room, roomState(room));

      return;
    }

    const room = rooms.get(ws.roomId);
    const player = room?.players.get(ws.playerId);

    if (!room || !player) return;

    if (m.type === "rename") {

      player.name = String(
        m.name || player.name
      ).slice(0, 20);

      broadcast(room, roomState(room));

    }

    else if (m.type === "host-action") {

      if (room.hostId !== player.id) return;

      const target = room.players.get(
        String(m.target || "")
      );

      if (m.action === "vote" && target) {
        target.nominated = !target.nominated;
      }

      if (m.action === "kill" && target) {
        target.dead = !target.dead;
        target.nominated = false;
      }

      if (m.action === "clear" && target) {
        target.dead = false;
        target.nominated = false;
      }

      if (m.action === "mute" && target) {

        if (target.id !== room.hostId) {
          target.muted = !target.muted;
        }

      }

      if (m.action === "mute-all") {

        const shouldMute = [
          ...room.players.values()
        ].some(
          p => p.id !== room.hostId && !p.muted
        );

        for (const p of room.players.values()) {

          if (p.id !== room.hostId) {
            p.muted = shouldMute;
          }

        }
      }

      if (m.action === "transfer-host" && target) {
        room.hostId = target.id;
      }

      broadcast(room, roomState(room));

      if (
        m.action === "mute" ||
        m.action === "mute-all"
      ) {

        for (const p of room.players.values()) {

          if (p.ws) {

            send(p.ws, {
              type: "force-mute",
              muted: !!p.muted
            });

          }

        }
      }
    }

    else if (m.type === "order") {

      if (room.hostId !== player.id) return;

      const ids = Array.isArray(m.ids)
        ? m.ids
            .map(String)
            .filter(x => room.players.has(x))
            .slice(0, 12)
        : [];

      const map = new Map(
        ids.map((x, i) => [x, i])
      );

      const ordered = [
        ...room.players.values()
      ].sort(
        (a, b) =>
          (map.get(a.id) ?? 999) -
          (map.get(b.id) ?? 999)
      );

      room.players = new Map(
        ordered.map(p => [p.id, p])
      );

      broadcast(room, roomState(room));
    }

    else if (m.type === "signal") {

      const target = room.players.get(
        String(m.to || "")
      );

      if (target?.ws) {

        send(target.ws, {
          type: "signal",
          from: player.id,
          data: m.data
        });

      }
    }

    else if (m.type === "ping") {

      player.lastSeen = Date.now();

      send(ws, {
        type: "pong"
      });

    }

  });

  ws.on("close", () => {

    const room = rooms.get(ws.roomId);

    const p = room?.players.get(ws.playerId);

    if (!room || !p || p.ws !== ws) return;

    p.ws = null;
    p.lastSeen = Date.now();

    broadcast(room, roomState(room));

    setTimeout(() => {

      const current = room.players.get(p.id);

      if (
        current &&
        !current.ws &&
        Date.now() - current.lastSeen >= RECONNECT_TTL
      ) {

        room.players.delete(p.id);

        if (room.hostId === p.id) {

          room.hostId =
            [...room.players.values()][0]?.id || null;

        }

        broadcast(room, roomState(room));

        cleanupRoom(room);
      }

    }, RECONNECT_TTL + 1000);

  });

});

setInterval(() => {

  const now = Date.now();

  for (const room of rooms.values()) {

    for (const p of room.players.values()) {

      if (
        !p.ws &&
        now - p.lastSeen > RECONNECT_TTL
      ) {

        room.players.delete(p.id);

      }

    }

    if (
      room.hostId &&
      !room.players.has(room.hostId)
    ) {

      room.hostId =
        [...room.players.values()][0]?.id || null;

    }

    cleanupRoom(room);

  }

}, 60_000);

const PORT = process.env.PORT || 3000;

server.listen(PORT, () => {
  console.log(
    `Mafia Room running on port ${PORT}`
  );
});
