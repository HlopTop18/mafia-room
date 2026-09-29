import express from "express";
import { createServer } from "http";
import { WebSocketServer } from "ws";
import crypto from "crypto";

const app = express();
const server = createServer(app);
const wss = new WebSocketServer({ server });

const rooms = new Map();

app.use(express.static("public"));



function generateId() {
  return crypto.randomBytes(3).toString("hex").toUpperCase();
}

wss.on("connection", (ws) => {

  ws.on("message", (raw) => {

    let message;

    try {
      message = JSON.parse(raw);
    } catch {
      return;
    }

    // Приєднання до кімнати
    if (message.type === "join") {

      const roomId = (message.room || "")
        .toUpperCase()
        .slice(0, 6);

      if (!roomId) return;

      if (!rooms.has(roomId)) {
        rooms.set(roomId, new Map());
      }

      const room = rooms.get(roomId);

      // Максимум 12 гравців
      if (room.size >= 12) {
        ws.send(JSON.stringify({
          type: "error",
          message: "У лобі вже 12 учасників."
        }));

        return;
      }

      const playerId = generateId();

      ws.playerId = playerId;
      ws.roomId = roomId;
      ws.name = (message.name || "Гравець").slice(0, 20);

      room.set(playerId, ws);

      ws.send(JSON.stringify({
        type: "joined",
        id: playerId,
        host: room.size === 1,
        players: [...room].map(([id, client]) => ({
          id,
          name: client.name
        }))
      }));

      broadcast(room, {
        type: "player-joined",
        id: playerId,
        name: ws.name
      }, ws);

      return;
    }

    const room = rooms.get(ws.roomId);

    if (!room) return;

    // Зміна нікнейму
    if (message.type === "rename") {

      ws.name = (message.name || ws.name).slice(0, 20);

      broadcast(room, {
        type: "renamed",
        id: ws.playerId,
        name: ws.name
      });

    }

    // WebRTC сигнал
    else if (message.type === "signal") {

      const target = room.get(message.to);

      if (target) {

        target.send(JSON.stringify({
          type: "signal",
          from: ws.playerId,
          data: message.data
        }));

      }

    }

    // Дії ведучого
    else if (message.type === "host-action") {

      broadcast(room, {
        type: "host-action",
        action: message.action,
        target: message.target
      });

    }

  });

  // Гравець вийшов
  ws.on("close", () => {

    const room = rooms.get(ws.roomId);

    if (!room) return;

    room.delete(ws.playerId);

    broadcast(room, {
      type: "player-left",
      id: ws.playerId
    });

    if (room.size === 0) {
      rooms.delete(ws.roomId);
    }

  });

});

function broadcast(room, message, except = null) {

  const data = JSON.stringify(message);

  for (const client of room.values()) {

    if (
      client !== except &&
      client.readyState === 1
    ) {

      client.send(data);

    }

  }

}

const PORT = process.env.PORT || 3000;

server.listen(PORT, () => {
  console.log(`Mafia Room running on port ${PORT}`);
});
