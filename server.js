const express = require("express");
const http = require("http");
const path = require("path");
const { AccessToken } = require("livekit-server-sdk");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);

const PORT = process.env.PORT || 3000;

const LIVEKIT_URL = process.env.LIVEKIT_URL;
const LIVEKIT_API_KEY = process.env.LIVEKIT_API_KEY;
const LIVEKIT_API_SECRET = process.env.LIVEKIT_API_SECRET;

const MAX_PLAYERS = 12;

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const rooms = new Map();

function getRoom(roomName) {
    if (!rooms.has(roomName)) {
        rooms.set(roomName, {
            players: new Map(),
            host: null
        });
    }

    return rooms.get(roomName);
}

/* =========================
   LIVEKIT TOKEN
========================= */

app.post("/api/livekit-token", async (req, res) => {
    try {
        const { roomName, participantName } = req.body;

        if (!roomName || !participantName) {
            return res.status(400).json({
                error: "roomName та participantName обов'язкові"
            });
        }

        if (!LIVEKIT_API_KEY || !LIVEKIT_API_SECRET || !LIVEKIT_URL) {
            return res.status(500).json({
                error: "LiveKit не налаштований на сервері"
            });
        }

        const room = getRoom(roomName);

        if (
            !room.players.has(participantName) &&
            room.players.size >= MAX_PLAYERS
        ) {
            return res.status(403).json({
                error: `У кімнаті може бути максимум ${MAX_PLAYERS} учасників`
            });
        }

        if (!room.players.has(participantName)) {
            room.players.set(participantName, {
                name: participantName,
                joinedAt: Date.now()
            });
        }

        if (!room.host) {
            room.host = participantName;
        }

        const token = new AccessToken(
            LIVEKIT_API_KEY,
            LIVEKIT_API_SECRET,
            {
                identity: participantName,
                name: participantName,
                ttl: "6h"
            }
        );

        token.addGrant({
            roomJoin: true,
            room: roomName,
            canPublish: true,
            canSubscribe: true,
            canPublishData: true
        });

        const jwt = await token.toJwt();

        res.json({
            token: jwt,
            url: LIVEKIT_URL,
            roomName,
            participantName,
            host: room.host === participantName,
            maxPlayers: MAX_PLAYERS
        });

    } catch (error) {
        console.error("LiveKit token error:", error);

        res.status(500).json({
            error: "Не вдалося створити токен"
        });
    }
});

/* =========================
   ROOM INFO
========================= */

app.get("/api/room/:roomName", (req, res) => {
    const roomName = req.params.roomName;
    const room = getRoom(roomName);

    res.json({
        roomName,
        host: room.host,
        players: Array.from(room.players.values()),
        count: room.players.size,
        maxPlayers: MAX_PLAYERS
    });
});

/* =========================
   WEBSOCKET
========================= */

const wss = new WebSocket.Server({
    server,
    path: "/ws"
});

function broadcast(roomName, message, exclude = null) {
    wss.clients.forEach(client => {
        if (
            client.readyState === WebSocket.OPEN &&
            client.roomName === roomName &&
            client !== exclude
        ) {
            client.send(JSON.stringify(message));
        }
    });
}

wss.on("connection", socket => {

    socket.roomName = null;
    socket.playerName = null;

    socket.on("message", rawMessage => {
        try {
            const message = JSON.parse(rawMessage.toString());

            /* JOIN */

            if (message.type === "join") {
                const roomName = String(message.roomName || "").trim();
                const playerName = String(message.playerName || "").trim();

                if (!roomName || !playerName) {
                    return;
                }

                const room = getRoom(roomName);

                if (
                    !room.players.has(playerName) &&
                    room.players.size >= MAX_PLAYERS
                ) {
                    socket.send(JSON.stringify({
                        type: "error",
                        message: `Кімната вже заповнена. Максимум ${MAX_PLAYERS} учасників.`
                    }));

                    return;
                }

                socket.roomName = roomName;
                socket.playerName = playerName;

                if (!room.players.has(playerName)) {
                    room.players.set(playerName, {
                        name: playerName,
                        joinedAt: Date.now()
                    });
                }

                if (!room.host) {
                    room.host = playerName;
                }

                socket.send(JSON.stringify({
                    type: "room-info",
                    roomName,
                    host: room.host,
                    players: Array.from(room.players.values()),
                    maxPlayers: MAX_PLAYERS
                }));

                broadcast(
                    roomName,
                    {
                        type: "player-joined",
                        player: {
                            name: playerName
                        },
                        players: Array.from(room.players.values())
                    },
                    socket
                );
            }

            /* CHAT / DATA */

            if (
                message.type === "chat" ||
                message.type === "game-action" ||
                message.type === "player-action"
            ) {
                if (!socket.roomName) {
                    return;
                }

                broadcast(
                    socket.roomName,
                    {
                        ...message,
                        from: socket.playerName
                    }
                );
            }

            /* HOST */

            if (message.type === "host-action") {
                if (!socket.roomName) {
                    return;
                }

                const room = getRoom(socket.roomName);

                if (room.host !== socket.playerName) {
                    return;
                }

                broadcast(socket.roomName, {
                    type: "host-action",
                    action: message.action,
                    target: message.target || null,
                    from: socket.playerName
                });
            }

        } catch (error) {
            console.error("WebSocket message error:", error);
        }
    });

    socket.on("close", () => {

        if (!socket.roomName || !socket.playerName) {
            return;
        }

        const room = rooms.get(socket.roomName);

        if (!room) {
            return;
        }

        room.players.delete(socket.playerName);

        /* Якщо вийшов ведучий — передаємо роль наступному */

        if (room.host === socket.playerName) {
            const nextPlayer = room.players.keys().next().value;

            room.host = nextPlayer || null;

            if (nextPlayer) {
                broadcast(socket.roomName, {
                    type: "host-changed",
                    host: nextPlayer
                });
            }
        }

        broadcast(socket.roomName, {
            type: "player-left",
            playerName: socket.playerName,
            players: Array.from(room.players.values()),
            host: room.host
        });

        /* Видаляємо порожню кімнату */

        if (room.players.size === 0) {
            rooms.delete(socket.roomName);
        }
    });
});

/* =========================
   FRONTEND FALLBACK
========================= */

app.get("*", (req, res) => {
    res.sendFile(
        path.join(__dirname, "public", "index.html")
    );
});

/* =========================
   START SERVER
========================= */

server.listen(PORT, () => {
    console.log(`Server started on port ${PORT}`);

    console.log(
        `LiveKit URL: ${LIVEKIT_URL || "НЕ ВКАЗАНО"}`
    );

    console.log(
        `Maximum players: ${MAX_PLAYERS}`
    );
});
