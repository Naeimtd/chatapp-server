const WebSocket = require('ws');
const http = require('http');

const server = http.createServer((req, res) => {
    // CORS headers
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Content-Type', 'application/json');

    if (req.url === '/health') {
        res.writeHead(200);
        res.end(JSON.stringify({
            status: 'running',
            users: users.size,
            uptime: process.uptime()
        }));
    } else if (req.url === '/') {
        res.writeHead(200);
        res.end(JSON.stringify({
            app: 'ChatApp Server',
            status: 'online',
            onlineUsers: Array.from(users.keys())
        }));
    } else {
        res.writeHead(404);
        res.end('Not Found');
    }
});

const wss = new WebSocket.Server({ server });

// Store connected users: username -> { ws, lastSeen }
const users = new Map();

console.log('ChatApp Server Starting...');

wss.on('connection', (ws, req) => {
    let currentUser = null;
    console.log('New connection from:', req.socket.remoteAddress);

    // Send welcome
    ws.send(JSON.stringify({
        type: 'welcome',
        message: 'Connected to ChatApp Server'
    }));

    ws.isAlive = true;

    ws.on('pong', () => {
        ws.isAlive = true;
    });

    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data.toString());

            switch (msg.type) {

                case 'register': {
                    currentUser = msg.username.toLowerCase().trim();

                    // Check if username already connected
                    const existing = users.get(currentUser);
                    if (existing && existing.ws.readyState === WebSocket.OPEN) {
                        // Disconnect old connection
                        existing.ws.close();
                    }

                    users.set(currentUser, {
                        ws: ws,
                        lastSeen: Date.now()
                    });

                    console.log(`✅ User registered: ${currentUser} | Total: ${users.size}`);

                    // Send confirmation
                    ws.send(JSON.stringify({
                        type: 'registered',
                        username: currentUser,
                        success: true
                    }));

                    // Notify ALL other users about new online user
                    broadcast({
                        type: 'user_online',
                        username: currentUser
                    }, currentUser);

                    // Send current online users list to this user
                    const onlineList = [];
                    users.forEach((value, key) => {
                        if (key !== currentUser && value.ws.readyState === WebSocket.OPEN) {
                            onlineList.push(key);
                        }
                    });

                    ws.send(JSON.stringify({
                        type: 'online_users',
                        users: onlineList
                    }));

                    break;
                }

                case 'message': {
                    const receiver = msg.receiver.toLowerCase().trim();
                    const sender = msg.sender.toLowerCase().trim();
                    const timestamp = msg.timestamp || Date.now();

                    console.log(`💬 Message: ${sender} → ${receiver}: ${msg.content.substring(0, 50)}`);

                    // Forward to receiver if online
                    const receiverData = users.get(receiver);
                    if (receiverData && receiverData.ws.readyState === WebSocket.OPEN) {
                        receiverData.ws.send(JSON.stringify({
                            type: 'message',
                            sender: sender,
                            receiver: receiver,
                            content: msg.content,
                            timestamp: timestamp
                        }));
                        console.log(`  ✅ Delivered to ${receiver}`);
                    } else {
                        console.log(`  ⚠️ ${receiver} is offline`);
                        // Notify sender that receiver is offline
                        ws.send(JSON.stringify({
                            type: 'delivery_status',
                            receiver: receiver,
                            status: 'offline',
                            timestamp: timestamp
                        }));
                    }
                    break;
                }

                case 'get_online_users': {
                    const onlineUsers = [];
                    users.forEach((value, key) => {
                        if (key !== currentUser && value.ws.readyState === WebSocket.OPEN) {
                            onlineUsers.push(key);
                        }
                    });

                    ws.send(JSON.stringify({
                        type: 'online_users',
                        users: onlineUsers
                    }));
                    break;
                }

                case 'check_username': {
                    const checkName = msg.username.toLowerCase().trim();
                    const exists = users.has(checkName);
                    ws.send(JSON.stringify({
                        type: 'username_check',
                        username: checkName,
                        exists: exists
                    }));
                    break;
                }

                case 'typing': {
                    const typingTo = msg.receiver.toLowerCase().trim();
                    const typingUser = users.get(typingTo);
                    if (typingUser && typingUser.ws.readyState === WebSocket.OPEN) {
                        typingUser.ws.send(JSON.stringify({
                            type: 'typing',
                            sender: currentUser,
                            isTyping: msg.isTyping
                        }));
                    }
                    break;
                }

                case 'ping': {
                    ws.isAlive = true;
                    if (currentUser) {
                        const userData = users.get(currentUser);
                        if (userData) userData.lastSeen = Date.now();
                    }
                    ws.send(JSON.stringify({ type: 'pong' }));
                    break;
                }

                default:
                    console.log('Unknown message type:', msg.type);
            }
        } catch (e) {
            console.error('Error parsing message:', e.message);
        }
    });

    ws.on('close', (code, reason) => {
        if (currentUser) {
            users.delete(currentUser);
            console.log(`❌ User disconnected: ${currentUser} | Total: ${users.size}`);

            // Notify all users
            broadcast({
                type: 'user_offline',
                username: currentUser
            });
        }
    });

    ws.on('error', (err) => {
        console.error('WebSocket error:', err.message);
    });
});

// Broadcast to all connected users except excluded one
function broadcast(message, excludeUser = null) {
    const msgStr = JSON.stringify(message);
    users.forEach((userData, username) => {
        if (username !== excludeUser && userData.ws.readyState === WebSocket.OPEN) {
            try {
                userData.ws.send(msgStr);
            } catch (e) {
                console.error(`Failed to send to ${username}:`, e.message);
            }
        }
    });
}

// Heartbeat - clean dead connections every 30 seconds
const heartbeatInterval = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) {
            return ws.terminate();
        }
        ws.isAlive = false;
        ws.ping();
    });

    // Also clean users map
    users.forEach((userData, username) => {
        if (userData.ws.readyState !== WebSocket.OPEN) {
            users.delete(username);
            broadcast({
                type: 'user_offline',
                username: username
            });
        }
    });
}, 30000);

wss.on('close', () => {
    clearInterval(heartbeatInterval);
});

// Self-ping to prevent Render free tier sleep (every 14 minutes)
setInterval(() => {
    const url = process.env.RENDER_EXTERNAL_URL || `http://localhost:${PORT}`;
    http.get(`${url}/health`, (res) => {
        console.log(`🔄 Self-ping: ${res.statusCode} | Users: ${users.size}`);
    }).on('error', (err) => {
        console.log('Self-ping error:', err.message);
    });
}, 840000); // 14 minutes

const PORT = process.env.PORT || 10000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 ChatApp Server running on port ${PORT}`);
    console.log(`📡 WebSocket ready for connections`);
});
