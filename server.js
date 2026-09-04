const express = require('express');
const cors = require('cors');
const { ExpressPeerServer } = require('peer');

const app = express();

// Required for host platforms like Render behind reverse proxies
app.enable('trust proxy');
app.use(cors());

const PORT = process.env.PORT || 10000;

// Start HTTP Server
const server = app.listen(PORT, () => {
  console.log(`OmeChat Signaling Server running on port ${PORT}`);
});

// Setup PeerJS Server middleware
const peerServer = ExpressPeerServer(server, {
  debug: true,
  path: '/',
  proxied: true
});

app.use('/peerjs', peerServer);

// Separate matchmaking queues by mode
const waitingQueues = {
  text: [],
  video: []
};

// Clean up when client disconnects from signaling server
peerServer.on('disconnect', (client) => {
  const peerId = client.getId();
  ['text', 'video'].forEach((mode) => {
    waitingQueues[mode] = waitingQueues[mode].filter((id) => id !== peerId);
  });
});

// Matchmaking Endpoint
app.get('/match', (req, res) => {
  const { id, mode } = req.query;

  if (!id || !mode || !waitingQueues[mode]) {
    return res.status(400).json({ error: 'Invalid peer ID or chat mode.' });
  }

  // Remove existing occurrences of the requester
  waitingQueues[mode] = waitingQueues[mode].filter((peerId) => peerId !== id);

  // If someone is waiting in queue, pair them up
  if (waitingQueues[mode].length > 0) {
    const partnerId = waitingQueues[mode].shift();
    return res.json({ partnerId });
  }

  // Otherwise, add current peer to the waiting queue
  waitingQueues[mode].push(id);
  return res.json({ partnerId: null });
});

// Health check endpoint
app.get('/', (req, res) => {
  res.send({ status: 'online', service: 'OmeChat PeerJS Signaling' });
});
