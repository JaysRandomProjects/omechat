const express = require('express');
const cors = require('cors');
const { ExpressPeerServer } = require('peer');

const app = express();
app.use(cors());

const PORT = process.env.PORT || 10000;

// Start Express server
const server = app.listen(PORT, () => {
  console.log(`OmeChat server running on port ${PORT}`);
});

// Setup PeerJS Server endpoint
const peerServer = ExpressPeerServer(server, {
  debug: true,
  path: '/'
});

app.use('/peerjs', peerServer);

// Matchmaking queues by mode ('text' | 'video')
const waitingQueues = {
  text: [],
  video: []
};

// Remove disconnected peers from queues
peerServer.on('disconnect', (client) => {
  const peerId = client.getId();
  ['text', 'video'].forEach((mode) => {
    waitingQueues[mode] = waitingQueues[mode].filter(id => id !== peerId);
  });
});

// Matchmaking API endpoint
app.get('/match', (req, res) => {
  const { id, mode } = req.query;

  if (!id || !mode || !waitingQueues[mode]) {
    return res.status(400).json({ error: 'Missing or invalid id/mode parameters.' });
  }

  // Remove stale entries of the current client
  waitingQueues[mode] = waitingQueues[mode].filter(peerId => peerId !== id);

  // Match with waiting partner if available
  if (waitingQueues[mode].length > 0) {
    const partnerId = waitingQueues[mode].shift();
    return res.json({ partnerId });
  }

  // Otherwise, add to waiting queue
  waitingQueues[mode].push(id);
  return res.json({ partnerId: null });
});

// Basic healthcheck route
app.get('/', (req, res) => {
  res.send('OmeChat PeerJS Signaling Server is live.');
});
