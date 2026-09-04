const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const xss = require('xss');

const app = express();
const server = http.createServer(app);

app.set('trust proxy', 1);

const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] }
});

app.use(helmet({ contentSecurityPolicy: false, crossOriginEmbedderPolicy: false }));

app.use(
  rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 300,
    message: 'Too many requests from this IP.'
  })
);

const queues = { text: new Set(), video: new Set() };
const activePairs = new Map();
const userModes = new Map();
const rateLimitTracker = new Map();

io.on('connection', (socket) => {

  const isRateLimited = () => {
    const now = Date.now();
    const timestamps = rateLimitTracker.get(socket.id) || [];
    const recent = timestamps.filter((t) => now - t < 3000);
    recent.push(now);
    rateLimitTracker.set(socket.id, recent);
    return recent.length > 5;
  };

  const leaveQueue = () => {
    queues.text.delete(socket.id);
    queues.video.delete(socket.id);
  };

  const disconnectPartner = () => {
    const partnerId = activePairs.get(socket.id);
    if (partnerId) {
      activePairs.delete(socket.id);
      activePairs.delete(partnerId);
      io.to(partnerId).emit('peer_disconnected');
    }
  };

  socket.on('join_queue', ({ mode }) => {
    if (mode !== 'text' && mode !== 'video') return;
    disconnectPartner();
    leaveQueue();

    userModes.set(socket.id, mode);
    const queue = queues[mode];

    if (queue.size > 0) {
      const partnerId = queue.values().next().value;
      queue.delete(partnerId);

      const partnerSocket = io.sockets.sockets.get(partnerId);
      if (!partnerSocket) {
        socket.emit('matchmaking_status', 'searching');
        queue.add(socket.id);
        return;
      }

      activePairs.set(socket.id, partnerId);
      activePairs.set(partnerId, socket.id);

      socket.emit('matched', { partnerId, initiator: true, mode });
      partnerSocket.emit('matched', { partnerId: socket.id, initiator: false, mode });
    } else {
      queue.add(socket.id);
      socket.emit('matchmaking_status', 'searching');
    }
  });

  socket.on('next_stranger', () => {
    disconnectPartner();
    leaveQueue();
    socket.emit('matchmaking_status', 'idle');
  });

  socket.on('send_message', (text) => {
    if (isRateLimited()) return socket.emit('error_message', 'Sending messages too fast.');
    const partnerId = activePairs.get(socket.id);
    if (!partnerId) return;

    const sanitizedText = xss(text.trim());
    if (!sanitizedText) return;

    io.to(partnerId).emit('receive_message', {
      sender: 'stranger',
      text: sanitizedText,
      timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
    });
  });

  socket.on('webrtc_offer', ({ offer }) => {
    const partnerId = activePairs.get(socket.id);
    if (partnerId) io.to(partnerId).emit('webrtc_offer', { offer });
  });

  socket.on('webrtc_answer', ({ answer }) => {
    const partnerId = activePairs.get(socket.id);
    if (partnerId) io.to(partnerId).emit('webrtc_answer', { answer });
  });

  socket.on('webrtc_ice_candidate', ({ candidate }) => {
    const partnerId = activePairs.get(socket.id);
    if (partnerId) io.to(partnerId).emit('webrtc_ice_candidate', { candidate });
  });

  socket.on('report_partner', () => {
    const partnerId = activePairs.get(socket.id);
    if (partnerId) {
      disconnectPartner();
      socket.emit('partner_reported');
    }
  });

  socket.on('disconnect', () => {
    disconnectPartner();
    leaveQueue();
    userModes.delete(socket.id);
    rateLimitTracker.delete(socket.id);
  });
});

app.get('/', (req, res) => {
  res.send(`
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>OmeChat — Talk to someone new</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://unpkg.com/react@18/umd/react.production.min.js"></script>
  <script src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>
  <script src="https://unpkg.com/@babel/standalone/babel.min.js"></script>
  <script src="/socket.io/socket.io.js"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    body { font-family: 'Inter', sans-serif; }
    ::-webkit-scrollbar { width: 6px; }
    ::-webkit-scrollbar-track { background: #121318; }
    ::-webkit-scrollbar-thumb { background: #2A2D3A; border-radius: 4px; }
  </style>
</head>
<body class="bg-[#0B0C10] text-gray-100 antialiased min-h-screen">
  <div id="root"></div>

  <script type="text/babel">
    const { useState, useEffect, useRef } = React;

    const LOGO_IMAGE = "https://i.imgur.com/7b5dK2M.png"; // Replace with your uploaded image URL or relative path (/logo.png)

    function App() {
      const [view, setView] = useState('landing');
      const [chatMode, setChatMode] = useState('text');
      const [status, setStatus] = useState('idle');
      const [messages, setMessages] = useState([]);
      const [inputMsg, setInputMsg] = useState('');
      const [micActive, setMicActive] = useState(true);
      const [camActive, setCamActive] = useState(true);
      const [systemAlert, setSystemAlert] = useState('');

      const socketRef = useRef(null);
      const peerConnectionRef = useRef(null);
      const localStreamRef = useRef(null);
      const localVideoRef = useRef(null);
      const remoteVideoRef = useRef(null);
      const messagesEndRef = useRef(null);

      useEffect(() => {
        socketRef.current = io();

        socketRef.current.on('matchmaking_status', (st) => setStatus(st));

        socketRef.current.on('matched', async ({ initiator, mode }) => {
          setStatus('connected');
          setMessages([]);
          setSystemAlert('');

          if (mode === 'video') {
            await setupWebRTC(initiator);
          }
        });

        socketRef.current.on('receive_message', (msgData) => {
          setMessages((prev) => [...prev, msgData]);
        });

        socketRef.current.on('peer_disconnected', () => {
          cleanupWebRTC();
          setStatus('disconnected');
        });

        socketRef.current.on('webrtc_offer', async ({ offer }) => {
          if (!peerConnectionRef.current) return;
          try {
            await peerConnectionRef.current.setRemoteDescription(new RTCSessionDescription(offer));
            const answer = await peerConnectionRef.current.createAnswer();
            await peerConnectionRef.current.setLocalDescription(answer);
            socketRef.current.emit('webrtc_answer', { answer });
          } catch (e) {
            console.error('WebRTC Offer Error:', e);
          }
        });

        socketRef.current.on('webrtc_answer', async ({ answer }) => {
          if (peerConnectionRef.current) {
            try {
              await peerConnectionRef.current.setRemoteDescription(new RTCSessionDescription(answer));
            } catch (e) {
              console.error('WebRTC Answer Error:', e);
            }
          }
        });

        socketRef.current.on('webrtc_ice_candidate', async ({ candidate }) => {
          if (peerConnectionRef.current && candidate) {
            try {
              await peerConnectionRef.current.addIceCandidate(new RTCIceCandidate(candidate));
            } catch (e) {
              console.error('ICE Candidate Error:', e);
            }
          }
        });

        socketRef.current.on('error_message', (msg) => {
          setSystemAlert(msg);
          setTimeout(() => setSystemAlert(''), 4000);
        });

        socketRef.current.on('partner_reported', () => {
          setSystemAlert('Partner was reported. Finding someone new...');
          handleNext();
        });

        return () => {
          cleanupWebRTC();
          if (socketRef.current) socketRef.current.disconnect();
        };
      }, []);

      useEffect(() => {
        messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
      }, [messages]);

      const fetchIceServers = async () => {
        try {
          const response = await fetch("https://docuscout.metered.live/api/v1/turn/credentials?apiKey=4538db28cfe97fd54d044680145600e8374752ec");
          const iceServers = await response.json();
          return { iceServers };
        } catch (e) {
          console.error("Failed to fetch TURN servers, using fallback", e);
          return {
            iceServers: [
              { urls: 'stun:stun.l.google.com:19302' },
              { urls: 'stun:stun1.l.google.com:19302' },
              {
                urls: 'turn:openrelay.metered.ca:80',
                username: 'openrelay',
                credential: 'openrelay'
              },
              {
                urls: 'turn:openrelay.metered.ca:443',
                username: 'openrelay',
                credential: 'openrelay'
              }
            ]
          };
        }
      };

      const setupWebRTC = async (isInitiator) => {
        try {
          if (!localStreamRef.current) {
            const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
            localStreamRef.current = stream;
            if (localVideoRef.current) localVideoRef.current.srcObject = stream;
          }

          const rtcConfig = await fetchIceServers();
          const pc = new RTCPeerConnection(rtcConfig);
          peerConnectionRef.current = pc;

          localStreamRef.current.getTracks().forEach(track => {
            pc.addTrack(track, localStreamRef.current);
          });

          pc.ontrack = (event) => {
            if (remoteVideoRef.current && event.streams[0]) {
              remoteVideoRef.current.srcObject = event.streams[0];
            }
          };

          pc.onicecandidate = (event) => {
            if (event.candidate) {
              socketRef.current.emit('webrtc_ice_candidate', { candidate: event.candidate });
            }
          };

          if (isInitiator) {
            const offer = await pc.createOffer();
            await pc.setLocalDescription(offer);
            socketRef.current.emit('webrtc_offer', { offer });
          }
        } catch (err) {
          setSystemAlert("Camera/Mic permission required for video chat.");
        }
      };

      const cleanupWebRTC = () => {
        if (peerConnectionRef.current) {
          peerConnectionRef.current.close();
          peerConnectionRef.current = null;
        }
        if (remoteVideoRef.current) {
          remoteVideoRef.current.srcObject = null;
        }
      };

      const stopMediaStream = () => {
        if (localStreamRef.current) {
          localStreamRef.current.getTracks().forEach(track => track.stop());
          localStreamRef.current = null;
        }
      };

      const startChatting = async (mode) => {
        setChatMode(mode);
        setView('chat');
        setMessages([]);

        if (mode === 'video') {
          try {
            const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
            localStreamRef.current = stream;
            if (localVideoRef.current) localVideoRef.current.srcObject = stream;
          } catch(e) {
            setSystemAlert("Camera and Microphone access are required.");
          }
        }

        socketRef.current.emit('join_queue', { mode });
      };

      const handleNext = () => {
        cleanupWebRTC();
        setMessages([]);
        socketRef.current.emit('next_stranger');
        socketRef.current.emit('join_queue', { mode: chatMode });
      };

      const handleStop = () => {
        cleanupWebRTC();
        stopMediaStream();
        socketRef.current.emit('next_stranger');
        setStatus('idle');
        setView('landing');
      };

      const sendMessage = (e) => {
        e.preventDefault();
        if (!inputMsg.trim() || status !== 'connected') return;

        const time = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        setMessages((prev) => [...prev, { sender: 'you', text: inputMsg, timestamp: time }]);
        socketRef.current.emit('send_message', inputMsg);
        setInputMsg('');
      };

      const toggleMic = () => {
        if (localStreamRef.current) {
          const track = localStreamRef.current.getAudioTracks()[0];
          if (track) {
            track.enabled = !track.enabled;
            setMicActive(track.enabled);
          }
        }
      };

      const toggleCam = () => {
        if (localStreamRef.current) {
          const track = localStreamRef.current.getVideoTracks()[0];
          if (track) {
            track.enabled = !track.enabled;
            setCamActive(track.enabled);
          }
        }
      };

      return (
        <div className="flex flex-col min-h-screen bg-[#0B0C10] text-gray-100">
          <header className="border-b border-gray-800 bg-[#121318]/90 backdrop-blur sticky top-0 z-50">
            <div className="max-w-7xl mx-auto px-4 h-16 flex items-center justify-between">
              <div onClick={handleStop} className="flex items-center gap-3 cursor-pointer">
                <img 
                  src={LOGO_IMAGE} 
                  alt="OmeChat Logo" 
                  className="h-12 w-auto object-contain py-1"
                />
              </div>
              <nav className="flex items-center gap-4 text-sm font-medium">
                <button onClick={() => { handleStop(); setView('landing'); }} className="text-gray-400 hover:text-white">Home</button>
                <button onClick={() => setView('guidelines')} className="text-gray-400 hover:text-white">Safety</button>
              </nav>
            </div>
          </header>

          <main className="flex-1 flex flex-col">
            {systemAlert && (
              <div className="bg-amber-500/10 border-b border-amber-500/20 text-amber-300 px-4 py-2 text-xs text-center font-medium">
                ⚠️ {systemAlert}
              </div>
            )}

            {view === 'landing' && (
              <div className="flex-1 flex flex-col justify-center items-center px-4 py-12 max-w-4xl mx-auto text-center">
                <div className="inline-block px-3 py-1 rounded-full bg-indigo-500/10 border border-indigo-500/20 text-indigo-400 text-xs font-semibold mb-6">
                  🌍 Global WebRTC & Instant Matching
                </div>
                <h1 className="text-5xl font-extrabold text-white mb-4 tracking-tight">Talk to someone new.</h1>
                <p className="text-gray-400 text-lg mb-10 max-w-xl">Meet random strangers globally over real-time text or video streams.</p>
                
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6 w-full max-w-lg mb-12">
                  <div onClick={() => startChatting('text')} className="bg-[#121318] border border-gray-800 hover:border-indigo-500 p-6 rounded-2xl cursor-pointer text-left transition hover:-translate-y-1">
                    <div className="text-3xl mb-3">💬</div>
                    <h3 className="text-xl font-bold text-white mb-1">Text Chat</h3>
                    <p className="text-xs text-gray-400">Pure text-based anonymous conversations.</p>
                    <button className="mt-6 w-full py-2 rounded-xl bg-indigo-600 text-white font-semibold text-xs">Start Texting</button>
                  </div>

                  <div onClick={() => startChatting('video')} className="bg-[#121318] border border-gray-800 hover:border-cyan-500 p-6 rounded-2xl cursor-pointer text-left transition hover:-translate-y-1">
                    <div className="text-3xl mb-3">📹</div>
                    <h3 className="text-xl font-bold text-white mb-1">Video Chat</h3>
                    <p className="text-xs text-gray-400">P2P video streaming across networks.</p>
                    <button className="mt-6 w-full py-2 rounded-xl bg-gradient-to-r from-indigo-600 to-cyan-600 text-white font-semibold text-xs">Start Video</button>
                  </div>
                </div>
              </div>
            )}

            {view === 'chat' && (
              <div className="flex-1 flex flex-col md:flex-row max-w-7xl w-full mx-auto p-4 gap-4 h-[calc(100vh-4rem)]">
                {chatMode === 'video' && (
                  <div className="flex-1 bg-[#121318] border border-gray-800 rounded-2xl relative overflow-hidden flex items-center justify-center min-h-[250px]">
                    <video ref={remoteVideoRef} autoPlay playsInline className="w-full h-full object-cover" />
                    
                    {status !== 'connected' && (
                      <div className="absolute inset-0 bg-[#121318]/90 flex flex-col items-center justify-center p-6 text-center">
                        <div className="w-10 h-10 border-2 border-indigo-500 border-t-transparent rounded-full animate-spin mb-4"></div>
                        <p className="text-sm text-gray-300 font-medium">
                          {status === 'searching' ? 'Finding an available stranger...' : 'Stranger Disconnected.'}
                        </p>
                      </div>
                    )}

                    <div className="absolute bottom-4 right-4 w-36 h-28 bg-gray-900 border border-gray-700 rounded-xl overflow-hidden shadow-2xl">
                      <video ref={localVideoRef} autoPlay muted playsInline className="w-full h-full object-cover transform -scale-x-100" />
                    </div>

                    <div className="absolute bottom-4 left-4 flex gap-2 bg-[#0B0C10]/80 backdrop-blur p-2 rounded-xl border border-gray-800">
                      <button onClick={toggleMic} className={`p-2 rounded-lg text-xs font-semibold ${micActive ? 'bg-gray-800 text-gray-200' : 'bg-red-500/20 text-red-400'}`}>
                        {micActive ? '🎙️ Mic On' : '🎙️ Mic Off'}
                      </button>
                      <button onClick={toggleCam} className={`p-2 rounded-lg text-xs font-semibold ${camActive ? 'bg-gray-800 text-gray-200' : 'bg-red-500/20 text-red-400'}`}>
                        {camActive ? '📹 Cam On' : '📹 Cam Off'}
                      </button>
                    </div>
                  </div>
                )}

                <div className={`flex-1 flex flex-col bg-[#121318] border border-gray-800 rounded-2xl overflow-hidden ${chatMode === 'video' ? 'md:max-w-md' : 'w-full'}`}>
                  <div className="p-3 border-b border-gray-800 bg-[#161820] flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <div className={`w-2.5 h-2.5 rounded-full ${status === 'connected' ? 'bg-emerald-500 animate-pulse' : status === 'searching' ? 'bg-amber-500 animate-ping' : 'bg-red-500'}`}></div>
                      <span className="text-xs font-semibold text-gray-300">
                        {status === 'connected' ? 'Connected to Stranger' : status === 'searching' ? 'Finding someone...' : 'Disconnected'}
                      </span>
                    </div>
                    {status === 'connected' && (
                      <button onClick={() => socketRef.current.emit('report_partner')} className="text-xs px-2 py-1 rounded bg-red-500/10 text-red-400 border border-red-500/20">
                        🚩 Report
                      </button>
                    )}
                  </div>

                  <div className="flex-1 p-4 overflow-y-auto space-y-3">
                    <div className="text-center text-xs text-gray-500 my-2">🔒 Anonymous & Encrypted Messaging</div>
                    {messages.map((msg, i) => (
                      <div key={i} className={`flex flex-col ${msg.sender === 'you' ? 'items-end' : 'items-start'}`}>
                        <div className={`max-w-[80%] px-4 py-2 rounded-xl text-sm ${msg.sender === 'you' ? 'bg-indigo-600 text-white' : 'bg-[#1E202B] text-gray-200 border border-gray-800'}`}>
                          {msg.text}
                        </div>
                        <span className="text-[10px] text-gray-500 mt-1 px-1">{msg.timestamp}</span>
                      </div>
                    ))}
                    <div ref={messagesEndRef} />
                  </div>

                  <div className="p-3 border-t border-gray-800 bg-[#161820] flex flex-col gap-2">
                    <form onSubmit={sendMessage} className="flex gap-2">
                      <input
                        type="text"
                        placeholder={status === 'connected' ? "Type a message..." : "Waiting for partner..."}
                        value={inputMsg}
                        disabled={status !== 'connected'}
                        onChange={(e) => setInputMsg(e.target.value)}
                        className="flex-1 bg-[#0B0C10] border border-gray-800 rounded-xl px-4 py-2 text-sm text-gray-200 focus:outline-none focus:border-indigo-500 disabled:opacity-50"
                      />
                      <button type="submit" disabled={status !== 'connected' || !inputMsg.trim()} className="bg-indigo-600 hover:bg-indigo-500 text-white text-sm px-4 py-2 rounded-xl disabled:opacity-40">
                        Send
                      </button>
                    </form>
                    <div className="flex gap-2">
                      <button onClick={handleStop} className="flex-1 py-2 rounded-xl bg-gray-800 text-gray-300 font-semibold text-xs">Stop</button>
                      <button onClick={handleNext} className="flex-[2] py-2 rounded-xl bg-gradient-to-r from-indigo-600 to-cyan-600 text-white font-semibold text-xs">Next Stranger ➔</button>
                    </div>
                  </div>
                </div>
              </div>
            )}

            {view === 'guidelines' && (
              <div className="flex-1 max-w-2xl mx-auto px-4 py-10 text-sm text-gray-300 space-y-4">
                <h1 className="text-2xl font-bold text-white mb-4">Safety & Guidelines</h1>
                <div className="p-4 bg-[#121318] border border-gray-800 rounded-xl">1. Must be 18 years or older.</div>
                <div className="p-4 bg-[#121318] border border-gray-800 rounded-xl">2. Explicit or offensive behavior results in instant ban.</div>
                <div className="p-4 bg-[#121318] border border-gray-800 rounded-xl">3. Never share private details with strangers.</div>
              </div>
            )}
          </main>
        </div>
      );
    }

    ReactDOM.createRoot(document.getElementById('root')).render(<App />);
  </script>
</body>
</html>
  `);
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
