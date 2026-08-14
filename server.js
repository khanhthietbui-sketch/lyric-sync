const express = require('express');
const cors = require('cors');
const path = require('path');
const app = express();

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

let current = {
  song: '',
  artist: '',
  cover: '',
  lyric: '',
  elapsed: 0,
  duration: 0,
  syncing: false,
  updatedAt: Date.now()
};

let clients = [];

function broadcast(data) {
  const msg = `data: ${JSON.stringify(data)}\n\n`;
  clients.forEach(c => c.res.write(msg));
}

app.get('/stream', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Access-Control-Allow-Origin': '*'
  });
  res.write(`data: ${JSON.stringify(current)}\n\n`);
  const client = { id: Date.now(), res };
  clients.push(client);
  req.on('close', () => {
    clients = clients.filter(c => c.id !== client.id);
  });
});

app.post('/sync', (req, res) => {
  const { song, artist, cover, lyric, elapsed, duration, syncing } = req.body;
  if (song !== undefined) current.song = song;
  if (artist !== undefined) current.artist = artist;
  if (cover !== undefined) current.cover = cover;
  if (lyric !== undefined) current.lyric = lyric;
  if (elapsed !== undefined) current.elapsed = elapsed;
  if (duration !== undefined) current.duration = duration;
  if (syncing !== undefined) current.syncing = syncing;
  current.updatedAt = Date.now();
  broadcast(current);
  res.json({ ok: true });
});

app.get('/status', (req, res) => {
  res.json(current);
});

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Lyric sync running on port ${PORT}`));
