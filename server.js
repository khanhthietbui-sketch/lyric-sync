const express = require('express');
const cors = require('cors');
const path = require('path');
const fetch = require('node-fetch');
const { McpServer } = require('@modelcontextprotocol/sdk/server/mcp.js');
const { StreamableHTTPServerTransport } = require('@modelcontextprotocol/sdk/server/streamableHttp.js');
const { z } = require('zod');
const crypto = require('crypto');

const API = 'https://netease-cloud-music-api-backup-five-red.vercel.app';
const BARK_KEY = '6CXsys8uTEhxJXQo6GNvQd';

const app = express();
app.use(cors());

// express.json() 只用于非 /mcp 路由，避免抢占 MCP SDK 的 body 解析
app.use((req, res, next) => {
  if (req.path === '/mcp') return next();
  express.json()(req, res, next);
});

app.use(express.static(path.join(__dirname, 'public')));

// ---- 当前播放状态 ----
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

// ---- 播放指令队列（MCP -> 前端）----
let commandQueue = [];

let clients = [];

function broadcast(data) {
  const msg = `data: ${JSON.stringify(data)}\n\n`;
  clients.forEach(c => c.res.write(msg));
}

// ---- 原有 HTTP 路由 ----
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

// ---- 播放指令轮询（前端用）----
app.get('/commands', (req, res) => {
  const cmds = commandQueue.splice(0);
  res.json(cmds);
});

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ---- 网易云辅助函数 ----
async function searchSong(keyword, limit = 10) {
  const r = await fetch(`${API}/cloudsearch?keywords=${encodeURIComponent(keyword)}&limit=${limit}`);
  const j = await r.json();
  const songs = (j.result && j.result.songs) || [];
  return songs.map(s => ({
    id: s.id,
    name: s.name,
    artist: (s.ar || []).map(a => a.name).join('/'),
    album: (s.al && s.al.name) || '',
    cover: (s.al && s.al.picUrl) ? s.al.picUrl + '?param=200y200' : '',
    duration: s.dt ? Math.round(s.dt / 1000) : 0
  }));
}

async function getLyrics(id) {
  const r = await fetch(`${API}/lyric?id=${id}`);
  const j = await r.json();
  const raw = (j.lrc && j.lrc.lyric) || '';
  const lines = [];
  raw.split('\n').forEach(line => {
    const m = line.match(/\[(\d+):(\d+)\.(\d+)\](.*)/);
    if (m) {
      const t = parseInt(m[1]) * 60 + parseInt(m[2]) + parseInt(m[3]) / (m[3].length === 2 ? 100 : 1000);
      const txt = m[4].trim();
      if (txt) lines.push({ time: t, text: txt });
    }
  });
  return lines;
}

async function getSongUrl(id) {
  const r = await fetch(`${API}/song/url/v1?id=${id}&level=exhigh`);
  const j = await r.json();
  return (j.data && j.data[0] && j.data[0].url) || '';
}

// ---- MCP Server ----
const sessions = new Map();

function createMcpServer() {
  const mcp = new McpServer({
    name: 'lyric-sync-mcp',
    version: '2.0.0'
  });

  mcp.tool(
    'get_now_playing',
    '查看蕊蕊当前正在听的歌曲、歌词、播放进度',
    {},
    async () => {
      if (!current.syncing || !current.song) {
        return { content: [{ type: 'text', text: JSON.stringify({ status: '蕊蕊当前没有在听歌', syncing: false }) }] };
      }
      const mins = Math.floor(current.elapsed / 60);
      const secs = Math.floor(current.elapsed % 60);
      const durMins = Math.floor(current.duration / 60);
      const durSecs = Math.floor(current.duration % 60);
      const info = {
        song: current.song,
        artist: current.artist,
        current_lyric: current.lyric,
        progress: `${mins}:${String(secs).padStart(2, '0')} / ${durMins}:${String(durSecs).padStart(2, '0')}`,
        elapsed_seconds: Math.round(current.elapsed),
        duration_seconds: Math.round(current.duration),
        cover: current.cover,
        syncing: current.syncing
      };
      return { content: [{ type: 'text', text: JSON.stringify(info, null, 2) }] };
    }
  );

  mcp.tool(
    'search_song',
    '通过关键词搜索网易云音乐歌曲',
    { keyword: z.string().describe('搜索关键词，歌名或歌手名'), limit: z.number().optional().describe('返回数量，默认10') },
    async ({ keyword, limit }) => {
      try {
        const results = await searchSong(keyword, limit || 10);
        return { content: [{ type: 'text', text: JSON.stringify(results, null, 2) }] };
      } catch (e) {
        return { content: [{ type: 'text', text: '搜索失败: ' + e.message }] };
      }
    }
  );

  mcp.tool(
    'play_song',
    '向蕊蕊的播放器发送播放指令，让她的播放器播放指定的网易云歌曲',
    { song_id: z.number().describe('网易云歌曲ID'), song_name: z.string().describe('歌曲名'), artist: z.string().describe('歌手名') },
    async ({ song_id, song_name, artist }) => {
      try {
        const url = await getSongUrl(song_id);
        if (!url) {
          return { content: [{ type: 'text', text: `"${song_name}" 无法获取播放链接，可能没有版权。` }] };
        }
        const cover = await searchSong(song_name, 1).then(r => r[0] ? r[0].cover : '');

        commandQueue.push({
          action: 'play',
          song: { title: song_name, artist, neteaseId: song_id, cover, url },
          timestamp: Date.now()
        });

        broadcast({
          command: 'play',
          song: { title: song_name, artist, neteaseId: song_id, cover, url }
        });

        return { content: [{ type: 'text', text: `已向蕊蕊的播放器发送播放指令："${song_name}" - ${artist}` }] };
      } catch (e) {
        return { content: [{ type: 'text', text: '播放失败: ' + e.message }] };
      }
    }
  );

  mcp.tool(
    'get_lyrics',
    '获取指定网易云歌曲的完整歌词',
    { song_id: z.number().describe('网易云歌曲ID') },
    async ({ song_id }) => {
      try {
        const lyrics = await getLyrics(song_id);
        if (!lyrics.length) {
          return { content: [{ type: 'text', text: '这首歌没有歌词' }] };
        }
        const text = lyrics.map(l => {
          const m = Math.floor(l.time / 60);
          const s = Math.floor(l.time % 60);
          return `[${m}:${String(s).padStart(2, '0')}] ${l.text}`;
        }).join('\n');
        return { content: [{ type: 'text', text: text }] };
      } catch (e) {
        return { content: [{ type: 'text', text: '获取歌词失败: ' + e.message }] };
      }
    }
  );

  return mcp;
}

// ---- MCP Streamable HTTP 路由 ----
app.post('/mcp', async (req, res) => {
  try {
    let sessionId = req.headers['mcp-session-id'];
    let transport;

    if (sessionId && sessions.has(sessionId)) {
      transport = sessions.get(sessionId);
    } else {
      const mcp = createMcpServer();
      transport = new StreamableHTTPServerTransport({ sessionIdGenerator: () => crypto.randomUUID() });
      await mcp.connect(transport);
      transport.onclose = () => {
        const sid = transport.sessionId;
        if (sid) sessions.delete(sid);
      };
    }

    await transport.handleRequest(req, res);

    if (transport.sessionId && !sessions.has(transport.sessionId)) {
      sessions.set(transport.sessionId, transport);
    }
  } catch (e) {
    console.error('MCP error:', e);
    if (!res.headersSent) {
      res.status(500).json({ error: e.message });
    }
  }
});

app.get('/mcp', async (req, res) => {
  const sessionId = req.headers['mcp-session-id'];
  if (!sessionId || !sessions.has(sessionId)) {
    res.status(400).json({ error: 'Missing or invalid session ID' });
    return;
  }
  const transport = sessions.get(sessionId);
  await transport.handleRequest(req, res);
});

app.delete('/mcp', async (req, res) => {
  const sessionId = req.headers['mcp-session-id'];
  if (sessionId && sessions.has(sessionId)) {
    const transport = sessions.get(sessionId);
    await transport.handleRequest(req, res);
    sessions.delete(sessionId);
  } else {
    res.status(404).json({ error: 'Session not found' });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Lyric sync + MCP server running on port ${PORT}`));
