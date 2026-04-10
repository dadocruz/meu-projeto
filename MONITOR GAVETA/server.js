const express = require('express');
const cors = require('cors');
const dotenv = require('dotenv');
const fs = require('node:fs/promises');
const path = require('node:path');

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json({ limit: '1mb' }));

const {
  SPOTIFY_CLIENT_ID,
  SPOTIFY_CLIENT_SECRET,
  GEMINI_API_KEY,
  YOUTUBE_API_KEY,
  PORT = 3000,
  MAX_ARTISTS_PER_REFRESH,
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  SUPABASE_ARTISTS_TABLE = 'artists_registry',
} = process.env;

let spotifyAccessToken = null;
let spotifyAccessExpiresAt = 0;

const TTL = {
  dashboard: 24 * 60 * 60 * 1000,
  artistData: 12 * 60 * 60 * 1000,
  topTracks: 12 * 60 * 60 * 1000,
  recentReleases: 12 * 60 * 60 * 1000,
  geminiSignals: 6 * 60 * 60 * 1000,
  youtube: 6 * 60 * 60 * 1000,
};

const cache = new Map();
const inflight = new Map();
const DATA_DIR = path.join(__dirname, 'data');
const ARTISTS_STORE_PATH = path.join(DATA_DIR, 'artists.json');
let artistsWriteQueue = Promise.resolve();
const supabaseEnabled = Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);
const supabaseBaseUrl = SUPABASE_URL ? SUPABASE_URL.replace(/\/$/, '') : '';
const supabaseTablePath = `/rest/v1/${encodeURIComponent(SUPABASE_ARTISTS_TABLE)}`;

function supabaseHeaders(extra = {}) {
  return {
    apikey: SUPABASE_SERVICE_ROLE_KEY,
    Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
    ...extra,
  };
}

function normalizeArtistInput(raw = {}) {
  const artistName = String(raw.artistName || raw.name || '').trim();
  const spotifyUrl = String(raw.spotifyUrl || '').trim();
  const youtubeUrl = String(raw.youtubeUrl || '').trim();
  const spotifyArtistId = extractSpotifyArtistId(spotifyUrl)
    || (isValidSpotifyArtistId(raw.spotifyArtistId) ? String(raw.spotifyArtistId).trim() : undefined);

  if (!artistName || !spotifyArtistId) return null;

  return {
    artistName,
    spotifyUrl,
    youtubeUrl,
    spotifyArtistId,
  };
}

function sanitizeArtistsInput(input) {
  const rawList = Array.isArray(input) ? input : [];
  const bySpotifyArtistId = new Map();

  rawList.forEach(item => {
    const normalized = normalizeArtistInput(item);
    if (!normalized) return;
    bySpotifyArtistId.set(normalized.spotifyArtistId, normalized);
  });

  return [...bySpotifyArtistId.values()];
}

function fromSupabaseRow(row = {}) {
  return normalizeArtistInput({
    artistName: row.artist_name,
    spotifyUrl: row.spotify_url,
    youtubeUrl: row.youtube_url,
    spotifyArtistId: row.spotify_artist_id,
  });
}

function toSupabaseRow(artist = {}) {
  return {
    artist_name: artist.artistName,
    spotify_url: artist.spotifyUrl,
    youtube_url: artist.youtubeUrl || null,
    spotify_artist_id: artist.spotifyArtistId,
    updated_at: new Date().toISOString(),
  };
}

async function readArtistsStoreSupabase() {
  const url = `${supabaseBaseUrl}${supabaseTablePath}?select=artist_name,spotify_url,youtube_url,spotify_artist_id,updated_at&order=updated_at.desc`;
  const res = await fetch(url, { headers: supabaseHeaders() });
  if (!res.ok) throw new Error(`Supabase GET ${res.status}: ${(await res.text()).slice(0, 260)}`);

  const rows = await res.json();
  const artists = sanitizeArtistsInput((rows || []).map(fromSupabaseRow));
  const updatedAt = Array.isArray(rows) && rows.length ? rows[0].updated_at || null : null;
  return { artists, updatedAt };
}

async function writeArtistsStoreSupabase(artistsInput) {
  const artists = sanitizeArtistsInput(artistsInput);

  if (!artists.length) {
    const deleteUrl = `${supabaseBaseUrl}${supabaseTablePath}?spotify_artist_id=not.is.null`;
    const delRes = await fetch(deleteUrl, {
      method: 'DELETE',
      headers: supabaseHeaders(),
    });
    if (!delRes.ok) throw new Error(`Supabase DELETE ${delRes.status}: ${(await delRes.text()).slice(0, 260)}`);
    return { artists: [], updatedAt: new Date().toISOString() };
  }

  const upsertUrl = `${supabaseBaseUrl}${supabaseTablePath}?on_conflict=spotify_artist_id`;
  const upsertRes = await fetch(upsertUrl, {
    method: 'POST',
    headers: supabaseHeaders({
      'Content-Type': 'application/json',
      Prefer: 'resolution=merge-duplicates',
    }),
    body: JSON.stringify(artists.map(toSupabaseRow)),
  });
  if (!upsertRes.ok) throw new Error(`Supabase UPSERT ${upsertRes.status}: ${(await upsertRes.text()).slice(0, 260)}`);

  const currentStore = await readArtistsStoreSupabase();
  const desiredIds = new Set(artists.map(a => a.spotifyArtistId));
  const toDelete = currentStore.artists
    .map(a => a.spotifyArtistId)
    .filter(id => !desiredIds.has(id));

  if (toDelete.length) {
    const values = toDelete.map(id => `"${id}"`).join(',');
    const deleteUrl = `${supabaseBaseUrl}${supabaseTablePath}?spotify_artist_id=in.(${values})`;
    const delRes = await fetch(deleteUrl, {
      method: 'DELETE',
      headers: supabaseHeaders(),
    });
    if (!delRes.ok) throw new Error(`Supabase DELETE-MISS ${delRes.status}: ${(await delRes.text()).slice(0, 260)}`);
  }

  return readArtistsStoreSupabase();
}

async function readArtistsStore() {
  if (supabaseEnabled) {
    return readArtistsStoreSupabase();
  }

  try {
    const raw = await fs.readFile(ARTISTS_STORE_PATH, 'utf8');
    const parsed = JSON.parse(raw);
    const artists = sanitizeArtistsInput(parsed?.artists);
    return {
      artists,
      updatedAt: parsed?.updatedAt || null,
    };
  } catch {
    return { artists: [], updatedAt: null };
  }
}

function writeArtistsStoreQueued(artists) {
  if (supabaseEnabled) {
    return writeArtistsStoreSupabase(artists);
  }

  artistsWriteQueue = artistsWriteQueue
    .catch(() => null)
    .then(async () => {
      await fs.mkdir(DATA_DIR, { recursive: true });
      const payload = {
        updatedAt: new Date().toISOString(),
        artists: sanitizeArtistsInput(artists),
      };
      await fs.writeFile(ARTISTS_STORE_PATH, JSON.stringify(payload, null, 2), 'utf8');
      return payload;
    });

  return artistsWriteQueue;
}

function compactNumber(value) {
  if (value === null || value === undefined || value === '' || Number.isNaN(Number(value))) return null;
  return Number(value);
}

function cacheGet(key) {
  const hit = cache.get(key);
  if (!hit) return null;
  if (Date.now() > hit.expiresAt) {
    cache.delete(key);
    return null;
  }
  return hit.value;
}

function cacheSet(key, value, ttlMs) {
  cache.set(key, { value, expiresAt: Date.now() + ttlMs });
  return value;
}

async function remember(key, ttlMs, factory) {
  const hit = cacheGet(key);
  if (hit !== null) return hit;
  if (inflight.has(key)) return inflight.get(key);

  const p = (async () => {
    try {
      const value = await factory();
      return cacheSet(key, value, ttlMs);
    } finally {
      inflight.delete(key);
    }
  })();

  inflight.set(key, p);
  return p;
}

function extractSpotifyArtistId(input = '') {
  const value = String(input).trim();
  for (const pattern of [
    /open\.spotify\.com\/(?:intl-[^/]+\/)?artist\/([A-Za-z0-9]{22})/i,
    /spotify:artist:([A-Za-z0-9]{22})/i,
    /^([A-Za-z0-9]{22})$/,
  ]) {
    const m = value.match(pattern);
    if (m) return m[1];
  }
  return null;
}

function isValidSpotifyArtistId(value = '') {
  return /^[A-Za-z0-9]{22}$/.test(String(value).trim());
}

function extractYouTubeChannelId(input = '') {
  const v = String(input).trim();
  if (!v) return null;
  const m1 = v.match(/youtube\.com\/channel\/([A-Za-z0-9_-]+)/i); if (m1) return m1[1];
  const m2 = v.match(/youtube\.com\/@([A-Za-z0-9_.-]+)/i); if (m2) return `@${m2[1]}`;
  const m3 = v.match(/youtube\.com\/(?:c|user)\/([A-Za-z0-9_.-]+)/i); if (m3) return m3[1];
  const m4 = v.match(/^([A-Za-z0-9_-]{20,})$/); if (m4) return m4[1];
  return null;
}

async function getSpotifyAccessToken() {
  if (!SPOTIFY_CLIENT_ID || !SPOTIFY_CLIENT_SECRET) {
    throw new Error('SPOTIFY_CLIENT_ID ou SPOTIFY_CLIENT_SECRET ausentes');
  }
  
  if (spotifyAccessToken && Date.now() < spotifyAccessExpiresAt - 60_000) {
    return spotifyAccessToken;
  }

  const auth = Buffer.from(`${SPOTIFY_CLIENT_ID}:${SPOTIFY_CLIENT_SECRET}`).toString('base64');
  const res = await fetch('https://accounts.spotify.com/api/token', {
    method: 'POST',
    headers: {
      'Authorization': `Basic ${auth}`,
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body: 'grant_type=client_credentials',
  });

  if (!res.ok) {
    throw new Error(`Spotify token ${res.status}: ${(await res.text()).slice(0, 200)}`);
  }

  const data = await res.json();
  spotifyAccessToken = data.access_token;
  spotifyAccessExpiresAt = Date.now() + (data.expires_in || 3600) * 1000;
  return spotifyAccessToken;
}

async function spotifyGet(path) {
  const token = await getSpotifyAccessToken();
  const res = await fetch(`https://api.spotify.com/v1${path}`, {
    headers: { 'Authorization': `Bearer ${token}` },
  });

  if (!res.ok) {
    throw new Error(`Spotify ${res.status}: ${(await res.text()).slice(0, 260)}`);
  }

  return res.json();
}

async function getArtistData(spotifyArtistId) {
  return remember(`spotify:artist:${spotifyArtistId}`, TTL.artistData, async () => {
    const data = await spotifyGet(`/artists/${spotifyArtistId}`);
    return {
      name: data.name || null,
      followers: compactNumber(data.followers?.total),
      popularity: compactNumber(data.popularity),
      imageUrl: data.images?.[0]?.url || null,
      spotifyUrl: data.external_urls?.spotify || null,
    };
  });
}

async function getTopTracks(spotifyArtistId) {
  return remember(`spotify:artist:${spotifyArtistId}:top-tracks`, TTL.topTracks, async () => {
    const data = await spotifyGet(`/artists/${spotifyArtistId}/top-tracks?market=BR`);
    const tracks = (data.tracks || []).slice(0, 3);
    
    return tracks.map(track => ({
      title: track.name || 'Track',
      spotifyUrl: track.external_urls?.spotify || null,
      popularity: compactNumber(track.popularity),
      imageUrl: track.album?.images?.[0]?.url || null,
      releaseDate: track.album?.release_date || null,
    }));
  });
}

async function getRecentReleases(spotifyArtistId) {
  return remember(`spotify:artist:${spotifyArtistId}:releases`, TTL.recentReleases, async () => {
    const data = await spotifyGet(`/artists/${spotifyArtistId}/albums?limit=10&include_groups=album,single`);
    const items = (data.items || []).slice(0, 3);
    
    return items.map(album => ({
      title: album.name || 'Album',
      spotifyUrl: album.external_urls?.spotify || null,
      releaseDate: album.release_date || null,
      imageUrl: album.images?.[0]?.url || null,
      type: album.album_type || 'album',
      popularity: compactNumber(album.popularity),
    }));
  });
}

function sanitizeNumericText(value) {
  if (value === null || value === undefined) return null;
  const normalized = String(value).replace(/[^0-9.,]/g, '').replace(/\./g, '').replace(',', '.');
  const num = Number(normalized);
  return Number.isFinite(num) ? num : null;
}

function parseGeminiJsonText(raw = '') {
  const text = String(raw || '').trim();
  if (!text) return null;

  const codeBlock = text.match(/```(?:json)?\s*([\s\S]*?)```/i);
  const candidate = codeBlock ? codeBlock[1].trim() : text;

  try {
    return JSON.parse(candidate);
  } catch {
    return null;
  }
}

function spotifyUriToWebUrl(uri = '') {
  const value = String(uri || '').trim();
  const m = value.match(/^spotify:(artist|album|track):([A-Za-z0-9]+)$/i);
  if (!m) return null;
  return `https://open.spotify.com/${m[1].toLowerCase()}/${m[2]}`;
}

function parseSpotifyInitialState(html, spotifyArtistId) {
  const marker = /<script id="initialState" type="text\/plain">([\s\S]*?)<\/script>/i;
  const match = String(html || '').match(marker);
  if (!match?.[1]) return null;

  const decoded = Buffer.from(match[1].trim(), 'base64').toString('utf8');
  const parsed = JSON.parse(decoded);
  const artistKey = `spotify:artist:${spotifyArtistId}`;
  const artist = parsed?.entities?.items?.[artistKey];
  if (!artist) return null;

  const monthlyListenersValue = compactNumber(artist?.stats?.monthlyListeners);
  const topTracks = Array.isArray(artist?.discography?.topTracks?.items)
    ? artist.discography.topTracks.items
    : [];

  const singles = topTracks.slice(0, 5).map(row => {
    const track = row?.track || {};
    const title = String(track?.name || '').trim() || 'Single';
    const plays = compactNumber(track?.playcount);
    const coverUrl = track?.albumOfTrack?.coverArt?.sources?.[1]?.url
      || track?.albumOfTrack?.coverArt?.sources?.[0]?.url
      || null;
    const spotifyUrl = spotifyUriToWebUrl(track?.uri) || null;

    return {
      title,
      plays,
      releaseDate: null,
      spotifyUrl,
      coverUrl,
    };
  }).filter(item => item.title);

  return {
    monthlyListenersValue,
    singles,
    source: 'spotify-page',
  };
}

async function getSpotifyPublicSignals(artistName, spotifyArtistId) {
  const key = `spotify:public-signals:${spotifyArtistId}`;
  return remember(key, TTL.geminiSignals, async () => {
    const artistUrl = `https://open.spotify.com/artist/${spotifyArtistId}`;
    const res = await fetch(artistUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
      },
    });

    if (!res.ok) {
      throw new Error(`Spotify page ${res.status} for ${artistName}`);
    }

    const html = await res.text();
    const signals = parseSpotifyInitialState(html, spotifyArtistId);
    if (!signals) return null;

    return signals;
  });
}

function normalizeGeminiSingles(singles, spotifyArtistUrl) {
  if (!Array.isArray(singles)) return [];

  return singles.slice(0, 5).map(item => {
    const title = String(item?.title || item?.name || '').trim() || 'Single';
    const plays = sanitizeNumericText(item?.plays);
    const releaseDate = String(item?.releaseDate || item?.date || '').trim() || null;
    const spotifyUrl = String(item?.spotifyUrl || '').trim() || spotifyArtistUrl;
    const coverUrl = String(item?.coverUrl || item?.imageUrl || '').trim() || null;
    return { title, plays, releaseDate, spotifyUrl, coverUrl };
  });
}

async function getGeminiSpotifySignals(artistName, spotifyArtistUrl) {
  if (!GEMINI_API_KEY) return null;

  const key = `gemini:spotify-signals:${artistName.toLowerCase()}:${spotifyArtistUrl}`;
  return remember(key, TTL.geminiSignals, async () => {
    const prompt = [
      'Busque na web dados recentes de Spotify para este artista e retorne somente JSON válido.',
      `Artista: ${artistName}`,
      `URL Spotify: ${spotifyArtistUrl}`,
      'Formato JSON obrigatório:',
      '{',
      '  "monthlyListeners": number|null,',
      '  "singles": [',
      '    {"title": string, "plays": number|null, "releaseDate": string|null, "spotifyUrl": string|null, "coverUrl": string|null}',
      '  ]',
      '}',
      'Regras: sem markdown, sem texto extra. Use null se não tiver confiança no valor.'
    ].join('\n');

    const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${encodeURIComponent(GEMINI_API_KEY)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        contents: [{ role: 'user', parts: [{ text: prompt }] }],
        tools: [{ google_search: {} }],
        generationConfig: {
          temperature: 0.1,
          responseMimeType: 'application/json',
        },
      }),
    });

    if (!res.ok) {
      const msg = (await res.text()).slice(0, 240);
      throw new Error(`Gemini ${res.status}: ${msg}`);
    }

    const data = await res.json();
    const rawText = (data.candidates?.[0]?.content?.parts || []).map(p => p.text || '').join('\n').trim();
    const parsed = parseGeminiJsonText(rawText);
    if (!parsed) return null;

    const monthlyListenersValue = sanitizeNumericText(parsed.monthlyListeners);
    const singles = normalizeGeminiSingles(parsed.singles, spotifyArtistUrl);

    return {
      monthlyListenersValue,
      singles,
      source: 'gemini-web',
    };
  });
}

async function youtubeGet(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`YT ${res.status}: ${(await res.text()).slice(0, 260)}`);
  return res.json();
}

async function getYouTubeChannelBundle(channelUrl) {
  const channelId = extractYouTubeChannelId(channelUrl);
  const empty = { channelId: null, title: null, thumbnail: null, subscribers: null, views: null, latestVideos: [] };

  if (!channelId) return empty;
  if (!YOUTUBE_API_KEY) return { ...empty, channelId };

  return remember(`yt:channel:${channelId}`, TTL.youtube, async () => {
    let qp;
    if (channelId.startsWith('@')) qp = `forHandle=${encodeURIComponent(channelId)}`;
    else if (/^UC[A-Za-z0-9_-]{20,}$/.test(channelId)) qp = `id=${encodeURIComponent(channelId)}`;
    else qp = `forUsername=${encodeURIComponent(channelId)}`;

    let channelData = await youtubeGet(
      `https://www.googleapis.com/youtube/v3/channels?part=snippet,statistics,contentDetails&${qp}&key=${encodeURIComponent(YOUTUBE_API_KEY)}`
    );

    if (!channelData.items?.length && !channelId.startsWith('@') && !/^UC/.test(channelId)) {
      try {
        channelData = await youtubeGet(
          `https://www.googleapis.com/youtube/v3/channels?part=snippet,statistics,contentDetails&forHandle=${encodeURIComponent('@' + channelId)}&key=${encodeURIComponent(YOUTUBE_API_KEY)}`
        );
      } catch {
        channelData = { items: [] };
      }
    }

    const channel = channelData.items?.[0];
    if (!channel) return { ...empty, channelId };

    const uploadsPlaylist = channel.contentDetails?.relatedPlaylists?.uploads;
    let latestVideos = [];

    if (uploadsPlaylist) {
      const playlistData = await youtubeGet(
        `https://www.googleapis.com/youtube/v3/playlistItems?part=snippet,contentDetails&playlistId=${encodeURIComponent(uploadsPlaylist)}&maxResults=3&key=${encodeURIComponent(YOUTUBE_API_KEY)}`
      );

      const items = playlistData.items || [];
      const ids = items.map(i => i.contentDetails?.videoId).filter(Boolean);

      let statsById = new Map();
      if (ids.length) {
        const videosData = await youtubeGet(
          `https://www.googleapis.com/youtube/v3/videos?part=statistics&id=${encodeURIComponent(ids.join(','))}&key=${encodeURIComponent(YOUTUBE_API_KEY)}`
        );
        statsById = new Map((videosData.items || []).map(v => [v.id, v.statistics || {}]));
      }

      const now = Date.now();
      latestVideos = items.slice(0, 3).map(item => {
        const videoId = item.contentDetails?.videoId;
        const stats = statsById.get(videoId) || {};
        const publishedAt = item.contentDetails?.videoPublishedAt || item.snippet?.publishedAt || null;
        const views = compactNumber(stats.viewCount);
        const daysOld = publishedAt ? Math.floor((now - new Date(publishedAt).getTime()) / 86400000) : 999;
        const needsTraffic = daysOld <= 14 && (views === null || views < 5000);

        return {
          id: videoId,
          youtubeUrl: videoId ? `https://www.youtube.com/watch?v=${videoId}` : null,
          title: item.snippet?.title || 'Video',
          publishedAt,
          daysOld,
          thumbnail: item.snippet?.thumbnails?.medium?.url || item.snippet?.thumbnails?.default?.url || null,
          views,
          needsTraffic,
        };
      });
    }

    return {
      channelId,
      title: channel.snippet?.title || null,
      thumbnail: channel.snippet?.thumbnails?.medium?.url || channel.snippet?.thumbnails?.default?.url || null,
      subscribers: compactNumber(channel.statistics?.subscriberCount),
      views: compactNumber(channel.statistics?.viewCount),
      latestVideos,
    };
  });
}

function computePriority(artist) {
  let score = 0;
  score += Number(artist.spotify?.monthlyListeners?.value || 0) / 100000;
  score += Number(artist.spotify?.popularity || 0) / 100;
  score += Number(artist.youtube?.channelViews || 0) / 10000000;
  score += Number(artist.spotify?.followers || 0) / 200000;
  score += Number(artist.spotify?.singles?.[0]?.plays || 0) / 1000000;
  score += Number(artist.spotify?.topTracks?.[0]?.popularity || 0) / 100;
  score += Number(artist.youtube?.latestVideos?.[0]?.views || 0) / 1000000;
  return score;
}

function dashboardCacheKey(inputArtists) {
  const normalized = inputArtists.map(a => ({
    artistName: String(a?.artistName || a?.name || '').trim().toLowerCase(),
    spotifyUrl: String(a?.spotifyUrl || '').trim(),
    youtubeUrl: String(a?.youtubeUrl || '').trim(),
  }));
  return `dashboard:${JSON.stringify(normalized)}`;
}

app.get('/', (_req, res) => {
  res.json({ ok: true, service: 'gaveta-creative-radar-backend', health: '/api/health', dashboard: '/api/dashboard' });
});

app.get('/api/health', async (_req, res) => {
  try {
    await getSpotifyAccessToken();
    res.json({
      ok: true,
      build: 'spotify-scrape-2026-04-10-02',
      spotify: true,
      geminiConfigured: Boolean(GEMINI_API_KEY),
      youtubeConfigured: Boolean(YOUTUBE_API_KEY),
      cacheEntries: cache.size,
      artistsStorage: supabaseEnabled ? 'supabase' : 'local-file',
      ts: new Date().toISOString(),
    });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message, youtubeConfigured: Boolean(YOUTUBE_API_KEY) });
  }
});

app.get('/api/artists', async (_req, res) => {
  try {
    const store = await readArtistsStore();
    return res.json({ ok: true, artists: store.artists, updatedAt: store.updatedAt });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message });
  }
});

app.put('/api/artists', async (req, res) => {
  try {
    const artists = sanitizeArtistsInput(req.body?.artists);
    const saved = await writeArtistsStoreQueued(artists);
    return res.json({ ok: true, artists: saved.artists, updatedAt: saved.updatedAt });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message });
  }
});

app.post('/api/dashboard', async (req, res) => {
  try {
    const inputArtistsRaw = Array.isArray(req.body?.artists) ? req.body.artists : [];
    const parsedMax = Number(MAX_ARTISTS_PER_REFRESH);
    const hasLimit = Number.isFinite(parsedMax) && parsedMax > 0;
    const maxArtists = hasLimit ? Math.max(1, parsedMax) : null;
    const inputArtists = hasLimit ? inputArtistsRaw.slice(0, maxArtists) : inputArtistsRaw;
    const skippedArtists = hasLimit ? Math.max(0, inputArtistsRaw.length - inputArtists.length) : 0;
    if (!inputArtists.length) {
      return res.json({ ok: true, fetchedAt: new Date().toISOString(), artists: [], ranking: [], trafficAlerts: [], processedArtists: 0, skippedArtists: 0 });
    }

    const key = dashboardCacheKey(inputArtists);
    const cachedDashboard = cacheGet(key);
    if (cachedDashboard) {
      return res.json({ ...cachedDashboard, fromCache: true });
    }

    const artists = await Promise.all(inputArtists.map(async (raw) => {
      const artistName = String(raw.artistName || raw.name || '').trim();
      const spotifyUrl = String(raw.spotifyUrl || '').trim();
      const youtubeUrl = String(raw.youtubeUrl || '').trim();
      const spotifyArtistId = extractSpotifyArtistId(spotifyUrl)
        || (isValidSpotifyArtistId(raw.spotifyArtistId) ? String(raw.spotifyArtistId).trim() : null);

      if (!artistName || !spotifyArtistId) {
        return { artistName: artistName || 'Sem nome', error: 'Nome e link do Spotify são obrigatórios.', spotifyArtistId };
      }

      try {
        const spotifyArtistUrl = `https://open.spotify.com/artist/${spotifyArtistId}`;
        const [artistData, topTracks, recentReleases, youtube, spotifyPublicSignals, geminiSignals] = await Promise.all([
          getArtistData(spotifyArtistId),
          getTopTracks(spotifyArtistId),
          getRecentReleases(spotifyArtistId),
          getYouTubeChannelBundle(youtubeUrl),
          getSpotifyPublicSignals(artistName, spotifyArtistId).catch(() => null),
          getGeminiSpotifySignals(artistName, spotifyArtistUrl).catch(() => null),
        ]);

        const listenersValue = spotifyPublicSignals?.monthlyListenersValue ?? geminiSignals?.monthlyListenersValue ?? null;
        const listenersSource = spotifyPublicSignals?.source || geminiSignals?.source || null;
        const singles = Array.isArray(spotifyPublicSignals?.singles) && spotifyPublicSignals.singles.length
          ? spotifyPublicSignals.singles
          : (geminiSignals?.singles || []);

        return {
          artistName,
          spotifyArtistId,
          spotifyArtistUrl,
          fetchedAt: new Date().toISOString(),
          imageUrl: artistData.imageUrl || youtube.thumbnail || null,
          spotify: { 
            monthlyListeners: listenersValue != null
              ? { value: listenersValue, source: listenersSource }
              : null,
            popularity: artistData.popularity, 
            followers: artistData.followers, 
            singles,
            topTracks,
            recentReleases,
          },
          youtube: {
            channelTitle: youtube.title,
            channelThumbnail: youtube.thumbnail,
            subscribers: youtube.subscribers,
            channelViews: youtube.views,
            latestVideos: youtube.latestVideos,
          },
        };
      } catch (error) {
        return { artistName, spotifyArtistId, error: error.message };
      }
    }));

    const validArtists = artists.filter(a => !a.error);

    const ranking = validArtists
      .map(artist => ({
        artistName: artist.artistName,
        imageUrl: artist.imageUrl,
        monthlyListeners: artist.spotify?.monthlyListeners?.value || 0,
        spotifyPopularity: artist.spotify?.popularity || 0,
        youtubeViews: artist.youtube?.channelViews || 0,
        priorityScore: computePriority(artist),
      }))
      .sort((a, b) => b.priorityScore - a.priorityScore)
      .slice(0, 10);

    const trafficAlerts = [];
    for (const artist of validArtists) {
      for (const video of (artist.youtube?.latestVideos || [])) {
        if (video.needsTraffic) {
          trafficAlerts.push({
            artistName: artist.artistName,
            artistImage: artist.imageUrl,
            videoTitle: video.title,
            videoUrl: video.youtubeUrl,
            thumbnail: video.thumbnail,
            views: video.views,
            daysOld: video.daysOld,
            publishedAt: video.publishedAt,
          });
        }
      }
    }

    const payload = {
      ok: true,
      fetchedAt: new Date().toISOString(),
      artists,
      ranking,
      trafficAlerts,
      processedArtists: inputArtists.length,
      skippedArtists,
      maxArtistsPerRefresh: maxArtists,
    };
    cacheSet(key, payload, TTL.dashboard);
    return res.json(payload);
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message });
  }
});

app.listen(PORT, () => {
  console.log(`Gaveta backend -> http://localhost:${PORT}`);
});
