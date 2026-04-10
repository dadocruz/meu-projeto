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
  CHARTMETRIC_REFRESH_TOKEN,
  YOUTUBE_API_KEY,
  PORT = 3000,
  MAX_ARTISTS_PER_REFRESH,
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  SUPABASE_ARTISTS_TABLE = 'artists_registry',
} = process.env;

const CM_HOST = 'https://api.chartmetric.com';
let cmAccessToken = null;
let cmAccessExpiresAt = 0;

const TTL = {
  dashboard: 24 * 60 * 60 * 1000,
  cmArtistId: 7 * 24 * 60 * 60 * 1000,
  artistMeta: 7 * 24 * 60 * 60 * 1000,
  listenersFollowers: 24 * 60 * 60 * 1000,
  artistAlbums: 7 * 24 * 60 * 60 * 1000,
  albumTracks: 7 * 24 * 60 * 60 * 1000,
  trackStreams: 7 * 24 * 60 * 60 * 1000,
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
  const cmArtistId = Number(raw.cmArtistId || raw.chartmetricArtistId || 0) || undefined;

  if (!artistName || !spotifyArtistId) return null;

  return {
    artistName,
    spotifyUrl,
    youtubeUrl,
    spotifyArtistId,
    cmArtistId,
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
    cmArtistId: row.cm_artist_id,
  });
}

function toSupabaseRow(artist = {}) {
  return {
    artist_name: artist.artistName,
    spotify_url: artist.spotifyUrl,
    youtube_url: artist.youtubeUrl || null,
    spotify_artist_id: artist.spotifyArtistId,
    cm_artist_id: artist.cmArtistId || null,
    updated_at: new Date().toISOString(),
  };
}

async function readArtistsStoreSupabase() {
  const url = `${supabaseBaseUrl}${supabaseTablePath}?select=artist_name,spotify_url,youtube_url,spotify_artist_id,cm_artist_id,updated_at&order=updated_at.desc`;
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

async function getChartmetricAccessToken() {
  if (!CHARTMETRIC_REFRESH_TOKEN) throw new Error('CHARTMETRIC_REFRESH_TOKEN ausente');
  if (cmAccessToken && Date.now() < cmAccessExpiresAt - 60_000) return cmAccessToken;

  const res = await fetch(`${CM_HOST}/api/token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ refreshtoken: CHARTMETRIC_REFRESH_TOKEN }),
  });

  if (!res.ok) throw new Error(`CM token ${res.status}: ${(await res.text()).slice(0, 200)}`);
  const data = await res.json();

  cmAccessToken = data.token;
  cmAccessExpiresAt = Date.now() + (Number(data.expires_in) || 3600) * 1000;
  return cmAccessToken;
}

async function chartmetricGet(path) {
  const token = await getChartmetricAccessToken();
  const res = await fetch(`${CM_HOST}${path}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!res.ok) throw new Error(`CM ${res.status}: ${(await res.text()).slice(0, 260)}`);
  return res.json();
}

async function youtubeGet(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`YT ${res.status}: ${(await res.text()).slice(0, 260)}`);
  return res.json();
}

async function getLatestChartmetricStat(cmArtistId, field) {
  return remember(`cm:artist:${cmArtistId}:stat:${field}`, TTL.listenersFollowers, async () => {
    const data = await chartmetricGet(`/api/artist/${cmArtistId}/stat/spotify?field=${field}&latest=true`);
    const values = Array.isArray(data.obj?.[field]) ? data.obj[field] : [];
    const latest = values[values.length - 1] || values[0] || null;
    if (!latest) {
      return {
        value: null,
        weeklyDiff: null,
        weeklyDiffPercent: null,
        monthlyDiff: null,
        monthlyDiffPercent: null,
        timestamp: null,
      };
    }

    return {
      value: compactNumber(latest.value),
      weeklyDiff: compactNumber(latest.weekly_diff),
      weeklyDiffPercent: compactNumber(latest.weekly_diff_percent),
      monthlyDiff: compactNumber(latest.monthly_diff),
      monthlyDiffPercent: compactNumber(latest.monthly_diff_percent),
      timestamp: latest.timestp || null,
    };
  });
}

async function getArtistMeta(cmArtistId) {
  return remember(`cm:artist:${cmArtistId}:meta`, TTL.artistMeta, async () => {
    try {
      return (await chartmetricGet(`/api/artist/${cmArtistId}`)).obj || {};
    } catch {
      return {};
    }
  });
}

async function getChartmetricArtistIdFromSpotify(spotifyArtistId) {
  return remember(`cm:map:spotify:${spotifyArtistId}`, TTL.cmArtistId, async () => {
    const data = await chartmetricGet(`/api/artist/spotify/${encodeURIComponent(spotifyArtistId)}/get-ids`);
    const obj = Array.isArray(data.obj) ? data.obj : [];
    const match = obj.find(x => x.cm_artist) || null;
    return match ? Number(match.cm_artist) : null;
  });
}

async function getArtistAlbums(cmArtistId) {
  return remember(`cm:artist:${cmArtistId}:albums`, TTL.artistAlbums, async () => {
    const albumsData = await chartmetricGet(
      `/api/artist/${cmArtistId}/albums?sortColumn=release_date&sortOrderDesc=true&isPrimary=true&limit=30`
    );
    return Array.isArray(albumsData.obj) ? albumsData.obj : [];
  });
}

async function getAlbumTracks(cmAlbumId) {
  return remember(`cm:album:${cmAlbumId}:tracks`, TTL.albumTracks, async () => {
    const tracksData = await chartmetricGet(`/api/album/${cmAlbumId}/tracks`);
    return Array.isArray(tracksData.obj) ? tracksData.obj : [];
  });
}

async function getTrackSpotifyStreams(cmTrackId, spotifyTrackId = null) {
  const attempts = [];
  if (cmTrackId) {
    attempts.push(`/api/track/${cmTrackId}/spotify/stats/highest-playcounts?type=streams&latest=true`);
    attempts.push(`/api/track/${cmTrackId}/spotify/stats/most-history?type=streams&latest=true`);
  }
  if (spotifyTrackId) {
    attempts.push(`/api/track/${encodeURIComponent(spotifyTrackId)}/spotify/stats/highest-playcounts?type=streams&latest=true&isDomainId=true`);
    attempts.push(`/api/track/${encodeURIComponent(spotifyTrackId)}/spotify/stats/most-history?type=streams&latest=true&isDomainId=true`);
  }

  for (const path of attempts) {
    try {
      const data = await chartmetricGet(path);
      const rows = Array.isArray(data.obj) ? data.obj : [];
      for (const row of rows) {
        const points = Array.isArray(row.data) ? row.data : [];
        const latest = points[points.length - 1] || points[0];
        if (latest?.value != null) return compactNumber(latest.value);
      }
    } catch {
      // tenta próxima rota
    }
  }
  return null;
}

async function getRecentSingles(cmArtistId, spotifyArtistId) {
  const albums = await getArtistAlbums(cmArtistId);
  const singles = albums.filter(a => Number(a.num_track) === 1).slice(0, 3);

  const results = [];
  for (const album of singles) {
    let trackName = album.name || 'Single';
    let coverUrl = album.image_url || null;
    let plays = null;
    let spotifyTrackId = null;
    let cmTrackId = null;

    try {
      const tracks = await getAlbumTracks(album.cm_album);
      const first = tracks[0];
      if (first) {
        trackName = first.name || trackName;
        coverUrl = first.image_url || coverUrl;
        cmTrackId = first.cm_track || null;
        if (Array.isArray(first.spotify_track_ids) && first.spotify_track_ids.length) {
          spotifyTrackId = first.spotify_track_ids[0];
        }

        plays = await getTrackSpotifyStreams(cmTrackId, spotifyTrackId);
      }
    } catch {
      // Mantem os dados do album se track falhar.
    }

    results.push({
      cmAlbum: album.cm_album,
      cmTrack: cmTrackId,
      spotifyTrackId,
      spotifyUrl: spotifyTrackId
        ? `https://open.spotify.com/track/${spotifyTrackId}`
        : `https://open.spotify.com/artist/${spotifyArtistId}`,
      releaseDate: album.release_date || null,
      title: trackName,
      coverUrl,
      plays,
    });
  }

  return results;
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
  score += Number(artist.youtube?.channelViews || 0) / 10000000;
  score += Number(artist.spotify?.followers?.value || 0) / 200000;
  score += Number(artist.spotify?.singles?.[0]?.plays || 0) / 1000000;
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
    await getChartmetricAccessToken();
    res.json({
      ok: true,
      chartmetric: true,
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

      const rawCmArtistId = Number(raw.cmArtistId || raw.chartmetricArtistId || 0);
      let cmArtistId = Number.isFinite(rawCmArtistId) && rawCmArtistId > 0 ? rawCmArtistId : null;

      if (!artistName || !spotifyArtistId) {
        return { artistName: artistName || 'Sem nome', error: 'Nome e link do Spotify sao obrigatorios.', spotifyArtistId };
      }

      try {
        if (!cmArtistId) {
          cmArtistId = await getChartmetricArtistIdFromSpotify(spotifyArtistId);
        } else {
          cacheSet(`cm:map:spotify:${spotifyArtistId}`, cmArtistId, TTL.cmArtistId);
        }

        if (!cmArtistId) {
          return { artistName, spotifyArtistId, error: 'Artista não encontrado.' };
        }

        const [meta, listeners, followers, singles, youtube] = await Promise.all([
          getArtistMeta(cmArtistId),
          getLatestChartmetricStat(cmArtistId, 'listeners'),
          getLatestChartmetricStat(cmArtistId, 'followers'),
          getRecentSingles(cmArtistId, spotifyArtistId),
          getYouTubeChannelBundle(youtubeUrl),
        ]);

        return {
          artistName,
          spotifyArtistId,
          spotifyArtistUrl: `https://open.spotify.com/artist/${spotifyArtistId}`,
          chartmetricArtistId: cmArtistId,
          fetchedAt: new Date().toISOString(),
          imageUrl: meta.image_url || meta.image || youtube.thumbnail || null,
          spotify: { monthlyListeners: listeners, followers, singles },
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
