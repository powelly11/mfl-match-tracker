const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const PROXY = 'https://mfl-proxy.kustom2-02.workers.dev';
const MFL_BASE = 'https://z519wdyajg.execute-api.us-east-1.amazonaws.com/prod';
const MFL_CDN = 'https://d13e14gtps4iwl.cloudfront.net/matches';
const LIMIT = 25;

const hdrs = {
  'apikey': SUPABASE_KEY,
  'Authorization': `Bearer ${SUPABASE_KEY}`,
  'Content-Type': 'application/json',
};

// ─── Protobuf schema (from Ricky Hyde's analyzer) ───────────────────────────
const PROTO_SCHEMA = {nested:{mfl:{nested:{
  States:{fields:{states:{rule:'repeated',type:'State',id:1}}},
  State:{fields:{time:{type:'float',id:1},ball:{type:'Ball',id:2},homePlayers:{rule:'repeated',type:'Player',id:3},awayPlayers:{rule:'repeated',type:'Player',id:4},events:{rule:'repeated',type:'Event',id:5}}},
  Ball:{fields:{coord:{rule:'repeated',type:'float',id:1},player:{type:'uint32',id:3}}},
  Player:{fields:{coord:{rule:'repeated',type:'float',id:1}}},
  Event:{fields:{type:{type:'uint32',id:1},data:{keyType:'uint32',type:'float',id:2},coord:{rule:'repeated',type:'float',id:4},side:{type:'uint32',id:6}}}
}}}};

let protobufRoot = null;

async function loadProtobuf() {
  if (protobufRoot) return;
  const protobuf = require('protobufjs');
  protobufRoot = protobuf.Root.fromJSON(PROTO_SCHEMA);
}

// ─── Proxy fetch ─────────────────────────────────────────────────────────────
async function proxyFetch(url) {
  const res = await fetch(`${PROXY}?url=${encodeURIComponent(url)}`);
  if (res.status === 404) return null;
  if (!res.ok) throw new Error(`Proxy error ${res.status} for ${url}`);
  return res;
}

// ─── Fetch binary parts and extract shots ────────────────────────────────────
async function extractShots(matchId) {
  await loadProtobuf();
  const States = protobufRoot.lookupType('mfl.States');
  const shots = [];
  let lastShotHome = null, lastShotAway = null;
  let emptyCount = 0;

  for (let part = 0; part <= 15; part++) {
    const res = await proxyFetch(`${MFL_CDN}/${matchId}/data/part-${part}.bin`);
    if (!res) { emptyCount++; if (emptyCount >= 2) break; continue; }
    const buf = await res.arrayBuffer();
    const bytes = new Uint8Array(buf);
    // Valid protobuf starts with field tag byte — field 1 (states), wire type 2 = byte 0x0A (10)
    if (bytes.length < 4 || (bytes[0] !== 10 && bytes[0] !== 0x0a)) {
      emptyCount++; if (emptyCount >= 2) break; continue;
    }
    emptyCount = 0;

    let decoded;
    try {
      decoded = States.toObject(States.decode(bytes), { defaults: false });
    } catch(e) {
      console.warn(`  Part ${part} decode error: ${e.message} — skipping`);
      continue;
    }
    if (!decoded.states?.length) break;

    for (const state of decoded.states) {
      if (!state.events) continue;
      const time = state.time ?? 0;

      for (const ev of state.events) {
        if (ev.type === 14) {
          // Shot event
          const side = ev.side === 0 ? 'home' : 'away';
          const shot = {
            time_seconds: time,
            x: ev.coord?.[0] ?? 0,
            y: ev.coord?.[1] ?? 0,
            xg: ev.data?.[23] ?? 0,
            outcome: 'unknown',
            is_header: (ev.data?.[25] ?? 0) === 1,
            is_penalty: (ev.data?.[4] ?? 0) === 5,
            is_free_kick: (ev.data?.[4] ?? 0) === 3,
            side,
          };
          shots.push(shot);
          if (side === 'home') lastShotHome = shot;
          else lastShotAway = shot;
        }

        // Resolve outcomes
        if (ev.type === 19) { // goal
          if (ev.side === 0 && lastShotHome?.outcome === 'unknown') lastShotHome.outcome = 'goal';
          if (ev.side === 1 && lastShotAway?.outcome === 'unknown') lastShotAway.outcome = 'goal';
        } else if (ev.type === 16) { // off target
          if (ev.side === 0 && lastShotHome?.outcome === 'unknown') lastShotHome.outcome = 'off_target';
          if (ev.side === 1 && lastShotAway?.outcome === 'unknown') lastShotAway.outcome = 'off_target';
        } else if (ev.type === 18 || ev.type === 15) { // on target (saved)
          if (ev.side === 0 && lastShotAway?.outcome === 'unknown') lastShotAway.outcome = 'on_target';
          if (ev.side === 1 && lastShotHome?.outcome === 'unknown') lastShotHome.outcome = 'on_target';
        }
      }
    }
    await sleep(200);
  }

  // Any remaining unknown = blocked
  shots.forEach(s => { if (s.outcome === 'unknown') s.outcome = 'blocked'; });
  return shots;
}

// ─── API fetches ─────────────────────────────────────────────────────────────
async function fetchMatchList(beforeMatchId) {
  let url = `${MFL_BASE}/matches?past=true&limit=${LIMIT}`;
  if (beforeMatchId) url += `&beforeMatchId=${beforeMatchId}`;
  const res = await proxyFetch(url);
  if (!res) return [];
  const data = await res.json();
  return Array.isArray(data) ? data : [];
}

async function fetchReport(matchId) {
  const res = await proxyFetch(`${MFL_BASE}/matches/${matchId}/report`);
  if (!res) return null;
  return res.json();
}

async function fetchFormations(matchId) {
  const res = await proxyFetch(`${MFL_BASE}/matches/${matchId}?withFormations=true`);
  if (!res) return null;
  return res.json();
}

// ─── Supabase helpers ─────────────────────────────────────────────────────────
async function getExistingMatchIds() {
  const res = await fetch(
    `${SUPABASE_URL}/rest/v1/mfl_matches?select=id&order=id.desc&limit=2000`,
    { headers: hdrs }
  );
  if (!res.ok) throw new Error(`Supabase fetch error: ${res.status}`);
  const data = await res.json();
  return new Set(data.map(r => r.id));
}

async function insertMatch(match) {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/mfl_matches`, {
    method: 'POST',
    headers: { ...hdrs, 'Prefer': 'resolution=ignore-duplicates' },
    body: JSON.stringify(match),
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Insert match error: ${err}`);
  }
}

async function insertPlayerStats(stats) {
  if (!stats.length) return;
  const res = await fetch(`${SUPABASE_URL}/rest/v1/mfl_match_player_stats`, {
    method: 'POST',
    headers: { ...hdrs, 'Prefer': 'resolution=ignore-duplicates' },
    body: JSON.stringify(stats),
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Insert player stats error: ${err}`);
  }
}

async function insertShots(shots) {
  if (!shots.length) return;
  const res = await fetch(`${SUPABASE_URL}/rest/v1/mfl_match_shots`, {
    method: 'POST',
    headers: { ...hdrs, 'Prefer': 'resolution=ignore-duplicates' },
    body: JSON.stringify(shots),
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Insert shots error: ${err}`);
  }
}

// ─── Transform helpers ────────────────────────────────────────────────────────
function transformMatch(summary, formations) {
  const hf = formations?.homeFormation;
  const af = formations?.awayFormation;
  const hSquad = formations?.homeSquad;
  const aSquad = formations?.awaySquad;

  return {
    id: summary.id,
    status: summary.status,
    type: summary.type,
    start_date: summary.startDate ? new Date(summary.startDate).toISOString() : null,
    home_team: summary.homeTeamName || formations?.homeTeamName || '',
    home_club_id: hSquad?.club?.id ?? null,
    home_squad_id: hSquad?.id ?? summary.homeSquad?.id ?? null,
    home_score: summary.homeScore ?? formations?.homeScore ?? 0,
    home_formation: hf?.type ?? null,
    home_coach: formations?.homeCoachName ?? null,
    home_coach_wallet: formations?.homeCoachWalletAddress ?? summary.homeCoach ?? null,
    away_team: summary.awayTeamName || formations?.awayTeamName || '',
    away_club_id: aSquad?.club?.id ?? null,
    away_squad_id: aSquad?.id ?? summary.awaySquad?.id ?? null,
    away_score: summary.awayScore ?? formations?.awayScore ?? 0,
    away_formation: af?.type ?? null,
    away_coach: formations?.awayCoachName ?? null,
    away_coach_wallet: formations?.awayCoachWalletAddress ?? summary.awayCoach ?? null,
    stadium: formations?.stadium ?? null,
    engine: formations?.engine ?? null,
    seed: formations?.seed ?? null,
  };
}

function transformPlayerStats(matchId, report, formations) {
  const stats = [];

  const processPlayers = (playerStats, side) => {
    if (!playerStats?.length) return;
    const formationPositions = side === 'home'
      ? formations?.homeFormation?.positions
      : formations?.awayFormation?.positions;

    playerStats.forEach(p => {
      // Find formation data for this player
      const formPos = formationPositions?.find(fp => fp.player?.id === p.playerId);
      const meta = formPos?.player?.metadata ?? {};

      stats.push({
        match_id: matchId,
        player_id: p.playerId,
        side,
        first_name: meta.firstName ?? '',
        last_name: meta.lastName ?? '',
        overall: meta.overall ?? 0,
        position: p.position ?? '',
        age: meta.age ?? 0,
        nationality: (meta.nationalities ?? [])[0] ?? '',
        foot: meta.preferredFoot ?? '',
        pace: meta.pace ?? 0,
        shooting: meta.shooting ?? 0,
        passing: meta.passing ?? 0,
        dribbling: meta.dribbling ?? 0,
        defense: meta.defense ?? 0,
        physical: meta.physical ?? 0,
        goalkeeping: meta.goalkeeping ?? 0,
        captain: formPos?.captain ?? false,
        formation_index: formPos?.index ?? null,
        time_played: p.time ?? 5400,
        rating: p.rating ?? 0,
        goals: p.goals ?? 0,
        assists: p.assists ?? 0,
        shots: p.shots ?? 0,
        shots_on_target: p.shotsOnTarget ?? 0,
        shots_intercepted: p.shotsIntercepted ?? 0,
        xg: p.xG ?? 0,
        chances_created: p.chancesCreated ?? 0,
        passes: p.passes ?? 0,
        passes_accurate: p.passesAccurate ?? 0,
        crosses: p.crosses ?? 0,
        crosses_accurate: p.crossesAccurate ?? 0,
        dribbling_success: p.dribblingSuccess ?? 0,
        dribbled_past: p.dribbledPast ?? 0,
        defensive_duels_won: p.defensiveDuelsWon ?? 0,
        clearances: p.clearances ?? 0,
        yellow_cards: p.yellowCards ?? 0,
        red_cards: p.redCards ?? 0,
        fouls_committed: p.foulsCommitted ?? 0,
        fouls_suffered: p.foulsSuffered ?? 0,
        saves: p.saves ?? 0,
        goals_conceded: p.goalsConceded ?? 0,
        own_goals: p.ownGoals ?? 0,
        goal_times: p.goalsTimes ?? '',
      });
    });
  };

  processPlayers(report?.home?.playersStats, 'home');
  processPlayers(report?.away?.playersStats, 'away');
  return stats;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ─── Main ─────────────────────────────────────────────────────────────────────
async function main() {
  if (!SUPABASE_URL || !SUPABASE_KEY) throw new Error('Missing SUPABASE_URL or SUPABASE_KEY');

  console.log('Fetching existing match IDs from Supabase...');
  const existingIds = await getExistingMatchIds();
  console.log(`${existingIds.size} existing matches in database`);

  let newMatches = 0, newPlayerStats = 0, newShots = 0;
  let beforeMatchId = null;
  let done = false;
  let page = 1;

  while (!done) {
    console.log(`Fetching match list page ${page}${beforeMatchId ? ` (before ${beforeMatchId})` : ''}...`);
    const matches = await fetchMatchList(beforeMatchId);

    if (!matches.length) {
      console.log('No more matches returned — done.');
      break;
    }

    let hitExisting = false;
    for (const summary of matches) {
      const matchId = summary.id;

      if (existingIds.has(matchId)) {
        console.log(`Hit existing match ${matchId} — stopping.`);
        hitExisting = true;
        break;
      }

      console.log(`Processing match ${matchId}...`);
      try {
        const [report, formations, shots] = await Promise.all([
          fetchReport(matchId),
          fetchFormations(matchId),
          extractShots(matchId).catch(e => { console.warn(`  Shots failed: ${e.message}`); return []; }),
        ]);

        // Insert match
        const matchRow = transformMatch(summary, formations);
        await insertMatch(matchRow);

        // Insert player stats
        if (report) {
          const playerStats = transformPlayerStats(matchId, report, formations);
          await insertPlayerStats(playerStats);
          newPlayerStats += playerStats.length;
        }

        // Insert shots
        if (shots.length) {
          const shotRows = shots.map(s => ({ ...s, match_id: matchId }));
          await insertShots(shotRows);
          newShots += shots.length;
        }

        newMatches++;
        console.log(`  ✓ Match ${matchId}: ${shots.length} shots, ${report ? 22 : 0} player stats`);
      } catch (e) {
        console.error(`  ✗ Match ${matchId} failed: ${e.message}`);
      }

      beforeMatchId = matchId;
      await sleep(500);
    }

    if (hitExisting || matches.length < LIMIT) {
      done = true;
    } else {
      beforeMatchId = matches[matches.length - 1].id;
      page++;
      await sleep(500);
    }
  }

  console.log(`\nDone! ${newMatches} matches, ${newPlayerStats} player stats, ${newShots} shots inserted.`);
}

main().catch(err => {
  console.error('Fatal:', err.message);
  process.exit(1);
});
