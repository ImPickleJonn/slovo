# Slovo

Daily 5-letter word puzzle for Telegram. Stars-only IAP, 20-language UI, 12-language puzzle. Single-file frontend + Express backend.

## Quick start (local)

1. Install [Node.js LTS](https://nodejs.org).
2. Double-click `RUN.bat`. First run installs deps (~20s), then opens `http://localhost:3000` in your browser.
3. Play in dev mode — no Telegram needed. Server stubs a dev user.

## Architecture

```
slovo-project/
├── index.html              # Single-file frontend (UI + i18n + game logic + animations)
├── server.js               # Express server (game API + Stars IAP + bot + cron)
├── data/
│   ├── answers-{lang}.json # Curated daily-puzzle pool, per language
│   ├── valid-{lang}.json   # Accepted-guess superset
│   └── _raw-*.txt          # Raw downloads (git-ignored — rebuild via script)
├── scripts/
│   └── build-wordlists.js  # Cleans raw lists → clean JSON
├── versions/               # Manual snapshots before substantive edits
├── package.json
├── RUN.bat                 # Local dev launcher
└── render.yaml             # Render/Railway deploy config
```

**Server-authoritative gameplay** — the answer never reaches the client until the game ends. Each guess hits `/api/guess`, server computes the color pattern and returns it.

## Deploy (Render)

1. Push to GitHub.
2. **render.com** → **New +** → **Blueprint** → pick this repo. `render.yaml` provisions web service + Postgres in one shot.
3. In the dashboard, set the three secret env vars:
   - `BOT_TOKEN` — your @BotFather bot token
   - `ADMIN_TG_IDS` — comma-separated Telegram user IDs allowed to use admin endpoints
   - `PUZZLE_SALT` — any long random string (so the daily word can't be derived from a source-only view of the answer list)

   `DATABASE_URL` and `PUBLIC_DOMAIN` are auto-injected by the Blueprint — don't set manually.
4. After first deploy, hit `https://<your-host>/api/setup-webhook` once to register the webhook + bot commands with Telegram.
5. In **@BotFather**: `/newapp` → set Web App URL to `https://<your-host>` → `/setmenubutton` → set "▶️ Play" pointing at the same URL.

## Word lists

- `scripts/build-wordlists.js` reads `data/_raw-*.txt` files and emits clean `data/{answers,valid}-{lang}.json`.
- Cleanup: lowercase, NFC normalize, exact 5-codepoint length, script filter (Cyrillic vs Latin), Ё→Е for ru/uk, dedupe.
- Drop in a better list anytime: just replace `data/answers-{lang}.json` and restart. No code change needed.

### Sources (all permissive or NOASSERTION; verify if reusing commercially in your jurisdiction)
- **EN**: cfreshman gists (the original NYT Wordle lists, widely mirrored)
- **RU answers**: Hugo0/wordle (4,687 words); **RU valid**: mediahope/Wordle-Russian-Dictionary
- **UK**: Hugo0/wordle answers + sysfab/slovak valid
- **PL**: Hugo0/wordle (Apache-2.0 + MIT, the one with explicit license)
- **All others**: Hugo0/wordle (NOASSERTION; derived from Hunspell)

## IAP catalog (Telegram Stars)

| SKU | Stars | Grant |
|-----|-------|-------|
| `hint_pack` | 75⭐ | +5 single-letter hints |
| `streak_shield` | 99⭐ | 7-day streak insurance |
| `archive_unlock` | 149⭐ | Permanent access to all past puzzles |
| `theme_pack` | 199⭐ | 6 premium themes |
| `pro_monthly` | 299⭐ | 30 days of Pro (everything) |
| `pro_yearly` | 2,499⭐ | 365 days of Pro (30% off) |
| `gift_pro` | 299⭐ | Gift a friend 1 month of Pro |

Server is the source of truth — never trust client-claimed grants. Payment ledger is idempotent on `telegram_payment_charge_id`.

## Languages

**Full UI translation:** en, es, pt, fr, de, nl, it, sv, pl, tr, ru, uk, ar, he, hi, id, vi, zh, ko, ja (20).

**Full puzzle (curated word list):** en, es, pt, fr, de, nl, it, sv, pl, tr, ru, uk (12).

**UI-only (puzzle falls back to EN):** ar, he, hi, id, vi, zh, ko, ja. Wordle mechanics don't translate cleanly to logographic/syllabary scripts — these get the menu translated, but play the English word.

## Bot commands

| Command | Where | Action |
|---------|-------|--------|
| `/start` | DM | Welcome + Play button. Captures `ref_<uid>` for referrals. |
| `/play` | DM | Returns the play button. |
| `/stats` | DM | Your streak + win % |
| `/leaderboard` | Group | Today's group standings |
| `/share` | Anywhere | Posts your last result's share grid |

## Notification rhythm

Cron tick every 5 minutes:
- **Daily reminder** at user's preferred local hour (default 9:00).
- **Streak warning** at 22:00 local if streak ≥ 3 and not played today.

Users opt in via the settings toggle (`web_app_request_write_access`). No notifications unless explicitly enabled.

## Workflow

- Snapshot to `versions/slovo-vN.html` before substantive edits to `index.html`.
- Parse-check after any HTML edit:
  ```
  node -e "const h=require('fs').readFileSync('index.html','utf8');const m=[...h.matchAll(/<script>([\s\S]*?)<\/script>/g)];let i=0;for(const s of m){i++;try{new Function(s[1])}catch(e){console.log('FAIL '+i+': '+e.message);process.exit(1)}}console.log('OK ('+i+' blocks)')"
  ```
- Adding a new dep: `npm install <pkg> && git add package-lock.json` — Railway uses `npm ci` which requires the lockfile in sync.
