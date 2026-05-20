// Slovo — icon renderer. Reads SVGs from assets/ and emits PNGs at the
// sizes Telegram BotFather expects.
//
// Usage:
//   npm install --no-save sharp   # one-time
//   node scripts/build-icons.js
//
// Telegram targets:
//   - Bot avatar (via @BotFather → /setuserpic)   → 512×512 PNG square
//   - Mini App photo (via @BotFather → /newapp)   → 640×360 PNG landscape
//
// We also drop a 200×200 preview so you can see what the icon looks like
// at chat-list thumbnail size — that's where conversion happens.

const sharp = require('sharp');
const path = require('path');
const ASSETS = path.join(__dirname, '..', 'assets');

async function go() {
  const jobs = [
    { src: 'icon-512.svg',        out: 'icon-512.png',        w: 512, h: 512 },
    { src: 'icon-512.svg',        out: 'icon-200.png',        w: 200, h: 200 },  // preview
    { src: 'launch-640x360.svg',  out: 'launch-640x360.png',  w: 640, h: 360 },
  ];
  for (const j of jobs) {
    await sharp(path.join(ASSETS, j.src))
      .resize(j.w, j.h)
      .png({ compressionLevel: 9 })
      .toFile(path.join(ASSETS, j.out));
    console.log(`✓ ${j.out} (${j.w}×${j.h})`);
  }
}
go().catch(e => { console.error(e); process.exit(1); });
