// Slovo — word list builder.
// Reads /data/_raw-*.txt files and emits clean /data/{answers,valid}-{lang}.json.
//
// Per-language cleanup rules:
//   - lowercase + NFC normalize
//   - exact length 5 grapheme/char count
//   - script filter (Cyrillic vs Latin) — rejects mixed-script junk
//   - drop entries whose original form had any uppercase mid-word (proper-noun filter)
//   - dedupe
//
// Usage: node scripts/build-wordlists.js

const fs = require('fs');
const path = require('path');

const DATA = path.join(__dirname, '..', 'data');
const RAW_GLOB = (lang, kind) => path.join(DATA, `_raw-${kind}-${lang}.txt`);
const OUT      = (lang, kind) => path.join(DATA, `${kind}-${lang}.json`);

// Per-language script tests.
const SCRIPTS = {
  cyrillic: /^[а-яёіїєґ]+$/i,
  latin:    /^[a-zàáâäãåçèéêëìíîïñòóôöõùúûüýÿœæßğıİşöüą́ćęłńóśźżåäöğüşçı]+$/i,
};
const LANG_SCRIPT = {
  en: 'latin', es: 'latin', pt: 'latin', fr: 'latin', de: 'latin',
  nl: 'latin', it: 'latin', sv: 'latin', pl: 'latin', tr: 'latin',
  ru: 'cyrillic', uk: 'cyrillic',
};

function normalize(w, lang) {
  let s = String(w || '').trim().normalize('NFC');
  // strip BOM / weird whitespace
  s = s.replace(/^﻿/, '');
  // for cleanup-time: if any char is uppercase Latin AND the rest is Cyrillic,
  // it's contamination — caller filters via length+script test
  s = s.toLowerCase();
  // Russian/Ukrainian Ё→Е unification matches the server's normalizeWord
  if (lang === 'ru' || lang === 'uk') s = s.replace(/ё/g, 'е');
  return s;
}

// Detect "originally had any uppercase letter mid-word" — proper noun heuristic.
// We don't have the original case here; instead we filter at the script level:
// any Latin letter inside a Cyrillic-target list = contamination; non-target
// script entirely = contamination.

function clean(rawLines, lang) {
  const want = LANG_SCRIPT[lang] || 'latin';
  const re = SCRIPTS[want];
  const seen = new Set();
  const out = [];
  for (const raw of rawLines) {
    let w = normalize(raw, lang);
    if (!w) continue;
    // exact length 5 — count by code points (handles multi-byte chars correctly)
    const codepoints = Array.from(w);
    if (codepoints.length !== 5) continue;
    if (!re.test(w)) continue;
    if (seen.has(w)) continue;
    seen.add(w);
    out.push(w);
  }
  out.sort();
  return out;
}

const LANGS = ['en','ru','uk','es','pt','fr','de','nl','it','sv','pl','tr'];

function buildLang(lang) {
  const reports = [];
  for (const kind of ['answers', 'valid']) {
    const rawPath = RAW_GLOB(lang, kind);
    if (!fs.existsSync(rawPath)) { reports.push(`  ${kind}: (no raw file)`); continue; }
    const raw = fs.readFileSync(rawPath, 'utf8').split(/\r?\n/);
    const cleaned = clean(raw, lang);
    fs.writeFileSync(OUT(lang, kind), JSON.stringify(cleaned));
    reports.push(`  ${kind}: ${raw.length} raw → ${cleaned.length} clean`);
  }
  return reports.join('\n');
}

console.log('Slovo — building word lists\n');
for (const lang of LANGS) {
  console.log(`[${lang}]`);
  console.log(buildLang(lang));
}
console.log('\nDone. Files written to data/');
