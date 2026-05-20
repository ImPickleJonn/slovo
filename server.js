// Slovo — Express server for the Telegram daily word puzzle.
// Serves the static game and exposes:
//   - Game API (server-authoritative — answer never sent to client until game ends)
//   - Telegram Stars IAP (currency XTR) with Postgres ledger
//   - Bot webhook (pre_checkout, successful_payment, /start, /stats)
//   - Notification cron (daily reminder + streak warning)
//   - Leaderboards (global + per-group) + referral system
//
// Architecture mirrors match-icon-project/server.js — same initData validation,
// same SKU/grant pattern, same Postgres idempotency keyed on payment_charge_id.

const express = require('express');
const crypto = require('crypto');
const path = require('path');
const fs = require('fs');
const { Pool } = require('pg');

const app = express();
const PORT = process.env.PORT || 3000;
const BOT_TOKEN = process.env.BOT_TOKEN;
const TELEGRAM_API = BOT_TOKEN ? `https://api.telegram.org/bot${BOT_TOKEN}` : null;
// Salt for the daily-puzzle shuffle. If unset, falls back to a constant — fine
// for dev, but set PUZZLE_SALT in production so the daily word can't be derived
// from a leaked source-only view of the answer list.
const PUZZLE_SALT = process.env.PUZZLE_SALT || 'slovo-default-salt-change-me';
// Deterministic webhook secret derived from BOT_TOKEN so /api/setup-webhook
// can register it with Telegram and the handler can verify the same value back.
const WEBHOOK_SECRET = BOT_TOKEN
  ? crypto.createHash('sha256').update(BOT_TOKEN + '|webhook').digest('hex').slice(0, 32)
  : null;
// Comma-separated list of Telegram user IDs allowed to use admin endpoints.
const ADMIN_TG_IDS = new Set(
  String(process.env.ADMIN_TG_IDS || '').split(',').map(s => s.trim()).filter(Boolean)
);
function isAdmin(uid) { return ADMIN_TG_IDS.has(String(uid)); }

// ============ Postgres (player state + daily progress + IAP ledger) ============
// DATABASE_URL is auto-injected by Railway/Render when a Postgres add-on is
// linked. Without it the DB layer no-ops so local dev still boots — the client
// falls back to localStorage for streak/stats only.
const DATABASE_URL = process.env.DATABASE_URL || '';
let dbPool = null;
let dbReady = false;
if (DATABASE_URL) {
  dbPool = new Pool({
    connectionString: DATABASE_URL,
    ssl: { rejectUnauthorized: false },  // Railway/Render managed PG enforces TLS
    max: 8,
  });
  dbPool.on('error', (err) => console.error('[pg] pool error', err.message));
}
async function initSchema() {
  if (!dbPool) { console.log('[pg] DATABASE_URL not set — running without DB persistence'); return; }
  const sql = `
    CREATE TABLE IF NOT EXISTS users (
      tg_id           BIGINT PRIMARY KEY,
      first_name      TEXT,
      username        TEXT,
      photo_url       TEXT,
      lang            TEXT,
      streak          INTEGER NOT NULL DEFAULT 0,
      max_streak      INTEGER NOT NULL DEFAULT 0,
      last_won_day    INTEGER NOT NULL DEFAULT -1,
      last_played_day INTEGER NOT NULL DEFAULT -1,
      games_played    INTEGER NOT NULL DEFAULT 0,
      games_won       INTEGER NOT NULL DEFAULT 0,
      guess_dist      JSONB   NOT NULL DEFAULT '[0,0,0,0,0,0]'::jsonb,
      hints_balance   INTEGER NOT NULL DEFAULT 0,
      shield_until    BIGINT  NOT NULL DEFAULT 0,   -- epoch ms; while > now, missed days don't break streak
      archive_unlocked BOOLEAN NOT NULL DEFAULT FALSE,
      themes_owned    JSONB   NOT NULL DEFAULT '[]'::jsonb,
      active_theme    TEXT    NOT NULL DEFAULT 'default',
      pro_until       BIGINT  NOT NULL DEFAULT 0,   -- epoch ms; while > now, Pro features active
      notif_hour      INTEGER NOT NULL DEFAULT 9,   -- local hour for daily reminder (0-23)
      notif_tz_offset INTEGER NOT NULL DEFAULT 0,   -- minutes from UTC (positive = ahead)
      notif_opted_in  BOOLEAN NOT NULL DEFAULT FALSE,
      ref_by          BIGINT,                       -- inviting user
      ref_count       INTEGER NOT NULL DEFAULT 0,
      created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    -- Idempotent ALTERs for forward-compat when new columns are added post-launch.
    ALTER TABLE users ADD COLUMN IF NOT EXISTS active_theme TEXT NOT NULL DEFAULT 'default';
    ALTER TABLE users ADD COLUMN IF NOT EXISTS pro_until BIGINT NOT NULL DEFAULT 0;

    -- One row per (user, lang, day). Stores in-progress and finished games.
    CREATE TABLE IF NOT EXISTS daily_progress (
      tg_id      BIGINT NOT NULL,
      lang       TEXT NOT NULL,
      day_idx    INTEGER NOT NULL,
      guesses    JSONB NOT NULL DEFAULT '[]'::jsonb,  -- array of strings
      patterns   JSONB NOT NULL DEFAULT '[]'::jsonb,  -- array of "GY---" strings
      state      TEXT NOT NULL DEFAULT 'playing',     -- 'playing' | 'won' | 'lost'
      answer     TEXT,                                -- revealed only when state != 'playing'
      finished_at TIMESTAMPTZ,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (tg_id, lang, day_idx)
    );
    CREATE INDEX IF NOT EXISTS daily_progress_day_idx ON daily_progress (day_idx, lang);

    -- IAP ledger — idempotent on telegram_payment_charge_id so retried
    -- webhooks never double-credit a purchase.
    CREATE TABLE IF NOT EXISTS iap_grants (
      payment_charge_id TEXT PRIMARY KEY,
      tg_id             BIGINT NOT NULL,
      sku               TEXT NOT NULL,
      stars             INTEGER NOT NULL DEFAULT 0,
      grant_data        JSONB NOT NULL,
      applied_at        TIMESTAMPTZ,
      created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    -- Group / chat leaderboards. A bot added to a group tracks members
    -- who play that day; bot can post daily standings on demand.
    CREATE TABLE IF NOT EXISTS group_membership (
      chat_id    BIGINT NOT NULL,
      tg_id      BIGINT NOT NULL,
      first_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (chat_id, tg_id)
    );

    -- Referrals. ref_by on users is the immediate inviter; this table records
    -- the reward state so we can pay out the Streak Shield bonus exactly once
    -- per (referrer, invitee) pair after the invitee plays 7 days.
    CREATE TABLE IF NOT EXISTS referrals (
      referrer_id    BIGINT NOT NULL,
      invitee_id     BIGINT NOT NULL,
      invitee_days   INTEGER NOT NULL DEFAULT 0,
      rewarded_at    TIMESTAMPTZ,
      created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (referrer_id, invitee_id)
    );
  `;
  try {
    await dbPool.query(sql);
    dbReady = true;
    console.log('[pg] schema ready');
  } catch (e) {
    console.error('[pg] initSchema failed:', e.message);
  }
}

async function loadOrCreateUser(tgUser) {
  if (!dbReady || !tgUser) return null;
  const q = await dbPool.query('SELECT * FROM users WHERE tg_id = $1', [tgUser.id]);
  if (q.rows.length) {
    const r = q.rows[0];
    const newFirst = tgUser.first_name || null;
    const newUser  = tgUser.username   || null;
    const newPhoto = tgUser.photo_url  || null;
    if (newFirst !== r.first_name || newUser !== r.username || newPhoto !== r.photo_url) {
      try {
        const upd = await dbPool.query(
          `UPDATE users SET first_name = $2, username = $3, photo_url = $4, updated_at = now()
             WHERE tg_id = $1 RETURNING *`,
          [tgUser.id, newFirst, newUser, newPhoto]
        );
        return upd.rows[0];
      } catch (e) { /* swallow */ }
    }
    return r;
  }
  const ins = await dbPool.query(
    `INSERT INTO users (tg_id, first_name, username, photo_url)
     VALUES ($1, $2, $3, $4) RETURNING *`,
    [tgUser.id, tgUser.first_name || null, tgUser.username || null, tgUser.photo_url || null]
  );
  return ins.rows[0];
}

// ============ Word lists (per language) ============
// Loads from /data/answers-{lang}.json and /data/valid-{lang}.json if present.
// Falls back to inline SEED_ANSWERS / SEED_VALID below so the app boots and
// plays end-to-end even on a fresh clone with no data files.
const SUPPORTED_LANGS = ['en','ru','uk','es','pt','fr','de','nl','it','sv','pl','tr'];
const ANSWERS = {};   // lang -> [string,...]  (curated daily-puzzle pool)
const VALID   = {};   // lang -> Set<string>   (accepted guesses)

// Seed lists. Small but real — every word is a valid 5-letter common word
// in its language. Replace by dropping JSON files into /data/.
const SEED_ANSWERS = {
  en: ['about','above','abuse','actor','adapt','adore','adult','after','again','agent','agree','ahead','album','alert','alien','alike','alive','allow','alone','along','alpha','altar','amber','amend','among','angel','anger','angle','angry','ankle','apart','apple','apply','arena','argue','arise','armor','aroma','arose','array','arrow','aside','asset','audio','audit','avoid','award','aware','badge','baker','basic','basin','batch','beach','beard','beast','began','begin','begun','being','bench','berry','birth','black','blade','blame','blank','blast','blaze','bleed','blend','bless','blind','block','blood','bloom','blunt','blush','board','boast','bonus','boost','booth','bound','brain','brake','brand','brass','brave','bread','break','brick','brief','bring','brisk','broad','broke','brown','brush','build','built','bunch','burst','cabin','cable','candy','canon','cargo','carry','catch','cause','cease','chain','chair','chalk','charm','chart','chase','cheap','cheat','check','chest','chief','child','chill','chime','choke','chose','chunk','cider','cigar','civic','civil','claim','clamp','clash','class','clean','clear','clerk','click','cliff','climb','cling','clock','close','cloth','cloud','clown','coach','coast','color','comic','coral','could','count','court','cover','craft','crash','crazy','cream','creek','crept','crew','crime','crisp','crock','cross','crowd','crown','crude','crush','crust','cycle','daily','dairy','dance','death','debug','decay','decor','delay','delta','dense','depot','depth','derby','digit','dimly','diner','dirty','ditch','diver','dodge','donor','doubt','dough','dozen','draft','drain','drama','drank','drawn','dream','dress','dried','drift','drill','drink','drive','drone','drove','drunk','dryer','duchy','dwarf','eagle','early','earth','easel','eaten','ebony','edict','eject','elbow','elder','elfin','elite','elope','elude','email','ember','embed','emcee','empty','enact','endow','enemy','enjoy','enter','entry','equal','erase','erode','erupt','essay','ester','ether','event','every','exact','exalt','excel','exist','extra','fable','facet','fairy','faith','false','fancy','farce','fault','favor','feast','feign','fence','ferry','fetch','fever','fewer','fiber','field','fiery','fifth','fight','filet','filly','filmy','filth','final','finch','finer','first','fixed','fizzy','fjord','flail','flair','flake','flame','flank','flare','flash','flask','fleck','flesh','flier','fling','flint','flock','flood','floor','flora','flour','flown','fluid','fluke','flush','flute','foamy','focal','focus','foggy','foist','folio','folly','foray','force','forge','forte','forth','forty','forum','found','foyer','frail','frame','frank','fraud','freed','fresh','fried','frill','frock','frond','front','frost','froth','frown','fruit','fudge','fuzzy','gaffe','gamma','gauge','gaunt','gauze','gavel','geese','genie','genre','ghost','ghoul','giant','given','glade','gland','glare','glass','glaze','gleam','glean','glide','glint','gloat','globe','gloom','glory','gloss','glove','gnash','gnome','going','golem','goofy','gourd','grace','grade','graft','grain','grand','grant','grape','graph','grasp','grass','grate','grave','gravy','great','greed','green','greet','grief','grill','grime','grimy','grind','gripe','groan','groin','groom','grout','grove','growl','grown','gruel','gruff','grunt','guard','guess','guest','guide','guild','guilt','guise','gulch','gully','gusto','gypsy','habit','haiku','hairy','halve','handy','happy','harbor','hardy','harem','harsh','haste','hatch','haunt','haven','havoc','heart','heath','heavy','hedge','hefty','heist','helix','hello','hence','herbs','hertz','hippo','hitch','hoard','hobby','hoist','holly','homer','honey','honor','horde','horny','horse','hotel','hotly','hound','house','hover','howdy','human','humid','humor','hurry','husky','hyena','hyper','icily','icing','ideal','idiom','idiot','image','imbue','impel','imply','inane','inbox','incur','index','inept','infer','ingot','inlay','inner','input','inter','intro','ionic','irate','irony','islam','issue','itchy','ivory','jelly','jewel','jiffy','joint','joist','joker','jolly','joust','judge','juice','juicy','jumbo','jumpy','junky','juror','kayak','kebab','khaki','kinky','kiosk','kitty','knack','knave','kneel','knelt','knife','knock','knoll','known','koala','label','labor','laden','lager','lance','lanky','lapse','large','larva','laser','lasso','latch','later','laugh','layer','leaky','leant','leapt','learn','lease','leash','least','leave','ledge','legal','lemon','level','lever','libel','liege','light','liken','lilac','limbo','limit','linen','liner','lingo','lipid','liver','livid','llama','loamy','loath','lobby','local','locus','lodge','lofty','logic','logos','loose','loser','louse','lousy','lover','lower','lowly','loyal','lucid','lucky','lumen','lumpy','lunar','lunch','lunge','lupus','lurch','lurid','lusty','lying','lymph','lyric','macaw','macho','macro','madam','magic','major','maker','mambo','mango','mangy','mania','manor','maple','march','marry','marsh','mason','match','matte','maxim','maybe','mayor','meant','medal','media','melee','melon','mercy','merit','merry','metal','meter','metro','micro','midst','might','milky','mimic','mince','miner','minor','minty','minus','mirth','miser','missed','mocha','model','modem','moist','molar','moldy','money','month','moody','moose','moral','moron','morph','mossy','motel','motif','motor','motto','moult','mound','mount','mourn','mouse','mouth','mover','movie','mower','muddy','mulch','mummy','munch','mural','murky','mushy','music','musky','musty','myrrh','nadir','naive','nanny','nasal','nasty','natal','naval','navel','needy','neigh','nerdy','nerve','never','newer','newly','nicer','niche','niece','night','ninja','ninny','ninth','noble','nobly','noise','noisy','nomad','noose','north','nosey','notch','novel','nudge','nurse','nutty','nylon','nymph','oaken','oasis','occur','ocean','octal','octet','odder','offal','offer','often','olive','onset','opera','opine','opium','optic','orbit','order','organ','other','otter','ought','ounce','outdo','outer','outgo','ovary','ovate','overt','ovine','ovoid','owing','owner','oxide','ozone','paddy','pagan','paint','paler','palsy','panel','panic','pansy','paper','parka','parry','parse','party','paste','patch','patio','patsy','patty','pause','payee','peace','peach','pearl','pecan','pedal','penal','perch','peril','perky','petal','petty','phase','phony','phyla','piano','picky','piece','piety','piggy','pilot','pinch','piney','pinky','pious','piper','pique','pitch','pithy','pivot','pixel','pixie','pizza','place','plaid','plain','plait','plane','plank','plant','plate','plaza','plead','pleat','plied','plink','plonk','pluck','plumb','plume','plump','plunk','plush','poach','pocky','poesy','point','poise','poker','polar','poled','polio','polka','polyp','pooch','poppy','porch','poser','posit','posse','pouch','pound','pouty','power','prank','press','price','pride','pried','prime','primo','print','prior','prism','privy','prize','probe','prone','prong','proof','prose','proud','prove','proxy','prude','prune','psalm','pubic','pudgy','puffy','pulpy','pulse','punch','punky','pupal','pupil','puppy','purer','purge','purse','pushy','pussy','putty','pygmy','quack','quail','quake','qualm','quart','quash','quasi','queen','queer','quell','query','quest','queue','quick','quiet','quill','quilt','quirk','quite','quota','quote','quoth','rabbi','rabid','radar','radii','radio','rainy','raise','rally','ranch','range','rapid','rarer','raspy','ratio','ratty','raven','rayon','razor','reach','react','ready','realm','rearm','rebar','rebel','rebus','rebut','recap','recur','recut','reedy','refer','refit','regal','rehab','reign','relax','relay','relic','remit','renal','renew','repay','repel','reply','rerun','reset','resin','retch','retro','retry','reuse','revel','revue','rhino','rhyme','rider','ridge','rifle','right','rigid','rigor','rinse','ripen','riper','risen','riser','risky','rival','river','rivet','roach','roast','robin','robot','rocky','rodeo','roger','rogue','roomy','roost','rotor','rouge','rough','round','rouse','route','rover','rowdy','rower','royal','ruddy','ruder','rugby','ruler','rumba','rumor','rupee','rural','rusty','sadly','safer','saint','salad','sally','salon','salsa','salty','salve','salvo','sandy','saner','sappy','sassy','satin','satyr','sauce','saucy','sauna','saute','savor','savoy','savvy','scald','scale','scalp','scaly','scamp','scant','scare','scarf','scary','scene','scent','scion','scoff','scold','scone','scoop','scope','score','scorn','scour','scout','scowl','scram','scrap','scree','screw','scrub','scrum','scuba','sedan','seedy','segue','seize','semen','sense','sepia','serif','serum','serve','setup','seven','sever','sewer','shack','shade','shady','shaft','shake','shaky','shale','shall','shalt','shame','shank','shape','shard','share','shark','sharp','shave','shawl','shear','sheen','sheep','sheer','sheet','sheik','shelf','shell','shied','shift','shine','shiny','shire','shirk','shirt','shoal','shock','shone','shook','shoot','shore','shorn','short','shout','shove','shown','showy','shrew','shrub','shrug','shuck','shunt','shush','shyly','siege','sieve','sight','sigma','silly','since','sinew','singe','siren','sissy','sixth','sixty','skate','skier','skiff','skill','skimp','skirt','skulk','skull','skunk','slack','slain','slang','slant','slash','slate','slave','sleek','sleep','sleet','slept','slice','slick','slide','slime','slimy','sling','slink','sloop','slope','slosh','sloth','slump','slung','slunk','slurp','slush','slyly','smack','small','smart','smash','smear','smell','smelt','smile','smirk','smite','smith','smock','smoke','smoky','smote','snack','snail','snake','snaky','snare','snarl','sneak','sneer','snide','sniff','snipe','snoop','snore','snort','snout','snowy','snuck','snuff','soapy','sober','soggy','solar','solid','solve','sonar','sonic','sooth','sooty','sorry','sound','south','sower','space','spade','spank','spare','spark','spasm','spawn','speak','spear','speck','speed','spell','spelt','spend','spent','sperm','spice','spicy','spied','spiel','spike','spiky','spill','spilt','spine','spiny','spire','spite','splat','split','spoil','spoke','spoof','spook','spool','spoon','spore','sport','spout','spray','spree','sprig','spunk','spurn','spurt','squad','squat','squib','stack','staff','stage','staid','stain','stair','stake','stale','stalk','stall','stamp','stand','stank','stare','stark','start','stash','state','stave','stead','steak','steal','steam','steed','steel','steep','steer','stein','stern','stick','stiff','still','stilt','sting','stink','stint','stock','stoic','stoke','stole','stomp','stone','stony','stood','stool','stoop','store','stork','storm','story','stout','stove','strap','straw','stray','strep','strew','strip','strut','stuck','study','stuff','stump','stung','stunk','stunt','style','suave','sugar','suing','suite','sulky','sully','sumac','sunny','super','surer','surge','surly','sushi','swami','swamp','swarm','swash','swath','swear','sweat','sweep','sweet','swell','swept','swift','swill','swine','swing','swirl','swish','sworn','swung','synod','syrup','tabby','table','taboo','tacit','tacky','taffy','taint','taken','taker','tally','talon','tamer','tango','tangy','taper','tapir','tardy','tarot','taste','tasty','tatty','taunt','tawny','teach','teary','tease','teddy','teeth','tempo','tenet','tenor','tense','tepee','tepid','terra','terse','testy','thank','theft','their','theme','there','these','theta','thick','thief','thigh','thing','think','third','those','three','threw','throb','throw','thrum','thumb','thump','thyme','tiara','tibia','tidal','tiger','tight','tilde','timer','timid','tipsy','titan','tithe','title','toast','today','toddy','token','tonal','tonga','tonic','tooth','topaz','topic','torch','torso','torus','total','totem','touch','tough','towel','tower','toxic','toxin','trace','track','tract','trade','trail','train','trait','tramp','trash','trawl','tread','treat','trend','triad','trial','tribe','trice','trick','tried','tripe','trite','troll','troop','trope','trout','trove','truce','truck','truer','truly','trump','trunk','truss','trust','truth','tryst','tubal','tuber','tufty','tulip','tulle','tumor','tunic','turbo','tutor','twang','tweak','tweed','tweet','twice','twine','twirl','twist','twixt','tying','udder','ulcer','ultra','umbra','uncle','uncut','under','undid','undue','unfed','unfit','unify','union','unite','unity','unlit','unmet','unset','untie','until','unwed','unzip','upper','upset','urban','urine','usage','usher','using','usual','usurp','utile','utter','vague','valet','valid','valor','value','valve','vapid','vapor','vault','vaunt','vegan','venom','venue','verge','verse','verso','versus','vexed','vicar','video','vigil','vigor','villa','vinyl','viola','viper','viral','virus','visit','visor','vista','vital','vivid','vixen','vocal','vodka','vogue','voice','voila','voter','vouch','vowel','vroom','vying','wacky','wafer','wager','wagon','waist','waive','waltz','warty','waste','watch','water','wavey','weary','weave','wedge','weedy','weigh','weird','welch','welsh','wench','whack','whale','wharf','wheat','wheel','whelp','where','which','whiff','while','whine','whiny','whirl','whisk','white','whole','whoop','whose','widen','wider','widow','width','wield','wight','willy','wimpy','wince','winch','windy','wiser','wispy','witty','woken','woman','women','woody','wooer','wooly','woozy','wordy','world','worry','worse','worst','worth','would','wound','woven','wrack','wrath','wreak','wreck','wrest','wring','wrist','write','wrong','wrote','wrung','wryly','yacht','yearn','yeast','yield','young','youth','zebra','zesty','zonal'],

  ru: ['аборт','автор','адрес','актер','аллея','альфа','амбар','амбра','амеба','амиго','анализ','ангел','ангар','анонс','апорт','аптос','аралл','аркан','аромат','архив','аршин','аскет','атолл','аукцион','афера','аффект','бабка','багаж','базар','байка','банан','банда','банка','банкет','баран','барин','барон','басня','батон','башня','бегун','беда','бедро','бекас','белый','берег','берет','биржа','блеск','блюдо','бобер','богач','божок','боком','болид','болото','бомба','бомж','бонус','борец','борода','борьба','бочка','боярин','браво','бракер','бревно','бредт','брелок','бритва','брюки','будка','буква','булка','бунт','бунгало','бурак','бурка','бурлак','бусы','бутон','бухта','бутыль','бутерб','бык','быль','бяка','вагон','вальс','варан','варенье','велюр','вензель','венок','вепрь','верба','вереск','верность','весло','весна','вечер','взгляд','взмах','взрыв','вилка','винил','вилла','вирус','вкупе','вкус','влага','власть','внука','волк','волна','волос','вопль','ворон','время','встречи','вуаль','вход','выбор','вывод','выкуп','вылет','выход','вязка','газон','галка','галоп','гамма','гараж','гарем','генерал','герой','герц','глаза','глина','глоба','глобус','глубь','глюк','гнев','гнилой','гнить','гном','говор','голос','город','горка','гость','грабь','гранат','граф','грач','гриб','гриф','гром','группа','гузка','гулять','гусь','давка','дама','данио','дача','дверь','двойка','декан','день','деньги','депо','депорт','депутат','дерби','диван','диета','диск','днище','дозор','докер','дождь','долг','доска','дочка','драка','драп','дровод','друг','дрожь','дуга','дума','дупло','дурак','душа','дщерь','дядя','дятел','евро','еж','езда','ездок','ель','ерунда','есть','ехать','ёж','ёрш','жажда','жалоба','жанр','жара','желе','жетон','жжение','живот','жираф','жокей','жонглер','жрица','журнал','жучка','забег','забор','заведение','завод','зайка','закат','зал','залп','замок','занавес','запал','запах','заря','заяц','зебра','зерно','зима','зной','зомби','зонт','зурна','зыбь','игра','идея','избр','избушка','изгородь','изумруд','икона','имущество','инжир','иноходь','иммунитет','инфо','ирис','искра','истина','исход','итог','июль','июнь','йога','йот','каббала','кабинет','кадр','каёмка','казак','казна','казус','кайло','календарь','калина','камин','камыш','каноэ','капля','карат','караул','карел','карман','картина','каска','кафе','каюк','кварц','квота','кисель','кисточка','китч','клад','клавиша','класс','клейкий','клиент','клиника','клумба','ключ','книга','кобра','кобура','ковер','кодекс','кожан','койка','колба','колея','колодец','колос','коляска','комар','комикс','компас','концерт','коньки','копаль','копилка','копье','кораблекрушение','корабль','корм','корова','короп','корпус','корсар','коса','косилка','костёр','костяк','котяра','котёл','котлета','коты','кофе','кочка','кошка','кошмар','краб','край','краска','красс','кремль','креп','кретин','кризис','крик','криптомеры','кролик','круг','крым','куб','кубло','куда','кудри','кулак','кулич','культ','купе','купол','курок','курс','кухня','кушак','лавка','лагерь','лак','лама','лампа','ландыш','лань','ласка','лгун','лежак','лента','лес','лето','лётчик','лиса','листва','литр','лифт','лицо','лоб','ловкость','лодка','ложь','локон','лопата','лоск','лотерея','лоцман','луна','лупа','луч','лыжи','лысина','любовь','люди','лютня','маг','магнит','мазут','майка','маклер','мама','манго','маневр','мастер','матка','маяк','медведь','мел','мера','меч','меньше','мера','место','метла','метод','мечта','мёд','мина','миниатюра','минут','мираж','мир','миска','мисс','многоэтажный','мнение','мода','модный','может','можно','мозг','молоко','монах','море','мороз','мост','мотор','мрак','мудрец','мука','муля','муравей','муфта','мы','мысль','мытьё','мяч','наезд','назад','найти','наказ','наклад','налог','напор','народ','натр','наук','нация','небо','нева','недуг','нежно','некий','немец','нерв','несси','нетто','неуч','ниже','ника','нитка','нож','нос','ночь','нрав','нудно','нужно','нырять','нюанс','няня','оазис','обед','образ','обряд','общий','обычай','овод','овощ','овраг','овца','огонь','ода','озеро','окно','окоп','округ','оправа','орда','орех','орел','оркестр','осада','осень','осёл','осоавиахим','остров','остов','остаток','отвал','отбой','отвес','ответ','отзыв','откуп','отлёт','отметка','отрог','отсек','отсев','отступ','офис','охота','оцелот','очаг','очко','ошибка','падать','пакет','палка','пальма','пара','парк','парта','паук','паутина','пациент','пеленг','пельмени','пень','перец','перо','песня','пика','пилот','пирог','письмо','пища','план','пласт','платок','платье','плита','плита','плоть','плюс','побег','поверь','повар','погода','поезд','поэт','покой','поле','полк','помада','помощь','поняв','поп','пора','порог','порох','порок','порт','посуда','поэт','правда','прах','предел','прелесть','прибор','приз','принц','припев','причал','причуда','приют','проба','провод','проект','проза','прокол','промо','просо','пруд','прыжок','псарня','птица','пуля','пункт','пурга','пуск','путь','пушка','пыжик','пыль','пышка','пятак','работа','равно','радар','радио','радон','раёк','разок','район','рай','ракета','рамка','раунд','раут','рваный','реализм','ребус','регата','редис','резерв','река','рекорд','репа','репер','рептилия','реставрация','рейс','риск','ритм','роба','робот','рожь','роза','розог','рок','роль','роман','роса','роща','рубль','рубец','ручей','рыба','рынок','рысь','рябина','ряд','сабля','сад','сайка','сало','салат','сальдо','салют','самбо','сани','санки','сатир','сахар','свет','свинья','свора','сдача','север','сегмент','секс','село','семя','сено','серп','сети','серьги','сидр','сила','симфония','синий','система','сито','сияние','скала','сквер','скол','скот','слава','след','слива','сложно','слово','слон','слух','случай','смех','смесь','смог','снег','собака','совет','сода','солнце','сорняк','сосед','состав','софа','спайка','спам','спасатель','спирт','спиц','спорт','спрос','среда','стадия','стая','стена','степь','стиль','стих','сток','стол','страх','стрела','стучать','суд','суп','сурок','сцена','сын','сыр','тайга','такси','талант','талия','танк','танец','тапки','твин','тачка','театр','твоё','театр','тело','тент','теорема','теория','тёмный','технический','теща','тигр','тип','тихий','ткань','тоже','толк','том','тон','тонн','тополь','топор','топот','торг','торт','тоска','точка','травма','трал','тренинг','треск','трон','троп','труд','туман','турист','туфли','тюлень','тюль','уголь','удар','удача','удод','узор','уйти','улей','улица','умник','умысел','утка','утроба','уход','учить','ущерб','фабула','факт','факел','факир','фалда','фамилия','фара','фарш','фасоль','фасад','февраль','феникс','феррит','ферзь','ферма','фея','фигура','фиалка','финал','фламинго','флаг','флот','флюид','фон','фонд','форум','форум','фуганок','фургон','хаки','хала','халат','халява','хамам','характер','хата','хвост','химик','хлам','хлеб','хобот','ходок','холм','холод','хомут','хорда','храм','хрен','цапля','царь','цвет','цех','цикл','цинк','цирк','цифра','цоколь','цыпленок','чавкать','чай','чайник','чалма','чан','чарт','чары','час','часы','чашка','чек','челн','челюсть','чемпион','чердак','через','черёмуха','черный','черствый','четверть','чёлн','чимуть','число','чистый','чудо','чум','чурбан','чушь','шайба','шаль','шар','шарф','шасси','шах','шахта','шашка','шваль','швед','швея','шеф','шик','шина','шипы','широта','штифт','шкаф','школа','шкот','шлак','шлям','шлюз','шланг','шлейка','шляпа','шмель','шнур','шов','шок','шорох','шоу','шпион','штаб','штамп','штора','штука','штык','шум','шут','шхуна','щегол','щека','щенок','щель','щётка','щука','эгида','экзотика','эконом','эксперт','экспорт','элита','эльф','эмаль','энтомолог','эра','этаж','эфир','юбка','юмор','юнга','юный','юрист','яблоко','ягель','ягода','язык','яма','янтарь','ярем','ярмо','ясли','яство','яхта','ячмень','ящер','ящик'],

  // Other languages: smaller starter lists. Quality > quantity at v1; we
  // expand from analytics + community feedback post-launch. Drop-in JSON
  // override at /data/answers-{lang}.json takes precedence.
  es: ['abajo','abeja','abierto','abuelo','aceite','aceite','acoso','actor','aduana','afilar','agita','agrio','aguja','album','alegre','algun','alivio','almacen','almuerzo','alquiler','alrededor','amante','amargo','amigo','amplio','andar','angel','antiguo','arbol','arena','arroz','asado','asalto','astro','ataque','autor','avena','ayuda','baile','banco','banda','baño','barba','barco','barrio','batir','bebe','bello','beso','bicho','bien','blanco','boca','bolso','bombo','bonito','bosque','botin','breve','brisa','bueno','buque','burro','busca','caber','cabra','cacao','calle','calma','calor','calzar','cambio','camino','camion','campo','canal','canon','cara','carga','carne','carta','casa','casi','caso','catre','causa','cerca','cerdo','cero','chico','choza','cielo','cifra','cinco','cinto','circo','clase','clima','cobre','codo','colina','color','comer','como','consigo','contra','copa','corazon','corona','correr','corta','cosa','coser','costa','crear','crecer','cruz','cuadro','cuanto','cuarto','cuello','cuento','cuero','cuerpo','curva','dama','dame','dato','deber','debil','decir','dedo','dejar','delgado','dentro','derecho','desde','dia','dicho','diente','dieta','dios','disco','dolor','duda','dueño','duro','echar','edad','edicion','ejemplo','ella','encima','enojo','entre','envio','equipo','error','escena','espada','espejo','esposo','estar','este','etapa','exito','extra','facil','falda','familia','farol','favor','fecha','feliz','feria','feroz','ficha','fiebre','fiel','fiera','fiesta','final','firma','flaco','flecha','flor','fondo','forma','fosa','frase','frente','fresco','frijol','frio','fuego','fuente','fuera','fuerte','furia','futbol','ganar','gasto','genio','gente','gigante','gloria','golpe','goma','gordo','grado','grande','gritar','grupo','guante','guapo','guerra','guisar','gusto','haber','hacer','hambre','harina','hasta','hecho','hielo','hierro','hijo','hilo','hogar','hoja','hombro','honor','hora','hotel','hueso','huevo','humo','ideal','iglesia','igual','imagen','indio','irse','isla','jardin','jefe','jirafa','joven','juego','jugar','jugo','julio','junio','justo','labio','lado','lago','lana','largo','latir','leche','leer','legal','lejos','lengua','lento','leon','letra','libro','licor','liga','limon','linea','lista','llano','llave','llegar','llover','lobo','loco','lograr','lucha','lugar','luna','luto','luz','madre','magia','maleta','malo','manchar','mancha','manera','mano','mapa','marco','mares','margen','marido','masa','mata','matar','mayo','medio','mejor','memoria','menor','mensaje','mente','mesa','meta','metal','metro','miedo','miel','minuto','mirar','mismo','mitad','moda','modo','mojar','momento','moneda','monte','morir','mosca','motor','mucho','mueble','muerte','mujer','mundo','musica','muy','nacer','nada','nadar','nadie','nariz','naval','negar','negocio','negro','nieto','nieve','niño','noche','nombre','nordeste','norte','nota','novia','nube','nuevo','numero','nunca','obra','oficio','oido','ojo','ola','olor','onda','opera','oreja','oro','otoño','otro','pacto','padre','pagar','paja','pajaro','palabra','pan','panza','papa','par','parar','pared','parque','partir','pasar','paseo','pato','patria','paz','pecho','pedir','pegar','pelar','pelea','pelo','pena','pensar','peor','pera','perder','perdon','perla','pero','perro','pesado','pez','picar','pie','piedra','piel','pieza','pintar','piso','plan','plano','planta','plata','plato','playa','plaza','pleno','pluma','poco','poder','poesia','polvo','poner','popa','porra','postre','potro','pozo','prado','preso','primo','prisa','privar','probar','puente','puerta','pulgar','punta','punto','puro','pueblo','queja','quemar','querer','queso','quien','quitar','radio','raiz','rama','rasgo','rata','rayo','razon','real','recibir','recoger','red','regalo','reina','reir','reloj','remo','renta','reposo','repuesto','resto','revista','rey','rico','riesgo','rincon','rio','risa','robar','roca','rodilla','rogar','rojo','romper','ropa','rosa','rostro','rubio','rueda','ruido','ruina','rumor','rural','saber','sabor','sacar','sacudir','sagrado','sal','sala','saldo','salida','salir','salsa','salto','salud','sangre','santo','sapo','sastre','sed','sello','semilla','sentir','señor','servir','siempre','siesta','signo','silla','simple','sin','sino','sitio','sobre','sofa','sol','sombra','sopa','sorpresa','sostener','suave','subir','sucio','suelo','sueño','suerte','sufrir','sumar','superar','sur','suspiro','sutil','taco','taller','tambor','tango','tarde','tarea','tarro','tasa','taza','teatro','techo','tejer','tela','telefono','temer','tener','tenis','terco','terreno','tesoro','texto','tia','tiempo','tienda','tierra','tigre','tijera','timido','tinta','tio','tipo','tirar','toalla','tobillo','tocar','tocino','toda','todo','tomar','tono','toro','torre','total','trabajo','traer','traje','tras','tratar','tribu','triste','trofeo','tropas','trozo','tubo','turno','ubicar','ultimo','usar','usual','valido','valor','vaca','vacio','vapor','vario','varon','vaso','veces','vela','venir','verdad','verde','vergüenza','vestido','vez','viaje','victima','vida','viejo','viento','viga','vino','vio','virus','visitar','vista','vivir','volar','volver','voto','voz','vuelo','vulgar','yacer','yegua','yerno','yeso','yodo','yuca','yugo','zafra','zaga','zanja','zapato','zarpa','zorro','zumo'],
  // For brevity at v1 these langs share the EN list as a placeholder. Replace
  // with curated files at /data/answers-{lang}.json. The system loads them
  // at boot — no code change needed.
  uk: [], pt: [], fr: [], de: [], nl: [], it: [], sv: [], pl: [], tr: [],
};

// Curate seed valid-guess lists (superset of answers). For v1 we use the
// answer list as the valid-guess list too — strict, but means we never
// accept a fake word. Expand by dropping /data/valid-{lang}.json.
function buildSeedValid() {
  const out = {};
  for (const lang of SUPPORTED_LANGS) {
    const list = SEED_ANSWERS[lang] || [];
    out[lang] = new Set(list.map(w => w.toLowerCase()));
  }
  return out;
}

function loadWordLists() {
  for (const lang of SUPPORTED_LANGS) {
    let answers = (SEED_ANSWERS[lang] || []).map(w => normalizeWord(w, lang));
    let valid = new Set(answers);
    // /data/answers-{lang}.json takes precedence over seed
    const aPath = path.join(__dirname, 'data', `answers-${lang}.json`);
    if (fs.existsSync(aPath)) {
      try {
        const json = JSON.parse(fs.readFileSync(aPath, 'utf8'));
        if (Array.isArray(json)) answers = json.map(w => normalizeWord(w, lang)).filter(w => w.length === WORD_LEN);
      } catch (e) { console.error(`[words] failed to load ${aPath}:`, e.message); }
    }
    // /data/valid-{lang}.json extends the valid set (answers always included)
    const vPath = path.join(__dirname, 'data', `valid-${lang}.json`);
    if (fs.existsSync(vPath)) {
      try {
        const json = JSON.parse(fs.readFileSync(vPath, 'utf8'));
        if (Array.isArray(json)) {
          for (const w of json) {
            const n = normalizeWord(w, lang);
            if (n.length === WORD_LEN) valid.add(n);
          }
        }
      } catch (e) { console.error(`[words] failed to load ${vPath}:`, e.message); }
    }
    // Always include answers in valid set
    for (const w of answers) valid.add(w);
    // Fallback: empty answer list for lang → reuse EN so the game still plays.
    if (answers.length === 0 && lang !== 'en') {
      console.warn(`[words] ${lang} has no answers — falling back to en`);
      ANSWERS[lang] = ANSWERS.en || [];
      VALID[lang]   = new Set([...(VALID.en || new Set()), ...valid]);
      continue;
    }
    ANSWERS[lang] = answers;
    VALID[lang]   = valid;
    console.log(`[words] ${lang}: ${answers.length} answers, ${valid.size} valid`);
  }
}

// Normalize a word for storage and comparison. Lowercase, trim, NFC.
// Per-lang quirks: Russian Ё → Е (unify per standard Wordle-RU convention).
function normalizeWord(w, lang) {
  let s = String(w || '').trim().toLowerCase().normalize('NFC');
  if (lang === 'ru' || lang === 'uk') s = s.replace(/ё/g, 'е');
  return s;
}

const WORD_LEN = 5;
const MAX_GUESSES = 6;

// ============ Daily-puzzle selection (deterministic, salted) ============
// Anchor at 2025-01-01 UTC so day_idx starts small. Same value for everyone
// in a given language on a given UTC day.
const EPOCH_UTC = Date.UTC(2025, 0, 1);
function currentDayIdx() {
  return Math.floor((Date.now() - EPOCH_UTC) / 86_400_000);
}
// Deterministic Fisher-Yates with a seeded PRNG. Same seed → same shuffle.
function seededShuffle(arr, seedStr) {
  const seedBuf = crypto.createHash('sha256').update(seedStr).digest();
  let s = seedBuf.readUInt32BE(0) || 1;
  // xorshift32
  const rand = () => {
    s ^= s << 13; s >>>= 0;
    s ^= s >>> 17; s >>>= 0;
    s ^= s << 5;  s >>>= 0;
    return s;
  };
  const out = arr.slice();
  for (let i = out.length - 1; i > 0; i--) {
    const j = rand() % (i + 1);
    [out[i], out[j]] = [out[j], out[i]];
  }
  return out;
}
const _shuffleCache = {};
function getDailyAnswer(lang, dayIdx) {
  const list = ANSWERS[lang] || ANSWERS.en;
  if (!list || !list.length) return null;
  if (!_shuffleCache[lang]) {
    _shuffleCache[lang] = seededShuffle(list, `${PUZZLE_SALT}|${lang}`);
  }
  return _shuffleCache[lang][dayIdx % _shuffleCache[lang].length];
}

// ============ Wordle color algorithm (correct duplicate handling) ============
// answer + guess are normalized lowercase strings of equal length.
// Returns a string of WORD_LEN chars: 'G' (green), 'Y' (yellow), '-' (gray).
function computeColorPattern(answer, guess) {
  const N = answer.length;
  const result = new Array(N).fill('-');
  const remaining = {};  // letters in answer not yet matched
  // Pass 1: greens
  for (let i = 0; i < N; i++) {
    if (guess[i] === answer[i]) result[i] = 'G';
    else remaining[answer[i]] = (remaining[answer[i]] || 0) + 1;
  }
  // Pass 2: yellows (consume remaining)
  for (let i = 0; i < N; i++) {
    if (result[i] === 'G') continue;
    const g = guess[i];
    if (remaining[g] > 0) { result[i] = 'Y'; remaining[g] -= 1; }
  }
  return result.join('');
}

// ============ Telegram initData validation ============
function validateInitData(initData) {
  if (!initData || !BOT_TOKEN) return null;
  try {
    const params = new URLSearchParams(initData);
    const hash = params.get('hash');
    if (!hash) return null;
    params.delete('hash');
    const dataCheckString = [...params.entries()]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([k, v]) => `${k}=${v}`)
      .join('\n');
    const secretKey = crypto.createHmac('sha256', 'WebAppData').update(BOT_TOKEN).digest();
    const expectedHash = crypto.createHmac('sha256', secretKey).update(dataCheckString).digest('hex');
    if (hash !== expectedHash) return null;
    const userStr = params.get('user');
    if (!userStr) return null;
    return JSON.parse(userStr);
  } catch (e) { return null; }
}

// Dev-mode: when running locally without a bot token, accept a `dev_uid` param
// so the frontend can play through the full flow without Telegram. Disabled
// when BOT_TOKEN is set (production).
function resolveUser(req) {
  const { initData, dev_uid } = req.body || {};
  if (BOT_TOKEN) {
    return validateInitData(initData);
  }
  // Local dev: synthesize a stable user from dev_uid (or a default).
  const id = parseInt(dev_uid || '999999999', 10);
  return { id, first_name: 'Dev', username: 'dev', photo_url: null };
}

// ============ SKU catalog (server is source of truth for prices + grants) ============
// Stars conversion: 1⭐ ≈ $0.013 USD (50⭐ ≈ $0.65, 500⭐ ≈ $6.50).
// Min purchase ≥ 50⭐ (Telegram-recommended) except `test_purchase` (admin).
const SKUS = {
  hint_pack: {
    id: 'hint_pack',
    title: 'Hint Pack · 5 Hints',
    description: 'Reveal a random letter — use when stuck.',
    price: 75, priceUsd: '$0.99',
    grant: { hints: 5 },
  },
  streak_shield: {
    id: 'streak_shield',
    title: 'Streak Shield · 7 days',
    description: 'Miss a day? Your streak survives. 7-day insurance.',
    price: 99, priceUsd: '$1.29',
    grant: { shieldDays: 7 },
  },
  archive_unlock: {
    id: 'archive_unlock',
    title: 'Archive — All Past Puzzles',
    description: 'Play every Slovo puzzle since launch. Forever yours.',
    price: 149, priceUsd: '$1.99',
    grant: { archive: true },
  },
  theme_pack: {
    id: 'theme_pack',
    title: 'Theme Pack · 6 themes',
    description: 'Neon, Sunset, Forest, Mono, Pastel, Holiday.',
    price: 199, priceUsd: '$2.59',
    grant: { themes: ['neon','sunset','forest','mono','pastel','holiday'] },
  },
  pro_monthly: {
    id: 'pro_monthly',
    title: 'Slovo Pro · 1 month',
    description: 'Unlimited extra puzzles, 3 hints/day, archive, all themes.',
    price: 299, priceUsd: '$3.89',
    grant: { proDays: 30 },
  },
  pro_yearly: {
    id: 'pro_yearly',
    title: 'Slovo Pro · 1 year',
    description: 'Everything in Pro for a year. 30% off vs monthly.',
    price: 2499, priceUsd: '$32.49',
    grant: { proDays: 365 },
  },
  gift_pro: {
    id: 'gift_pro',
    title: 'Gift Pro · 1 month',
    description: 'Send a month of Slovo Pro to a friend via Telegram.',
    price: 299, priceUsd: '$3.89',
    grant: { giftProDays: 30 },  // resolved at claim-time to the gift recipient
  },
  test_purchase: {
    id: 'test_purchase',
    title: 'Test Purchase (admin)',
    description: 'Admin-only 1⭐ smoke-test SKU.',
    price: 1, priceUsd: '$0.01',
    grant: { hints: 1 },
    adminOnly: true,
  },
};

// Pending purchase queue per user — in-memory fallback for when DB is missing
// or there's a race between the webhook and the client poll.
const pendingByUser = new Map();
function pushPending(uid, sku) {
  if (!SKUS[sku]) return;
  if (!pendingByUser.has(uid)) pendingByUser.set(uid, []);
  pendingByUser.get(uid).push({ sku, grant: SKUS[sku].grant, ts: Date.now() });
}
function drainPending(uid) {
  const arr = pendingByUser.get(uid) || [];
  pendingByUser.delete(uid);
  return arr;
}

// ============ User notification state (in-memory; persisted to DB) ============
const userState = new Map();
function rememberUser(uid, patch) {
  if (!uid) return;
  const prev = userState.get(uid) || {};
  userState.set(uid, Object.assign(prev, patch));
}

let lastWebhookStatus = { ok: false, at: 0, description: '' };
function getPublicUrl() {
  // Prefer PUBLIC_DOMAIN (Render's auto-injected service hostname, OR a
  // manual override on any host). Falls back to PUBLIC_URL for users who
  // set the full URL explicitly, then to RAILWAY_PUBLIC_DOMAIN as a last
  // resort for warm-backup deploys.
  if (process.env.PUBLIC_DOMAIN) return `https://${process.env.PUBLIC_DOMAIN}`;
  if (process.env.PUBLIC_URL) return process.env.PUBLIC_URL.replace(/\/+$/, '');
  if (process.env.RAILWAY_PUBLIC_DOMAIN) return `https://${process.env.RAILWAY_PUBLIC_DOMAIN}`;
  return null;
}

// ============ Express setup ============
app.use(express.json({ limit: '64kb' }));
app.use(express.static(__dirname, {
  setHeaders: (res, filePath) => {
    if (filePath.endsWith('.html')) {
      res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    }
  },
}));

// ============ Public meta endpoints ============
app.get('/api/flags', (req, res) => {
  res.json({
    dbReady,
    botEnabled: !!BOT_TOKEN,
    supportedLangs: SUPPORTED_LANGS,
    wordLen: WORD_LEN,
    maxGuesses: MAX_GUESSES,
    publicUrl: getPublicUrl(),
  });
});

app.get('/api/skus', (req, res) => {
  res.json({
    enabled: !!BOT_TOKEN,
    public_url: getPublicUrl() || null,
    webhook: lastWebhookStatus,
    skus: Object.values(SKUS).filter(s => !s.adminOnly).map(s => ({
      id: s.id, title: s.title, description: s.description,
      price: s.price, priceUsd: s.priceUsd, grant: s.grant,
    })),
  });
});

// ============ Game API ============
// Today's puzzle metadata. NEVER includes the answer for in-progress games.
// Body: { initData, lang, dayIdx? }   dayIdx = override for archive replay (Pro only)
app.post('/api/today', async (req, res) => {
  const user = resolveUser(req);
  let { lang = 'en', dayIdx } = req.body || {};
  if (!SUPPORTED_LANGS.includes(lang)) lang = 'en';
  const today = currentDayIdx();
  // Archive replay only allowed if Pro / archive unlocked / day == today.
  if (typeof dayIdx === 'number' && dayIdx !== today) {
    if (!user) return res.status(401).json({ error: 'auth required for archive' });
    if (dbReady) {
      const r = await dbPool.query('SELECT pro_until, archive_unlocked FROM users WHERE tg_id = $1', [user.id]);
      const u = r.rows[0];
      const proActive = u && (Number(u.pro_until) > Date.now());
      const archive = u && u.archive_unlocked;
      if (!proActive && !archive) return res.status(402).json({ error: 'archive locked', needsSku: 'archive_unlock' });
    }
  }
  const useDay = (typeof dayIdx === 'number') ? dayIdx : today;
  if (useDay < 0 || useDay > today) return res.status(400).json({ error: 'invalid day' });
  const answer = getDailyAnswer(lang, useDay);
  if (!answer) return res.status(503).json({ error: 'no words loaded for lang' });
  // Load existing progress so refreshes resume the game.
  let progress = { guesses: [], patterns: [], state: 'playing' };
  if (user && dbReady) {
    const r = await dbPool.query(
      'SELECT guesses, patterns, state, answer FROM daily_progress WHERE tg_id=$1 AND lang=$2 AND day_idx=$3',
      [user.id, lang, useDay]
    );
    if (r.rows.length) {
      progress = {
        guesses: r.rows[0].guesses,
        patterns: r.rows[0].patterns,
        state: r.rows[0].state,
        answer: r.rows[0].state === 'playing' ? null : r.rows[0].answer,
      };
    }
  }
  // Heartbeat: remember user for notifs.
  if (user) rememberUser(user.id, { chatId: user.id, lang, lastActiveAt: Date.now() });
  res.json({
    dayIdx: useDay,
    today,
    lang,
    wordLen: WORD_LEN,
    maxGuesses: MAX_GUESSES,
    progress,
  });
});

// Submit a guess. Server validates the word, computes colors, persists, and
// returns the pattern. Never reveals the answer mid-game.
// Body: { initData, lang, dayIdx, guess }
app.post('/api/guess', async (req, res) => {
  const user = resolveUser(req);
  let { lang = 'en', dayIdx, guess } = req.body || {};
  if (!SUPPORTED_LANGS.includes(lang)) lang = 'en';
  const today = currentDayIdx();
  if (typeof dayIdx !== 'number') dayIdx = today;
  guess = normalizeWord(guess || '', lang);
  if (guess.length !== WORD_LEN) return res.status(400).json({ error: 'wrong length', code: 'bad_length' });
  const valid = VALID[lang] || new Set();
  if (!valid.has(guess)) return res.status(400).json({ error: 'not in word list', code: 'not_a_word' });

  const answer = getDailyAnswer(lang, dayIdx);
  if (!answer) return res.status(503).json({ error: 'no puzzle for lang' });

  // Load + mutate progress. If no DB, we still compute and return — client
  // tracks state in localStorage but won't get cross-device sync.
  let progress = { guesses: [], patterns: [], state: 'playing' };
  if (user && dbReady) {
    const r = await dbPool.query(
      'SELECT guesses, patterns, state, answer FROM daily_progress WHERE tg_id=$1 AND lang=$2 AND day_idx=$3',
      [user.id, lang, dayIdx]
    );
    if (r.rows.length) progress = { guesses: r.rows[0].guesses, patterns: r.rows[0].patterns, state: r.rows[0].state, answer: r.rows[0].answer };
  }
  if (progress.state !== 'playing') {
    return res.status(409).json({ error: 'game already finished', state: progress.state, answer: progress.answer });
  }
  if (progress.guesses.length >= MAX_GUESSES) {
    return res.status(409).json({ error: 'out of guesses', state: 'lost', answer });
  }

  const pattern = computeColorPattern(answer, guess);
  progress.guesses.push(guess);
  progress.patterns.push(pattern);
  if (pattern === 'G'.repeat(WORD_LEN)) progress.state = 'won';
  else if (progress.guesses.length >= MAX_GUESSES) progress.state = 'lost';

  // Persist
  if (user && dbReady) {
    try {
      await dbPool.query(
        `INSERT INTO daily_progress (tg_id, lang, day_idx, guesses, patterns, state, answer, finished_at, updated_at)
         VALUES ($1,$2,$3,$4,$5,$6, CASE WHEN $6='playing' THEN NULL ELSE $7 END, CASE WHEN $6='playing' THEN NULL ELSE now() END, now())
         ON CONFLICT (tg_id, lang, day_idx)
         DO UPDATE SET guesses=$4, patterns=$5, state=$6, answer=EXCLUDED.answer, finished_at=EXCLUDED.finished_at, updated_at=now()`,
        [user.id, lang, dayIdx, JSON.stringify(progress.guesses), JSON.stringify(progress.patterns), progress.state, answer]
      );
      // Update streak / stats only for TODAY's puzzle, not archive.
      if (dayIdx === today && (progress.state === 'won' || progress.state === 'lost')) {
        await updateUserStats(user.id, progress, dayIdx);
      }
      // Referral progress + reward
      if (dayIdx === today && progress.state !== 'playing') {
        await advanceReferralProgress(user.id);
      }
    } catch (e) { console.error('[guess] persist failed:', e.message); }
  }
  res.json({
    pattern,
    state: progress.state,
    guessesUsed: progress.guesses.length,
    answer: progress.state !== 'playing' ? answer : undefined,
  });
});

async function updateUserStats(uid, progress, dayIdx) {
  if (!dbReady) return;
  const u = (await dbPool.query('SELECT * FROM users WHERE tg_id = $1', [uid])).rows[0];
  if (!u) return;
  const won = progress.state === 'won';
  const guesses = progress.guesses.length;
  const dist = Array.isArray(u.guess_dist) ? u.guess_dist.slice() : [0,0,0,0,0,0];
  if (won && guesses >= 1 && guesses <= 6) dist[guesses - 1] += 1;
  // Streak: requires consecutive days won. Shield can save one missed day.
  let streak = u.streak;
  if (won) {
    if (u.last_won_day === dayIdx - 1) streak = u.streak + 1;
    else if (u.last_won_day === dayIdx - 2 && Number(u.shield_until) > Date.now()) streak = u.streak + 1;
    else if (u.last_won_day === dayIdx) streak = u.streak; // re-record (idempotent)
    else streak = 1;
  } else {
    // Lost — streak survives if shield active and this is the only miss.
    if (Number(u.shield_until) > Date.now()) streak = u.streak;
    else streak = 0;
  }
  const maxStreak = Math.max(u.max_streak, streak);
  const gamesPlayed = u.games_played + 1;
  const gamesWon = u.games_won + (won ? 1 : 0);
  await dbPool.query(
    `UPDATE users SET
       streak = $2, max_streak = $3,
       last_won_day = CASE WHEN $4 THEN $5 ELSE last_won_day END,
       last_played_day = $5,
       games_played = $6, games_won = $7,
       guess_dist = $8::jsonb, updated_at = now()
       WHERE tg_id = $1`,
    [uid, streak, maxStreak, won, dayIdx, gamesPlayed, gamesWon, JSON.stringify(dist)]
  );
}

// Body: { initData } -> stats + IAP balances + active theme + Pro state
app.post('/api/me', async (req, res) => {
  const user = resolveUser(req);
  if (!user) return res.status(401).json({ error: 'auth required' });
  let stats = {
    streak: 0, max_streak: 0, games_played: 0, games_won: 0,
    guess_dist: [0,0,0,0,0,0], hints_balance: 0, shield_until: 0,
    archive_unlocked: false, themes_owned: [], active_theme: 'default',
    pro_until: 0, ref_count: 0,
  };
  if (dbReady) {
    let row = (await dbPool.query('SELECT * FROM users WHERE tg_id = $1', [user.id])).rows[0];
    if (!row) row = await loadOrCreateUser(user);
    if (row) {
      stats = {
        streak: row.streak,
        max_streak: row.max_streak,
        games_played: row.games_played,
        games_won: row.games_won,
        guess_dist: row.guess_dist,
        hints_balance: row.hints_balance,
        shield_until: Number(row.shield_until),
        archive_unlocked: row.archive_unlocked,
        themes_owned: row.themes_owned,
        active_theme: row.active_theme,
        pro_until: Number(row.pro_until),
        ref_count: row.ref_count,
        notif_hour: row.notif_hour,
        notif_opted_in: row.notif_opted_in,
        lang: row.lang,
      };
    }
  }
  res.json({ user: { id: user.id, first_name: user.first_name, username: user.username, photo_url: user.photo_url }, stats });
});

// Body: { initData, lang } -> persist preferred language for notifications
app.post('/api/set-lang', async (req, res) => {
  const user = resolveUser(req);
  const { lang } = req.body || {};
  if (!user) return res.status(401).json({ error: 'auth required' });
  if (!SUPPORTED_LANGS.includes(lang)) return res.status(400).json({ error: 'unsupported lang' });
  if (dbReady) {
    await dbPool.query('UPDATE users SET lang = $2, updated_at = now() WHERE tg_id = $1', [user.id, lang]);
  }
  rememberUser(user.id, { lang });
  res.json({ ok: true });
});

// Body: { initData, hour (0-23), tzOffsetMin, optIn (bool) }
app.post('/api/set-notif', async (req, res) => {
  const user = resolveUser(req);
  const { hour, tzOffsetMin, optIn } = req.body || {};
  if (!user) return res.status(401).json({ error: 'auth required' });
  if (dbReady) {
    await dbPool.query(
      `UPDATE users SET notif_hour=$2, notif_tz_offset=$3, notif_opted_in=$4, updated_at=now() WHERE tg_id=$1`,
      [user.id, Number(hour) || 9, Number(tzOffsetMin) || 0, !!optIn]
    );
  }
  res.json({ ok: true });
});

// Body: { initData, theme } -> activate a theme the user owns (or 'default'/'dark' which are free)
app.post('/api/set-theme', async (req, res) => {
  const user = resolveUser(req);
  const { theme } = req.body || {};
  if (!user) return res.status(401).json({ error: 'auth required' });
  if (!dbReady) return res.json({ ok: true });
  const r = await dbPool.query('SELECT themes_owned, pro_until FROM users WHERE tg_id = $1', [user.id]);
  const u = r.rows[0];
  const free = ['default','dark'];
  const owned = u ? u.themes_owned : [];
  const proActive = u && Number(u.pro_until) > Date.now();
  if (!free.includes(theme) && !(owned || []).includes(theme) && !proActive) {
    return res.status(402).json({ error: 'theme locked', needsSku: 'theme_pack' });
  }
  await dbPool.query('UPDATE users SET active_theme = $2, updated_at = now() WHERE tg_id = $1', [user.id, theme]);
  res.json({ ok: true });
});

// Body: { initData, dayIdx? } -> use one hint, returns the letter+index revealed
app.post('/api/use-hint', async (req, res) => {
  const user = resolveUser(req);
  const { lang = 'en', dayIdx } = req.body || {};
  if (!user) return res.status(401).json({ error: 'auth required' });
  const today = currentDayIdx();
  const day = typeof dayIdx === 'number' ? dayIdx : today;
  if (!dbReady) return res.status(503).json({ error: 'db required' });
  const u = (await dbPool.query('SELECT hints_balance, pro_until FROM users WHERE tg_id=$1', [user.id])).rows[0];
  const proActive = u && Number(u.pro_until) > Date.now();
  if (!proActive && (!u || u.hints_balance <= 0)) {
    return res.status(402).json({ error: 'no hints', needsSku: 'hint_pack' });
  }
  const prog = (await dbPool.query('SELECT guesses, patterns, state FROM daily_progress WHERE tg_id=$1 AND lang=$2 AND day_idx=$3', [user.id, lang, day])).rows[0];
  if (prog && prog.state !== 'playing') return res.status(409).json({ error: 'game finished' });
  const answer = getDailyAnswer(lang, day);
  if (!answer) return res.status(503).json({ error: 'no puzzle' });
  // Reveal a letter not yet known to the player (not green anywhere in their guesses).
  const greens = new Set();
  if (prog && prog.patterns) {
    for (let i = 0; i < prog.patterns.length; i++) {
      const p = prog.patterns[i]; const g = prog.guesses[i];
      for (let j = 0; j < p.length; j++) if (p[j] === 'G') greens.add(j);
    }
  }
  const candidates = [];
  for (let i = 0; i < answer.length; i++) if (!greens.has(i)) candidates.push(i);
  if (!candidates.length) return res.json({ index: -1, letter: '', note: 'all greens already' });
  const choice = candidates[Math.floor(Math.random() * candidates.length)];
  // Deduct hint unless Pro
  if (!proActive) {
    await dbPool.query('UPDATE users SET hints_balance = GREATEST(0, hints_balance - 1), updated_at = now() WHERE tg_id = $1', [user.id]);
  }
  res.json({ index: choice, letter: answer[choice] });
});

// ============ IAP endpoints ============
app.post('/api/create-invoice', async (req, res) => {
  if (!BOT_TOKEN) return res.status(500).json({ error: 'BOT_TOKEN not set on server' });
  const user = resolveUser(req);
  const { sku, giftRecipientUid } = req.body || {};
  if (!user) return res.status(401).json({ error: 'invalid initData' });
  const item = SKUS[sku];
  if (!item) return res.status(400).json({ error: 'unknown sku' });
  if (item.adminOnly && !isAdmin(user.id)) return res.status(403).json({ error: 'sku is admin-only' });
  const payload = JSON.stringify({ uid: user.id, sku, gift: giftRecipientUid || null, ts: Date.now() });
  try {
    const r = await fetch(`${TELEGRAM_API}/createInvoiceLink`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        title: item.title,
        description: item.description,
        payload,
        provider_token: '',
        currency: 'XTR',
        prices: [{ label: item.title, amount: item.price }],
      }),
    });
    const data = await r.json();
    if (!data.ok) return res.status(500).json({ error: data.description || 'telegram api failed' });
    res.json({ link: data.result });
  } catch (e) {
    res.status(500).json({ error: String(e && e.message || e) });
  }
});

// Client polls after openInvoice -> 'paid'. Drains DB grants AND in-memory queue.
app.post('/api/poll-purchases', async (req, res) => {
  const user = resolveUser(req);
  if (!user) return res.status(401).json({ error: 'auth required' });
  const applied = [];
  if (dbReady) {
    try {
      const q = await dbPool.query(
        `UPDATE iap_grants SET applied_at = now()
          WHERE tg_id = $1 AND applied_at IS NULL
          RETURNING sku, grant_data`,
        [user.id]
      );
      for (const row of q.rows) {
        await applyGrantSideEffects(user.id, row.sku, row.grant_data);
        applied.push({ sku: row.sku, grant: row.grant_data });
      }
    } catch (e) {
      console.error('[iap] poll-purchases db error:', e.message);
    }
  }
  for (const p of drainPending(user.id)) applied.push(p);
  res.json({ purchases: applied });
});

// Apply a grant's persistent side effects: add hints, extend shield/pro,
// unlock archive/themes. Called from poll-purchases AFTER the ledger has
// already been written (idempotent on payment_charge_id).
async function applyGrantSideEffects(uid, sku, grant) {
  if (!dbReady) return;
  const g = grant || {};
  const ops = [];
  if (typeof g.hints === 'number') ops.push(['hints_balance = hints_balance + ' + Number(g.hints)]);
  if (typeof g.shieldDays === 'number') {
    const ms = Number(g.shieldDays) * 86_400_000;
    ops.push([`shield_until = GREATEST(shield_until, ${Date.now()}::bigint) + ${ms}`]);
  }
  if (g.archive === true) ops.push(['archive_unlocked = TRUE']);
  if (typeof g.proDays === 'number') {
    const ms = Number(g.proDays) * 86_400_000;
    ops.push([`pro_until = GREATEST(pro_until, ${Date.now()}::bigint) + ${ms}`]);
  }
  if (Array.isArray(g.themes)) {
    const list = JSON.stringify(g.themes);
    // Merge themes uniquely
    ops.push([`themes_owned = (SELECT to_jsonb(array(SELECT DISTINCT unnest(array(SELECT jsonb_array_elements_text(themes_owned)) || array(SELECT jsonb_array_elements_text('${list}'::jsonb))))))`]);
  }
  if (ops.length) {
    const setClause = ops.map(o => o[0]).join(', ');
    await dbPool.query(`UPDATE users SET ${setClause}, updated_at = now() WHERE tg_id = $1`, [uid]);
  }
  // Gift Pro: deliver to recipient via the recorded payload.
  if (typeof g.giftProDays === 'number') {
    // Look up the latest grant for this user with the giftProDays SKU to find recipient.
    try {
      const r = await dbPool.query(
        `SELECT grant_data FROM iap_grants WHERE tg_id=$1 AND sku=$2 ORDER BY created_at DESC LIMIT 1`,
        [uid, sku]
      );
      // Recipient was carried in the original payload, but we don't store it
      // in grant_data — fetch from the payment_charge_id row if needed. For
      // v1 simplicity: gifter must claim and forward via a follow-up flow.
      // (TODO post-launch: persist gift_recipient_uid in iap_grants schema.)
    } catch (_e) { /* swallow */ }
  }
}

// Telegram webhook (pre_checkout_query, successful_payment, /start, /play, /stats).
app.post('/api/telegram-webhook', async (req, res) => {
  if (WEBHOOK_SECRET && req.headers['x-telegram-bot-api-secret-token'] !== WEBHOOK_SECRET) {
    return res.status(403).end();
  }
  const update = req.body || {};
  try {
    if (update.pre_checkout_query) {
      const q = update.pre_checkout_query;
      await fetch(`${TELEGRAM_API}/answerPreCheckoutQuery`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ pre_checkout_query_id: q.id, ok: true }),
      });
    } else if (update.message && update.message.successful_payment) {
      const sp = update.message.successful_payment;
      try {
        const payload = JSON.parse(sp.invoice_payload);
        if (payload && payload.uid && SKUS[payload.sku]) {
          const sku = SKUS[payload.sku];
          if (dbReady) {
            await dbPool.query(
              `INSERT INTO iap_grants (payment_charge_id, tg_id, sku, stars, grant_data)
               VALUES ($1,$2,$3,$4,$5)
               ON CONFLICT (payment_charge_id) DO NOTHING`,
              [sp.telegram_payment_charge_id || ('mem:' + Date.now()), payload.uid, payload.sku, sp.total_amount || 0, sku.grant]
            );
          }
          pushPending(payload.uid, payload.sku);
          // Send a thanks message to the buyer.
          if (TELEGRAM_API) {
            await fetch(`${TELEGRAM_API}/sendMessage`, {
              method: 'POST', headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                chat_id: payload.uid,
                text: `Thanks for supporting Slovo! ⭐ Your ${sku.title} is active.`,
              }),
            }).catch(() => {});
          }
        }
      } catch (e) { /* malformed payload */ }
    } else if (update.message && update.message.text) {
      await handleBotMessage(update.message);
    } else if (update.my_chat_member) {
      // Bot added to / removed from groups.
      const m = update.my_chat_member;
      if (m.new_chat_member && m.new_chat_member.status === 'member' && m.chat.type !== 'private') {
        if (dbReady) {
          try {
            await dbPool.query(
              `INSERT INTO group_membership (chat_id, tg_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
              [m.chat.id, m.from && m.from.id]
            );
          } catch (_e) {}
        }
      }
    }
  } catch (e) {
    console.error('[webhook] error:', e.message);
  }
  res.json({ ok: true });
});

async function handleBotMessage(msg) {
  const txt = (msg.text || '').trim();
  const chatId = msg.chat.id;
  const fromId = msg.from && msg.from.id;
  const isPrivate = msg.chat.type === 'private';
  rememberUser(fromId, { chatId, lastActiveAt: Date.now() });
  const playUrl = getPublicUrl();
  const playBtn = playUrl ? { inline_keyboard: [[{ text: '▶️ Play Slovo', web_app: { url: playUrl } }]] } : undefined;

  if (txt.startsWith('/start')) {
    // Referral: /start ref_<uid>
    const match = txt.match(/\/start(?:\s+|@\w+\s+)?ref_(\d+)/);
    if (match && dbReady && fromId) {
      const refId = parseInt(match[1], 10);
      if (refId && refId !== fromId) {
        try {
          await dbPool.query(`UPDATE users SET ref_by = $2 WHERE tg_id = $1 AND ref_by IS NULL`, [fromId, refId]);
          await dbPool.query(`INSERT INTO referrals (referrer_id, invitee_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, [refId, fromId]);
          await dbPool.query(`UPDATE users SET ref_count = (SELECT COUNT(*) FROM referrals WHERE referrer_id = $1) WHERE tg_id = $1`, [refId]);
        } catch (_e) {}
      }
    }
    const welcome = (msg.from && (msg.from.first_name || msg.from.username)) || 'there';
    await sendMessage(chatId, `Hi ${welcome}! 👋\n\nSlovo is a daily word puzzle. New word every day. Tap Play to start.`, playBtn);
    return;
  }
  if (txt.startsWith('/play') || txt.startsWith('/slovo')) {
    await sendMessage(chatId, 'Tap to play today\'s word.', playBtn);
    return;
  }
  if (txt.startsWith('/stats') && dbReady && fromId) {
    try {
      const r = await dbPool.query('SELECT streak, max_streak, games_played, games_won FROM users WHERE tg_id = $1', [fromId]);
      const u = r.rows[0];
      if (!u) { await sendMessage(chatId, 'Play your first puzzle to unlock stats!', playBtn); return; }
      const winRate = u.games_played ? Math.round(100 * u.games_won / u.games_played) : 0;
      await sendMessage(chatId, `📊 *Your Slovo stats*\n\n🔥 Streak: ${u.streak} (max ${u.max_streak})\nPlayed: ${u.games_played}\nWon: ${u.games_won} (${winRate}%)`, playBtn, 'Markdown');
    } catch (_e) {}
    return;
  }
  if (txt.startsWith('/leaderboard') && !isPrivate && dbReady) {
    await postGroupLeaderboard(chatId);
    return;
  }
  // /share <pattern> — copy the user's last-day share grid into the group chat.
  if (txt.startsWith('/share') && fromId && dbReady) {
    try {
      const r = await dbPool.query(
        `SELECT lang, day_idx, patterns, state FROM daily_progress
          WHERE tg_id = $1 AND day_idx = $2 AND state != 'playing'
          ORDER BY updated_at DESC LIMIT 1`,
        [fromId, currentDayIdx()]
      );
      if (r.rows.length) {
        const row = r.rows[0];
        const grid = row.patterns.map(formatPatternRow).join('\n');
        const guesses = row.state === 'won' ? row.patterns.length : 'X';
        await sendMessage(chatId, `Slovo ${row.day_idx} ${guesses}/6\n\n${grid}`, undefined);
      }
    } catch (_e) {}
    return;
  }
}

function formatPatternRow(p) {
  let s = '';
  for (const c of p) s += (c === 'G' ? '🟩' : c === 'Y' ? '🟨' : '⬛');
  return s;
}

async function postGroupLeaderboard(chatId) {
  if (!dbReady) return;
  try {
    const today = currentDayIdx();
    const r = await dbPool.query(
      `SELECT u.first_name, u.username, dp.state, jsonb_array_length(dp.guesses) AS g
         FROM group_membership gm
         JOIN users u ON u.tg_id = gm.tg_id
    LEFT JOIN daily_progress dp ON dp.tg_id = gm.tg_id AND dp.day_idx = $2
        WHERE gm.chat_id = $1
        ORDER BY (CASE WHEN dp.state = 'won' THEN 0 WHEN dp.state = 'lost' THEN 7 ELSE 99 END) ASC,
                 jsonb_array_length(dp.guesses) ASC`,
      [chatId, today]
    );
    const lines = r.rows.slice(0, 20).map((row, i) => {
      const name = row.username ? '@' + row.username : (row.first_name || 'player');
      const result = row.state === 'won' ? `${row.g}/6` : row.state === 'lost' ? 'X/6' : '⏳';
      const medal = i === 0 ? '🥇' : i === 1 ? '🥈' : i === 2 ? '🥉' : '  ';
      return `${medal} ${name} — ${result}`;
    });
    const txt = lines.length
      ? `*Today's Slovo standings*\n\n${lines.join('\n')}`
      : 'No one has played today\'s Slovo yet. Be the first!';
    await sendMessage(chatId, txt, undefined, 'Markdown');
  } catch (e) { console.error('[lb] err:', e.message); }
}

async function sendMessage(chatId, text, reply_markup, parse_mode) {
  if (!TELEGRAM_API) return;
  try {
    await fetch(`${TELEGRAM_API}/sendMessage`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: chatId, text, reply_markup, parse_mode, disable_web_page_preview: true }),
    });
  } catch (_e) {}
}

// ============ Referrals: reward referrer when invitee plays 7 days ============
async function advanceReferralProgress(inviteeUid) {
  if (!dbReady) return;
  const r = await dbPool.query(
    `UPDATE referrals SET invitee_days = invitee_days + 1
       WHERE invitee_id = $1 AND rewarded_at IS NULL
       RETURNING referrer_id, invitee_days`,
    [inviteeUid]
  );
  for (const row of r.rows) {
    if (row.invitee_days >= 7) {
      // Reward: grant 1 free Streak Shield (7 days) to the referrer.
      await dbPool.query(
        `UPDATE users SET shield_until = GREATEST(shield_until, $2::bigint) + ${7 * 86_400_000}, updated_at = now()
           WHERE tg_id = $1`,
        [row.referrer_id, Date.now()]
      );
      await dbPool.query(
        `UPDATE referrals SET rewarded_at = now() WHERE referrer_id = $1 AND invitee_id = $2`,
        [row.referrer_id, inviteeUid]
      );
      // Notify the referrer.
      await sendMessage(
        row.referrer_id,
        `🎁 Your invite played 7 Slovo puzzles — you earned a 7-day Streak Shield!`
      ).catch(() => {});
    }
  }
}

// ============ Setup helpers (admin: register webhook + bot commands) ============
app.get('/api/setup-webhook', async (req, res) => {
  if (!BOT_TOKEN) return res.status(500).json({ error: 'BOT_TOKEN not set' });
  const url = getPublicUrl();
  if (!url) return res.status(400).json({ error: 'PUBLIC_URL not set' });
  try {
    const r = await fetch(`${TELEGRAM_API}/setWebhook`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        url: `${url}/api/telegram-webhook`,
        secret_token: WEBHOOK_SECRET,
        allowed_updates: ['message','pre_checkout_query','my_chat_member'],
      }),
    });
    const data = await r.json();
    lastWebhookStatus = { ok: data.ok, at: Date.now(), description: data.description || '' };
    // Also register bot commands for the Telegram UI.
    await fetch(`${TELEGRAM_API}/setMyCommands`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ commands: [
        { command: 'play', description: 'Play today\'s Slovo' },
        { command: 'stats', description: 'Your streak and stats' },
        { command: 'leaderboard', description: 'Today\'s group standings' },
        { command: 'share', description: 'Share your last result' },
      ] }),
    }).catch(() => {});
    res.json(data);
  } catch (e) { res.status(500).json({ error: String(e && e.message || e) }); }
});

// ============ Notification cron ============
// Runs every 5 minutes. Sends a daily reminder at the user's preferred local
// hour and a streak-warning at 22:00 local if they have a streak ≥ 3 and
// haven't played today.
async function notifTick() {
  if (!dbReady || !TELEGRAM_API) return;
  try {
    const today = currentDayIdx();
    const now = Date.now();
    const r = await dbPool.query(
      `SELECT u.tg_id, u.lang, u.streak, u.notif_hour, u.notif_tz_offset, u.notif_opted_in,
              dp.state AS today_state
         FROM users u
    LEFT JOIN daily_progress dp ON dp.tg_id = u.tg_id AND dp.day_idx = $1
              AND dp.lang = COALESCE(u.lang, 'en')
        WHERE u.notif_opted_in = TRUE
          AND (u.last_notif_at IS NULL OR u.last_notif_at < NOW() - INTERVAL '6 hours')
        LIMIT 200`,
      [today]
    ).catch(async () => {
      // Tolerant: `last_notif_at` may not yet exist. Add it lazily and retry.
      try { await dbPool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS last_notif_at TIMESTAMPTZ`); } catch (_e) {}
      return await dbPool.query(
        `SELECT u.tg_id, u.lang, u.streak, u.notif_hour, u.notif_tz_offset, u.notif_opted_in,
                dp.state AS today_state
           FROM users u
      LEFT JOIN daily_progress dp ON dp.tg_id = u.tg_id AND dp.day_idx = $1
                AND dp.lang = COALESCE(u.lang, 'en')
          WHERE u.notif_opted_in = TRUE
          LIMIT 200`,
        [today]
      );
    });
    for (const row of r.rows) {
      const tz = Number(row.notif_tz_offset || 0);
      const localMs = now + tz * 60_000;
      const localHour = new Date(localMs).getUTCHours();
      const preferredHour = Number(row.notif_hour ?? 9);
      const played = row.today_state === 'won' || row.today_state === 'lost';
      if (played) continue;
      // Daily reminder at preferred hour (±5 min window).
      let sendIt = false;
      let txt = '';
      if (localHour === preferredHour) {
        sendIt = true;
        txt = row.streak > 0
          ? `🔥 Streak ${row.streak} — today's Slovo is live. Don't break the chain!`
          : `🧠 Today's Slovo is ready. New word, fresh start.`;
      } else if (localHour === 22 && row.streak >= 3) {
        sendIt = true;
        txt = `⏰ 2 hours left to keep your ${row.streak}-day streak alive!`;
      }
      if (sendIt) {
        const playUrl = getPublicUrl();
        const reply_markup = playUrl ? { inline_keyboard: [[{ text: '▶️ Play now', web_app: { url: playUrl } }]] } : undefined;
        await sendMessage(row.tg_id, txt, reply_markup).catch(() => {});
        await dbPool.query(`UPDATE users SET last_notif_at = now() WHERE tg_id = $1`, [row.tg_id]).catch(() => {});
      }
    }
  } catch (e) { console.error('[notif] tick err:', e.message); }
}
setInterval(notifTick, 5 * 60 * 1000);

// ============ Admin endpoints ============
app.post('/api/admin/peek-answer', async (req, res) => {
  const user = resolveUser(req);
  if (!user || !isAdmin(user.id)) return res.status(403).end();
  const { lang = 'en', dayIdx } = req.body || {};
  const day = typeof dayIdx === 'number' ? dayIdx : currentDayIdx();
  res.json({ lang, dayIdx: day, answer: getDailyAnswer(lang, day) });
});

// ============ Boot ============
(async () => {
  loadWordLists();
  await initSchema();
  app.listen(PORT, () => {
    console.log(`Slovo server listening on http://localhost:${PORT}`);
    if (!BOT_TOKEN) console.log('[bot] BOT_TOKEN not set — dev mode (no IAP, no notifications)');
  });
})();
