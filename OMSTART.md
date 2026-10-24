# Omstart: en skiss

Så här skulle lazy-storage se ut om det byggdes om från början med samma
mål men två andra grundval:

- **Records är enheten för lagring, regler och livscykel.** En store är
  collections av records och singletons, och varje record är en rad.
  Inuti en record får datan vara hur djup som helst.
- **En regel per sökväg, utan tombstones utanför en levande record.** En
  op säger om den skapar eller ändrar, en ändring kräver att föräldern
  finns, och tidsstämplar som klienten stämplar kausalt och servern
  klämmer avgör resten. Det ersätter dagens tombstones, barriärer,
  horisont och hybridklocka.

Allt som står som "behålls" finns i dagens repo och kan lyftas över mer
eller mindre orört.

## Mål och icke-mål

Målen är desamma som idag:

- State du bara redigerar: tilldela till `db.state` och det synkar.
- Offline som standard: en outbox som överlever omladdning, skickas i
  ordning när servern är tillbaka.
- Konvergens: alla klienter landar i samma state, och den som förlorade
  får veta det.
- Undo och redo som bara rör dina egna ändringar.
- En process och en SQLite-fil på servern, auth som hooks, presence.
- Vue- och React-bindningar.

Icke-mål, nu uttalade så att de inte smyger in:

- Fan-out som mål i sig. Ett relä finns, men som en följarstore (§6),
  för att en plats ska fungera när servern är borta, inte för att sprida
  last.
- Text-CRDT och sammanslagning av två ändringar av samma värde.
- Ett fritt träd utan records. Storens översta nivå är collections och
  singletons, och det som ska skapas och raderas med egen identitet är
  en record.
- Partiell synk, pagination eller queries mot servern. En klient som
  öppnar en store får hela storen, som idag.
- Flera auktoriteter. Servern är den enda merge-punkten.

## 1. Datamodell

```
store
├── collection 'tasks'       records nycklade på id
│   └── record 'k7f2'        { title: 'Milk', pos: 'a0', meta: { color: 'red', size: 'M' }, tags: ['x'] }
│       └── sökväg           ['meta', 'color']: enheten för merge
├── collection 'subtasks'    med en förälder: taskId pekar på tasks
└── singleton 'settings'     en record utan id: { theme: 'dark' }
```

Reglerna:

- **Storens översta nivå deklareras i `initial`.** En collection är `{}`.
  En singleton är en nyckel listad i `singletons`, eller en nyckel vars
  startvärde inte är ett objekt, som Mageeks `build: ''`. En klient kan
  inte ta bort en toppnyckel, bara det som finns i den.
- **En collection är records nycklade på id.** Nyckeln är id:t; ett
  `id`-fält inuti recorden behövs inte.
- **En record är ett JSON-objekt på vilket djup som helst.** Varje
  sökväg i den mergas för sig: två personer som samtidigt ändrar
  `meta.color` och `meta.size` behåller båda sina ändringar.
- **En array är ett blad.** Den skrivs och mergas hel och får innehålla
  vad som helst. En lista som flera lägger till i samtidigt hör hemma i
  en nycklad map med positioner, nedan.
- **Registers behålls, som ett val.** En sökväg deklarerad som register,
  med `*` för ett segment som idag, är ett blad: dess objekt skrivs och
  mergas helt. Det är för det som ska vara atomärt, som ett fönsters
  geometri eller en spells `sheet`. Utan deklaration ger en tilldelning
  av ett helt objekt ändå ersättning, eftersom lazy-watch sänder nycklar
  som försvann som `null`; skillnaden är att ett register inte mergas
  med samtidiga ändringar inuti det.
- **Ordning är ett fält.** En nycklad map vars records bär `pos`, en
  fraktionell nyckel ur `core/positions.js`, är en lista. Det gäller en
  collection och lika väl en map inuti en record, som Mageeks
  `state.lists.items`.
- **Nästla det som delar livscykel, dela upp det som har eget liv.** Det
  som ligger inuti en record raderas med recorden. Det som läggs till,
  flyttas och raderas på egen hand, och i stort antal, är en egen
  collection med en deklarerad förälder:
  `parents: { subtasks: { taskId: 'tasks' } }`. Servern släpper aldrig
  in ett barn vars förälder saknas, och tar bort barnen med föräldern
  (§2).
- Förbjudna nycklar (`__proto__`, `constructor`, `prototype`) och
  storleksgränser per record och op behålls.

Snapshot, `db.state` och cachen på klienten har alla samma form:

```json
{
  "tasks":    { "k7f2": { "title": "Milk", "pos": "a0", "meta": { "color": "red", "size": "M" } } },
  "subtasks": { "q1":   { "taskId": "k7f2", "title": "Skimmed", "pos": "a0" } },
  "settings": { "theme": "dark" }
}
```

## 2. Ändringar och konfliktregel

En op är en batch av ändringar, var och en på en sökväg i en record:

```json
{ "replica": "r1", "seq": 12, "ts": 1761300000000, "changes": [
  { "c": "tasks",    "id": "k7f2", "path": ["meta", "color"], "set": "red" },
  { "c": "tasks",    "id": "k7f2", "path": ["meta", "size"],  "del": true },
  { "c": "tasks",    "id": "m3",   "put": { "title": "Eggs", "pos": "a1" } },
  { "c": "tasks",    "id": "x9",   "del": true },
  { "c": "settings", "path": ["theme"], "set": "dark" }
]}
```

`path` saknas för recorden själv, och `id` för en singleton.
Ändringarna kompileras ur lazy-watch:s diff och invers (§3): en
container som inte fanns före batchen blir `put`, ett värde som skrivs
blir `set`, och `null` blir `del`.

Servern tillämpar ops i ankomstordning, och den ordningen är storens
version `v`. Varje ändring avgörs för sig mot recordens **klockkarta**,
en tidsstämpel per sökväg: för ett blad när det skrevs, för en container
när den skapades (dess **födelse**), och för det som raderats när det
raderades.

| Ändring | Landar om | Annars |
|---|---|---|
| `put P` | recorden och P:s förälder finns, eller P är recorden själv, och inget raderat vid P är nyare. Finns P redan mergas innehållet in | `missing` eller `newer` |
| `set P` | recorden och P:s förälder finns, och inget vid P är nyare. Ersätter värdet en container får inget under den vara nyare | `missing` eller `newer` |
| `del P` | inget vid eller under P är nyare; för en record räknas dess barn i andra collections in. Finns P inte händer ingenting | `newer` |
| alla | opens `ts` är inte äldre än födelsen hos någon container ovanför P, recorden inräknad | `stale` |

"Nyare" betyder strikt större tidsstämpel; vid lika vinner den som kom
sist. En förlorad ändring hindrar inte resten av open, och ack:en listar
den under `lost` med orsaken.

Tre följder:

- **Inga tombstones utanför en levande record.** En raderad record
  försvinner som rad. Det som sedan skrivs i den saknar förälder och
  förloras som `missing`. En record som skapas igen med samma id har en
  ny födelse, och det som skrevs före den förloras som `stale`. Raderade
  sökvägar inuti en record ligger kvar i klockkartan så länge recorden
  gör, och rensas när de är äldre än retentionsfönstret (§5).
- **En radering förlorar mot nyare arbete.** En radering gjord offline
  kl 9 tar inte bort det någon redigerat kl 10, varken i recorden eller
  i dess barn.
- **Ett barn blir aldrig föräldralöst.** En `put` av en subtask vars
  `taskId` saknas förloras som `missing`, och en task som raderas tar
  sina subtasks med sig i samma commit, stämplade med raderingens `ts`.

Så här faller racen ut:

| Race | Resultat |
|---|---|
| A och B sätter samma sökväg | senare `ts` vinner; samma millisekund: den som nådde servern sist |
| A och B ändrar olika sökvägar i samma record, även djupt | båda landar |
| A ändrar offline; B tar bort recorden; A synkar | A:s ändring förloras, `missing` |
| B tar bort recorden offline kl 9; A ändrar den online kl 10; B synkar | B:s radering förloras, `newer`; recorden står kvar |
| Ett fält raderas kl 10:05; en skrivning från 10:01 kommer sedan | skrivningen förloras, `newer` |
| A tar bort x; B skapar x igen med samma id; C:s ändring från före återskapandet kommer | C:s ändring förloras, `stale` |
| A tar bort en task; B lade till en subtask offline | subtasken förloras, `missing` |
| A och B lägger till olika records | båda landar |
| A flyttar en record, B redigerar den | båda landar, olika sökvägar |
| A och B flyttar samma record | senare `ts` vinner, listan är konsistent överallt |
| C ändrade x offline **efter** att x skapats igen, utan att ha sett det | landar på nya x |

Den sista raden är den enda lucka som återstår mot dagens barriärer. Tid
kan inte säga vad C hade sett; det kunde bara dagens `v` som "sett", och
det var den som drog in epoker på ops och en horisont. Med slumpade id
kan det inte hända. Med deterministiska id, eller en ångrad radering, är
utfallet att ändringen hamnar på samma logiska sak, vilket sällan är fel.

### Tid

`ts` är millisekunder, ett heltal per op. Tre regler ger tidsstämplar
som går att lita på utan hybridklocka:

1. **Klienten stämplar kausalt:** `max(väggklocka + offset, högsta
   sedda + 1)`. `offset` lärs från serverns `now` i varje snapshot, delta
   och ack, så en klocka som går efter rättas. Högsta sedda är den största
   `ts` klienten tagit emot, så en ändring gjord efter att ha sett något
   är alltid nyare än det. Det är vad som gör `stale`-kontrollen säker.
2. **Servern klämmer** en `ts` som ligger före dess egen tid till dess
   egen tid. En klocka som går före vinner ingenting, och ingen
   `clock-skew` eller omstämpling behövs. Klämningen gör också att en op
   aldrig är stämplad senare än den kom fram, vilket retention bygger på
   (§5).
3. **Serverns egna skrivningar** stämplas med dess tid, och vinner därför
   över det som redan finns.

En följare klämmer och stämplar med sin skattning av centrals tid, inte
med sin egen klocka, så en hubb som startar med fel tid inte flyttar
platsens ändringar (§6).

**Varför tid alls**, när Replicache och Linear klarar sig med ren
serverordning: Mageeks hubb. Två skärmar redigerar samma fält under ett
avbrott och hubben visar den senare. Med ren ankomstordning skulle
utfallet hos central bero på vems hello som kom först, och platsen kunde
slå om. Med tid på varje sökväg beror utfallet nästan inte på ordningen:
bara lika tidsstämplar gör det, och den kausala stämplingen ser till att
en ändring aldrig kan komma före den `put` den bygger på.

### Klienten: bas och väntande

Klienten håller isär det servern har bekräftat och det som väntar:

- **Basen** är serverns state så som klienten senast hört den, vid
  version `db.version`. Varje patch tillämpas på basen.
- **Väntande** är outboxens ops.
- **`db.state` är basen med de väntande opsen ovanpå**, optimistiskt: en
  väntande ändring visas om dess record och förälder finns, annars hoppas
  den över, precis som servern skulle göra.
- **Basen hålls separat bara för records med väntande ändringar.** För
  alla andra är basen och vyn samma objekt, och en patch tillämpas rakt
  på vyn. En patch på en record med väntande ändringar går till
  recordens baskopia, och recorden räknas om: kopian plus de väntande
  ändringar som rör den, skrivet in i vyn med `overwrite` så att
  lazy-watch bara sänder det som faktiskt ändrades.
- **Vid ack** tas open bort ur väntande och de records den rörde räknas
  om. Har en ändring förlorat visar vyn nu basens värde, som kom med den
  vinnande patchen. `lost` i ack:en går till appen som en händelse; state
  behöver ingen korrigering.
- **En snapshot** ersätter basen, och varje record med väntande
  ändringar räknas om.
- **Persistensen** håller basen, en rad per record, outboxen och
  baskopiorna. En omladdning bygger vyn på samma sätt.

Så faller det fall ut där den förra skissens ägda celler hade gett fel:

```
B sätter title = 'Havremjölk' kl 10:02, och servern har den
A sätter title = 'Mjölk' offline kl 10:01; A:s vy visar 'Mjölk'
B:s patch når A och går till baskopian; recorden räknas om och visar fortfarande 'Mjölk'
A synkar; servern behåller B:s värde, och ack:en säger lost: title, newer
open tas bort ur väntande, recorden räknas om, och A visar 'Havremjölk' som alla andra
```

Mellan B:s patch och A:s ack visar A sitt eget värde, som idag fram till
korrigeringen. Eftersom varje patch bär sin `ts` kan klienten senare
hålla samma klockkartor som servern och döma exakt i stället för
optimistiskt, om det flimret någonsin märks.

### Undo

Behålls som idag: lazy-watch:s undo-manager med ett filter som avböjer
fjärrbatcher, så bara egna ändringar hamnar i historiken, och ett steg
som ångras blir en vanlig op. En ångrad radering är en `put` med ny
födelse. En ångrad `set` skriver det gamla värdet med ny `ts`, och
skriver därmed över en kollegas senare ändring av samma sökväg, precis
som idag.

## 3. Klient-API

```js
import { openClient, webSocketTransport, indexedDBStorage } from 'lazy-storage';

const db = await openClient({
  transport: webSocketTransport('wss://example.com/ws?token=...'),
  store: 'todo',
  initial: { tasks: {}, subtasks: {}, settings: { theme: 'light' } },
  singletons: ['settings'],
  parents: { subtasks: { taskId: 'tasks' } },
  storage: indexedDBStorage('todo')      // bas, outbox och baskopior; await:en är laddningen
});
db.connect();
```

Formen, alltså `initial`, `singletons`, `parents` och `registers`, är
densamma på server och klient, som idag i Mageeks `model/stores.js`. En
klient vars form skiljer sig från serverns får ett fel.

Skrivningar går genom lazy-watch-proxyn på `db.state`:

```js
db.state.tasks[id].done = true;                 // set ['done']
db.state.tasks[id].meta.color = 'red';          // set ['meta','color']; någon annans meta.size står kvar
db.state.tasks[id].meta = { color: 'red' };     // fanns meta: set color, del size; fanns den inte: put meta
db.state.tasks[id].tags.push('urgent');         // set ['tags']: en array skrivs hel
db.state.tasks[newId()] = { title: 'Eggs' };    // put av recorden
db.state.tasks[id] = null;                      // del, och dess subtasks med den
db.state.settings.theme = 'dark';               // set i singletonen
```

Listor är ett API, inte muterbara arrayer. Datan är en nycklad map; en
array får man ur listan:

```js
const tasks = db.list('tasks');                               // en collection
const items = db.list(['state', 'lists', 'items']);           // eller en nycklad map inuti en record
tasks.all();                                                  // en array i pos-ordning, färsk per batch
tasks.add({ title: 'Milk' }, { at: 0 });                      // myntar id, sätter pos
tasks.move(id, { after: otherId });
tasks.remove(id);
tasks.reconcile(ids);                                         // minsta antal pos-skrivningar; LIS-logiken ur list.js

const subs = db.list('subtasks', { where: { taskId: id } });  // en förälders barn, genom ett index på taskId
```

Läsning och händelser är record-nivå rakt igenom, vilket gör dagens
`collection.watch` till standardfallet:

```js
db.watch((changes, meta) => ...);     // [{ c, id, type: 'put'|'set'|'del', record, previous }], meta.origin 'local'|'remote'|'undo'
db.collection('tasks').watch(...);    // samma, en collection
db.on('status' | 'sync' | 'lost' | 'rejected' | 'reset' | 'closed' | 'presence' | 'relay');
db.status; db.pending; db.version; db.relayed; db.peers; db.share(data); db.isPending(path);
db.undo(); db.redo(); db.canUndo;
```

Vue och React behålls som idag men blir mindre: Vue får en reaktiv
spegel som patchas per batch och den skrivbara vyn, React går via
`useSyncExternalStore` med `useClientSelector`. Inga vyer över två träd.

**Kompileringen.** Klienten behåller lazy-watch med `inverse: true`, som
idag, och kompilerar varje batch genom att gå igenom diffen tillsammans
med inversen: där inversen säger att inget fanns blir värdet en `put`,
ett objekt som fanns gås igenom nyckel för nyckel, ett blad, en array
eller ett register blir en `set`, och `null` blir en `del`. En array som
lazy-watch rapporterar som fragment läses hel ur state. Det är ett
sextiotal rader, och ersätter dagens `expandRegisters`, `made` och
fasaden.

## 4. Protokoll

Som idag: JSON-meddelanden med `t`, alla utom ping och pong märkta med
`store`, godtyckligt många stores över en socket, versionerat med
`proto`.

Klient till server:

| Meddelande | Fält | Betydelse |
|---|---|---|
| `hello` | `proto`, `replica`, `ops`, `since?`, `epoch?`, `share?`, `presence?`, `fetch?`, `clocks?` | Anslut: hela outboxen, de första 1000, och var klientens kunskap slutar. `clocks` ber om klockkartorna, vilket en följare gör |
| `op` | `op` | En batch live |
| `leave` | | Stäng storens session, behåll socketen |
| `share` | `data` | Det här sessionen delar med sina peers |
| `ping` | | |

Server till klient:

| Meddelande | Fält | Betydelse |
|---|---|---|
| `snapshot` | `state` eller `fetch`, `v`, `epoch`, `seq`, `now`, `shape`, `lost?` | Hela staten att ersätta basen med; `fetch` pekar på HTTP-rutten när den är stor |
| `delta` | `ops: [{ v, ts, changes }]`, `v`, `epoch`, `seq`, `now`, `shape`, `lost?` | Det som hänt sedan `since`, i ordning; `lost` för hellons egna ops |
| `patch` | `changes`, `v`, `ts` | En accepterad op från vilken replika som helst, avsändaren inräknad, med bara de ändringar som landade |
| `ack` | `seq`, `v`, `now`, `lost?` | Open är lagrad; `lost` är `[{ c, id, path, why }]` med `why` som `missing`, `newer` eller `stale` |
| `error` | `seq?`, `code`, `message`, `retryAfter?` | Med `seq`: open avvisades |
| `presence` | `peers` eller `left?`, `joined?`, `shared?` | Som idag |
| `relay` | `status` | Från en följare: `local` när den svarar ensam, `through` när central gör det |
| `closed` | `code`, `message` | Slutgiltigt för storen, eller för socketen utan `store` |
| `pong` | | |

`shape` är en hash av formen, och `now` serverns tid, som klienten lär
sin `offset` av.

Avvisningskoder: `invalid`, `forbidden`, `too-large`, `rate-limited`,
`expired`. Stängningskoder: `unauthorized`, `forbidden`,
`unknown-store`, `invalid-store`, `evicted`, `replica-taken`,
`outdated`, `unavailable`.

En följares länk till servern talar samma protokoll och dessutom:

| Meddelande | Riktning | Betydelse |
|---|---|---|
| `vouch` | följare till server | `as`, `credential`: får den här klienten öppna storen? Dömt av `authenticate` och `authorizeId` som klientens egen socket |
| `admitted` | server till följare | `as`, `user`, `until` |
| `refused` | server till följare | `as`, `code` |
| `revoked` | server till följare | `as`, `code`: tillgången togs bort, och följaren stänger klientens session |
| `op`, `share`, `leave` med `as` | följare till server | Klientens egna, dömda som klientens; ack och fel kommer tillbaka med replika och `seq` |

Garantier att skriva ner: `v` är total ordning; en op med redan sedd
`seq` ignoreras; en replika ser sin egen patch före sin ack; en delta
ges när loggen räcker tillbaka till `since` i samma `epoch`, annars en
snapshot.

**Borta ur protokollet:** hybridklockans `[ms, count, replica]`, `v`
som "sett", `epoch` på ops, `made`, `correction`, `conflict` (som heter
`lost` och bär orsaken), `clock-skew`, horisonten, och reläets dubbla
vägar: skrivsessioner med `follow: false`, `unvouch` och pass-through
av stores reläet inte får bära.

## 5. Server

```js
import { createStore, createStores } from 'lazy-storage/server';
import { sqliteStorage } from 'lazy-storage/server/sqlite';
import { serve } from 'lazy-storage/server/bun';

const sqlite = sqliteStorage('data/app.sqlite');
const stores = createStores(id => createStore({
  initial: { tasks: {}, subtasks: {}, settings: { theme: 'light' } },
  singletons: ['settings'],
  parents: { subtasks: { taskId: 'tasks' } },
  storage: sqlite.store(id),
  rules: {
    tasks:    { write: (user, before, after) => (after ?? before).owner === user.id },
    settings: { write: user => user.role === 'admin' }
  }
}));
serve({ stores, port: 3200, authenticate, authorizeId });
```

- **Regler per collection eller singleton** ersätter `readOnly` och
  `validate`. `write(user, before, after, { state, replica })` anropas en
  gång per record och op, med recorden före och efter opens ändringar.
  `after` är `null` för en radering, `before` är `null` för en ny record,
  och `state` är storens state, för regler som tittar på andra records.
  `false`, eller ett kastat fel med ett meddelande, avvisar hela open med
  `forbidden`.
- **Serverns egna skrivningar** är `store.patch(diff)` som idag, i
  trädets form: collection, id, sökväg. Servern är auktoritet: den skapar
  containrar och records som saknas, stämplar med sin tid och passerar
  inga regler. `store.del(c, id)` raderar. `store.flush()`,
  `store.observe('op')` och gruppcommit behålls som de är.
- **Lagring** är en rad per record med dess klockkarta:

```sql
stores   (store PRIMARY KEY, v, epoch, lease_owner, lease_until)
records  (store, collection, id, data TEXT, clock TEXT, v, PRIMARY KEY (store, collection, id))
log      (store, v, ts, replica, seq, changes TEXT, PRIMARY KEY (store, v))   -- de senaste N
replicas (store, replica, seq, seen, PRIMARY KEY (store, replica))
```

  Index på föräldrafälten byggs i minnet när storen laddas. En record
  skrivs om hel vid varje op som rör den, så en stor singleton med
  tusentals blad hör hellre hemma i en collection.
- **Lease, migrationer, backup och restore** lyfts över från
  `sqlite-shared.js`. En migration blir en funktion över records per
  collection. En restore byter `epoch` och klienterna hör `reset`.
- **Retention** är tre regler med samma fönster, 30 dagar som standard:
  - loggen håller de senaste N ops, 1000, för deltas;
  - en op vars `ts` är äldre än fönstret avvisas med `expired`, och
    klienten släpper den och synkar om;
  - replikor som inte hörts av inom fönstret glöms, och raderade
    sökvägar äldre än fönstret rensas ur klockkartorna.

  Det håller av ett skäl: servern klämmer `ts` till sin egen tid, så en
  op den tillämpat är stämplad senast när den kom fram. En glömd replika
  har inte fått något tillämpat inom fönstret, så varje op av den som kan
  ha tillämpats är äldre än fönstret och avvisas om den skickas igen.
  Ingen op tillämpas två gånger, och en gammal `put` kan inte återskapa
  det som raderats sedan. En rensad radering kan bara ha skuggat en op
  äldre än sig själv, och den avvisas också. Ingen horisont behövs.

## 6. Följarstore: ledartabb, relä och port är en sak

Tre saker i dagens kod är samma sak byggd tre gånger: ledartabben
(`client/shared.js`, 700 rader), reläet (`relay/` och `server/relays.js`,
3 500 rader) och porten till en sandlådad frame (`client/port.js`). Alla
tre är klienter bakom en kopia som talar protokollet åt dem. I omstarten
är det ett begrepp, **följarstoren**: en store som har en store över sig.

```js
const edge = createStore({
  initial, singletons, parents, registers, rules,
  storage: fileStorage('/data/copies/site-1'),
  upstream: link,               // en socket till central, med följarens egen credential
  acknowledge: 'upstream'       // hubben; 'local' för ledartabben och porten
})
serve({ stores: () => edge, port: 36610, authenticate: req => credentialOf(req) })
```

- **Den serverar sessioner** med det vanliga protokollet, så en klient,
  en tabb eller en frame märker ingen skillnad mot servern.
- **Kopian är centrals bas med följarens kö ovanpå**, samma mekanik som
  klientens (§2). Följaren ber om klockkartorna i sin hello och dömer
  själv, så en op tillämpas på kopian direkt och syns för de andra
  sessionerna med en gång.
- **Två lägen för ack:**
  - `local`, för ledartabben och porten. Följaren är replikan: opsen går
    upp under dess eget replika-id från dess persisterade outbox, och en
    tabbs op ackas när den ligger där. Tabbarna är en replika, som idag.
  - `upstream`, för hubben. Följaren tillämpar direkt så att platsen ser
    ändringen, lägger open i sin kö och skickar den upp med `as`, men
    ackar först när central ackat, vidarebefordrat per replika och `seq`.
    Klienten behåller open i sin outbox under tiden, så den finns på två
    diskar tills central har den, och servern ignorerar dubbletten när
    båda skickar. Avvisar central open tar följaren bort den ur kön,
    räknar om kopian och skickar `error` vidare till klientens session.
    Är klienten borta får den samma svar när den skickar open igen.
    Mageeks toast för avvisade ändringar fungerar alltså även genom
    hubben.
- **Den skickar uppåt i den ordning den tillämpade**, på en enda länk,
  var och en märkt med klientens credential, så servern dömer, loggar och
  ackar varje op som klientens egen.
- **Tid är centrals.** Följaren lär sig centrals tid över länken, klämmer
  inkommande `ts` mot sin skattning av den och stämplar sina egna
  skrivningar med den. Dess egna skrivningar är `edge.patch(...)`, köade
  under dess egen credential och dömda hos central som följarens. I
  Mageek är det hubbens ring och sensorer.
- **Servern vouchar per credential**, en gång per länk, och `revoked`
  stänger klientens session hos följaren. Offline svarar följaren bara
  credentials som central släppt in genom den förut, sparade hashade som
  dagens `known.json`.
- **Den följer uppströms.** Centrals patchar går till basen och vidare
  till sessionerna. En snapshot diffas mot basen record för record, så
  en klient hör `reset` bara när centrals `epoch` bytts av en restore,
  inte för att central kom tillbaka. Är central borta svarar följaren ur
  kopian efter `grace`, och skickar `relay` med status `local`.
- **Den kan döma offline** när den har reglerna. `rules` är rena
  funktioner över användare, före och efter, så ett paket som delar dem
  mellan server och följare, som Mageeks `model/`, ger samma avvisningar
  på platsen som hos central.

Vad det ger efter ett avbrott: med tid på varje sökväg är det platsen
såg under avbrottet det central behåller, utom ops som central avvisar
och ändringar som förlorar mot senare skrivningar gjorda hos central
under tiden. Platsen slår inte om beroende på ankomstordning.

Transporterna är det som skiljer: ledartabben är en följarstore med
BroadcastChannel-sessioner och IndexedDB, hubben en med
WebSocket-sessioner och en fil, porten en session över MessagePort.
Skattning: 1 500 till 2 000 rader för alltihop, mot 4 200 idag. Det
mesta av reläets kantfall finns kvar: vilka credentials som får svar
offline, återkallelser, en restore medan följaren var borta, grace och
probing, och en följare som startar om under ett avbrott.

## 7. Behålls, krymper, försvinner

| Modul idag | Öde |
|---|---|
| `core/positions.js`, `core/ids.js` | Behålls orört |
| `client/list.js` (reconcile, LIS) | Behålls, blir listans kärna, över en collection eller en sökväg |
| `client/connection.js`, `transport.js` | Behålls: backoff, keepalive, wake |
| `client/storage.js`, `indexeddb.js`, `sqlite-bun.js` | Behålls, en rad blir en record i stället för ett blad |
| `client/shared.js`, `port.js`, `relay/*`, `server/relays.js` | Ersätts av följarstoren (§6): ledarval med Web Locks och fil-kopiorna lyfts över, resten blir en sak |
| `server/registry.js`, `hub.js`, `bun.js`, `node.js`, `snapshot.js`, `wire.js` | Behålls |
| `server/sqlite-shared.js` | Behålls: lease, migrationer, backup, checkpoint-worker |
| `server/store.js` | Krymper rejält: sessions, presence, limits, gruppcommit kvar; merge-grindarna borta |
| `vue/`, `react/` | Behålls, mindre |
| `testing/createNetwork`, `fakeTime`, fuzzarna, `bench/` | Behålls, det mest värdefulla i repot |
| `core/paths.js`, registers | Behålls: nycklar, sökvägar och register-matchningen |
| `core/model.js` | Krymper till validering och kompileringen av diff och invers till ändringar |
| `core/merge.js`, `core/clocks.js` | Skrivs om: regeln per sökväg mot en records klockkarta, några hundra rader; indexet över hela storen behövs inte |
| `core/hlc.js` | Ersätts av ett fyrtiotal rader: offset, högsta sedda, klämning |
| `client/facade.js` | Försvinner |

En grov gissning på storlek: runt 7 000 rader källkod mot dagens 13 000,
och de 3 800 raderna docs blir kanske hälften, för att `model.md` inte
längre behöver förklara barriärer och `relays.md` blir ett avsnitt om
följarstoren.

## 8. Migrering från dagens lazy-storage

- Ett skript över `store.export()` grupperar bladraderna per collection
  och id till records. Varje blads `ts` blir dess post i klockkartan,
  med millisekunderna ur dagens `[ms, count, replica]`. Tombstones inuti
  levande records blir raderade sökvägar, tombstones för hela records
  släpps, och barriärer blir födelser.
- Toppnycklar som inte är collections deklareras som `singletons`.
- Nästlade listor kan ligga kvar som nycklade maps inuti sin record och
  läsas med `db.list(sökväg)`, eller lyftas till en egen collection med
  en förälder när de har eget liv.
- `pos` behåller sitt format, och `registers` behåller sina
  deklarationer.
- Protokollversionen stegas, så gamla klienter hör `outdated` och
  laddar om, som idag.

## 9. Beslut, avgjorda mot Mageek

1. **LAN-reläet behövs, som följarstore i kärnan.** Mageeks hubb bygger
   på det, och planens §5.11 säger varför: inte fan-out, tre till fem
   skärmar per site sparar inget, utan att en site ska fungera när
   central är borta. Ledartabben behöver samma sak, med lokal ack.
2. **Tid på varje sökväg, stämplad kausalt och klämd av servern** (§2).
   Utan den kan en site slå om när central kommer tillbaka.
3. **lazy-watch som proxy, med inversen för att skilja `put` från
   `set`.** Mageek driver redan filträdet med `LazyWatch.on`, den
   skrivbara Vue-vyn bygger på den, och plugin-bryggan också.
4. **Formen deklareras, och delas av server och klienter:** collections
   och singletons i `initial`, `registers` och `parents`. Mageeks
   `model/stores.js` är redan den filen.
5. **Djup inuti en record, egen collection för eget liv.** Varje sökväg
   i en record mergas för sig; det som läggs till och raderas på egen
   hand blir en collection med en deklarerad förälder.
6. **Klienten är optimistisk först.** Exakt dömning med klockkartor på
   klienten kommer om flimret mellan en förlorande skrivning och dess ack
   någonsin märks.

## 10. Vad omskrivningen offrar, mätt mot Mageek

Mageek använder nästan allt: sju stores, femton register, listor,
delad anslutning i IndexedDB, porten till web-appar, den skrivbara
Vue-vyn, `validate` med tillgång till storens state, `readOnly`,
presence med egen `user`-funktion, `closeSessions`, minnesstores, och
hubben på reläet med `link`, `fileCopies`, `upstreamUp` och `stats`.
Det här är vad det kostar att flytta det:

- **Listor som arrayer.** `lists: ['windows', 'state/lists/*']` gör idag
  skrivbordets fönster och bryggans listor till arrayer som går att
  `push`a och `splice`a. I omstarten läses de med `db.list(...)` och
  skrivs med `add`, `move` och `remove`. Datan ligger kvar där den
  ligger, men skrivbordets kod och bryggans API ändras:
  `app.state.lists.items.push(...)` blir `app.list('items').add(...)`.
- **Reläet som det är.** Hubben skrivs om mot en följarstore i läget
  `upstream`. Den blir mindre: hubbens egna skrivningar av ringen och
  sensorerna går rakt på följaren med `edge.patch`, och klienten som
  hubben idag öppnar genom sitt eget relä försvinner.
- **Direktläsning av rader.** `storedAppState` läser tabellen `leaves`
  och bygger med `rebuild` och `registerSet`. Med en rad per record är
  det en `SELECT data FROM records WHERE store = ? AND collection =
  'windows'`, utan hjälp från biblioteket.
- **`validate` över diffar** blir regler per collection över före och
  efter, med storens state för att slå upp enheter och nivåer. Mageeks
  `rules.js` blir kortare i den formen, och kan flyttas till `model/` så
  att hubben dömer samma sak offline.
- **En förlorande skrivning syns tills dess ack.** Som idag, där
  korrigeringen kommer med ack:en.
- **Det som inte kostar något:**
  - app-storens `state` mergas på varje djup som idag, så bryggans
    `app.update(patch)` håller sitt löfte; formen blir `initial: { meta:
    {}, state: {}, windows: {} }` med `singletons: ['meta', 'state']`;
  - `registers` behåller sina femton deklarationer;
  - `store.patch` behåller sin form, så `spellRuns`, `spellKeep`,
    händelseringen och filträdet skrivs som idag;
  - barriärer och `made` saknas inte, eftersom Mageeks klienter aldrig
    tar bort indexrecords, och inte heller `conflict`, som ingen lyssnar
    på.

## 11. Ny version eller nytt projekt

**Samma repo och namn, som 1.0 på en gren, med 0.x kvar på en
`v0`-gren tills Mageek har flyttat.** Skälen:

- Pitchen, målen och dokumentationens indelning är oförändrade, och
  runt fyrtio procent av koden, hela testnätet, fuzzarna, benchen, CI
  och publiceringen följer med. Ett nytt projekt skulle kopiera allt
  det och lämna ett namn som två andra repon, Mageek och lazy-sofa,
  redan pekar på.
- 0.x till 1.0 med brutet protokoll är vad semver är till för, och
  `outdated` finns redan för en sida som är öppen över ett byte.
- Mageek pinnar `^0.26`, så ingenting går sönder förrän det väljer att
  flytta. Flytten är ändå allt på en gång, eftersom server, sida,
  agenter och hubbar talar samma protokoll, vilket Mageek redan klarar.

Gör det inte stegvis i `master`: modellbytet är för djupt för att
strypa fram modul för modul. Byt ut `src/` på en ren gren och plocka
över modulerna i §7, i tre steg:

1. **En tidsboxad spike.** Kärnan, alltså modellen, regeln,
   kompileringen ur lazy-watch, klienten med bas och väntande och en
   store i minnet, och följarstoren i båda lägena. Fuzzarna skrivs om
   först, mot de nya reglerna.
2. **Två acceptanstester som grind.**
   - Konvergensfuzzern, i vanligt och fientligt läge, grön på de nya
     reglerna, med en följare genom avbrott och omstarter. Invarianter
     utöver konvergens: inget barn utan förälder, ingen ackad op
     förlorad, ingen op tillämpad två gånger, och efter ett avbrott är
     platsens state centrals, utom det central avvisat eller som
     förlorat mot senare skrivningar där.
   - Mageeks `test/e2e/outage.test.js` grönt mot en hubb på
     följarstoren, på en gren i Mageek.
3. **Först därefter resten:** SQLite med records och migreringsskriptet,
   bindningarna och docs. I Mageek: `model/stores.js` med singletons,
   `rules.js` till regler i `model/`, `storedAppState` till en query,
   hubben till en följarstore, och listorna till list-API:t.

Klarar spiken inte grinden inom sin tid stannar 0.x som det är, och det
man lärt sig går in där.
