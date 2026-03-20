import asyncio
import aiohttp
import json
import re
import os
import time
from datetime import datetime
from playwright.async_api import async_playwright

# ══════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════
BASE_URL      = "https://anime-sama.to"
CATALOGUE_URL = "https://anime-sama.to/catalogue/?page={page}"
ANIME_ONLY    = True
TMDB_API_KEY  = "cfc454f98433e15eaa3b67f178fd8774"
TMDB_BASE     = "https://api.themoviedb.org/3"
JIKAN_BASE    = "https://api.jikan.moe/v4"
KITSU_BASE    = "https://kitsu.io/api/edge"
OUTPUT_DIR    = "AnimeData"

PAGE_BEGIN          = int(os.environ.get("PAGE_BEGIN",          "1"))
PAGE_END            = int(os.environ.get("PAGE_END",            "43"))
ANIME_BATCH_SIZE    = int(os.environ.get("ANIME_BATCH_SIZE",    "4"))
SAISON_BATCH_SIZE   = int(os.environ.get("SAISON_BATCH_SIZE",   "2"))
MAX_EPISODE_WORKERS = int(os.environ.get("MAX_EPISODE_WORKERS", "4"))
JIKAN_DELAY         = 0.35
MAX_RETRIES         = 3

_start = time.time()
def log(msg):
    e = int(time.time() - _start)
    print(f"[{e//60:02d}m{e%60:02d}s] {msg}", flush=True)

# ══════════════════════════════════════════════════════════════
#  RATE LIMITER ADAPTATIF
# ══════════════════════════════════════════════════════════════
class AdaptiveRateLimiter:
    MIN_DELAY   = 0.3
    MAX_DELAY   = 8.0
    STEP_UP     = 2.0
    STEP_DOWN   = 0.85
    BLOCK_PAUSE = 20.0

    def __init__(self):
        self._delay      = self.MIN_DELAY
        self._last_block = 0
        self._success    = 0
        self._failures   = 0

    async def wait(self):
        await asyncio.sleep(self._delay)

    def on_success(self):
        self._success += 1
        self._delay = max(self.MIN_DELAY, self._delay * self.STEP_DOWN)

    async def on_block(self):
        self._failures += 1
        self._delay = min(self.MAX_DELAY, self._delay * self.STEP_UP)
        now = time.time()
        if now - self._last_block > 30:
            self._last_block = now
            log(f"  [rate-limit] pause {self.BLOCK_PAUSE}s | delay={self._delay:.1f}s")
            await asyncio.sleep(self.BLOCK_PAUSE)
        else:
            await asyncio.sleep(self._delay)

    @property
    def stats(self):
        total = self._success + self._failures
        rate  = self._failures / total * 100 if total else 0
        return f"ok={self._success} fail={self._failures} ({rate:.0f}%) delay={self._delay:.1f}s"

_rl = None  # init dans main()

# ══════════════════════════════════════════════════════════════
#  CHECKPOINT
# ══════════════════════════════════════════════════════════════
def checkpoint_path(page_num):
    return os.path.join(OUTPUT_DIR, f".checkpoint_page_{page_num}.json")

def load_checkpoint(page_num):
    path = checkpoint_path(page_num)
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            pass
    return {"done": [], "animes": {}}

def save_checkpoint(page_num, nom, anime_data):
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        cp = load_checkpoint(page_num)
        if nom not in cp["done"]:
            cp["done"].append(nom)
        cp["animes"][nom] = anime_data
        with open(checkpoint_path(page_num), "w") as f:
            json.dump(cp, f, ensure_ascii=False)
    except Exception:
        pass

def clear_checkpoint(page_num):
    path = checkpoint_path(page_num)
    if os.path.exists(path):
        os.remove(path)

# ══════════════════════════════════════════════════════════════
#  UTILS
# ══════════════════════════════════════════════════════════════
def build_url(href):
    if not href:
        return None
    if href.startswith("http"):
        return href
    return BASE_URL + ("" if href.startswith("/") else "/") + href

def parse_info_rows(rows):
    info = {"genres": [], "type": None, "langues": []}
    for row in rows:
        label = row.get("label", "").lower()
        value = row.get("value", "").strip()
        if "genre" in label:
            info["genres"]  = [g.strip() for g in value.split(",") if g.strip()]
        elif "type" in label:
            info["type"]    = value
        elif "lang" in label:
            info["langues"] = [l.strip() for l in value.split(",") if l.strip()]
    return info

def clean_title(title):
    t = re.sub(r'\s*(saison|season|partie|part|film)\s*\d*', '', title, flags=re.IGNORECASE)
    return re.sub(r'\s*\d+$', '', t).strip()

def slug_from_url(url):
    m = re.search(r'/catalogue/([^/]+)/?$', url or "")
    return m.group(1) if m else None

def build_saison_url(anime_lien, titre, langue):
    slug = slug_from_url(anime_lien)
    if not slug:
        return None
    t = titre.lower().strip()
    if "film" in t:
        num = re.search(r'\d+', t)
        segment = "film" + (num.group() if num else "")
    else:
        s_num = re.search(r'saison\s*(\d+)', t)
        p_num = re.search(r'partie\s*(\d+)', t)
        if s_num:
            segment = "saison" + s_num.group(1)
            if p_num:
                segment += "-partie" + p_num.group(1)
        elif p_num:
            segment = "partie" + p_num.group(1)
        else:
            num = re.search(r'\d+', t)
            segment = "saison" + num.group() if num else "saison1"
    return BASE_URL + "/catalogue/" + slug + "/" + segment + "/" + langue.lower() + "/"

def save_json(data, page_num):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, f"page_{page_num}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log(f"saved {path} ({data['total']} animes)")

def new_ctx(browser):
    return browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        locale="fr-FR",
    )

# ══════════════════════════════════════════════════════════════
#  APIS
# ══════════════════════════════════════════════════════════════
async def get_jikan_id(session, title, is_film=False):
    query = clean_title(title)
    if not query:
        return None
    await asyncio.sleep(JIKAN_DELAY)
    media = "movie" if is_film else "tv"
    try:
        async with session.get(
            JIKAN_BASE + "/anime?q=" + query + "&type=" + media + "&limit=1",
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            data = (await r.json()).get("data", []) if r.status == 200 else []
        if not data:
            async with session.get(
                JIKAN_BASE + "/anime?q=" + query + "&limit=1",
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r2:
                data = (await r2.json()).get("data", []) if r2.status == 200 else []
        return data[0].get("mal_id") if data else None
    except Exception:
        return None

async def get_tmdb_id(session, title, is_film=False):
    query = clean_title(title)
    if not query:
        return None
    media = "movie" if is_film else "tv"
    try:
        async with session.get(
            TMDB_BASE + "/search/" + media
            + "?api_key=" + TMDB_API_KEY
            + "&query=" + query + "&language=fr-FR&page=1",
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            results = (await r.json()).get("results", []) if r.status == 200 else []
            return results[0].get("id") if results else None
    except Exception:
        return None

async def get_kitsu_id(session, title, is_film=False):
    query = clean_title(title)
    if not query:
        return None
    subtype = "movie" if is_film else "TV"
    hdrs = {"Accept": "application/vnd.api+json"}
    try:
        async with session.get(
            KITSU_BASE + "/anime?filter[text]=" + query
            + "&filter[subtype]=" + subtype + "&page[limit]=1",
            headers=hdrs,
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            data = (await r.json()).get("data", []) if r.status == 200 else []
        if not data:
            async with session.get(
                KITSU_BASE + "/anime?filter[text]=" + query + "&page[limit]=1",
                headers=hdrs,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r2:
                data = (await r2.json()).get("data", []) if r2.status == 200 else []
        return data[0].get("id") if data else None
    except Exception:
        return None

async def fetch_ids(session, title, is_film=False):
    j, t, k = await asyncio.gather(
        get_jikan_id(session, title, is_film),
        get_tmdb_id(session, title, is_film),
        get_kitsu_id(session, title, is_film),
    )
    return {"jikan_id": j, "tmdb_id": t, "kitsu_id": k}

async def check_url(session, url):
    try:
        async with session.head(
            url,
            timeout=aiohttp.ClientTimeout(total=6),
            allow_redirects=True
        ) as r:
            return r.status == 200
    except Exception:
        return False

# ══════════════════════════════════════════════════════════════
#  HELPERS PLAYWRIGHT
# ══════════════════════════════════════════════════════════════
async def goto_page(page, url):
    for strategy in ("networkidle", "domcontentloaded", "load"):
        try:
            await page.goto(url, wait_until=strategy, timeout=45000)
            return True
        except Exception:
            pass
    return False

async def wait_select(page, selector, timeout=20000):
    for _ in range(MAX_RETRIES):
        try:
            await page.wait_for_selector(selector, timeout=timeout)
            n = await page.evaluate(
                "() => document.querySelector('" + selector + "')?.options.length || 0"
            )
            if n > 0:
                return True
            await page.wait_for_timeout(800)
        except Exception:
            await page.wait_for_timeout(1000)
    return False

async def get_options(page, selector):
    for _ in range(MAX_RETRIES):
        opts = await page.evaluate(
            "() => { const s=document.querySelector('" + selector + "'); "
            "return s ? Array.from(s.options).map(o=>({value:o.value,label:o.text.trim()})) : []; }"
        )
        if opts:
            return opts
        await page.wait_for_timeout(800)
    return []

async def read_player(page):
    return await page.evaluate(
        "() => {"
        "  const f = document.querySelector('#playerDF');"
        "  if (!f) return null;"
        "  let s = f.getAttribute('src') || f.getAttribute('data-src');"
        "  if (s && s.length > 10 && !s.includes('about:blank')) return s;"
        "  for (const el of f.querySelectorAll('iframe,[src],[data-src]')) {"
        "    const v = el.getAttribute('src') || el.getAttribute('data-src') || '';"
        "    if (v.length > 10 && !v.includes('about:blank')) return v;"
        "  }"
        "  return null;"
        "}"
    )

async def wait_player(page, old_src="", timeout=6000):
    for attempt in range(MAX_RETRIES):
        try:
            await page.wait_for_function(
                "(old) => {"
                "  const f = document.querySelector('#playerDF');"
                "  if (!f) return false;"
                "  const srcs = ["
                "    f.getAttribute('src') || '',"
                "    f.getAttribute('data-src') || '',"
                "    ...[...f.querySelectorAll('iframe,[src],[data-src]')]"
                "      .map(e => e.getAttribute('src') || e.getAttribute('data-src') || '')"
                "  ].filter(s => s.length > 10 && !s.includes('about:blank'));"
                "  return srcs.length > 0 && srcs[0] !== old;"
                "}",
                arg=old_src,
                timeout=timeout
            )
        except Exception:
            pass
        src = await read_player(page)
        if src and src != old_src:
            return src
        await page.wait_for_timeout(700 * (attempt + 1))
    return await read_player(page)

async def is_blocked(page):
    try:
        title = (await page.title()).lower()
        url   = page.url
        if any(x in title for x in ["error", "403", "429", "blocked", "captcha", "access denied"]):
            return True
        if any(x in url for x in ["login", "captcha", "blocked"]):
            return True
        has_content = await page.evaluate(
            "() => document.querySelector('#selectEpisodes') !== null"
            " || document.querySelector('#playerDF') !== null"
            " || document.querySelector('h1') !== null"
        )
        return not has_content
    except Exception:
        return False

# ══════════════════════════════════════════════════════════════
#  SCRAPING — Catalogue
# ══════════════════════════════════════════════════════════════
async def scrape_catalogue(browser, page_num):
    ctx  = await new_ctx(browser)
    page = await ctx.new_page()
    await page.route("**/*.{png,jpg,jpeg,gif,webp,woff,woff2,ttf,mp4,mp3}", lambda r: r.abort())
    raw = []
    try:
        await page.goto(
            CATALOGUE_URL.format(page=page_num),
            wait_until="domcontentloaded",
            timeout=30000
        )
        await page.wait_for_selector("div.catalog-card", timeout=15000)
        raw = await page.evaluate(
            "() => Array.from(document.querySelectorAll('div.shrink-0.catalog-card.card-base')).map(card => {"
            "  const name = card.querySelector('h2.card-title')?.innerText.trim() || 'Inconnu';"
            "  let href = null;"
            "  card.querySelectorAll('a[href]').forEach(a => {"
            "    const h = a.getAttribute('href');"
            "    if (h?.includes('/catalogue/') && !href) href = h;"
            "  });"
            "  if (!href) href = card.querySelector('a[href]')?.getAttribute('href') || null;"
            "  const infoRows = [];"
            "  card.querySelectorAll('div.info-row span').forEach(span => {"
            "    const p = span.nextElementSibling;"
            "    if (p?.tagName === 'P') infoRows.push({ label: span.innerText.trim(), value: p.innerText.trim() });"
            "  });"
            "  return { name, href, infoRows };"
            "})"
        )
    except Exception as e:
        log(f"catalogue error p{page_num}: {e}")
    finally:
        try:
            await ctx.close()
        except Exception:
            pass

    animes = []
    for r in raw:
        info = parse_info_rows(r["infoRows"])
        if ANIME_ONLY and info["type"] and info["type"].lower() != "anime":
            continue
        animes.append({
            "nom":           r["name"],
            "type":          info["type"],
            "genres":        info["genres"],
            "langues":       info["langues"],
            "lien":          build_url(r["href"]),
            "image":         None,
            "noms_alt":      [],
            "synopsis":      None,
            "bande_annonce": None,
            "ids":           {"jikan_id": None, "tmdb_id": None, "kitsu_id": None},
            "saisons":       [],
        })
    return animes

# ══════════════════════════════════════════════════════════════
#  SCRAPING — Detail
# ══════════════════════════════════════════════════════════════
async def scrape_detail(browser, url):
    ctx  = await new_ctx(browser)
    page = await ctx.new_page()
    await page.route("**/*.{png,jpg,jpeg,gif,webp,woff,woff2,ttf,mp4,mp3}", lambda r: r.abort())
    result = {}
    try:
        await goto_page(page, url)
        try:
            await page.wait_for_function(
                "() => !!document.querySelector('#coverOeuvre') || !!document.querySelector('h1')",
                timeout=12000
            )
        except Exception:
            pass
        await page.wait_for_timeout(600)
        result = await page.evaluate(
            "() => {"
            "  const img = document.querySelector('#coverOeuvre');"
            "  const image = img?.getAttribute('src') || img?.getAttribute('data-src') || null;"
            "  const alt = document.querySelector('#titreAlter');"
            "  const nomsAlt = alt ? alt.innerText.trim().split(',').map(s=>s.trim()).filter(Boolean) : [];"
            "  const syn = document.querySelector('p.text-sm.text-gray-300.leading-relaxed');"
            "  const synopsis = syn?.innerText.trim() || null;"
            "  const ifr = document.querySelector('#bandeannonce');"
            "  const bandeAnnonce = ifr ? (ifr.getAttribute('src') || ifr.getAttribute('data-src')) : null;"
            "  const cont = document.querySelector('.flex.flex-wrap.overflow-y-hidden.justify-start.bg-slate-900.bg-opacity-70.rounded.mt-2.h-auto');"
            "  const saisons = [];"
            "  if (cont) {"
            "    cont.querySelectorAll('a').forEach(a => {"
            "      let lbl = a.querySelector('.text-white.font-bold.text-center.absolute.w-28')"
            "             || a.querySelector('[class*=\"font-bold\"][class*=\"text-center\"]');"
            "      const tv = lbl?.innerText.trim() || a.innerText.trim();"
            "      const tc = a.getAttribute('title') || a.getAttribute('aria-label') || tv;"
            "      if (tv) saisons.push({ titreVignette: tv, titreComplet: tc, isFilm: tv.toLowerCase().includes('film') });"
            "    });"
            "  }"
            "  return { image, nomsAlt, synopsis, bandeAnnonce, saisons };"
            "}"
        )
    except Exception as e:
        log(f"detail error {url}: {e}")
    finally:
        try:
            await ctx.close()
        except Exception:
            pass
    return result

# ══════════════════════════════════════════════════════════════
#  SCRAPING — Lecteurs d'un episode (helper)
# ══════════════════════════════════════════════════════════════
async def collect_lecteurs(page):
    lecteurs = []
    opts = await get_options(page, "#selectLecteurs")
    for lect in opts:
        old = await read_player(page) or ""
        selected = False
        try:
            await page.select_option("#selectLecteurs", value=lect["value"])
            await page.wait_for_timeout(300)
            selected = True
        except Exception:
            selected = False
        if selected:
            src = await wait_player(page, old_src=old, timeout=6000)
            if not src:
                await page.wait_for_timeout(1500)
                src = await read_player(page)
            if src and src != old:
                lecteurs.append({"lecteur": lect["label"], "url": src})
    return lecteurs

# ══════════════════════════════════════════════════════════════
#  SCRAPING — Une tentative d'episode (pas de continue dans try)
# ══════════════════════════════════════════════════════════════
async def _attempt_episode(browser, saison_url, ep_value, ep_label):
    ctx  = await new_ctx(browser)
    page = await ctx.new_page()
    found = None
    try:
        await _rl.wait()
        loaded = await goto_page(page, saison_url)
        if loaded:
            blocked = await is_blocked(page)
            if blocked:
                await _rl.on_block()
            else:
                has_eps = await wait_select(page, "#selectEpisodes")
                if has_eps:
                    await page.wait_for_timeout(300)
                    ep_ok = False
                    try:
                        await page.select_option("#selectEpisodes", value=ep_value)
                        await page.wait_for_timeout(500)
                        ep_ok = True
                    except Exception:
                        ep_ok = False
                    if ep_ok:
                        has_lect = await wait_select(page, "#selectLecteurs", timeout=12000)
                        if not has_lect:
                            await page.wait_for_timeout(1000)
                            src = await wait_player(page)
                            if src:
                                _rl.on_success()
                                found = {"episode": ep_label, "lecteurs": [{"lecteur": "default", "url": src}]}
                        else:
                            lecteurs = await collect_lecteurs(page)
                            if lecteurs:
                                _rl.on_success()
                                found = {"episode": ep_label, "lecteurs": lecteurs}
                            else:
                                await _rl.on_block()
    except Exception as e:
        log(f"    attempt error ep {ep_label}: {e}")
    finally:
        try:
            await ctx.close()
        except Exception:
            pass
    return found

async def scrape_episode(browser, saison_url, ep_value, ep_label):
    for _ in range(MAX_RETRIES):
        result = await _attempt_episode(browser, saison_url, ep_value, ep_label)
        if result is not None:
            return result
    return {"episode": ep_label, "lecteurs": []}

# ══════════════════════════════════════════════════════════════
#  SCRAPING — Liste episodes d'une saison
# ══════════════════════════════════════════════════════════════
async def _get_eps_list(browser, saison_url):
    ctx  = await new_ctx(browser)
    page = await ctx.new_page()
    eps  = []
    try:
        await _rl.wait()
        await goto_page(page, saison_url)
        blocked = await is_blocked(page)
        if not blocked:
            if await wait_select(page, "#selectEpisodes", timeout=25000):
                await page.wait_for_timeout(500)
                eps = await get_options(page, "#selectEpisodes")
                if eps:
                    _rl.on_success()
        else:
            await _rl.on_block()
    except Exception:
        pass
    finally:
        try:
            await ctx.close()
        except Exception:
            pass
    return eps

async def scrape_saison_episodes(browser, saison_url):
    eps_options = []
    for attempt in range(MAX_RETRIES):
        eps_options = await _get_eps_list(browser, saison_url)
        if eps_options:
            break
        log(f"    episode list empty attempt {attempt+1}/{MAX_RETRIES}")
        await asyncio.sleep(3)

    if not eps_options:
        log(f"    SKIP no episodes: {saison_url}")
        return []

    slug = "/".join(saison_url.rstrip("/").split("/")[-2:])
    log(f"    {len(eps_options)} ep [{slug}]")

    sem = asyncio.Semaphore(MAX_EPISODE_WORKERS)

    async def safe(ep):
        async with sem:
            return await scrape_episode(browser, saison_url, ep["value"], ep["label"])

    episodes = list(await asyncio.gather(*[safe(ep) for ep in eps_options]))

    vides = [i for i, e in enumerate(episodes) if not e["lecteurs"]]
    taux  = len(vides) / len(episodes) if episodes else 0

    if taux > 0.4:
        log(f"    {len(vides)}/{len(episodes)} empty (rate limit?), seq retry pause 15s")
        await asyncio.sleep(15)
        for i in vides:
            res = await scrape_episode(
                browser, saison_url,
                eps_options[i]["value"], eps_options[i]["label"]
            )
            if res["lecteurs"]:
                episodes[i] = res
            await asyncio.sleep(_rl._delay)
    elif vides:
        log(f"    retry: {len(vides)} empty")
        await asyncio.sleep(3)
        r1 = await asyncio.gather(*[safe(eps_options[i]) for i in vides])
        for i, res in zip(vides, r1):
            if res["lecteurs"]:
                episodes[i] = res
        vides2 = [i for i, e in enumerate(episodes) if not e["lecteurs"]]
        if vides2:
            await asyncio.sleep(8)
            r2 = await asyncio.gather(*[safe(eps_options[i]) for i in vides2])
            for i, res in zip(vides2, r2):
                if res["lecteurs"]:
                    episodes[i] = res

    ok = sum(1 for e in episodes if e["lecteurs"])
    log(f"    {ok}/{len(episodes)} OK | {_rl.stats}")
    return episodes

# ══════════════════════════════════════════════════════════════
#  SCRAPING — Saison
# ══════════════════════════════════════════════════════════════
async def process_saison(browser, session, anime_nom, anime_lien, saison, langues_anime):
    titre   = saison["titreVignette"]
    is_film = saison.get("isFilm", False)
    log(f"  saison: {titre}")

    saison["ids"] = await fetch_ids(session, anime_nom + " " + titre, is_film=is_film)

    url_vf     = build_saison_url(anime_lien, titre, "vf")
    url_vostfr = build_saison_url(anime_lien, titre, "vostfr")
    saison["lien_vf"]     = url_vf
    saison["lien_vostfr"] = url_vostfr

    prefer_vf  = "VF" in [l.upper() for l in langues_anime]
    url_cible  = None
    langue_eff = None

    if prefer_vf and url_vf:
        if await check_url(session, url_vf):
            url_cible  = url_vf
            langue_eff = "vf"
    if url_cible is None and url_vostfr:
        if await check_url(session, url_vostfr):
            url_cible  = url_vostfr
            langue_eff = "vostfr"
    if url_cible is None:
        if prefer_vf:
            url_cible, langue_eff = url_vf, "vf"
        else:
            url_cible, langue_eff = url_vostfr, "vostfr"

    saison["langue"] = langue_eff
    log(f"    langue: {langue_eff}")

    if url_cible:
        try:
            saison["episodes"] = await scrape_saison_episodes(browser, url_cible)
        except Exception as e:
            log(f"    episodes error: {e}")
            saison["episodes"] = []
    else:
        saison["episodes"] = []
    return saison

# ══════════════════════════════════════════════════════════════
#  SCRAPING — Anime
# ══════════════════════════════════════════════════════════════
async def process_anime(browser, session, anime, idx, total):
    nom = anime["nom"]
    log(f"[{idx}/{total}] {nom}")

    if anime["lien"]:
        try:
            detail = await scrape_detail(browser, anime["lien"])
            anime["image"]         = detail.get("image")
            anime["noms_alt"]      = detail.get("nomsAlt", [])
            anime["synopsis"]      = detail.get("synopsis")
            anime["bande_annonce"] = detail.get("bandeAnnonce")
            saisons = detail.get("saisons", [])
            for s in saisons:
                s["ids"]         = {"jikan_id": None, "tmdb_id": None, "kitsu_id": None}
                s["langue"]      = None
                s["lien_vf"]     = None
                s["lien_vostfr"] = None
                s["episodes"]    = []
            anime["saisons"] = saisons
            log(f"  detail OK — {len(saisons)} saison(s)")
        except Exception as e:
            log(f"  detail error: {e}")

    anime["ids"] = await fetch_ids(session, nom)
    log(f"  ids: jikan={anime['ids']['jikan_id']} tmdb={anime['ids']['tmdb_id']} kitsu={anime['ids']['kitsu_id']}")

    saisons = anime["saisons"]
    for b in range(0, len(saisons), SAISON_BATCH_SIZE):
        batch_s = saisons[b: b + SAISON_BATCH_SIZE]
        results = await asyncio.gather(*[
            process_saison(browser, session, nom, anime["lien"], s, anime.get("langues", []))
            for s in batch_s
        ], return_exceptions=True)
        for i, res in enumerate(results):
            if isinstance(res, Exception):
                log(f"  saison error: {res}")
                batch_s[i]["episodes"] = []

    nb_eps = sum(len(s.get("episodes", [])) for s in anime["saisons"])
    log(f"  DONE {nom} — {len(anime['saisons'])}s {nb_eps}ep")
    return anime

# ══════════════════════════════════════════════════════════════
#  SCRAPING — Page
# ══════════════════════════════════════════════════════════════
async def process_page(browser, session, page_num):
    t0 = time.time()
    log(f"=== PAGE {page_num} START ===")

    animes = await scrape_catalogue(browser, page_num)
    total  = len(animes)
    log(f"  {total} animes — batch={ANIME_BATCH_SIZE}")

    cp      = load_checkpoint(page_num)
    done    = set(cp.get("done", []))
    cp_data = cp.get("animes", {})

    if done:
        log(f"  checkpoint: {len(done)}/{total} already done")

    for anime in animes:
        if anime["nom"] in cp_data:
            anime.update(cp_data[anime["nom"]])

    todo = [a for a in animes if a["nom"] not in done]
    log(f"  {len(todo)} animes to process")

    for b_start in range(0, len(todo), ANIME_BATCH_SIZE):
        batch   = todo[b_start: b_start + ANIME_BATCH_SIZE]
        noms    = ", ".join(a["nom"][:18] for a in batch)
        log(f"  batch [{b_start+1}-{b_start+len(batch)}/{len(todo)}]: {noms}")

        results = await asyncio.gather(
            *[process_anime(browser, session, a, animes.index(a)+1, total) for a in batch],
            return_exceptions=True
        )

        ok = 0
        for i, res in enumerate(results):
            nom = batch[i]["nom"]
            if isinstance(res, dict):
                idx = next(j for j, a in enumerate(animes) if a["nom"] == nom)
                animes[idx] = res
                save_checkpoint(page_num, nom, res)
                ok += 1
            else:
                log(f"  ERROR {nom}: {res}")
                save_checkpoint(page_num, nom, batch[i])

        log(f"  batch done {ok}/{len(batch)} | rl: {_rl.stats}")

    elapsed = int(time.time() - t0)
    log(f"=== PAGE {page_num} DONE {total} animes in {elapsed//60}m{elapsed%60:02d}s ===")

    data = {
        "page":       page_num,
        "scraped_at": datetime.now().strftime("%Y%m%d_%H%M%S"),
        "duration_s": elapsed,
        "total":      total,
        "animes":     animes,
    }
    save_json(data, page_num)
    clear_checkpoint(page_num)
    return data

# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════
async def main():
    global _rl
    _rl   = AdaptiveRateLimiter()
    pages = list(range(PAGE_BEGIN, PAGE_END + 1))
    log(f"START pages {PAGE_BEGIN}->{PAGE_END} ({len(pages)} pages)")
    log(f"batch={ANIME_BATCH_SIZE} saisons={SAISON_BATCH_SIZE} eps={MAX_EPISODE_WORKERS}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox",
                  "--disable-dev-shm-usage", "--disable-gpu"]
        )
        connector = aiohttp.TCPConnector(limit=20)
        async with aiohttp.ClientSession(connector=connector) as session:
            for page_num in pages:
                try:
                    await process_page(browser, session, page_num)
                except Exception as e:
                    log(f"PAGE {page_num} ERROR: {e}")
        await browser.close()

    elapsed = int(time.time() - _start)
    log(f"ALL DONE in {elapsed//60}m{elapsed%60:02d}s")

if __name__ == "__main__":
    asyncio.run(main())
