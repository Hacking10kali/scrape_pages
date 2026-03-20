# ============================================================
#  ANIME-SAMA SCRAPER — GitHub Actions
#  Adaptatif : rapide par défaut, ralentit si rate-limited
#  Checkpoint : reprend là où on s'est arrêté
# ============================================================

import asyncio
import aiohttp
import json
import re
import os
import time
from datetime import datetime
from playwright.async_api import async_playwright

# ══════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════

BASE_URL      = "https://anime-sama.to"
CATALOGUE_URL = "https://anime-sama.to/catalogue/?page={page}"
ANIME_ONLY    = True

TMDB_API_KEY = "cfc454f98433e15eaa3b67f178fd8774"
TMDB_BASE    = "https://api.themoviedb.org/3"
JIKAN_BASE   = "https://api.jikan.moe/v4"
KITSU_BASE   = "https://kitsu.io/api/edge"

OUTPUT_DIR = "AnimeData"

PAGE_BEGIN          = int(os.environ.get("PAGE_BEGIN",          "1"))
PAGE_END            = int(os.environ.get("PAGE_END",            "43"))
ANIME_BATCH_SIZE    = int(os.environ.get("ANIME_BATCH_SIZE",    "4"))
SAISON_BATCH_SIZE   = int(os.environ.get("SAISON_BATCH_SIZE",   "2"))
MAX_EPISODE_WORKERS = int(os.environ.get("MAX_EPISODE_WORKERS", "4"))
JIKAN_DELAY         = 0.35
MAX_RETRIES         = 3

_start_time = time.time()

def log(msg):
    e = int(time.time() - _start_time)
    print(f"[{e//60:02d}m{e%60:02d}s] {msg}", flush=True)

# ══════════════════════════════════════════════════════════════
#  RATE LIMITER ADAPTATIF
#  Commence rapide, ralentit si le site bloque, accélère si OK
# ══════════════════════════════════════════════════════════════

class AdaptiveRateLimiter:
    """
    Gère le délai entre requêtes de façon adaptative.
    - Succès  → réduit le délai (min: MIN_DELAY)
    - Blocage → augmente le délai (max: MAX_DELAY) + pause globale
    """
    MIN_DELAY   = 0.3   # délai minimal entre requêtes (s)
    MAX_DELAY   = 8.0   # délai maximal
    STEP_UP     = 2.0   # multiplicateur si bloqué
    STEP_DOWN   = 0.85  # multiplicateur si OK
    BLOCK_PAUSE = 20.0  # pause globale si rate limited

    def __init__(self):
        self._delay      = self.MIN_DELAY
        self._lock       = asyncio.Lock()
        self._blocked    = False
        self._last_block = 0
        self._success    = 0
        self._failures   = 0

    async def wait(self):
        await asyncio.sleep(self._delay)

    def on_success(self):
        self._success += 1
        if self._delay > self.MIN_DELAY:
            self._delay = max(self.MIN_DELAY, self._delay * self.STEP_DOWN)

    async def on_block(self):
        self._failures += 1
        self._delay = min(self.MAX_DELAY, self._delay * self.STEP_UP)
        now = time.time()
        # Pause globale une seule fois toutes les 30s
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

# Instance globale partagée par tous les workers
_rl = None  # initialisé dans main()

# ══════════════════════════════════════════════════════════════
#  CHECKPOINT — reprendre là où on s'est arrêté
# ══════════════════════════════════════════════════════════════

def checkpoint_path(page_num):
    return os.path.join(OUTPUT_DIR, f".checkpoint_page_{page_num}.json")

def load_checkpoint(page_num):
    path = checkpoint_path(page_num)
    if os.path.exists(path):
        try:
            with open(path) as f:
                data = json.load(f)
            log(f"  checkpoint loaded: {len(data.get('done',[]))} animes done")
            return data
        except Exception:
            pass
    return {"done": [], "animes": {}}

def save_checkpoint(page_num, nom, anime_data):
    path = checkpoint_path(page_num)
    try:
        cp = load_checkpoint(page_num)
        if nom not in cp["done"]:
            cp["done"].append(nom)
        cp["animes"][nom] = anime_data
        with open(path, "w") as f:
            json.dump(cp, f, ensure_ascii=False)
    except Exception:
        pass

def clear_checkpoint(page_num):
    path = checkpoint_path(page_num)
    if os.path.exists(path):
        os.remove(path)

# ══════════════════════════════════════════════════════════════
#  UTILITAIRES
# ══════════════════════════════════════════════════════════════

def build_url(href):
    if not href: return None
    return href if href.startswith("http") else BASE_URL + ("" if href.startswith("/") else "/") + href

def parse_info_rows(rows):
    info = {"genres": [], "type": None, "langues": []}
    for row in rows:
        label = row.get("label", "").lower()
        value = row.get("value", "").strip()
        if   "genre" in label: info["genres"]  = [g.strip() for g in value.split(",") if g.strip()]
        elif "type"  in label: info["type"]    = value
        elif "lang"  in label: info["langues"] = [l.strip() for l in value.split(",") if l.strip()]
    return info

def clean_title(title):
    t = re.sub(r'\s*(saison|season|partie|part|film)\s*\d*', '', title, flags=re.IGNORECASE)
    return re.sub(r'\s*\d+$', '', t).strip()

def slug_from_url(url):
    m = re.search(r'/catalogue/([^/]+)/?$', url or "")
    return m.group(1) if m else None

def build_saison_url(anime_lien, titre, langue):
    slug = slug_from_url(anime_lien)
    if not slug: return None
    t = titre.lower().strip()
    if "film" in t:
        num = re.search(r'\d+', t)
        segment = "film" + (num.group() if num else "")
    else:
        s_num = re.search(r'saison\s*(\d+)', t)
        p_num = re.search(r'partie\s*(\d+)', t)
        if s_num:
            segment = f"saison{s_num.group(1)}"
            if p_num: segment += f"-partie{p_num.group(1)}"
        elif p_num:
            segment = f"partie{p_num.group(1)}"
        else:
            num = re.search(r'\d+', t)
            segment = f"saison{num.group()}" if num else "saison1"
    return f"{BASE_URL}/catalogue/{slug}/{segment}/{langue.lower()}/"

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
    if not query: return None
    await asyncio.sleep(JIKAN_DELAY)
    media = "movie" if is_film else "tv"
    try:
        async with session.get(f"{JIKAN_BASE}/anime?q={query}&type={media}&limit=1",
                               timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = (await r.json()).get("data", []) if r.status == 200 else []
        if not data:
            async with session.get(f"{JIKAN_BASE}/anime?q={query}&limit=1",
                                   timeout=aiohttp.ClientTimeout(total=10)) as r2:
                data = (await r2.json()).get("data", []) if r2.status == 200 else []
        return data[0].get("mal_id") if data else None
    except Exception: return None

async def get_tmdb_id(session, title, is_film=False):
    query = clean_title(title)
    if not query: return None
    media = "movie" if is_film else "tv"
    try:
        async with session.get(
            f"{TMDB_BASE}/search/{media}?api_key={TMDB_API_KEY}&query={query}&language=fr-FR&page=1",
            timeout=aiohttp.ClientTimeout(total=10)) as r:
            results = (await r.json()).get("results", []) if r.status == 200 else []
            return results[0].get("id") if results else None
    except Exception: return None

async def get_kitsu_id(session, title, is_film=False):
    query = clean_title(title)
    if not query: return None
    subtype = "movie" if is_film else "TV"
    hdrs = {"Accept": "application/vnd.api+json"}
    try:
        async with session.get(
            f"{KITSU_BASE}/anime?filter[text]={query}&filter[subtype]={subtype}&page[limit]=1",
            headers=hdrs, timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = (await r.json()).get("data", []) if r.status == 200 else []
        if not data:
            async with session.get(f"{KITSU_BASE}/anime?filter[text]={query}&page[limit]=1",
                                   headers=hdrs, timeout=aiohttp.ClientTimeout(total=10)) as r2:
                data = (await r2.json()).get("data", []) if r2.status == 200 else []
        return data[0].get("id") if data else None
    except Exception: return None

async def fetch_ids(session, title, is_film=False):
    j, t, k = await asyncio.gather(
        get_jikan_id(session, title, is_film),
        get_tmdb_id(session, title, is_film),
        get_kitsu_id(session, title, is_film),
    )
    return {"jikan_id": j, "tmdb_id": t, "kitsu_id": k}

async def check_url(session, url):
    try:
        async with session.head(url, timeout=aiohttp.ClientTimeout(total=6),
                                allow_redirects=True) as r:
            return r.status == 200
    except Exception: return False

# ══════════════════════════════════════════════════════════════
#  HELPERS PLAYWRIGHT
# ══════════════════════════════════════════════════════════════

async def goto_page(page, url):
    for strategy in ("networkidle", "domcontentloaded", "load"):
        try:
            await page.goto(url, wait_until=strategy, timeout=45000)
            return True
        except Exception:
            continue
    return False

async def wait_select(page, selector, timeout=20000):
    for _ in range(MAX_RETRIES):
        try:
            await page.wait_for_selector(selector, timeout=timeout)
            n = await page.evaluate(
                f"() => document.querySelector('{selector}')?.options.length || 0"
            )
            if n > 0: return True
            await page.wait_for_timeout(800)
        except Exception:
            await page.wait_for_timeout(1000)
    return False

async def get_options(page, selector):
    for _ in range(MAX_RETRIES):
        opts = await page.evaluate(
            f"() => {{ const s=document.querySelector('{selector}'); "
            f"return s ? Array.from(s.options).map(o=>({{value:o.value,label:o.text.trim()}})) : []; }}"
        )
        if opts: return opts
        await page.wait_for_timeout(800)
    return []

async def read_player(page):
    return await page.evaluate(
        """() => {
            const f = document.querySelector('#playerDF');
            if (!f) return null;
            let s = f.getAttribute('src') || f.getAttribute('data-src');
            if (s && s.length > 10 && !s.includes('about:blank')) return s;
            for (const el of f.querySelectorAll('iframe,[src],[data-src]')) {
                const v = el.getAttribute('src') || el.getAttribute('data-src') || '';
                if (v.length > 10 && !v.includes('about:blank')) return v;
            }
            return null;
        }"""
    )

async def wait_player(page, old_src="", timeout=6000):
    for attempt in range(MAX_RETRIES):
        try:
            await page.wait_for_function(
                """(old) => {
                    const f = document.querySelector('#playerDF');
                    if (!f) return false;
                    const srcs = [
                        f.getAttribute('src') || '',
                        f.getAttribute('data-src') || '',
                        ...[...f.querySelectorAll('iframe,[src],[data-src]')]
                          .map(e => e.getAttribute('src') || e.getAttribute('data-src') || '')
                    ].filter(s => s.length > 10 && !s.includes('about:blank'));
                    return srcs.length > 0 && srcs[0] !== old;
                }""",
                arg=old_src, timeout=timeout
            )
        except Exception:
            pass
        src = await read_player(page)
        if src and src != old_src: return src
        await page.wait_for_timeout(700 * (attempt + 1))
    return await read_player(page)

async def is_blocked(page):
    """Détecte si le site a bloqué la requête."""
    try:
        title = (await page.title()).lower()
        url   = page.url
        if any(x in title for x in ["error","403","429","blocked","captcha","access denied"]):
            return True
        if any(x in url for x in ["login","captcha","blocked"]):
            return True
        has_content = await page.evaluate(
            "() => document.querySelector('#selectEpisodes') !== null || "
            "document.querySelector('#playerDF') !== null || "
            "document.querySelector('h1') !== null"
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
    try:
        await page.goto(CATALOGUE_URL.format(page=page_num),
                        wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_selector("div.catalog-card", timeout=15000)
        raw = await page.evaluate("""
            () => Array.from(document.querySelectorAll('div.shrink-0.catalog-card.card-base')).map(card => {
                const name = card.querySelector('h2.card-title')?.innerText.trim() || 'Inconnu';
                let href = null;
                card.querySelectorAll('a[href]').forEach(a => {
                    const h = a.getAttribute('href');
                    if (h?.includes('/catalogue/') && !href) href = h;
                });
                if (!href) href = card.querySelector('a[href]')?.getAttribute('href') || null;
                const infoRows = [];
                card.querySelectorAll('div.info-row span').forEach(span => {
                    const p = span.nextElementSibling;
                    if (p?.tagName === 'P')
                        infoRows.push({ label: span.innerText.trim(), value: p.innerText.trim() });
                });
                return { name, href, infoRows };
            })
        """)
    finally:
        await ctx.close()

    animes = []
    for r in raw:
        info = parse_info_rows(r["infoRows"])
        if ANIME_ONLY and info["type"] and info["type"].lower() != "anime":
            continue
        animes.append({
            "nom": r["name"], "type": info["type"],
            "genres": info["genres"], "langues": info["langues"],
            "lien": build_url(r["href"]),
            "image": None, "noms_alt": [], "synopsis": None, "bande_annonce": None,
            "ids": {"jikan_id": None, "tmdb_id": None, "kitsu_id": None},
            "saisons": [],
        })
    return animes

# ══════════════════════════════════════════════════════════════
#  SCRAPING — Détail
# ══════════════════════════════════════════════════════════════

async def scrape_detail(browser, url):
    ctx  = await new_ctx(browser)
    page = await ctx.new_page()
    await page.route("**/*.{png,jpg,jpeg,gif,webp,woff,woff2,ttf,mp4,mp3}", lambda r: r.abort())
    try:
        await goto_page(page, url)
        try:
            await page.wait_for_function(
                "() => !!document.querySelector('#coverOeuvre') || !!document.querySelector('h1')",
                timeout=12000)
        except Exception:
            pass
        await page.wait_for_timeout(600)
        return await page.evaluate("""
            () => {
                const img = document.querySelector('#coverOeuvre');
                const image = img?.getAttribute('src') || img?.getAttribute('data-src') || null;
                const alt = document.querySelector('#titreAlter');
                const nomsAlt = alt
                    ? alt.innerText.trim().split(',').map(s=>s.trim()).filter(Boolean) : [];
                const syn = document.querySelector('p.text-sm.text-gray-300.leading-relaxed');
                const synopsis = syn?.innerText.trim() || null;
                const ifr = document.querySelector('#bandeannonce');
                const bandeAnnonce = ifr
                    ? (ifr.getAttribute('src') || ifr.getAttribute('data-src')) : null;
                const cont = document.querySelector(
                    '.flex.flex-wrap.overflow-y-hidden.justify-start' +
                    '.bg-slate-900.bg-opacity-70.rounded.mt-2.h-auto'
                );
                const saisons = [];
                cont?.querySelectorAll('a').forEach(a => {
                    let lbl = a.querySelector('.text-white.font-bold.text-center.absolute.w-28')
                           || a.querySelector('[class*="font-bold"][class*="text-center"]');
                    const tv = lbl?.innerText.trim() || a.innerText.trim();
                    const tc = a.getAttribute('title') || a.getAttribute('aria-label') || tv;
                    if (tv) saisons.push({
                        titreVignette: tv, titreComplet: tc,
                        isFilm: tv.toLowerCase().includes('film')
                    });
                });
                return { image, nomsAlt, synopsis, bandeAnnonce, saisons };
            }
        """)
    finally:
        await ctx.close()

# ══════════════════════════════════════════════════════════════
#  SCRAPING — Un épisode
# ══════════════════════════════════════════════════════════════

async def _scrape_lecteurs(page):
    """Extrait tous les lecteurs depuis la page courante."""
    lecteurs = []
    opts = await get_options(page, "#selectLecteurs")
    for lect in opts:
        old = await read_player(page) or ""
        ok = False
        try:
            await page.select_option("#selectLecteurs", value=lect["value"])
            await page.wait_for_timeout(300)
            ok = True
        except Exception:
            pass
        if ok:
            src = await wait_player(page, old_src=old, timeout=6000)
            if not src:
                await page.wait_for_timeout(1500)
                src = await read_player(page)
            if src and src != old:
                lecteurs.append({"lecteur": lect["label"], "url": src})
    return lecteurs


async def scrape_episode(browser, saison_url, ep_value, ep_label):
    """Scrape un épisode — zéro continue dans les try pour éviter SyntaxError."""
    for attempt in range(MAX_RETRIES):
        result = await _try_scrape_episode(browser, saison_url, ep_value, ep_label)
        if result is not None:
            return result
    return {"episode": ep_label, "lecteurs": []}


async def _try_scrape_episode(browser, saison_url, ep_value, ep_label):
    """Une seule tentative de scraping. Retourne le résultat ou None si échec."""
    ctx  = await new_ctx(browser)
    page = await ctx.new_page()
    try:
        await _rl.wait()

        if not await goto_page(page, saison_url):
            return None

        if await is_blocked(page):
            await _rl.on_block()
            return None

        if not await wait_select(page, "#selectEpisodes"):
            return None

        await page.wait_for_timeout(300)

        ep_selected = False
        try:
  
