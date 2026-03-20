# ============================================================
#  ANIME-SAMA SCRAPER — GitHub Actions
#  begin/end passés en variables d'environnement
#  Sauvegarde JSON dans AnimeData/ du repo
# ============================================================

import asyncio
import aiohttp
import json
import re
import os
import time
import threading
import sys
from datetime import datetime
from playwright.async_api import async_playwright, Browser

# ══════════════════════════════════════════════════════════════
#  CONFIGURATION — lues depuis les variables d'environnement
# ══════════════════════════════════════════════════════════════

BASE_URL      = "https://anime-sama.to"
CATALOGUE_URL = "https://anime-sama.to/catalogue/?page={page}"
ANIME_ONLY    = True

TMDB_API_KEY = "cfc454f98433e15eaa3b67f178fd8774"
TMDB_BASE    = "https://api.themoviedb.org/3"
JIKAN_BASE   = "https://api.jikan.moe/v4"
KITSU_BASE   = "https://kitsu.io/api/edge"

# Dossier de sortie dans le repo
OUTPUT_DIR = "AnimeData"

# Lus depuis les variables d'env (GitHub Actions les injecte)
PAGE_BEGIN = int(os.environ.get("PAGE_BEGIN", "1"))
PAGE_END   = int(os.environ.get("PAGE_END",   "43"))

# Workers
MAX_PAGE_WORKERS    = int(os.environ.get("MAX_PAGE_WORKERS",    "2"))
MAX_ANIME_WORKERS   = int(os.environ.get("MAX_ANIME_WORKERS",   "4"))
MAX_SAISON_WORKERS  = int(os.environ.get("MAX_SAISON_WORKERS",  "3"))
MAX_EPISODE_WORKERS = int(os.environ.get("MAX_EPISODE_WORKERS", "5"))
JIKAN_DELAY         = 0.35
MAX_RETRIES         = 3

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

def build_saison_url(anime_lien, titre_vignette, langue):
    slug = slug_from_url(anime_lien)
    if not slug: return None
    t = titre_vignette.lower().strip()
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
    log_info(f"saved {path}")
    return path

def new_ctx(browser: Browser):
    return browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        locale="fr-FR",
    )

# ── Logs simples (pas de dashboard en CI) ────────────────────
_start_time = time.time()

def log_info(msg):
    elapsed = int(time.time() - _start_time)
    ts = f"{elapsed//60:02d}m{elapsed%60:02d}s"
    print(f"[{ts}] {msg}", flush=True)


# ══════════════════════════════════════════════════════════════
#  APIS — IDs uniquement
# ══════════════════════════════════════════════════════════════

async def get_jikan_id(session, title, is_film=False, jikan_sem=None):
    query = clean_title(title)
    if not query: return None
    sem = jikan_sem or asyncio.Semaphore(1)
    async with sem:
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
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
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
            headers=hdrs, timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            data = (await r.json()).get("data", []) if r.status == 200 else []
        if not data:
            async with session.get(f"{KITSU_BASE}/anime?filter[text]={query}&page[limit]=1",
                                   headers=hdrs, timeout=aiohttp.ClientTimeout(total=10)) as r2:
                data = (await r2.json()).get("data", []) if r2.status == 200 else []
        return data[0].get("id") if data else None
    except Exception: return None

async def fetch_all_ids(session, title, is_film=False, jikan_sem=None):
    j, t, k = await asyncio.gather(
        get_jikan_id(session, title, is_film, jikan_sem),
        get_tmdb_id(session, title, is_film),
        get_kitsu_id(session, title, is_film),
    )
    return {"jikan_id": j, "tmdb_id": t, "kitsu_id": k}

async def check_url_exists(url, session=None):
    try:
        if session:
            async with session.head(url, timeout=aiohttp.ClientTimeout(total=6), allow_redirects=True) as r:
                return r.status == 200
        async with aiohttp.ClientSession() as s:
            async with s.head(url, timeout=aiohttp.ClientTimeout(total=6), allow_redirects=True) as r:
                return r.status == 200
    except Exception:
        return False

# ══════════════════════════════════════════════════════════════
#  SCRAPING — Catalogue
# ══════════════════════════════════════════════════════════════

async def scrape_catalogue(browser, page_num):
    ctx  = await new_ctx(browser)
    page = await ctx.new_page()
    await page.route("**/*.{png,jpg,jpeg,gif,webp,woff,woff2,ttf}", lambda r: r.abort())
    try:
        await page.goto(CATALOGUE_URL.format(page=page_num), wait_until="domcontentloaded", timeout=30000)
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
                    if (p?.tagName === 'P') infoRows.push({ label: span.innerText.trim(), value: p.innerText.trim() });
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
            "nom": r["name"], "type": info["type"], "genres": info["genres"],
            "langues": info["langues"], "lien": build_url(r["href"]),
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
    await page.route("**/*.{png,jpg,jpeg,gif,webp,woff,woff2,ttf}", lambda r: r.abort())
    try:
        try:
            await page.goto(url, wait_until="networkidle", timeout=40000)
        except Exception:
            pass
        try:
            await page.wait_for_function(
                "() => !!document.querySelector('#coverOeuvre') || !!document.querySelector('h1')",
                timeout=12000
            )
        except Exception:
            pass
        await page.wait_for_timeout(600)
        return await page.evaluate("""
            () => {
                const imgEl = document.querySelector('#coverOeuvre');
                const image = imgEl?.getAttribute('src') || imgEl?.getAttribute('data-src') || null;
                const altEl = document.querySelector('#titreAlter');
                const nomsAlt = altEl ? altEl.innerText.trim().split(',').map(s=>s.trim()).filter(Boolean) : [];
                const synEl = document.querySelector('p.text-sm.text-gray-300.leading-relaxed');
                const synopsis = synEl?.innerText.trim() || null;
                const ifrEl = document.querySelector('#bandeannonce');
                const bandeAnnonce = ifrEl ? (ifrEl.getAttribute('src') || ifrEl.getAttribute('data-src')) : null;
                const cont = document.querySelector(
                    '.flex.flex-wrap.overflow-y-hidden.justify-start.bg-slate-900.bg-opacity-70.rounded.mt-2.h-auto'
                );
                const saisons = [];
                cont?.querySelectorAll('a').forEach(a => {
                    let lbl = a.querySelector('.text-white.font-bold.text-center.absolute.w-28')
                           || a.querySelector('[class*="font-bold"][class*="text-center"]');
                    const tv = lbl?.innerText.trim() || a.innerText.trim();
                    const tc = a.getAttribute('title') || a.getAttribute('aria-label') || tv;
                    if (tv) saisons.push({ titreVignette: tv, titreComplet: tc, isFilm: tv.toLowerCase().includes('film') });
                });
                return { image, nomsAlt, synopsis, bandeAnnonce, saisons };
            }
        """)
    finally:
        await ctx.close()

# ══════════════════════════════════════════════════════════════
#  SCRAPING — Épisodes (robuste + retry)
# ══════════════════════════════════════════════════════════════

async def _goto_robust(page, url):
    """Charge une URL avec 3 stratégies de fallback."""
    for strategy in ("networkidle", "domcontentloaded", "load"):
        try:
            await page.goto(url, wait_until=strategy, timeout=45000)
            return True
        except Exception:
            continue
    return False

async def _wait_for_select(page, selector, timeout=20000):
    """Attend qu'un select soit présent ET ait au moins 1 option."""
    for _ in range(MAX_RETRIES):
        try:
            await page.wait_for_selector(selector, timeout=timeout)
            count = await page.evaluate(
                f"() => document.querySelector('{selector}')?.options.length || 0"
            )
            if count > 0:
                return True
            await page.wait_for_timeout(800)
        except Exception:
            await page.wait_for_timeout(1000)
    return False

async def _read_player_src(page):
    """Lit le src de #playerDF depuis toutes les sources possibles."""
    return await page.evaluate(
        """() => {
            const f = document.querySelector('#playerDF');
            if (!f) return null;
            let src = f.getAttribute('src');
            if (src && src.length > 10 && !src.includes('about:blank')) return src;
            src = f.getAttribute('data-src');
            if (src && src.length > 10) return src;
            const all = f.querySelectorAll('iframe,[src],[data-src]');
            for (const el of all) {
                const s = el.getAttribute('src') || el.getAttribute('data-src') || '';
                if (s.length > 10 && !s.includes('about:blank')) return s;
            }
            return null;
        }"""
    )

async def _wait_for_player(page, old_src=None, timeout=6000):
    """
    Attend que #playerDF ait un src valide ET différent de old_src.
    Retente MAX_RETRIES fois avec pause croissante.
    """
    for attempt in range(MAX_RETRIES):
        try:
            await page.wait_for_function(
                """(old) => {
                    const f = document.querySelector('#playerDF');
                    if (!f) return false;
                    const candidates = [
                        f.getAttribute('src'),
                        f.getAttribute('data-src'),
                        ...[...f.querySelectorAll('iframe,[src],[data-src]')].map(
                            e => e.getAttribute('src') || e.getAttribute('data-src') || ''
                        )
                    ].filter(s => s && s.length > 10 && !s.includes('about:blank'));
                    return candidates.length > 0 && candidates[0] !== old;
                }""",
                arg=old_src or "",
                timeout=timeout
            )
        except Exception:
            pass
        src = await _read_player_src(page)
        if src and len(src) > 10 and src != (old_src or ""):
            return src
        await page.wait_for_timeout(700 * (attempt + 1))
    return await _read_player_src(page)

async def _get_options(page, selector):
    """Lit les options d'un select avec retry si vide."""
    for _ in range(MAX_RETRIES):
        opts = await page.evaluate(
            f"() => {{ const s=document.querySelector('{selector}'); "
            f"return s ? Array.from(s.options).map(o=>({{value:o.value,label:o.text.trim()}})) : []; }}"
        )
        if opts:
            return opts
        await page.wait_for_timeout(800)
    return []

async def _select_and_wait(page, selector, value, wait_ms=400):
    """Sélectionne une option et attend la réaction du DOM."""
    try:
        await page.select_option(selector, value=value)
        await page.wait_for_timeout(wait_ms)
        return True
    except Exception:
        return False

async def scrape_single_episode(browser, saison_url, ep_value, ep_label):
    """
    Scrape UN épisode : tous les lecteurs disponibles.
    Retente toute la séquence jusqu'à MAX_RETRIES si résultat vide.
    """
    for attempt in range(MAX_RETRIES):
        ctx  = await new_ctx(browser)
        page = await ctx.new_page()
        try:
            if not await _goto_robust(page, saison_url):
                continue
            if not await _wait_for_select(page, "#selectEpisodes"):
                continue
            await page.wait_for_timeout(300)
            if not await _select_and_wait(page, "#selectEpisodes", ep_value, wait_ms=500):
                continue

            has_lect = await _wait_for_select(page, "#selectLecteurs", timeout=12000)

            # Cas : pas de select lecteur → lire playerDF directement
            if not has_lect:
                await page.wait_for_timeout(1000)
                src = await _wait_for_player(page)
                if src:
                    return {"episode": ep_label, "lecteurs": [{"lecteur": "default", "url": src}]}
                continue

            lecteurs_opts = await _get_options(page, "#selectLecteurs")
            if not lecteurs_opts:
                continue

            lecteurs = []
            for lect in lecteurs_opts:
                old_src = await _read_player_src(page) or ""
                if not await _select_and_wait(page, "#selectLecteurs", lect["value"], wait_ms=300):
                    continue
                src = await _wait_for_player(page, old_src=old_src, timeout=6000)
                if src:
                    lecteurs.append({"lecteur": lect["label"], "url": src})
                else:
                    # Dernier recours : pause longue + lecture forcée
                    await _select_and_wait(page, "#selectLecteurs", lect["value"], wait_ms=1500)
                    src = await _read_player_src(page)
                    if src and src != old_src:
                        lecteurs.append({"lecteur": lect["label"], "url": src})

            if lecteurs:
                return {"episode": ep_label, "lecteurs": lecteurs}

            await page.wait_for_timeout(1200 * (attempt + 1))

        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                log_info(f"  ep {ep_label} failed: {e}")
        finally:
            await ctx.close()

    return {"episode": ep_label, "lecteurs": []}


async def scrape_episodes(browser, saison_url):
    """
    Scrape tous les épisodes d'une saison.
    1. Récupère la liste (retry si vide)
    2. Scrape en parallèle
    3. Double retry sur les épisodes vides
    """
    # Étape 1 : liste des épisodes
    eps_options = []
    for attempt in range(MAX_RETRIES):
        ctx  = await new_ctx(browser)
        page = await ctx.new_page()
        try:
            await _goto_robust(page, saison_url)
            if await _wait_for_select(page, "#selectEpisodes", timeout=25000):
                await page.wait_for_timeout(500)
                eps_options = await _get_options(page, "#selectEpisodes")
        except Exception:
            pass
        finally:
            await ctx.close()
        if eps_options:
            break
        log_info(f"  episode list empty (attempt {attempt+1}/{MAX_RETRIES})")
        await asyncio.sleep(3)

    if not eps_options:
        log_info(f"  SKIP no episodes: {saison_url}")
        return []

    slug = "/".join(saison_url.rstrip("/").split("/")[-2:])
    log_info(f"  {len(eps_options)} ep [{slug}]")

    sem = asyncio.Semaphore(MAX_EPISODE_WORKERS)

    async def safe_ep(ep):
        async with sem:
            return await scrape_single_episode(browser, saison_url, ep["value"], ep["label"])

    # Étape 2 : scraping parallèle
    results  = await asyncio.gather(*[safe_ep(ep) for ep in eps_options])
    episodes = list(results)

    # Étape 3 : retry pass 1
    vides = [i for i, r in enumerate(episodes) if r and not r["lecteurs"]]
    if vides:
        log_info(f"  retry pass 1: {len(vides)} empty")
        await asyncio.sleep(2)
        r1 = await asyncio.gather(*[safe_ep(eps_options[i]) for i in vides])
        for i, res in zip(vides, r1):
            if res and res["lecteurs"]:
                episodes[i] = res

    # Étape 4 : retry pass 2 (pause plus longue)
    vides2 = [i for i, r in enumerate(episodes) if r and not r["lecteurs"]]
    if vides2:
        log_info(f"  retry pass 2: {len(vides2)} still empty (waiting 5s)")
        await asyncio.sleep(5)
        r2 = await asyncio.gather(*[safe_ep(eps_options[i]) for i in vides2])
        for i, res in zip(vides2, r2):
            if res and res["lecteurs"]:
                episodes[i] = res

    nb_ok    = sum(1 for e in episodes if e and e["lecteurs"])
    nb_vides = len(episodes) - nb_ok
    status   = "OK" if nb_vides == 0 else f"WARN {nb_vides} empty"
    log_info(f"  [{status}] {nb_ok}/{len(episodes)} ep with lecteurs")
    return [e for e in episodes if e is not None]


async def scrape_catalogue(browser, page_num):
    ctx  = await new_ctx(browser)
    page = await ctx.new_page()
    await page.route("**/*.{png,jpg,jpeg,gif,webp,woff,woff2,ttf}", lambda r: r.abort())
    try:
        await page.goto(CATALOGUE_URL.format(page=page_num), wait_until="domcontentloaded", timeout=30000)
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
                    if (p?.tagName === 'P') infoRows.push({ label: span.innerText.trim(), value: p.innerText.trim() });
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
            "nom": r["name"], "type": info["type"], "genres": info["genres"],
            "langues": info["langues"], "lien": build_url(r["href"]),
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
    await page.route("**/*.{png,jpg,jpeg,gif,webp,woff,woff2,ttf}", lambda r: r.abort())
    try:
        try:
            await page.goto(url, wait_until="networkidle", timeout=40000)
        except Exception:
            pass
        try:
            await page.wait_for_function(
                "() => !!document.querySelector('#coverOeuvre') || !!document.querySelector('h1')",
                timeout=12000
            )
        except Exception:
            pass
        await page.wait_for_timeout(600)
        return await page.evaluate("""
            () => {
                const imgEl = document.querySelector('#coverOeuvre');
                const image = imgEl?.getAttribute('src') || imgEl?.getAttribute('data-src') || null;
                const altEl = document.querySelector('#titreAlter');
                const nomsAlt = altEl ? altEl.innerText.trim().split(',').map(s=>s.trim()).filter(Boolean) : [];
                const synEl = document.querySelector('p.text-sm.text-gray-300.leading-relaxed');
                const synopsis = synEl?.innerText.trim() || null;
                const ifrEl = document.querySelector('#bandeannonce');
                const bandeAnnonce = ifrEl ? (ifrEl.getAttribute('src') || ifrEl.getAttribute('data-src')) : null;
                const cont = document.querySelector(
                    '.flex.flex-wrap.overflow-y-hidden.justify-start.bg-slate-900.bg-opacity-70.rounded.mt-2.h-auto'
                );
                const saisons = [];
                cont?.querySelectorAll('a').forEach(a => {
                    let lbl = a.querySelector('.text-white.font-bold.text-center.absolute.w-28')
                           || a.querySelector('[class*="font-bold"][class*="text-center"]');
                    const tv = lbl?.innerText.trim() || a.innerText.trim();
                    const tc = a.getAttribute('title') || a.getAttribute('aria-label') || tv;
                    if (tv) saisons.push({ titreVignette: tv, titreComplet: tc, isFilm: tv.toLowerCase().includes('film') });
                });
                return { image, nomsAlt, synopsis, bandeAnnonce, saisons };
            }
        """)
    finally:
        await ctx.close()

# ══════════════════════════════════════════════════════════════
#  SCRAPING — Épisodes (robuste + retry)
# ══════════════════════════════════════════════════════════════

async def _goto_robust(page, url):
    for strategy in ("networkidle", "domcontentloaded", "load"):
        try:
            await page.goto(url, wait_until=strategy, timeout=45000)
            return True
        except Exception:
            continue
    return False

async def _wait_for_select(page, selector, timeout=20000):
    for _ in range(MAX_RETRIES):
        try:
            await page.wait_for_selector(selector, timeout=timeout)
            count = await page.evaluate(f"() => document.querySelector('{selector}')?.options.length || 0")
            if count > 0: return True
            await page.wait_for_timeout(800)
        except Exception:
            await page.wait_for_timeout(1000)
    return False

async def _wait_for_player(page, old_src=None, timeout=5000):
    for attempt in range(MAX_RETRIES):
        try:
            await page.wait_for_function(
                """(old) => {
                    const f = document.querySelector('#playerDF');
                    if (!f) return false;
                    const src = f.getAttribute('src') || f.getAttribute('data-src') || '';
                    return src.length > 10 && src !== old;
                }""",
                arg=old_src or "", timeout=timeout
            )
        except Exception:
            pass
        src = await page.evaluate(
            "() => { const f=document.querySelector('#playerDF'); "
            "return f?.getAttribute('src') || f?.getAttribute('data-src') || null; }"
        )
        if src and len(src) > 10: return src
        await page.wait_for_timeout(600 * (attempt + 1))
    return None

async def scrape_single_episode(browser, saison_url, ep_value, ep_label):
    for attempt in range(MAX_RETRIES):
        ctx  = await new_ctx(browser)
        page = await ctx.new_page()
        try:
            if not await _goto_robust(page, saison_url): continue
            if not await _wait_for_select(page, "#selectEpisodes"): continue
            await page.wait_for_timeout(300)
            await page.select_option("#selectEpisodes", value=ep_value)
            if not await _wait_for_select(page, "#selectLecteurs", timeout=10000):
                src = await _wait_for_player(page)
                if src: return {"episode": ep_label, "lecteurs": [{"lecteur": "default", "url": src}]}
                continue
            lecteurs_opts = await page.evaluate(
                "() => { const s=document.querySelector('#selectLecteurs'); "
                "return s ? Array.from(s.options).map(o=>({value:o.value,label:o.text.trim()})) : []; }"
            )
            lecteurs = []
            for lect in lecteurs_opts:
                old_src = await page.evaluate(
                    "() => { const f=document.querySelector('#playerDF'); "
                    "return f?.getAttribute('src') || f?.getAttribute('data-src') || ''; }"
                )
                await page.select_option("#selectLecteurs", value=lect["value"])
                src = await _wait_for_player(page, old_src=old_src)
                if src: lecteurs.append({"lecteur": lect["label"], "url": src})
            if lecteurs: return {"episode": ep_label, "lecteurs": lecteurs}
            await page.wait_for_timeout(1000 * (attempt + 1))
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                log_info(f"  episode {ep_label} failed: {e}")
        finally:
            await ctx.close()
    return {"episode": ep_label, "lecteurs": []}

async def scrape_episodes(browser, saison_url):
    eps_options = []
    for attempt in range(MAX_RETRIES):
        ctx  = await new_ctx(browser)
        page = await ctx.new_page()
        try:
            await _goto_robust(page, saison_url)
            if await _wait_for_select(page, "#selectEpisodes", timeout=20000):
                await page.wait_for_timeout(400)
                eps_options = await page.evaluate(
                    "() => { const s=document.querySelector('#selectEpisodes'); "
                    "return s ? Array.from(s.options).map(o=>({value:o.value,label:o.text.trim()})) : []; }"
                )
        except Exception:
            pass
        finally:
            await ctx.close()
        if eps_options: break
        await asyncio.sleep(2)

    if not eps_options:
        log_info(f"  no episodes found: {saison_url}")
        return []

    slug = "/".join(saison_url.rstrip("/").split("/")[-2:])
    log_info(f"  {len(eps_options)} ep [{slug}]")

    sem = asyncio.Semaphore(MAX_EPISODE_WORKERS)
    async def safe_ep(ep):
        async with sem:
            return await scrape_single_episode(browser, saison_url, ep["value"], ep["label"])

    results  = await asyncio.gather(*[safe_ep(ep) for ep in eps_options])
    episodes = list(results)

    # Retry épisodes vides
    vides = [i for i, r in enumerate(episodes) if r and not r["lecteurs"]]
    if vides:
        log_info(f"  retry {len(vides)} empty episodes")
        retry = await asyncio.gather(*[safe_ep(eps_options[i]) for i in vides])
        for i, res in zip(vides, retry):
            if res and res["lecteurs"]: episodes[i] = res

    nb_ok = sum(1 for e in episodes if e and e["lecteurs"])
    log_info(f"  {nb_ok}/{len(episodes)} episodes OK")
    return [e for e in episodes if e is not None]

# ══════════════════════════════════════════════════════════════
#  WORKER — Anime
# ══════════════════════════════════════════════════════════════

async def process_anime(browser, session, anime, idx, total, page_num, jikan_sem):
    nom = anime["nom"]
    log_info(f"[p{page_num}] [{idx}/{total}] {nom}")

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
                s["lien_vf"]     = build_saison_url(anime["lien"], s["titreVignette"], "vf")
                s["lien_vostfr"] = build_saison_url(anime["lien"], s["titreVignette"], "vostfr")
                s["episodes"]    = []
            anime["saisons"] = saisons
        except Exception as e:
            log_info(f"  detail error {nom}: {e}")

    anime["ids"] = await fetch_all_ids(session, nom, jikan_sem=jikan_sem)

    langues_decl = [l.upper() for l in anime.get("langues", [])]
    prefer_vf    = "VF" in langues_decl

    s_sem = asyncio.Semaphore(MAX_SAISON_WORKERS)

    async def process_saison(s):
        async with s_sem:
            titre   = s["titreVignette"]
            is_film = s.get("isFilm", False)
            s["ids"] = await fetch_all_ids(session, f"{nom} {titre}", is_film=is_film, jikan_sem=jikan_sem)

            url_vf     = build_saison_url(anime["lien"], titre, "vf")
            url_vostfr = build_saison_url(anime["lien"], titre, "vostfr")
            s["lien_vf"]     = url_vf
            s["lien_vostfr"] = url_vostfr

            url_cible  = None
            langue_eff = None

            if prefer_vf and url_vf:
                if await check_url_exists(url_vf, session):
                    url_cible, langue_eff = url_vf, "vf"
            if url_cible is None and url_vostfr:
                if await check_url_exists(url_vostfr, session):
                    url_cible, langue_eff = url_vostfr, "vostfr"
            if url_cible is None:
                if prefer_vf and url_vf: url_cible, langue_eff = url_vf, "vf"
                elif url_vostfr:         url_cible, langue_eff = url_vostfr, "vostfr"

            s["langue"] = langue_eff
            if url_cible:
                try:    s["episodes"] = await scrape_episodes(browser, url_cible)
                except: s["episodes"] = []
            else:
                s["episodes"] = []
            return s

    if anime["saisons"]:
        anime["saisons"] = list(await asyncio.gather(*[process_saison(s) for s in anime["saisons"]]))

    nb_eps = sum(len(s.get("episodes", [])) for s in anime["saisons"])
    log_info(f"[p{page_num}] done {nom} — {len(anime['saisons'])}s {nb_eps}ep")
    return anime

# ══════════════════════════════════════════════════════════════
#  WORKER — Page
# ══════════════════════════════════════════════════════════════

async def process_page(browser, session, page_num, jikan_sem):
    t0 = time.time()
    log_info(f"=== page {page_num} start ===")

    animes = await scrape_catalogue(browser, page_num)
    log_info(f"[p{page_num}] {len(animes)} animes")

    a_sem = asyncio.Semaphore(MAX_ANIME_WORKERS)
    total = len(animes)

    async def safe_anime(anime, idx):
        async with a_sem:
            return await process_anime(browser, session, anime, idx, total, page_num, jikan_sem)

    results = await asyncio.gather(
        *[safe_anime(a, i+1) for i, a in enumerate(animes)],
        return_exceptions=True
    )
    animes = [r for r in results if isinstance(r, dict)]

    elapsed = int(time.time() - t0)
    log_info(f"=== page {page_num} done in {elapsed//60}m{elapsed%60:02d}s ===")

    data = {
        "page":       page_num,
        "scraped_at": datetime.now().strftime("%Y%m%d_%H%M%S"),
        "duration_s": elapsed,
        "total":      len(animes),
        "animes":     animes,
    }
    save_json(data, page_num)
    return data

# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════

async def main():
    pages = list(range(PAGE_BEGIN, PAGE_END + 1))
    start = time.time()

    log_info(f"START pages {PAGE_BEGIN}->{PAGE_END} ({len(pages)} pages)")
    log_info(f"workers: pages={MAX_PAGE_WORKERS} animes={MAX_ANIME_WORKERS} saisons={MAX_SAISON_WORKERS} eps={MAX_EPISODE_WORKERS}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox",
                  "--disable-dev-shm-usage", "--disable-gpu"]
        )
        connector = aiohttp.TCPConnector(limit=30)
        async with aiohttp.ClientSession(connector=connector) as session:
            jikan_sem = asyncio.Semaphore(3)
            page_sem  = asyncio.Semaphore(MAX_PAGE_WORKERS)

            async def safe_page(pn):
                async with page_sem:
                    return await process_page(browser, session, pn, jikan_sem)

            results = await asyncio.gather(*[safe_page(pn) for pn in pages])

        await browser.close()

    elapsed     = int(time.time() - start)
    total_anime = sum(r["total"] for r in results if isinstance(r, dict))
    total_eps   = sum(
        sum(len(s.get("episodes", [])) for s in a["saisons"])
        for r in results if isinstance(r, dict)
        for a in r["animes"]
    )
    log_info(f"DONE {len(pages)} pages | {total_anime} animes | {total_eps} episodes | {elapsed//60}m{elapsed%60:02d}s")

if __name__ == "__main__":
    asyncio.run(main())
