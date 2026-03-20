# ============================================================
#  ANIME-SAMA SCRAPER — GitHub Actions
#  Séquentiel + sauvegarde progressive (1 JSON par page)
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

PAGE_BEGIN          = int(os.environ.get("PAGE_BEGIN", "1"))
PAGE_END            = int(os.environ.get("PAGE_END",   "43"))
MAX_EPISODE_WORKERS = int(os.environ.get("MAX_EPISODE_WORKERS", "3"))
JIKAN_DELAY         = 0.4
MAX_RETRIES         = 3

_start_time = time.time()

def log(msg):
    e = int(time.time() - _start_time)
    print(f"[{e//60:02d}m{e%60:02d}s] {msg}", flush=True)

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

def new_browser_context(browser):
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
        async with session.get(
            f"{JIKAN_BASE}/anime?q={query}&type={media}&limit=1",
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            data = (await r.json()).get("data", []) if r.status == 200 else []
        if not data:
            async with session.get(
                f"{JIKAN_BASE}/anime?q={query}&limit=1",
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r2:
                data = (await r2.json()).get("data", []) if r2.status == 200 else []
        return data[0].get("mal_id") if data else None
    except Exception:
        return None

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
    except Exception:
        return None

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
            async with session.get(
                f"{KITSU_BASE}/anime?filter[text]={query}&page[limit]=1",
                headers=hdrs, timeout=aiohttp.ClientTimeout(total=10)
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
        async with session.head(url, timeout=aiohttp.ClientTimeout(total=6), allow_redirects=True) as r:
            return r.status == 200
    except Exception:
        return False

# ══════════════════════════════════════════════════════════════
#  SCRAPING — helpers page Playwright
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
                s = el.getAttribute('src') || el.getAttribute('data-src') || '';
                if (s.length > 10 && !s.includes('about:blank')) return s;
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
                arg=old_src,
                timeout=timeout
            )
        except Exception:
            pass
        src = await read_player(page)
        if src and src != old_src: return src
        await page.wait_for_timeout(700 * (attempt + 1))
    return await read_player(page)

# ══════════════════════════════════════════════════════════════
#  SCRAPING — Catalogue
# ══════════════════════════════════════════════════════════════

async def scrape_catalogue(browser, page_num):
    ctx  = await new_browser_context(browser)
    page = await ctx.new_page()
    await page.route("**/*.{png,jpg,jpeg,gif,webp,woff,woff2,ttf,mp4,mp3}", lambda r: r.abort())
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
            "nom": r["name"], "type": info["type"],
            "genres": info["genres"], "langues": info["langues"],
            "lien": build_url(r["href"]),
            "image": None, "noms_alt": [], "synopsis": None, "bande_annonce": None,
            "ids": {"jikan_id": None, "tmdb_id": None, "kitsu_id": None},
            "saisons": [],
        })
    return animes

# ══════════════════════════════════════════════════════════════
#  SCRAPING — Détail anime
# ══════════════════════════════════════════════════════════════

async def scrape_detail(browser, url):
    ctx  = await new_browser_context(browser)
    page = await ctx.new_page()
    await page.route("**/*.{png,jpg,jpeg,gif,webp,woff,woff2,ttf,mp4,mp3}", lambda r: r.abort())
    try:
        await goto_page(page, url)
        try:
            await page.wait_for_function(
                "() => !!document.querySelector('#coverOeuvre') || !!document.querySelector('h1')",
                timeout=12000
            )
        except Exception:
            pass
        await page.wait_for_timeout(800)
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
                    '.flex.flex-wrap.overflow-y-hidden.justify-start.bg-slate-900.bg-opacity-70.rounded.mt-2.h-auto'
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
#  SCRAPING — Un épisode (1 page dédiée)
# ══════════════════════════════════════════════════════════════

async def scrape_episode(browser, saison_url, ep_value, ep_label):
    for attempt in range(MAX_RETRIES):
        ctx  = await new_browser_context(browser)
        page = await ctx.new_page()
        try:
            if not await goto_page(page, saison_url): continue
            if not await wait_select(page, "#selectEpisodes"): continue
            await page.wait_for_timeout(300)

            try:
                await page.select_option("#selectEpisodes", value=ep_value)
                await page.wait_for_timeout(500)
            except Exception:
                continue

            has_lect = await wait_select(page, "#selectLecteurs", timeout=12000)
            if not has_lect:
                await page.wait_for_timeout(1000)
                src = await wait_player(page)
                if src:
                    return {"episode": ep_label, "lecteurs": [{"lecteur": "default", "url": src}]}
                continue

            lecteurs_opts = await get_options(page, "#selectLecteurs")
            if not lecteurs_opts: continue

            lecteurs = []
            for lect in lecteurs_opts:
                old = await read_player(page) or ""
                try:
                    await page.select_option("#selectLecteurs", value=lect["value"])
                    await page.wait_for_timeout(300)
                except Exception:
                    continue
                src = await wait_player(page, old_src=old, timeout=6000)
                if not src:
                    await page.wait_for_timeout(1500)
                    src = await read_player(page)
                if src and src != old:
                    lecteurs.append({"lecteur": lect["label"], "url": src})

            if lecteurs:
                return {"episode": ep_label, "lecteurs": lecteurs}
            await page.wait_for_timeout(1500 * (attempt + 1))

        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                log(f"    ep {ep_label} failed: {e}")
        finally:
            await ctx.close()

    return {"episode": ep_label, "lecteurs": []}

# ══════════════════════════════════════════════════════════════
#  SCRAPING — Tous les épisodes d'une saison
# ══════════════════════════════════════════════════════════════

async def scrape_saison_episodes(browser, saison_url):
    # Récupérer la liste des épisodes
    eps_options = []
    for attempt in range(MAX_RETRIES):
        ctx  = await new_browser_context(browser)
        page = await ctx.new_page()
        try:
            await goto_page(page, saison_url)
            if await wait_select(page, "#selectEpisodes", timeout=25000):
                await page.wait_for_timeout(500)
                eps_options = await get_options(page, "#selectEpisodes")
        except Exception:
            pass
        finally:
            await ctx.close()
        if eps_options: break
        log(f"    episode list empty (attempt {attempt+1}/{MAX_RETRIES})")
        await asyncio.sleep(3)

    if not eps_options:
        log(f"    SKIP: no episodes at {saison_url}")
        return []

    slug = "/".join(saison_url.rstrip("/").split("/")[-2:])
    log(f"    {len(eps_options)} episodes [{slug}]")

    # Scraper les épisodes — limité à MAX_EPISODE_WORKERS en parallèle
    sem = asyncio.Semaphore(MAX_EPISODE_WORKERS)

    async def safe(ep):
        async with sem:
            return await scrape_episode(browser, saison_url, ep["value"], ep["label"])

    episodes = list(await asyncio.gather(*[safe(ep) for ep in eps_options]))

    # Retry pass 1
    vides = [i for i, e in enumerate(episodes) if not e["lecteurs"]]
    if vides:
        log(f"    retry pass 1: {len(vides)} empty episodes")
        await asyncio.sleep(2)
        r1 = await asyncio.gather(*[safe(eps_options[i]) for i in vides])
        for i, res in zip(vides, r1):
            if res["lecteurs"]: episodes[i] = res

    # Retry pass 2
    vides2 = [i for i, e in enumerate(episodes) if not e["lecteurs"]]
    if vides2:
        log(f"    retry pass 2: {len(vides2)} still empty (pause 5s)")
        await asyncio.sleep(5)
        r2 = await asyncio.gather(*[safe(eps_options[i]) for i in vides2])
        for i, res in zip(vides2, r2):
            if res["lecteurs"]: episodes[i] = res

    ok = sum(1 for e in episodes if e["lecteurs"])
    log(f"    {ok}/{len(episodes)} episodes with lecteurs")
    return episodes

# ══════════════════════════════════════════════════════════════
#  SCRAPING — Une saison complète (IDs + épisodes)
# ══════════════════════════════════════════════════════════════

async def process_saison(browser, session, anime_nom, anime_lien, saison, langues_anime):
    titre   = saison["titreVignette"]
    is_film = saison.get("isFilm", False)
    log(f"  saison: {titre}")

    # IDs
    saison["ids"] = await fetch_ids(session, f"{anime_nom} {titre}", is_film=is_film)

    # URLs VF / VOSTFR
    url_vf     = build_saison_url(anime_lien, titre, "vf")
    url_vostfr = build_saison_url(anime_lien, titre, "vostfr")
    saison["lien_vf"]     = url_vf
    saison["lien_vostfr"] = url_vostfr

    # Choisir la langue : VF prioritaire, VOSTFR fallback
    prefer_vf  = "VF" in [l.upper() for l in langues_anime]
    url_cible  = None
    langue_eff = None

    if prefer_vf and url_vf and await check_url(session, url_vf):
        url_cible, langue_eff = url_vf, "vf"
    if url_cible is None and url_vostfr and await check_url(session, url_vostfr):
        url_cible, langue_eff = url_vostfr, "vostfr"
    if url_cible is None:
        url_cible  = url_vf if prefer_vf else url_vostfr
        langue_eff = "vf"  if prefer_vf else "vostfr"

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
#  SCRAPING — Un anime complet (séquentiel)
# ══════════════════════════════════════════════════════════════

async def process_anime(browser, session, anime, idx, total):
    nom = anime["nom"]
    log(f"[{idx}/{total}] {nom}")

    # Détail
    if anime["lien"]:
        try:
            detail = await scrape_detail(browser, anime["lien"])
            anime["image"]         = detail.get("image")
            anime["noms_alt"]      = detail.get("nomsAlt", [])
            anime["synopsis"]      = detail.get("synopsis")
            anime["bande_annonce"] = detail.get("bandeAnnonce")
            saisons = detail.get("saisons", [])
            for s in saisons:
                s["ids"]      = {"jikan_id": None, "tmdb_id": None, "kitsu_id": None}
                s["langue"]   = None
                s["lien_vf"]     = None
                s["lien_vostfr"] = None
                s["episodes"] = []
            anime["saisons"] = saisons
            log(f"  detail OK — {len(saisons)} saison(s)")
        except Exception as e:
            log(f"  detail error: {e}")

    # IDs anime principal
    anime["ids"] = await fetch_ids(session, nom)
    log(f"  ids: jikan={anime['ids']['jikan_id']} tmdb={anime['ids']['tmdb_id']} kitsu={anime['ids']['kitsu_id']}")

    # Saisons — séquentielles pour stabilité
    for s in anime["saisons"]:
        try:
            await process_saison(
                browser, session,
                nom, anime["lien"],
                s, anime.get("langues", [])
            )
        except Exception as e:
            log(f"  saison error ({s.get('titreVignette','')}): {e}")
            s["episodes"] = []

    nb_eps = sum(len(s.get("episodes", [])) for s in anime["saisons"])
    log(f"  DONE {nom} — {len(anime['saisons'])}s {nb_eps}ep")
    return anime

# ══════════════════════════════════════════════════════════════
#  SCRAPING — Une page complète
# ══════════════════════════════════════════════════════════════

async def process_page(browser, session, page_num):
    t0 = time.time()
    log(f"=== PAGE {page_num} START ===")

    animes = await scrape_catalogue(browser, page_num)
    log(f"  {len(animes)} animes detected")

    # Séquentiel : un animé après l'autre — fiable sur GitHub Actions
    for idx, anime in enumerate(animes, 1):
        try:
            await process_anime(browser, session, anime, idx, len(animes))
        except Exception as e:
            log(f"  ANIME ERROR [{idx}] {anime.get('nom','?')}: {e}")
            # On continue quand même avec l'anime suivant

    elapsed = int(time.time() - t0)
    log(f"=== PAGE {page_num} DONE in {elapsed//60}m{elapsed%60:02d}s ===")

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
    log(f"START pages {PAGE_BEGIN}->{PAGE_END} ({len(pages)} pages)")
    log(f"episode workers: {MAX_EPISODE_WORKERS}")

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
                    # On continue avec la page suivante

        await browser.close()

    elapsed = int(time.time() - _start_time)
    log(f"ALL DONE in {elapsed//60}m{elapsed%60:02d}s")

if __name__ == "__main__":
    asyncio.run(main())
