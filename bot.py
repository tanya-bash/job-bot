import os
import json
import hashlib
import asyncio
import logging
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup
from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, ContextTypes
from apscheduler.schedulers.asyncio import AsyncIOScheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
SCRAPER_API_KEY = os.getenv("SCRAPER_API_KEY", "")  # scraperapi.com — free tier, 1000 req/mo
SEEN_FILE = Path("seen_jobs.json")
ET = ZoneInfo("America/New_York")

SEND_HOUR_ET = int(os.getenv("SEND_HOUR_ET", "9"))
SEND_MINUTE_ET = int(os.getenv("SEND_MINUTE_ET", "30"))  # 9:30 AM ET

# ── Search queries ─────────────────────────────────────────────────────────────
SPECIFIC_QUERIES = [
    "Creative Project Manager",
    "Senior Creative Project Manager",
    "Creative Operations Manager",
    "Creative Operations Lead",
    "Creative Production Manager",
    "Creative Production Lead",
    "Senior Creative Producer",
    "Creative Producer",
    "Campaign Operations Manager",
    "Campaign Program Manager",
    "Marketing Program Manager",
    "Account Supervisor",
]

BROAD_QUERIES = [
    "Project Manager",
    "Senior Project Manager",
    "Client Manager",
    "Account Manager",
    "Senior Account Manager",
    "Program Manager",
]

ALL_QUERIES = SPECIFIC_QUERIES + BROAD_QUERIES
SPECIFIC_QUERY_TITLES = {q.lower() for q in SPECIFIC_QUERIES}
LOCATIONS_SEARCH = ["Atlanta, GA", "United States"]  # Used by Indeed/Glassdoor; LinkedIn handles locations internally

# ── Location filtering ────────────────────────────────────────────────────────
# After fetching, we keep only jobs that are remote/US or near Atlanta.
# Any job with a non-US city in its location text gets dropped.

# Words that confirm a job is OK
LOCATION_ALLOW = [
    "remote", "united states", "us", "u.s.", "atlanta", "georgia", "ga",
    # nearby metros also fine for hybrid
    "birmingham", "charlotte", "nashville", "tampa", "orlando",
]

# Non-US location signals → always drop regardless of other text
LOCATION_BLOCK = [
    "london", "uk", "united kingdom", "england", "manchester", "berlin",
    "germany", "madrid", "spain", "singapore", "canada", "toronto",
    "vancouver", "montreal", "australia", "sydney", "melbourne",
    "amsterdam", "netherlands", "paris", "france", "dubai", "uae",
    "india", "bangalore", "mumbai", "new zealand", "ireland", "dublin",
    "poland", "warsaw", "czech", "prague", "sweden", "norway", "denmark",
    # US cities that are on-site and too far from Atlanta
    "new york", " nyc", "san francisco", "los angeles", "seattle",
    "chicago", "boston", "denver", "austin", "portland", "minneapolis",
    "philadelphia", "new jersey", "washington dc", "dc,",
    # NJ / NY specific
    "hoboken", "jersey city", "brooklyn", "manhattan",
]

def location_is_ok(location_text: str) -> bool:
    """Return True if location is remote/US-wide or near Atlanta."""
    lt = location_text.lower()
    # Hard block: non-US or far US on-site
    if any(b in lt for b in LOCATION_BLOCK):
        return False
    # If empty or contains an allow keyword, pass
    if not lt or any(a in lt for a in LOCATION_ALLOW):
        return True
    # Unknown location — let it through (better too many than too few)
    return True

# ── Title / relevance filters ─────────────────────────────────────────────────
EXCLUDE_TITLE_KEYWORDS = [
    "junior", "intern", "entry level", "entry-level",
    "software engineer", "software developer", "data engineer",
    "data scientist", "data analyst", "devops", "finance manager",
    "hr manager", "human resources", "recruiter", "talent acquisition",
    "real estate", "construction", "healthcare", "nurse", "physician",
    "director", "analyst", "performance",
]

# Must appear somewhere in title OR description for broad-query jobs
RELEVANCE_KEYWORDS = [
    "creative", "marketing", "campaign", "production", "content",
    "brand", "advertising", "agency", "marcom", "integrated",
    "digital", "video", "design", "copy", "media", "launch",
    "studio", "communications", "art director", "copywriter",
]

# ── Helpers ───────────────────────────────────────────────────────────────────
def load_seen() -> set:
    if SEEN_FILE.exists():
        return set(json.loads(SEEN_FILE.read_text()))
    return set()

def save_seen(seen: set):
    SEEN_FILE.write_text(json.dumps(list(seen)))

def job_id(title: str, company: str, url: str) -> str:
    return hashlib.md5(f"{title.lower()}|{company.lower()}|{url}".encode()).hexdigest()

def is_excluded(title: str) -> bool:
    t = title.lower()
    return any(kw in t for kw in EXCLUDE_TITLE_KEYWORDS)

def is_relevant(title: str, description: str = "") -> bool:
    t = title.lower()
    # Specific creative titles always pass
    if any(kw in t for kw in RELEVANCE_KEYWORDS):
        return True
    if any(specific in t for specific in SPECIFIC_QUERY_TITLES):
        return True
    # Broad titles need relevance keyword in description
    combined = (t + " " + description.lower())
    return any(kw in combined for kw in RELEVANCE_KEYWORDS)

def make_job(title, company, date, url, source, location="", snippet=""):
    if is_excluded(title):
        log.info(f"[{source}] Excluded (title): {title}")
        return None
    if not location_is_ok(location):
        log.info(f"[{source}] Excluded (location '{location}'): {title}")
        return None
    if not is_relevant(title, snippet):
        log.info(f"[{source}] Excluded (not relevant): {title}")
        return None
    return {"title": title, "company": company, "date": date,
            "url": url, "source": source, "location": location}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

def scraper_url(target_url: str) -> str:
    """Wrap URL through ScraperAPI if key is set, otherwise use directly."""
    if SCRAPER_API_KEY:
        from urllib.parse import quote
        return (f"https://api.scraperapi.com/?api_key={SCRAPER_API_KEY}"
                f"&url={quote(target_url)}&country_code=us")
    return target_url

# ── LinkedIn ──────────────────────────────────────────────────────────────────
# Atlanta area location strings that are OK for hybrid roles
ATLANTA_LOCATION_SIGNALS = [
    "atlanta", "georgia", ", ga", "alpharetta", "marietta", "smyrna",
    "decatur", "sandy springs", "roswell", "duluth", "norcross",
    "peachtree", "athens", "birmingham",  # Birmingham AL ~2.5h, some consider it
]

async def _linkedin_search(client, query, params, mode_label) -> list[dict]:
    """Internal helper: run one LinkedIn search and parse results."""
    jobs = []
    try:
        r = await client.get("https://www.linkedin.com/jobs/search/",
                             params=params, timeout=20, headers=HEADERS)
        soup = BeautifulSoup(r.text, "html.parser")
        for card in soup.select("div.base-card")[:15]:
            title_el = card.select_one("h3.base-search-card__title")
            company_el = card.select_one("h4.base-search-card__subtitle")
            date_el = card.select_one("time")
            link_el = card.select_one("a.base-card__full-link")
            loc_el = card.select_one("span.job-search-card__location")
            snippet_el = card.select_one("p.base-search-card__metadata")
            if not (title_el and link_el):
                continue
            title = title_el.get_text(strip=True)
            company = company_el.get_text(strip=True) if company_el else "Unknown"
            date_str = date_el.get("datetime", "Today") if date_el else "Today"
            loc_text = loc_el.get_text(strip=True) if loc_el else ""
            snippet = snippet_el.get_text(strip=True) if snippet_el else ""
            url = link_el.get("href", "").split("?")[0]
            j = make_job(title=title, company=company, date=date_str,
                         url=url, source="LinkedIn",
                         location=loc_text, snippet=snippet)
            if j:
                jobs.append(j)
    except Exception as e:
        log.warning(f"LinkedIn error [{mode_label}] ({query}): {e}")
    return jobs

async def fetch_linkedin(client, query, _location_unused=None):
    """
    Two parallel searches per query:
    1. Remote US-wide  (f_WT=2, no location pin)
    2. Hybrid near Atlanta  (f_WT=3, location=Atlanta GA, radius ~50 mi)
    Post-filter ensures hybrid results are actually in the Atlanta area.
    """
    base_params = {"keywords": query, "f_TPR": "r86400", "sortBy": "DD"}

    # Search 1: Remote only, US-wide
    remote_params = {**base_params, "f_WT": "2", "location": "United States"}

    # Search 2: Hybrid/on-site within ~50 miles of Atlanta
    # distance=50 is supported by LinkedIn (miles)
    hybrid_params = {**base_params, "f_WT": "3", "location": "Atlanta, Georgia, United States",
                     "distance": "50"}

    remote_jobs, hybrid_jobs_raw = await asyncio.gather(
        _linkedin_search(client, query, remote_params, "remote"),
        _linkedin_search(client, query, hybrid_params, "hybrid-atlanta"),
    )

    # Extra guard for hybrid: keep only cards whose location text
    # contains an Atlanta-area signal (LinkedIn sometimes leaks other cities)
    hybrid_jobs = [
        j for j in hybrid_jobs_raw
        if any(sig in j.get("location", "").lower() for sig in ATLANTA_LOCATION_SIGNALS)
        or j.get("location", "") == ""  # empty location = probably remote, keep it
    ]

    return remote_jobs + hybrid_jobs

# ── Indeed ────────────────────────────────────────────────────────────────────
async def fetch_indeed(client, query, location):
    """Uses ScraperAPI to bypass Indeed's server-IP block."""
    jobs = []
    from urllib.parse import urlencode
    base = "https://www.indeed.com/jobs?" + urlencode(
        {"q": query, "l": location, "fromage": "1", "sort": "date",
         "remotejob": "032b3046-06a3-4876-8dfd-474eb5e7ed11"})  # remote filter
    url = scraper_url(base)
    try:
        r = await client.get(url, timeout=30)
        soup = BeautifulSoup(r.text, "html.parser")
        for card in soup.select("div.job_seen_beacon")[:12]:
            title_el = card.select_one("h2.jobTitle span[title]")
            company_el = card.select_one("[data-testid='company-name']")
            date_el = card.select_one("[data-testid='myJobsStateDate']")
            link_el = card.select_one("a[id^='job_']")
            loc_el = card.select_one("[data-testid='text-location']")
            snippet_el = card.select_one("div.job-snippet")
            if not (title_el and company_el and link_el):
                continue
            href = link_el.get("href", "")
            loc_text = loc_el.get_text(strip=True) if loc_el else location
            j = make_job(
                title=title_el.get_text(strip=True),
                company=company_el.get_text(strip=True),
                date=date_el.get_text(strip=True) if date_el else "Today",
                url=f"https://www.indeed.com{href}" if href.startswith("/") else href,
                source="Indeed",
                location=loc_text,
                snippet=snippet_el.get_text(strip=True) if snippet_el else "",
            )
            if j:
                jobs.append(j)
    except Exception as e:
        log.warning(f"Indeed error ({query}/{location}): {e}")
    return jobs

# ── Glassdoor ─────────────────────────────────────────────────────────────────
async def fetch_glassdoor(client, query):
    """Uses ScraperAPI to bypass Glassdoor's server-IP block."""
    jobs = []
    from urllib.parse import urlencode
    base = "https://www.glassdoor.com/Job/jobs.htm?" + urlencode(
        {"sc.keyword": query, "remoteWorkType": "1", "fromAge": "1"})
    url = scraper_url(base)
    try:
        r = await client.get(url, timeout=30,
                             headers={**HEADERS, "Referer": "https://www.glassdoor.com/"})
        soup = BeautifulSoup(r.text, "html.parser")
        for card in soup.select("li.react-job-listing")[:10]:
            title_el = card.select_one("a.jobLink span")
            company_el = card.select_one("div.job-search-key-yzn1t")
            link_el = card.select_one("a.jobLink")
            loc_el = card.select_one("div.job-search-key-zy1gg")
            snippet_el = card.select_one("div.job-search-key-l2wjgv")
            if not (title_el and link_el):
                continue
            href = link_el.get("href", "")
            loc_text = loc_el.get_text(strip=True) if loc_el else ""
            j = make_job(
                title=title_el.get_text(strip=True),
                company=company_el.get_text(strip=True) if company_el else "Unknown",
                date="Today",
                url=f"https://www.glassdoor.com{href}" if href.startswith("/") else href,
                source="Glassdoor",
                location=loc_text,
                snippet=snippet_el.get_text(strip=True) if snippet_el else "",
            )
            if j:
                jobs.append(j)
    except Exception as e:
        log.warning(f"Glassdoor error ({query}): {e}")
    return jobs

# ── Virtual Vocations ─────────────────────────────────────────────────────────
async def fetch_virtualvocations(client, query):
    jobs = []
    from urllib.parse import urlencode
    base = "https://www.virtualvocations.com/jobs?" + urlencode({"search": query})
    url = scraper_url(base)
    try:
        r = await client.get(url, timeout=30)
        soup = BeautifulSoup(r.text, "html.parser")
        for card in soup.select("div.job-result-item, article.job-card")[:10]:
            title_el = card.select_one("h2 a, h3 a, .job-title a")
            company_el = card.select_one(".company-name, .job-company")
            date_el = card.select_one(".job-date, time")
            if not title_el:
                continue
            href = title_el.get("href", "")
            snippet = card.get_text(strip=True)[:400]
            j = make_job(
                title=title_el.get_text(strip=True),
                company=company_el.get_text(strip=True) if company_el else "Unknown",
                date=date_el.get_text(strip=True) if date_el else "Today",
                url=f"https://www.virtualvocations.com{href}" if href.startswith("/") else href,
                source="VirtualVocations",
                location="Remote",  # VirtualVocations is remote-only
                snippet=snippet,
            )
            if j:
                jobs.append(j)
    except Exception as e:
        log.warning(f"VirtualVocations error ({query}): {e}")
    return jobs

# ── Synergis ──────────────────────────────────────────────────────────────────
async def fetch_synergis(client, query):
    jobs = []
    from urllib.parse import urlencode
    base = "https://www.synergishr.com/search-jobs/?" + urlencode({"s": query})
    url = scraper_url(base)
    try:
        r = await client.get(url, timeout=30)
        soup = BeautifulSoup(r.text, "html.parser")
        for card in soup.select("div.job-listing, article.job, .careers-job-item")[:10]:
            title_el = card.select_one("h2 a, h3 a, .job-title a, a.job-link")
            company_el = card.select_one(".company, .job-company")
            date_el = card.select_one(".date, time, .job-date")
            loc_el = card.select_one(".location, .job-location")
            if not title_el:
                continue
            href = title_el.get("href", "")
            loc_text = loc_el.get_text(strip=True) if loc_el else ""
            snippet = card.get_text(strip=True)[:400]
            j = make_job(
                title=title_el.get_text(strip=True),
                company=company_el.get_text(strip=True) if company_el else "Synergis Client",
                date=date_el.get_text(strip=True) if date_el else "Today",
                url=f"https://www.synergishr.com{href}" if href.startswith("/") else href,
                source="Synergis",
                location=loc_text,
                snippet=snippet,
            )
            if j:
                jobs.append(j)
    except Exception as e:
        log.warning(f"Synergis error ({query}): {e}")
    return jobs

# ── Apple Contingent Workforce ────────────────────────────────────────────────
async def fetch_apple_cw(client, query):
    jobs = []
    from urllib.parse import urlencode
    base = ("https://directsource.magnitglobal.com/us/applecontingentworkforce/jobs?"
            + urlencode({"keywords": query, "businessGroups": "Marcom"}))
    url = scraper_url(base)
    try:
        r = await client.get(url, timeout=30)
        soup = BeautifulSoup(r.text, "html.parser")
        for card in soup.select("div.job-card, article.assignment-card, .job-listing-item")[:10]:
            title_el = card.select_one("h2 a, h3 a, .job-title a, a.assignment-link")
            date_el = card.select_one(".date, time, .updated-date")
            loc_el = card.select_one(".location, .job-location")
            if not title_el:
                continue
            href = title_el.get("href", "")
            loc_text = loc_el.get_text(strip=True) if loc_el else "Remote"
            snippet = card.get_text(strip=True)[:400]
            j = make_job(
                title=title_el.get_text(strip=True),
                company="Apple (Contract)",
                date=date_el.get_text(strip=True) if date_el else "Today",
                url=(f"https://directsource.magnitglobal.com{href}"
                     if href.startswith("/") else href),
                source="Apple CW",
                location=loc_text,
                snippet=snippet,
            )
            if j:
                jobs.append(j)
    except Exception as e:
        log.warning(f"Apple CW error ({query}): {e}")
    return jobs

# ── Aggregate ─────────────────────────────────────────────────────────────────
import random

async def _run_batched(coros, batch_size: int, delay_range: tuple):
    """
    Run coroutines in batches with a random pause between batches.
    Mimics natural browsing behaviour — important for LinkedIn.
    """
    results = []
    for i in range(0, len(coros), batch_size):
        batch = coros[i:i + batch_size]
        batch_results = await asyncio.gather(*batch)
        results.extend(batch_results)
        if i + batch_size < len(coros):
            pause = random.uniform(*delay_range)
            log.info(f"Batch done, pausing {pause:.1f}s before next batch...")
            await asyncio.sleep(pause)
    return results

async def fetch_all_jobs() -> list[dict]:
    all_jobs = []
    async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True) as client:

        # ── LinkedIn: batches of 4, pause 3-7s between batches ──────────────
        # Each call already does 2 requests internally (remote + hybrid),
        # so batch_size=4 means 8 actual HTTP requests per batch — natural pace.
        li_coros = [fetch_linkedin(client, query) for query in ALL_QUERIES]
        li_results = await _run_batched(li_coros, batch_size=4, delay_range=(3, 7))
        for batch in li_results:
            all_jobs.extend(batch)

        # ── Other sources: all parallel, they're less sensitive ──────────────
        other_tasks = []
        for query in ALL_QUERIES:
            for loc in LOCATIONS_SEARCH:
                other_tasks.append(fetch_indeed(client, query, loc))
            other_tasks.append(fetch_glassdoor(client, query))
            other_tasks.append(fetch_virtualvocations(client, query))
            other_tasks.append(fetch_synergis(client, query))
            other_tasks.append(fetch_apple_cw(client, query))

        for batch in await asyncio.gather(*other_tasks):
            all_jobs.extend(batch)

    # Deduplicate by job_id
    seen_ids: set = set()
    unique = []
    for job in all_jobs:
        jid = job_id(job["title"], job["company"], job["url"])
        if jid not in seen_ids:
            seen_ids.add(jid)
            job["id"] = jid
            unique.append(job)
    return unique

# ── Formatting ────────────────────────────────────────────────────────────────
SOURCE_EMOJI = {
    "LinkedIn": "🔵", "Indeed": "🟢", "Glassdoor": "🟡",
    "VirtualVocations": "🟠", "Synergis": "🔴",
    "Apple CW": "⚫️", "Jobgether": "🟣",
}

def format_message(jobs: list[dict], header_suffix: str = "") -> str:
    today = datetime.now(ET).strftime("%d %b %Y")
    lines = [
        f"🔍 <b>Job Digest — {today}{header_suffix}</b>\n",
        f"<i>{len(jobs)} new listing(s) found</i>\n"
    ]
    for j in jobs:
        emoji = SOURCE_EMOJI.get(j["source"], "⚪️")
        loc_line = f"📍 {j['location']}\n" if j.get("location") else ""
        lines.append(
            f"{emoji} <b>{j['title']}</b>\n"
            f"🏢 {j['company']}\n"
            f"{loc_line}"
            f"📅 {j['date']}\n"
            f"🔗 <a href=\"{j['url']}\">Open listing ({j['source']})</a>\n"
        )
    return "\n".join(lines)

# ── Core send logic ───────────────────────────────────────────────────────────
async def run_search_and_send(bot: Bot, is_manual: bool = False):
    seen = load_seen()
    all_jobs = await fetch_all_jobs()
    new_jobs = [j for j in all_jobs if j["id"] not in seen]
    log.info(f"Found {len(all_jobs)} total, {len(new_jobs)} new.")

    suffix = " (manual)" if is_manual else ""
    if not new_jobs:
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=f"📭 <b>No new listings{suffix}</b> — everything seen before.",
            parse_mode="HTML"
        )
        return

    for i in range(0, len(new_jobs), 10):
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=format_message(new_jobs[i:i + 10], header_suffix=suffix),
            parse_mode="HTML",
            disable_web_page_preview=True
        )
        await asyncio.sleep(1)

    for j in new_jobs:
        seen.add(j["id"])
    save_seen(seen)
    log.info(f"Sent {len(new_jobs)} jobs.")

async def scheduled_digest():
    bot = Bot(token=TELEGRAM_TOKEN)
    await run_search_and_send(bot, is_manual=False)

# ── Telegram commands ─────────────────────────────────────────────────────────
async def cmd_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != str(TELEGRAM_CHAT_ID):
        return
    await update.message.reply_text("🔄 Searching now, give me a minute...")
    await run_search_and_send(context.bot, is_manual=True)

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != str(TELEGRAM_CHAT_ID):
        return
    seen = load_seen()
    scraper = "✅ ScraperAPI connected" if SCRAPER_API_KEY else "⚠️ No ScraperAPI key (Indeed/Glassdoor may not work)"
    msg = (
        f"🤖 <b>Job Bot Status</b>\n\n"
        f"🕐 Now: {datetime.now(ET).strftime('%d %b %Y, %H:%M ET')}\n"
        f"⏰ Daily digest: {SEND_HOUR_ET}:{SEND_MINUTE_ET:02d} AM ET\n"
        f"📋 Queries: {len(ALL_QUERIES)} search terms\n"
        f"🌐 Sources: LinkedIn, Indeed, Glassdoor, VirtualVocations, Synergis, Apple CW\n"
        f"📍 Locations: Atlanta + Remote (US only)\n"
        f"👀 Jobs seen so far: {len(seen)}\n"
        f"{scraper}\n\n"
        f"/check — run search now\n"
        f"/status — this info\n"
        f"/reset — clear history"
    )
    await update.message.reply_text(msg, parse_mode="HTML")

async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != str(TELEGRAM_CHAT_ID):
        return
    save_seen(set())
    await update.message.reply_text("🗑 History cleared. /check will show all current listings fresh.")

# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    log.info(f"Bot started. Daily digest at {SEND_HOUR_ET}:{SEND_MINUTE_ET:02d} ET.")

    scheduler = AsyncIOScheduler(timezone=ET)
    scheduler.add_job(scheduled_digest, "cron",
                      hour=SEND_HOUR_ET, minute=SEND_MINUTE_ET)
    scheduler.start()

    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("check", cmd_check))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("reset", cmd_reset))

    await app.initialize()
    await app.start()
    await app.updater.start_polling()

    await scheduled_digest()  # run once immediately on startup

    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        await app.updater.stop()
        await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
