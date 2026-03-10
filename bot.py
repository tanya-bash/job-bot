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
LOCATIONS = ["Atlanta, GA", "Remote"]

# ── Filters ───────────────────────────────────────────────────────────────────
EXCLUDE_TITLE_KEYWORDS = [
    "junior", "intern", "entry level", "entry-level", "associate",
    "software", "engineer", "developer", "data analyst", "data science",
    "finance", "financial", "hr ", "human resources", "real estate",
    "construction", "healthcare", "nurse", "clinical", "physician",
    "recruiter", "talent acquisition",
]

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
    if any(kw in t for kw in RELEVANCE_KEYWORDS):
        return True
    if any(specific in t for specific in SPECIFIC_QUERY_TITLES):
        return True
    return any(kw in description.lower() for kw in RELEVANCE_KEYWORDS)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}

def make_job(title, company, date, url, source, snippet=""):
    if is_excluded(title) or not is_relevant(title, snippet):
        log.info(f"[{source}] Skipped: {title} @ {company}")
        return None
    return {"title": title, "company": company, "date": date, "url": url, "source": source}

# ── Indeed ────────────────────────────────────────────────────────────────────
async def fetch_indeed(client, query, location):
    jobs = []
    params = {"q": query, "l": location, "fromage": "1", "sort": "date"}
    try:
        r = await client.get("https://www.indeed.com/jobs", params=params, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        for card in soup.select("div.job_seen_beacon")[:12]:
            title_el = card.select_one("h2.jobTitle span[title]")
            company_el = card.select_one("[data-testid='company-name']")
            date_el = card.select_one("[data-testid='myJobsStateDate']")
            link_el = card.select_one("a[id^='job_']")
            snippet_el = card.select_one("div.job-snippet")
            if not (title_el and company_el and link_el):
                continue
            href = link_el.get("href", "")
            j = make_job(
                title=title_el.get_text(strip=True),
                company=company_el.get_text(strip=True),
                date=date_el.get_text(strip=True) if date_el else "Today",
                url=f"https://www.indeed.com{href}" if href.startswith("/") else href,
                source="Indeed",
                snippet=snippet_el.get_text(strip=True) if snippet_el else "",
            )
            if j: jobs.append(j)
    except Exception as e:
        log.warning(f"Indeed error ({query}/{location}): {e}")
    return jobs

# ── Glassdoor ─────────────────────────────────────────────────────────────────
async def fetch_glassdoor(client, query):
    jobs = []
    params = {"sc.keyword": query, "locT": "N", "locId": "1", "fromAge": "1"}
    try:
        r = await client.get(
            "https://www.glassdoor.com/Job/jobs.htm", params=params, timeout=15,
            headers={**HEADERS, "Referer": "https://www.glassdoor.com/"}
        )
        soup = BeautifulSoup(r.text, "html.parser")
        for card in soup.select("li.react-job-listing")[:10]:
            title_el = card.select_one("a.jobLink span")
            company_el = card.select_one("div.job-search-key-yzn1t")
            link_el = card.select_one("a.jobLink")
            snippet_el = card.select_one("div.job-search-key-l2wjgv")
            if not (title_el and link_el):
                continue
            href = link_el.get("href", "")
            j = make_job(
                title=title_el.get_text(strip=True),
                company=company_el.get_text(strip=True) if company_el else "Unknown",
                date="Today",
                url=f"https://www.glassdoor.com{href}" if href.startswith("/") else href,
                source="Glassdoor",
                snippet=snippet_el.get_text(strip=True) if snippet_el else "",
            )
            if j: jobs.append(j)
    except Exception as e:
        log.warning(f"Glassdoor error ({query}): {e}")
    return jobs

# ── LinkedIn ──────────────────────────────────────────────────────────────────
async def fetch_linkedin(client, query, location):
    jobs = []
    params = {"keywords": query, "location": location, "f_TPR": "r86400", "sortBy": "DD"}
    try:
        r = await client.get(
            "https://www.linkedin.com/jobs/search/", params=params, timeout=15,
            headers={**HEADERS, "Accept-Language": "en-US,en;q=0.9"}
        )
        soup = BeautifulSoup(r.text, "html.parser")
        for card in soup.select("div.base-card")[:10]:
            title_el = card.select_one("h3.base-search-card__title")
            company_el = card.select_one("h4.base-search-card__subtitle")
            date_el = card.select_one("time")
            link_el = card.select_one("a.base-card__full-link")
            snippet_el = card.select_one("p.base-search-card__metadata")
            if not (title_el and link_el):
                continue
            j = make_job(
                title=title_el.get_text(strip=True),
                company=company_el.get_text(strip=True) if company_el else "Unknown",
                date=date_el.get("datetime", "Today") if date_el else "Today",
                url=link_el.get("href", "").split("?")[0],
                source="LinkedIn",
                snippet=snippet_el.get_text(strip=True) if snippet_el else "",
            )
            if j: jobs.append(j)
    except Exception as e:
        log.warning(f"LinkedIn error ({query}/{location}): {e}")
    return jobs

# ── Virtual Vocations ─────────────────────────────────────────────────────────
async def fetch_virtualvocations(client, query):
    """Virtual Vocations — remote-only job board, open HTML."""
    jobs = []
    params = {"search": query, "category": "", "type": ""}
    try:
        r = await client.get("https://www.virtualvocations.com/jobs", params=params, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        for card in soup.select("div.job-result-item, article.job-card")[:10]:
            title_el = card.select_one("h2 a, h3 a, .job-title a")
            company_el = card.select_one(".company-name, .job-company")
            date_el = card.select_one(".job-date, time")
            if not title_el:
                continue
            title = title_el.get_text(strip=True)
            company = company_el.get_text(strip=True) if company_el else "Unknown"
            date_str = date_el.get_text(strip=True) if date_el else "Today"
            href = title_el.get("href", "")
            url = f"https://www.virtualvocations.com{href}" if href.startswith("/") else href
            snippet = card.get_text(strip=True)[:300]
            j = make_job(title=title, company=company, date=date_str,
                         url=url, source="VirtualVocations", snippet=snippet)
            if j: jobs.append(j)
    except Exception as e:
        log.warning(f"VirtualVocations error ({query}): {e}")
    return jobs

# ── Synergis ──────────────────────────────────────────────────────────────────
async def fetch_synergis(client, query):
    """Synergis HR — Atlanta-based staffing agency, Creative & IT specialist."""
    jobs = []
    params = {"s": query}
    try:
        r = await client.get("https://www.synergishr.com/search-jobs/", params=params, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        for card in soup.select("div.job-listing, article.job, .careers-job-item")[:10]:
            title_el = card.select_one("h2 a, h3 a, .job-title a, a.job-link")
            company_el = card.select_one(".company, .job-company")
            date_el = card.select_one(".date, time, .job-date")
            if not title_el:
                continue
            title = title_el.get_text(strip=True)
            company = company_el.get_text(strip=True) if company_el else "Synergis Client"
            date_str = date_el.get_text(strip=True) if date_el else "Today"
            href = title_el.get("href", "")
            url = f"https://www.synergishr.com{href}" if href.startswith("/") else href
            snippet = card.get_text(strip=True)[:300]
            j = make_job(title=title, company=company, date=date_str,
                         url=url, source="Synergis", snippet=snippet)
            if j: jobs.append(j)
    except Exception as e:
        log.warning(f"Synergis error ({query}): {e}")
    return jobs

# ── Apple Contingent Workforce ────────────────────────────────────────────────
async def fetch_apple_cw(client, query):
    """Apple Contingent Workforce portal — Marcom & creative contract roles."""
    jobs = []
    params = {"keywords": query, "businessGroups": "Marcom"}
    try:
        r = await client.get(
            "https://directsource.magnitglobal.com/us/applecontingentworkforce/jobs",
            params=params, timeout=15
        )
        soup = BeautifulSoup(r.text, "html.parser")
        for card in soup.select("div.job-card, article.assignment-card, .job-listing-item")[:10]:
            title_el = card.select_one("h2 a, h3 a, .job-title a, a.assignment-link")
            date_el = card.select_one(".date, time, .updated-date")
            if not title_el:
                continue
            title = title_el.get_text(strip=True)
            date_str = date_el.get_text(strip=True) if date_el else "Today"
            href = title_el.get("href", "")
            url = (f"https://directsource.magnitglobal.com{href}"
                   if href.startswith("/") else href)
            snippet = card.get_text(strip=True)[:300]
            j = make_job(title=title, company="Apple (Contract)", date=date_str,
                         url=url, source="Apple CW", snippet=snippet)
            if j: jobs.append(j)
    except Exception as e:
        log.warning(f"Apple CW error ({query}): {e}")
    return jobs

# ── Jobgether (best effort — may be blocked) ──────────────────────────────────
async def fetch_jobgether(client, query):
    """Jobgether — remote-first jobs. May return 403 due to Cloudflare."""
    jobs = []
    try:
        r = await client.get(
            f"https://jobgether.com/remote-jobs?q={query}", timeout=15,
            headers={**HEADERS, "Accept": "text/html,application/xhtml+xml"}
        )
        if r.status_code != 200:
            log.info(f"Jobgether returned {r.status_code}, skipping.")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        for card in soup.select("article, div.job-card, li.job-item")[:10]:
            title_el = card.select_one("h2 a, h3 a, .job-title a")
            company_el = card.select_one(".company, .employer")
            if not title_el:
                continue
            title = title_el.get_text(strip=True)
            company = company_el.get_text(strip=True) if company_el else "Unknown"
            href = title_el.get("href", "")
            url = f"https://jobgether.com{href}" if href.startswith("/") else href
            snippet = card.get_text(strip=True)[:300]
            j = make_job(title=title, company=company, date="Today",
                         url=url, source="Jobgether", snippet=snippet)
            if j: jobs.append(j)
    except Exception as e:
        log.warning(f"Jobgether error ({query}): {e}")
    return jobs

# ── Aggregate all sources ─────────────────────────────────────────────────────
async def fetch_all_jobs() -> list[dict]:
    all_jobs = []
    async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True) as client:
        tasks = []
        for query in ALL_QUERIES:
            for loc in LOCATIONS:
                tasks.append(fetch_indeed(client, query, loc))
                tasks.append(fetch_linkedin(client, query, loc))
            tasks.append(fetch_glassdoor(client, query))
            tasks.append(fetch_virtualvocations(client, query))
            tasks.append(fetch_synergis(client, query))
        # Apple CW and Jobgether: search by query only (not location-filtered)
        for query in ALL_QUERIES:
            tasks.append(fetch_apple_cw(client, query))
            tasks.append(fetch_jobgether(client, query))

        for batch in await asyncio.gather(*tasks):
            all_jobs.extend(batch)

    # Deduplicate
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
    "LinkedIn": "🔵",
    "Indeed": "🟢",
    "Glassdoor": "🟡",
    "VirtualVocations": "🟠",
    "Synergis": "🔴",
    "Apple CW": "⚫️",
    "Jobgether": "🟣",
}

def format_message(jobs: list[dict], header_suffix: str = "") -> str:
    today = datetime.now(ET).strftime("%d %b %Y")
    lines = [
        f"🔍 <b>Job Digest — {today}{header_suffix}</b>\n",
        f"<i>{len(jobs)} new listing(s) found</i>\n"
    ]
    for j in jobs:
        emoji = SOURCE_EMOJI.get(j["source"], "⚪️")
        lines.append(
            f"{emoji} <b>{j['title']}</b>\n"
            f"🏢 {j['company']}\n"
            f"📅 {j['date']}\n"
            f"🔗 <a href=\"{j['url']}\">Open listing ({j['source']})</a>\n"
        )
    return "\n".join(lines)

# ── Core search logic ─────────────────────────────────────────────────────────
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
    now_et = datetime.now(ET).strftime("%d %b %Y, %H:%M ET")
    msg = (
        f"🤖 <b>Job Bot Status</b>\n\n"
        f"🕐 Now: {now_et}\n"
        f"⏰ Daily digest: {SEND_HOUR_ET}:{SEND_MINUTE_ET:02d} AM ET\n"
        f"📋 Queries: {len(ALL_QUERIES)} search terms\n"
        f"🌐 Sources: LinkedIn, Indeed, Glassdoor, VirtualVocations, Synergis, Apple CW, Jobgether\n"
        f"📍 Locations: Atlanta, GA + Remote\n"
        f"👀 Jobs seen so far: {len(seen)}\n\n"
        f"/check — run search now\n"
        f"/status — show this info\n"
        f"/reset — clear history (get fresh results)"
    )
    await update.message.reply_text(msg, parse_mode="HTML")

async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != str(TELEGRAM_CHAT_ID):
        return
    save_seen(set())
    await update.message.reply_text("🗑 History cleared. /check will now show all current listings fresh.")

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
