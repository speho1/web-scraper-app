"""
Operation 36 family admins scraper.
Logs into Operation 36, reads operation36_families.csv, and for each family_id
visits the family contact page to extract every Family Admin on it.

Work is parallelized across NUM_WORKERS workers pulling family_ids from a
shared queue. Any family_ids that fail (network error, no admins parsed) are
retried up to MAX_RETRIES more times.

Output: output/operation36_family_admins.csv
        (first_name, last_name, phone_number, email, family_id, member_id, admin)

Usage:  python scraper-operation-family-admins.py
"""

import os
import re
import csv
import asyncio
from dotenv import load_dotenv
from playwright.async_api import async_playwright
import config

load_dotenv()

FAMILIES_INPUT = "output/operation36_families.csv"
ADMINS_OUTPUT = "output/operation36_family_admins.csv"
FAMILY_CONTACT_URL = "https://operation36golf.com/members/families/{family_id}/contact"

NUM_WORKERS = 4
MAX_RETRIES = 2

# Matches one admin entry's full text content, e.g.
# "1Asia WilsonFamily AdminAge 44 / F"
# "2John SmithFamily Admin"
ADMIN_ENTRY_PATTERN = re.compile(
    r"^\d+([A-Z][\w'\-\.]*)\s+(.+?)Family Admin(?:Age\s*\d+\s*/\s*[MF])?\s*$"
)
PHONE_PATTERN = re.compile(r"\+?\d?\s*\(?\d{3}\)?[\s\-\.]\s*\d{3}[\s\-\.]\s*\d{4}")
EMAIL_PATTERN = re.compile(r"[\w\.\-+]+@[\w\.\-]+\.\w+")
PROFILE_HREF_PATTERN = re.compile(r"/profile/(\d+)")
EMAIL_LEADING_PHONE_FRAGMENT = re.compile(r"^[\d\-\.\+\s]+")
EMAIL_LEADING_GENDER_MARKER = re.compile(r"^[MF](?=[a-z])")


async def login(page):
    """Log into Operation 36."""
    await page.goto(config.OPERATION36_LOGIN_URL, wait_until="networkidle")
    await page.fill(config.OPERATION36_LOGIN_EMAIL_SELECTOR, os.getenv("OPERATION36_EMAIL"))
    await page.fill(config.OPERATION36_LOGIN_PASSWORD_SELECTOR, os.getenv("OPERATION36_PASSWORD"))
    await page.get_by_role(
        config.OPERATION36_LOGIN_SUBMIT_ROLE[0],
        name=config.OPERATION36_LOGIN_SUBMIT_ROLE[1]["name"],
    ).click()
    await page.wait_for_load_state("networkidle")


def read_family_ids(path):
    """Read family_ids from the families CSV. Returns a list of unique non-empty ids."""
    family_ids = []
    seen = set()
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            fid = (row.get("family_id") or "").strip()
            if not fid or fid in seen:
                continue
            seen.add(fid)
            family_ids.append(fid)
    return family_ids


async def find_admin_entries(page):
    """Return a list of admin entry locators on the current contact page.
    Filters divs whose entire text content matches one admin entry exactly,
    deduplicated by text so nested matches collapse to one."""
    candidates = page.locator("div").filter(has_text=re.compile(r"Family Admin"))
    count = await candidates.count()
    entries = []
    seen_texts = set()
    for i in range(count):
        loc = candidates.nth(i)
        text = (await loc.text_content() or "").strip()
        if text in seen_texts:
            continue
        m = ADMIN_ENTRY_PATTERN.match(text)
        if not m:
            continue
        seen_texts.add(text)
        first_name = m.group(1)
        last_name = m.group(2).strip()
        entries.append({"locator": loc, "first_name": first_name, "last_name": last_name})
    return entries


async def extract_member_id(admin_loc):
    """Pull member_id out of the link inside this admin entry."""
    link = admin_loc.get_by_role("link").first
    if await link.count() == 0:
        return ""
    href = await link.get_attribute("href") or ""
    if not href:
        href = await link.evaluate(
            "el => el.closest('a')?.getAttribute('href') "
            "|| el.querySelector('a')?.getAttribute('href') || ''"
        )
    m = PROFILE_HREF_PATTERN.search(href or "")
    return m.group(1) if m else ""


async def extract_contact_for_admin(admin_loc):
    """Search the admin entry and its nearest ancestors for phone + email."""
    locators_to_check = [admin_loc, admin_loc.locator("xpath=.."), admin_loc.locator("xpath=../..")]
    phone = ""
    email = ""
    for loc in locators_to_check:
        try:
            text = (await loc.text_content()) or ""
        except Exception:
            continue
        if not phone:
            pm = PHONE_PATTERN.search(text)
            if pm:
                phone = re.sub(r"\s+", " ", pm.group(0)).strip()
        if not email:
            # Strip phone matches first so the trailing digits of a phone
            # don't get pulled into the email's local part.
            text_for_email = PHONE_PATTERN.sub(" ", text)
            em = EMAIL_PATTERN.search(text_for_email)
            if em:
                local, _, domain = em.group(0).strip().partition("@")
                # Drop any phone-fragment leftovers (the strict phone regex
                # may have missed a malformed phone) and a leading gender
                # marker rendered directly before the email.
                local = EMAIL_LEADING_PHONE_FRAGMENT.sub("", local)
                local = EMAIL_LEADING_GENDER_MARKER.sub("", local)
                if local and domain:
                    email = f"{local}@{domain}"
        if phone and email:
            break
    return phone, email


async def scrape_family(page, family_id, worker_id):
    """Visit one family contact page and return its list of admin records."""
    url = FAMILY_CONTACT_URL.format(family_id=family_id)
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    except Exception as e:
        print(f"  [w{worker_id}] family {family_id}: goto warning: {e}")
    await page.wait_for_timeout(2000)
    try:
        await page.get_by_text("Family Admin", exact=False).first.wait_for(
            state="visible", timeout=10000
        )
    except Exception:
        pass

    entries = await find_admin_entries(page)
    records = []
    for entry in entries:
        member_id = await extract_member_id(entry["locator"])
        phone, email = await extract_contact_for_admin(entry["locator"])
        records.append({
            "first_name": entry["first_name"],
            "last_name": entry["last_name"],
            "phone_number": phone,
            "email": email,
            "family_id": family_id,
            "member_id": member_id,
            "admin": True,
        })
    return records


async def worker(page, queue, results, failed, worker_id):
    """Pull family_ids from the queue and scrape each one."""
    while True:
        try:
            family_id = queue.get_nowait()
        except asyncio.QueueEmpty:
            return
        try:
            records = await scrape_family(page, family_id, worker_id)
            if records:
                results.extend(records)
                print(
                    f"  [w{worker_id}] family {family_id}: "
                    f"{len(records)} admin(s)"
                )
                for r in records:
                    print(
                        f"  [w{worker_id}]   - {r['first_name']} {r['last_name']} "
                        f"| {r['phone_number']} | {r['email']} | member {r['member_id']}"
                    )
            else:
                print(f"  [w{worker_id}] family {family_id}: no admins found")
                failed.append(family_id)
        except Exception as e:
            print(f"  [w{worker_id}] family {family_id}: error: {e}")
            failed.append(family_id)


async def init_worker_page(page, worker_id):
    """Warm the worker page so the first contact-page hit isn't cold."""
    try:
        await page.goto(
            config.OPERATION36_FAMILIES_URL, wait_until="domcontentloaded", timeout=60000
        )
    except Exception as e:
        print(f"  [w{worker_id}] initial goto warning: {e}")
    await page.wait_for_timeout(2000)


def export_admins_csv(records):
    """Write admin records to CSV."""
    os.makedirs("output", exist_ok=True)
    with open(ADMINS_OUTPUT, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "first_name", "last_name", "phone_number",
                "email", "family_id", "member_id", "admin",
            ],
        )
        writer.writeheader()
        writer.writerows(records)
    print(f"\nExported {len(records)} family admins to {ADMINS_OUTPUT}")


async def main():
    email = os.getenv("OPERATION36_EMAIL")
    password = os.getenv("OPERATION36_PASSWORD")
    if not email or not password:
        print("Error: Set OPERATION36_EMAIL and OPERATION36_PASSWORD in .env")
        return

    if not os.path.exists(FAMILIES_INPUT):
        print(f"Error: {FAMILIES_INPUT} not found. Run scraper-operation36-families.py first.")
        return

    family_ids = read_family_ids(FAMILIES_INPUT)
    print(f"Loaded {len(family_ids)} family_ids from {FAMILIES_INPUT}")
    if not family_ids:
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(viewport={"width": 1280, "height": 900})

        login_page = await context.new_page()
        await login(login_page)
        await login_page.wait_for_timeout(2000)
        await login_page.close()

        worker_pages = [await context.new_page() for _ in range(NUM_WORKERS)]
        print(f"\nInitializing {NUM_WORKERS} worker pages...")
        await asyncio.gather(*[
            init_worker_page(worker_pages[i], i) for i in range(NUM_WORKERS)
        ])

        results = []

        async def run_pass(pass_ids, label):
            q = asyncio.Queue()
            for fid in pass_ids:
                q.put_nowait(fid)
            failed = []
            print(f"\n{label}: {len(pass_ids)} families, {NUM_WORKERS} workers")
            await asyncio.gather(*[
                worker(worker_pages[i], q, results, failed, i)
                for i in range(NUM_WORKERS)
            ])
            return failed

        to_process = family_ids
        for attempt in range(MAX_RETRIES + 1):
            label = "Initial pass" if attempt == 0 else f"Retry {attempt}/{MAX_RETRIES}"
            failed = await run_pass(to_process, label)
            if not failed:
                break
            to_process = failed

        if to_process:
            print(
                f"\nWarning: {len(to_process)} family/families produced no admins "
                f"after {MAX_RETRIES} retries:"
            )
            for fid in to_process:
                print(f"  - {fid}")

        for wp in worker_pages:
            await wp.close()

        export_admins_csv(results)
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
