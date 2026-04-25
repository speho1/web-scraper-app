"""
Operation 36 families scraper.
Logs into Operation 36, scrolls to load all families, extracts names and emails,
then searches each family to capture their family ID.

Collection is sequential; URL capture is parallelized across 4 workers
pulling from a single shared queue of family-name groups. After the first
pass, any families missing family_id are retried up to MAX_RETRIES more times.

Output: output/operation36_families.csv (family_name, email, phone, family_id)

Usage:  python scraper-operation36-families.py
"""

import os
import re
import csv
import asyncio
from collections import defaultdict
from dotenv import load_dotenv
from playwright.async_api import async_playwright
import config

load_dotenv()

FAMILIES_OUTPUT = "output/operation36_families.csv"

NUM_WORKERS = 4
MAX_RETRIES = 2


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


async def scroll_to_load_all_families(page):
    """Scroll the infinite scroll container to load all family rows."""
    container = page.locator(".infinite-scroll-component").first
    prev_count = 0
    stable_rounds = 0
    while True:
        rows = page.locator(".FamiliesTable_row__AQCjl")
        count = await rows.count()
        if count > 0:
            await rows.nth(count - 1).scroll_into_view_if_needed()
        await page.wait_for_timeout(3000)
        current_count = await rows.count()
        print(f"  Scrolling... {current_count} families loaded")
        if current_count == prev_count:
            stable_rounds += 1
            if stable_rounds >= 5:
                break
        else:
            stable_rounds = 0
        prev_count = current_count
    print(f"  Done scrolling. Total families: {current_count}")
    await container.evaluate("el => el.scrollTop = 0")
    await page.wait_for_timeout(1000)


async def navigate_to_families(page):
    """Navigate to families page and load all families."""
    await page.goto(config.OPERATION36_FAMILIES_URL, wait_until="networkidle")
    await page.wait_for_timeout(3000)
    await page.locator("#tableContent").wait_for(timeout=15000)
    await page.wait_for_timeout(2000)
    await scroll_to_load_all_families(page)


async def collect_families(page):
    """Scroll to load all families, extract name, email, and phone."""
    await navigate_to_families(page)
    rows = page.locator(".FamiliesTable_row__AQCjl")
    count = await rows.count()
    families = []
    for i in range(count):
        row = rows.nth(i)
        name_el = row.locator("h5")
        if await name_el.count() > 0:
            name = (await name_el.text_content() or "").strip()
        else:
            name = f"Family_{i}"
        # Strip "Family" suffix (e.g., "Drew Family" -> "Drew")
        name = re.sub(r"\s*Family$", "", name).strip()

        email = ""
        phone = ""
        admin_div = row.locator(".FamiliesTable_familyAdmin__o03AK")
        if await admin_div.count() > 0:
            spans = admin_div.locator("span")
            spans_count = await spans.count()
            for j in range(spans_count):
                text = (await spans.nth(j).text_content() or "").strip()
                if "@" in text:
                    email = text
                elif not phone and re.search(r"\d", text) and len(re.findall(r"\d", text)) >= 7:
                    phone = text

        families.append({"family_name": name, "email": email, "phone": phone, "family_id": ""})

    families.sort(key=lambda f: f["family_name"].lower())
    print(f"Collected {len(families)} families")
    return families


async def enter_search(page, query):
    """Fill the search box and poll until visible rows reflect the new query.
    Replaces a fixed 3s sleep that would occasionally undershoot under worker
    concurrency, leaving the table showing stale rows from the previous query
    (or no rows at all)."""
    search_box = page.get_by_role("textbox", name="Search for Families")
    await search_box.fill("")
    await search_box.fill(query)

    rows = page.locator(".FamiliesTable_row__AQCjl")
    target = query.strip().lower()
    for _ in range(30):  # up to ~15s
        await page.wait_for_timeout(500)
        count = await rows.count()
        if count == 0:
            continue
        first_text = ""
        h5 = rows.first.locator("h5")
        if await h5.count() > 0:
            first_text = ((await h5.text_content()) or "").lower()
        if target in first_text:
            return


async def ensure_search(page, query):
    """Make sure the families page is open and the search box contains `query`."""
    if config.OPERATION36_FAMILIES_URL not in page.url:
        await page.goto(
            config.OPERATION36_FAMILIES_URL, wait_until="domcontentloaded", timeout=60000
        )
        await page.wait_for_timeout(2000)
        try:
            await page.locator("#tableContent").wait_for(timeout=15000)
        except Exception:
            pass
    search_box = page.get_by_role("textbox", name="Search for Families")
    if await search_box.input_value() != query:
        await enter_search(page, query)


def group_by_family_name(families):
    """Group families by family name, preserving order within each group."""
    groups = defaultdict(list)
    for f in families:
        groups[f["family_name"]].append(f)
    return list(groups.items())


async def get_row_family_name(row):
    """Read a search-result row's displayed surname (strips trailing 'Family')."""
    h5 = row.locator("h5")
    if await h5.count() == 0:
        return ""
    text = (await h5.text_content() or "").strip()
    return re.sub(r"\s*Family$", "", text).strip()


async def process_group(page, family_name, family_group, worker_id):
    """Search for one family name and capture family_id only for rows whose
    displayed surname exactly matches family_name. This prevents substring
    matches (e.g., 'Baum' inside 'Nussbaum') and unrelated top-of-list rows
    from being cross-assigned when the search filter is loose."""
    expected_count = len(family_group)
    print(f"  [w{worker_id}] Searching: {family_name} (expect {expected_count})")

    query = family_name
    await ensure_search(page, query)
    rows = page.locator(".FamiliesTable_row__AQCjl")
    result_count = await rows.count()
    print(f"  [w{worker_id}]   Found {result_count} search result(s) for '{query}'")

    target_name = family_name.lower()
    matching_indexes = []
    for i in range(result_count):
        row_name = await get_row_family_name(rows.nth(i))
        if row_name.lower() == target_name:
            matching_indexes.append(i)

    if not matching_indexes:
        print(f"  [w{worker_id}]   No exact-match rows for '{family_name}'; skipping")
        return

    print(
        f"  [w{worker_id}]   {len(matching_indexes)} exact match(es) "
        f"for '{family_name}' (need {expected_count})"
    )

    for k, idx in enumerate(matching_indexes[:expected_count]):
        await ensure_search(page, query)
        rows = page.locator(".FamiliesTable_row__AQCjl")
        row = rows.nth(idx)

        actual = await get_row_family_name(row)
        if actual.lower() != target_name:
            print(
                f"  [w{worker_id}]   row {idx} no longer matches "
                f"('{actual}'); skipping"
            )
            continue

        view_btn = row.locator("button.btn-primary")
        await view_btn.click()
        await page.wait_for_load_state("networkidle", timeout=60000)
        await page.wait_for_timeout(1500)

        family_url = page.url
        match = re.search(r"/families/(\d+)", family_url)
        family_id = match.group(1) if match else ""

        target = family_group[k] if k < len(family_group) else family_group[0]
        target["family_id"] = family_id
        print(f"  [w{worker_id}]   [{k+1}/{expected_count}] {family_name} -> {family_id}")

        await page.go_back(timeout=60000)
        await page.wait_for_load_state("networkidle", timeout=60000)
        await page.wait_for_timeout(2000)


async def init_worker_page(page, worker_id):
    """One-time navigation to the families page for a worker. Uses
    `domcontentloaded` instead of `networkidle` because Operation36 keeps
    long-lived connections open and networkidle rarely resolves."""
    try:
        await page.goto(
            config.OPERATION36_FAMILIES_URL, wait_until="domcontentloaded", timeout=60000
        )
    except Exception as e:
        print(f"  [w{worker_id}] initial goto warning: {e}")
    await page.wait_for_timeout(3000)
    try:
        await page.locator("#tableContent").wait_for(timeout=15000)
    except Exception as e:
        print(f"  [w{worker_id}] tableContent wait warning: {e}")


async def worker(page, queue, worker_id):
    """Pull family-name groups from the queue and process until empty."""
    while True:
        try:
            family_name, family_group = queue.get_nowait()
        except asyncio.QueueEmpty:
            return
        try:
            await process_group(page, family_name, family_group, worker_id)
        except Exception as e:
            print(f"  [w{worker_id}] error on '{family_name}': {e}")


def export_families_csv(families):
    """Write families to CSV."""
    os.makedirs("output", exist_ok=True)
    with open(FAMILIES_OUTPUT, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["family_name", "email", "phone", "family_id"])
        writer.writeheader()
        writer.writerows(families)
    print(f"\nExported {len(families)} families to {FAMILIES_OUTPUT}")


async def main():
    email = os.getenv("OPERATION36_EMAIL")
    password = os.getenv("OPERATION36_PASSWORD")
    if not email or not password:
        print("Error: Set OPERATION36_EMAIL and OPERATION36_PASSWORD in .env")
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(viewport={"width": 1280, "height": 900})

        login_page = await context.new_page()
        await login(login_page)
        await login_page.wait_for_timeout(2000)

        print("Collecting families...")
        families = await collect_families(login_page)
        for f in families:
            print(f"  {f['family_name']} | {f['email']} | {f['phone']}")
        await login_page.close()

        groups = group_by_family_name(families)
        print(f"\nGrouped into {len(groups)} unique family names")

        worker_pages = [await context.new_page() for _ in range(NUM_WORKERS)]
        print(f"\nInitializing {NUM_WORKERS} worker pages...")
        await asyncio.gather(*[
            init_worker_page(worker_pages[i], i) for i in range(NUM_WORKERS)
        ])

        async def run_pass(pass_groups, label):
            q = asyncio.Queue()
            for g in pass_groups:
                q.put_nowait(g)
            total_families = sum(len(v) for _, v in pass_groups)
            print(
                f"\n{label}: {len(pass_groups)} family-name groups, "
                f"{total_families} families, {NUM_WORKERS} workers"
            )
            await asyncio.gather(*[
                worker(worker_pages[i], q, i)
                for i in range(NUM_WORKERS)
            ])

        await run_pass(groups, "Initial pass")

        for attempt in range(1, MAX_RETRIES + 1):
            unfilled = [f for f in families if not f["family_id"]]
            if not unfilled:
                break
            retry_groups = group_by_family_name(unfilled)
            await run_pass(retry_groups, f"Retry {attempt}/{MAX_RETRIES}")

        remaining = [f for f in families if not f["family_id"]]
        if remaining:
            print(
                f"\nWarning: {len(remaining)} family/families still missing IDs "
                f"after {MAX_RETRIES} retries:"
            )
            for f in remaining:
                print(f"  - {f['family_name']}")

        for wp in worker_pages:
            await wp.close()

        export_families_csv(families)
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
