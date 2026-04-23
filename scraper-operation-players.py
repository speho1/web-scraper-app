"""
Operation 36 players (students) scraper.
Logs into Operation 36, scrolls to load all students, extracts names,
then searches each student to capture member URL (View Profile) and
family URL (View Family Page).

Collection is sequential; URL capture is parallelized across 4 workers
pulling from a single shared queue of first-name groups. After the first
pass, any students missing member_url or family_url are retried up to
MAX_RETRIES more times.

Output: output/operation36_players.csv (first_name, last_name, member_url, family_url)

Usage:  python scraper-operation-players.py
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

OPERATION36_STUDENTS_URL = "https://operation36golf.com/members/students"
PLAYERS_OUTPUT = "output/operation36_players.csv"
STUDENT_BUTTON_PATTERN = re.compile(r"^student \d+ ")

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


async def get_button_name(btn):
    """Return the accessible name of a button (aria-label or text content)."""
    aria = await btn.get_attribute("aria-label")
    if aria:
        return aria.strip()
    return (await btn.text_content() or "").strip()


def parse_student_name(button_name):
    """Parse student button label to extract (first, last) name.
    Handles accessible-name form 'student 1 Mala Raman 55yrs /' and the
    text-content form '1Mala Raman55yrs / Female...' where the leading card
    index is concatenated and '55yrs' touches the last name with no space."""
    if not button_name:
        return ("", "")
    s = button_name.strip()
    s = re.sub(r"^student\s+\d+\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"^\d+\s*", "", s)
    s = re.split(r"\d+\s*y(?:r|ear)s?\b", s, maxsplit=1, flags=re.IGNORECASE)[0]
    s = s.strip().rstrip("/").strip()
    parts = s.split()
    if not parts:
        return ("", "")
    if len(parts) == 1:
        return (parts[0], "")
    return (parts[0], " ".join(parts[1:]))


async def scroll_to_load_all_students(page):
    """Scroll the page to load all student cards."""
    buttons = page.get_by_role("button", name=STUDENT_BUTTON_PATTERN)
    prev_count = 0
    stable_rounds = 0
    while True:
        count = await buttons.count()
        if count > 0:
            await buttons.nth(count - 1).scroll_into_view_if_needed()
        await page.wait_for_timeout(3000)
        current_count = await buttons.count()
        print(f"  Scrolling... {current_count} students loaded")
        if current_count == prev_count:
            stable_rounds += 1
            if stable_rounds >= 5:
                break
        else:
            stable_rounds = 0
        prev_count = current_count
    print(f"  Done scrolling. Total students: {current_count}")
    await page.evaluate("window.scrollTo(0, 0)")
    await page.wait_for_timeout(1000)


async def navigate_to_students(page):
    """Navigate to students page and load all students."""
    await page.goto(OPERATION36_STUDENTS_URL, wait_until="networkidle")
    await page.wait_for_timeout(3000)
    await scroll_to_load_all_students(page)


async def collect_students(page):
    """Scroll to load all students, extract first and last name."""
    await navigate_to_students(page)
    buttons = page.get_by_role("button", name=STUDENT_BUTTON_PATTERN)
    count = await buttons.count()
    students = []
    seen = set()
    for i in range(count):
        btn_name = await get_button_name(buttons.nth(i))
        first, last = parse_student_name(btn_name)
        if not first:
            continue
        key = (first.lower(), last.lower())
        if key in seen:
            continue
        seen.add(key)
        students.append({
            "first_name": first,
            "last_name": last,
            "member_url": "",
            "family_url": "",
        })

    students.sort(key=lambda s: (s["first_name"].lower(), s["last_name"].lower()))
    print(f"Collected {len(students)} students")
    return students


async def enter_search(page, query):
    """Fill the search box and wait for results."""
    search_box = page.get_by_role("textbox", name="Search for Students")
    await search_box.fill("")
    await page.wait_for_timeout(500)
    await search_box.fill(query)
    await page.wait_for_timeout(3000)


async def ensure_search(page, query):
    """Make sure the students page is open and the search box contains `query`."""
    if OPERATION36_STUDENTS_URL not in page.url:
        await page.goto(
            OPERATION36_STUDENTS_URL, wait_until="domcontentloaded", timeout=60000
        )
        await page.wait_for_timeout(2000)
    search_box = page.get_by_role("textbox", name="Search for Students")
    if await search_box.input_value() != query:
        await enter_search(page, query)


def _absolutize(href):
    if not href:
        return ""
    if href.startswith("http"):
        return href
    return f"https://operation36golf.com{href}"


async def extract_popup_url(page, label):
    """Read the href for a popup action (e.g., 'View Profile') without navigating."""
    for role in ("link", "button"):
        locator = page.get_by_role(role, name=label)
        if await locator.count() == 0:
            continue
        first = locator.first
        href = await first.get_attribute("href")
        if href:
            return _absolutize(href)
        href = await first.evaluate(
            "el => el.closest('a')?.getAttribute('href') "
            "|| el.querySelector('a')?.getAttribute('href') || ''"
        )
        if href:
            return _absolutize(href)
    return ""


async def close_popup(page):
    try:
        await page.keyboard.press("Escape")
        await page.wait_for_timeout(300)
    except Exception:
        pass


async def capture_student_urls(page, student_button):
    """Open popup once, read both hrefs, close popup.
    Falls back to click-and-navigate for any URL that can't be read directly."""
    await student_button.click()
    try:
        await page.get_by_role("button", name="View Profile").first.wait_for(
            state="visible", timeout=5000
        )
    except Exception:
        await page.wait_for_timeout(1500)

    member_url = await extract_popup_url(page, "View Profile")
    family_url = await extract_popup_url(page, "View Family Page")

    if member_url and family_url:
        await close_popup(page)
        return member_url, family_url

    if not member_url:
        await page.get_by_role("button", name="View Profile").click()
        await page.wait_for_load_state("networkidle", timeout=60000)
        await page.wait_for_timeout(1500)
        member_url = page.url
        await page.go_back(timeout=60000)
        await page.wait_for_load_state("networkidle", timeout=60000)
        await page.wait_for_timeout(2000)
        if not family_url:
            await student_button.click()
            await page.wait_for_timeout(1500)

    if not family_url:
        await page.get_by_role("button", name="View Family Page").click()
        await page.wait_for_load_state("networkidle", timeout=60000)
        await page.wait_for_timeout(1500)
        family_url = page.url
        await page.go_back(timeout=60000)
        await page.wait_for_load_state("networkidle", timeout=60000)
        await page.wait_for_timeout(2000)
    else:
        await close_popup(page)

    return member_url, family_url


async def find_button_for(page, first_name, last_name, fallback_index):
    """Return the student button locator matching the full name, or the fallback index."""
    buttons = page.get_by_role("button", name=STUDENT_BUTTON_PATTERN)
    count = await buttons.count()
    for bi in range(count):
        bname = await get_button_name(buttons.nth(bi))
        bfn, bln = parse_student_name(bname)
        if bfn.lower() == first_name.lower() and bln.lower() == last_name.lower():
            return buttons.nth(bi)
    if count > fallback_index:
        return buttons.nth(fallback_index)
    return None


def group_by_first_name(students):
    """Group students by first name, preserving order within each group."""
    groups = defaultdict(list)
    for s in students:
        groups[s["first_name"]].append(s)
    return list(groups.items())


async def process_group(page, first_name, student_group, worker_id):
    """Search for one first name and capture URLs for all matching students."""
    expected_count = len(student_group)
    print(f"  [w{worker_id}] Searching: {first_name} (expect {expected_count})")

    await ensure_search(page, first_name)
    buttons = page.get_by_role("button", name=STUDENT_BUTTON_PATTERN)
    result_count = await buttons.count()
    print(f"  [w{worker_id}]   Found {result_count} search result(s) for '{first_name}'")

    for k in range(min(result_count, expected_count)):
        btn_name = await get_button_name(buttons.nth(k))
        fn, ln = parse_student_name(btn_name)

        target_student = None
        for s in student_group:
            if s["last_name"].lower() == ln.lower() and not s["member_url"]:
                target_student = s
                break
        if target_student is None:
            target_student = student_group[k] if k < len(student_group) else student_group[0]

        target_btn = await find_button_for(page, fn, ln, k)
        if target_btn is None:
            print(f"  [w{worker_id}]   [{k+1}/{expected_count}] {fn} {ln} — no button, skipping")
            continue

        member_url, family_url = await capture_student_urls(page, target_btn)
        target_student["member_url"] = member_url
        target_student["family_url"] = family_url
        print(
            f"  [w{worker_id}]   [{k+1}/{expected_count}] {fn} {ln} "
            f"member -> {member_url} | family -> {family_url}"
        )

        buttons = page.get_by_role("button", name=STUDENT_BUTTON_PATTERN)


async def init_worker_page(page, worker_id):
    """One-time navigation to the students page for a worker. Uses
    `domcontentloaded` instead of `networkidle` because Operation36 keeps
    long-lived connections open and networkidle rarely resolves."""
    try:
        await page.goto(
            OPERATION36_STUDENTS_URL, wait_until="domcontentloaded", timeout=60000
        )
    except Exception as e:
        print(f"  [w{worker_id}] initial goto warning: {e}")
    await page.wait_for_timeout(3000)


async def worker(page, queue, worker_id):
    """Pull first-name groups from the queue and process until empty."""
    while True:
        try:
            first_name, student_group = queue.get_nowait()
        except asyncio.QueueEmpty:
            return
        try:
            await process_group(page, first_name, student_group, worker_id)
        except Exception as e:
            print(f"  [w{worker_id}] error on '{first_name}': {e}")


def export_players_csv(students):
    """Write students to CSV."""
    os.makedirs("output", exist_ok=True)
    with open(PLAYERS_OUTPUT, "w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["first_name", "last_name", "member_url", "family_url"]
        )
        writer.writeheader()
        writer.writerows(students)
    print(f"\nExported {len(students)} students to {PLAYERS_OUTPUT}")


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

        print("Collecting students...")
        students = await collect_students(login_page)
        for s in students:
            print(f"  {s['first_name']} {s['last_name']}")
        await login_page.close()

        groups = group_by_first_name(students)
        print(f"\nGrouped into {len(groups)} unique first names")

        worker_pages = [await context.new_page() for _ in range(NUM_WORKERS)]
        print(f"\nInitializing {NUM_WORKERS} worker pages...")
        await asyncio.gather(*[
            init_worker_page(worker_pages[i], i) for i in range(NUM_WORKERS)
        ])

        async def run_pass(pass_groups, label):
            q = asyncio.Queue()
            for g in pass_groups:
                q.put_nowait(g)
            total_students = sum(len(v) for _, v in pass_groups)
            print(
                f"\n{label}: {len(pass_groups)} first-name groups, "
                f"{total_students} students, {NUM_WORKERS} workers"
            )
            await asyncio.gather(*[
                worker(worker_pages[i], q, i)
                for i in range(NUM_WORKERS)
            ])

        await run_pass(groups, "Initial pass")

        for attempt in range(1, MAX_RETRIES + 1):
            unfilled = [
                s for s in students
                if not s["member_url"] or not s["family_url"]
            ]
            if not unfilled:
                break
            retry_groups = group_by_first_name(unfilled)
            await run_pass(retry_groups, f"Retry {attempt}/{MAX_RETRIES}")

        remaining = [
            s for s in students
            if not s["member_url"] or not s["family_url"]
        ]
        if remaining:
            print(
                f"\nWarning: {len(remaining)} student(s) still missing URLs "
                f"after {MAX_RETRIES} retries:"
            )
            for s in remaining:
                print(f"  - {s['first_name']} {s['last_name']}")

        for wp in worker_pages:
            await wp.close()

        export_players_csv(students)
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
