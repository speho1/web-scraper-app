"""
Operation 36 historical programs scraper.
Logs into Operation 36, navigates to the programs page, switches to the
"Completed" tab, scrolls to load all completed program cards, and extracts
each program's ID. For each program, visits /programs/{id}/overview to
capture program name, start date, end date, and weekday, then visits
/programs/{id}/roster to capture enrolled students.

Collection of program IDs is sequential; per-program metadata + student
capture is parallelized across NUM_WORKERS workers pulling from a shared
queue.

Outputs:
  output/operation36_historical_programs.csv          (programId, programName,
                                                       startDate, endDate, weekday)
  output/operation36_historical_enrolled_students.csv (studentId, studentName,
                                                       programId)

Usage:  python scraper-operation36-historical-programs.py
"""

import os
import re
import csv
import asyncio
from dotenv import load_dotenv
from playwright.async_api import async_playwright
import config

load_dotenv()

OPERATION36_PROGRAMS_URL = "https://operation36golf.com/programs"
PROGRAMS_OUTPUT = "output/operation36_historical_programs.csv"
ENROLLED_STUDENTS_OUTPUT = "output/operation36_historical_enrolled_students.csv"

PROGRAM_HREF_PATTERN = re.compile(r"/programs/(\d+)")
STUDENT_ROW_PATTERN = re.compile(r"^student \d+ ", re.IGNORECASE)
COMPLETED_TAB_PATTERN = re.compile(r"^Completed\b", re.IGNORECASE)

# "Mar 14, 2026 - Jun 6, 2026" or "Mar 14, 2026 - Jun 6" (no trailing year)
DATE_RANGE_PATTERN = re.compile(
    r"([A-Z][a-z]{2,9}\s+\d{1,2},\s*\d{4})"
    r"\s*[-–]\s*"
    r"([A-Z][a-z]{2,9}\s+\d{1,2}(?:,\s*\d{4})?)"
)
WEEKDAY_PATTERN = re.compile(
    r"\b(Mondays?|Tuesdays?|Wednesdays?|Thursdays?|Fridays?|Saturdays?|Sundays?)\b",
    re.IGNORECASE,
)

NUM_WORKERS = 4


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


async def click_completed_tab(page):
    """Switch the programs page to the Completed tab. The tab label includes a
    count (e.g. 'Completed (26)') that changes over time, so match by prefix."""
    tab = page.get_by_role("tab", name=COMPLETED_TAB_PATTERN).first
    await tab.wait_for(state="visible", timeout=30000)
    label = (await tab.text_content() or "").strip()
    print(f"  Clicking tab: {label}")
    await tab.click()
    await page.wait_for_timeout(3000)


async def scroll_to_load_all_programs(page):
    """Scroll the page to load all program cards."""
    links = page.locator("a[href*='/programs/']")
    prev_count = 0
    stable_rounds = 0
    while True:
        count = await links.count()
        if count > 0:
            await links.nth(count - 1).scroll_into_view_if_needed()
        await page.wait_for_timeout(3000)
        current_count = await links.count()
        print(f"  Scrolling... {current_count} program links loaded")
        if current_count == prev_count:
            stable_rounds += 1
            if stable_rounds >= 5:
                break
        else:
            stable_rounds = 0
        prev_count = current_count
    print(f"  Done scrolling. Total program links: {current_count}")
    await page.evaluate("window.scrollTo(0, 0)")
    await page.wait_for_timeout(1000)


async def navigate_to_programs(page):
    """Navigate to the programs page, switch to the Completed tab, and load
    all program cards."""
    await page.goto(OPERATION36_PROGRAMS_URL, wait_until="networkidle")
    await page.wait_for_timeout(3000)
    await click_completed_tab(page)
    await scroll_to_load_all_programs(page)


async def collect_program_ids(page):
    """Walk the program listing and return unique program IDs.
    Names on the listing page are unreliable, so only the ID is captured here.
    The name is read later from /programs/{id}/overview."""
    await navigate_to_programs(page)
    links = page.locator("a[href*='/programs/']")
    count = await links.count()
    program_ids = []
    seen = set()
    for i in range(count):
        href = await links.nth(i).get_attribute("href")
        if not href:
            continue
        match = PROGRAM_HREF_PATTERN.search(href)
        if not match:
            continue
        program_id = match.group(1)
        if program_id in seen:
            continue
        seen.add(program_id)
        program_ids.append(program_id)
    print(f"Collected {len(program_ids)} program IDs")
    return program_ids


async def get_program_name(page):
    """Read the program name from the page heading. Falls back to <h1>."""
    h1 = page.locator("h1").first
    if await h1.count() > 0:
        text = (await h1.text_content() or "").strip()
        if text:
            return text
    headings = page.get_by_role("heading")
    h_count = await headings.count()
    for i in range(h_count):
        text = (await headings.nth(i).text_content() or "").strip()
        if text:
            return text
    return ""


async def get_program_schedule_text(page):
    """Find the on-page text node that contains the program date range and
    return its full text (the same node typically also contains the weekday)."""
    locator = page.get_by_text(DATE_RANGE_PATTERN).first
    if await locator.count() == 0:
        return ""
    try:
        return (await locator.text_content() or "").strip()
    except Exception:
        return ""


def parse_schedule(schedule_text, body_text):
    """Pull (start_date, end_date, weekday) out of the schedule text. Falls
    back to scanning the full page body if the schedule node is missing."""
    start_date = ""
    end_date = ""
    weekday = ""

    sources = [s for s in (schedule_text, body_text) if s]
    for src in sources:
        m = DATE_RANGE_PATTERN.search(src)
        if m:
            start_date = m.group(1).strip()
            end_date = m.group(2).strip()
            break

    if schedule_text:
        wm = WEEKDAY_PATTERN.search(schedule_text)
        if wm:
            weekday = wm.group(1)
    if not weekday and body_text:
        wm = WEEKDAY_PATTERN.search(body_text)
        if wm:
            weekday = wm.group(1)

    return start_date, end_date, weekday


async def collect_program_metadata(page, program_id, worker_id):
    """Visit /programs/{id}/overview and capture name, start date, end date,
    and weekday."""
    overview_url = f"https://operation36golf.com/programs/{program_id}/overview"
    try:
        await page.goto(overview_url, wait_until="domcontentloaded", timeout=60000)
    except Exception as e:
        print(f"  [w{worker_id}]   overview goto warning: {e}")
    await page.wait_for_timeout(3000)

    name = await get_program_name(page)
    schedule_text = await get_program_schedule_text(page)
    body_text = ""
    if not schedule_text or not WEEKDAY_PATTERN.search(schedule_text):
        try:
            body_text = await page.inner_text("body")
        except Exception:
            body_text = ""

    start_date, end_date, weekday = parse_schedule(schedule_text, body_text)
    print(
        f"  [w{worker_id}]   meta: name='{name}' "
        f"dates={start_date} - {end_date} weekday={weekday}"
    )
    return {
        "programId": program_id,
        "programName": name,
        "startDate": start_date,
        "endDate": end_date,
        "weekday": weekday,
    }


def absolutize(href):
    if not href:
        return ""
    if href.startswith("http"):
        return href
    return f"https://operation36golf.com{href}"


def parse_student_name(row_text):
    """Extract a student's name from the row's accessible name/text. Handles
    'student 2 Anderson Beck Age 12 ...' (accessible-name form) and the
    run-together text variant 'student 2Anderson BeckAge 12...' by stripping
    the leading 'student N' marker, the leading level digit, and trimming at
    'Age'."""
    if not row_text:
        return ""
    s = row_text.strip()
    s = re.sub(r"^student\s*\d+\s*", "", s, flags=re.IGNORECASE)
    s = re.split(r"\s*Age\b", s, maxsplit=1, flags=re.IGNORECASE)[0]
    s = re.sub(r"^\d+\s*", "", s)
    return s.strip().rstrip("/").strip()


async def extract_visible_students(page, program_id, students_by_key):
    """Read every currently rendered student row and merge into the
    accumulator keyed by studentId (falling back to name)."""
    rows = page.get_by_role("row", name=STUDENT_ROW_PATTERN)
    count = await rows.count()
    for i in range(count):
        row = rows.nth(i)
        try:
            row_text = (await row.text_content() or "").strip()
        except Exception:
            continue
        student_name = parse_student_name(row_text)

        student_url = ""
        try:
            link = row.get_by_role("link").first
            if await link.count() > 0:
                href = await link.get_attribute("href")
                student_url = absolutize(href)
        except Exception:
            pass

        student_id = ""
        if student_url:
            id_match = re.search(r"/(\d+)(?:[/?#]|$)", student_url)
            if id_match:
                student_id = id_match.group(1)

        key = student_id or student_name
        if not key:
            continue
        if key not in students_by_key:
            students_by_key[key] = {
                "studentId": student_id,
                "studentName": student_name,
                "programId": program_id,
            }
    return count


async def collect_program_students(page, program_id, worker_id):
    """Visit /programs/{id}/roster and capture enrolled students. The roster
    is a virtualized list that unmounts off-screen rows, so collect rows as
    we scroll instead of scrolling first and reading at the end."""
    roster_url = f"https://operation36golf.com/programs/{program_id}/roster"
    try:
        await page.goto(roster_url, wait_until="domcontentloaded", timeout=60000)
    except Exception as e:
        print(f"  [w{worker_id}]   roster goto warning: {e}")
    await page.wait_for_timeout(3000)

    students_by_key = {}
    prev_total = 0
    stable_rounds = 0
    while True:
        visible = await extract_visible_students(page, program_id, students_by_key)
        rows = page.get_by_role("row", name=STUDENT_ROW_PATTERN)
        if visible > 0:
            try:
                await rows.nth(visible - 1).scroll_into_view_if_needed()
            except Exception:
                pass
        await page.wait_for_timeout(2000)

        total = len(students_by_key)
        print(
            f"  [w{worker_id}]   Roster scroll: {visible} visible / {total} unique"
        )
        if total == prev_total:
            stable_rounds += 1
            if stable_rounds >= 3:
                break
        else:
            stable_rounds = 0
        prev_total = total

    # One final pass after the list has settled, in case the last scroll
    # revealed rows that weren't yet rendered when we sampled.
    await extract_visible_students(page, program_id, students_by_key)

    students = list(students_by_key.values())
    print(f"  [w{worker_id}]   Collected {len(students)} unique student(s)")
    for s in students:
        print(f"  [w{worker_id}]     {s['studentName']} ({s['studentId']})")
    return students


async def init_worker_page(page, worker_id):
    """One-time navigation for a worker. Uses `domcontentloaded` because
    Operation 36 keeps long-lived connections open and `networkidle` rarely
    resolves."""
    try:
        await page.goto(
            OPERATION36_PROGRAMS_URL, wait_until="domcontentloaded", timeout=60000
        )
    except Exception as e:
        print(f"  [w{worker_id}] initial goto warning: {e}")
    await page.wait_for_timeout(2000)


async def worker(page, queue, programs, students, worker_id):
    """Pull program IDs from the queue, capture metadata + students."""
    while True:
        try:
            program_id = queue.get_nowait()
        except asyncio.QueueEmpty:
            return
        print(f"  [w{worker_id}] Program {program_id}")
        try:
            meta = await collect_program_metadata(page, program_id, worker_id)
            programs.append(meta)
        except Exception as e:
            print(f"  [w{worker_id}] metadata error on program {program_id}: {e}")
            programs.append({
                "programId": program_id,
                "programName": "",
                "startDate": "",
                "endDate": "",
                "weekday": "",
            })
        try:
            students.extend(
                await collect_program_students(page, program_id, worker_id)
            )
        except Exception as e:
            print(f"  [w{worker_id}] students error on program {program_id}: {e}")


def export_programs_csv(programs):
    """Write programs to CSV (programId, programName, startDate, endDate, weekday)."""
    os.makedirs("output", exist_ok=True)
    with open(PROGRAMS_OUTPUT, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["programId", "programName", "startDate", "endDate", "weekday"],
        )
        writer.writeheader()
        for p in programs:
            writer.writerow({
                "programId": p["programId"],
                "programName": p.get("programName", ""),
                "startDate": p.get("startDate", ""),
                "endDate": p.get("endDate", ""),
                "weekday": p.get("weekday", ""),
            })
    print(f"\nExported {len(programs)} programs to {PROGRAMS_OUTPUT}")


def export_enrolled_students_csv(students):
    """Write enrolled students to CSV (studentId, studentName, programId)."""
    os.makedirs("output", exist_ok=True)
    with open(ENROLLED_STUDENTS_OUTPUT, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["studentId", "studentName", "programId"])
        writer.writeheader()
        for s in students:
            writer.writerow({
                "studentId": s["studentId"],
                "studentName": s["studentName"],
                "programId": s["programId"],
            })
    print(f"Exported {len(students)} enrolled students to {ENROLLED_STUDENTS_OUTPUT}")


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

        print("Collecting completed program IDs...")
        program_ids = await collect_program_ids(login_page)
        for pid in program_ids:
            print(f"  {pid}")
        await login_page.close()

        if not program_ids:
            print("No programs found; exiting.")
            await browser.close()
            return

        worker_pages = [await context.new_page() for _ in range(NUM_WORKERS)]
        print(f"\nInitializing {NUM_WORKERS} worker pages...")
        await asyncio.gather(*[
            init_worker_page(worker_pages[i], i) for i in range(NUM_WORKERS)
        ])

        queue = asyncio.Queue()
        for pid in program_ids:
            queue.put_nowait(pid)

        programs = []
        students = []
        print(
            f"\nProcessing {len(program_ids)} programs across {NUM_WORKERS} workers..."
        )
        await asyncio.gather(*[
            worker(worker_pages[i], queue, programs, students, i)
            for i in range(NUM_WORKERS)
        ])

        for wp in worker_pages:
            await wp.close()

        programs.sort(key=lambda p: p["programName"].lower())
        students.sort(key=lambda s: (s["programId"], s["studentName"].lower()))

        export_programs_csv(programs)
        export_enrolled_students_csv(students)
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
