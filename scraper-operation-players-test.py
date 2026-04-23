"""
Operation 36 players (students) scraper — TEST run (first 10 only).
Logs into Operation 36, scrolls to load all students, extracts names,
keeps the first 10 after sorting by first name, then searches each student
to capture member URL (View Profile) and family URL (View Family Page).

Output: output/operation36_players_test.csv (first_name, last_name, member_url, family_url)

Usage:  python scraper-operation-players-test.py
"""

import os
import re
import csv
from collections import Counter
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
import config

load_dotenv()

OPERATION36_STUDENTS_URL = "https://operation36golf.com/members/students"
PLAYERS_OUTPUT = "output/operation36_players_test.csv"
STUDENT_BUTTON_PATTERN = re.compile(r"^student \d+ ")
TEST_LIMIT = 10


def login(page):
    """Log into Operation 36."""
    page.goto(config.OPERATION36_LOGIN_URL, wait_until="networkidle")
    page.fill(config.OPERATION36_LOGIN_EMAIL_SELECTOR, os.getenv("OPERATION36_EMAIL"))
    page.fill(config.OPERATION36_LOGIN_PASSWORD_SELECTOR, os.getenv("OPERATION36_PASSWORD"))
    page.get_by_role(
        config.OPERATION36_LOGIN_SUBMIT_ROLE[0],
        name=config.OPERATION36_LOGIN_SUBMIT_ROLE[1]["name"],
    ).click()
    page.wait_for_load_state("networkidle")


def get_button_name(btn):
    """Return the accessible name of a button (aria-label or text content)."""
    aria = btn.get_attribute("aria-label")
    if aria:
        return aria.strip()
    return (btn.text_content() or "").strip()


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


def scroll_to_load_all_students(page):
    """Scroll the page to load all student cards."""
    buttons = page.get_by_role("button", name=STUDENT_BUTTON_PATTERN)
    prev_count = 0
    stable_rounds = 0
    while True:
        count = buttons.count()
        if count > 0:
            buttons.nth(count - 1).scroll_into_view_if_needed()
        page.wait_for_timeout(3000)
        current_count = buttons.count()
        print(f"  Scrolling... {current_count} students loaded")
        if current_count == prev_count:
            stable_rounds += 1
            if stable_rounds >= 5:
                break
        else:
            stable_rounds = 0
        prev_count = current_count
    print(f"  Done scrolling. Total students: {current_count}")
    page.evaluate("window.scrollTo(0, 0)")
    page.wait_for_timeout(1000)


def navigate_to_students(page):
    """Navigate to students page and load all students."""
    page.goto(OPERATION36_STUDENTS_URL, wait_until="networkidle")
    page.wait_for_timeout(3000)
    scroll_to_load_all_students(page)


def collect_students(page):
    """Scroll to load all students, extract first and last name."""
    navigate_to_students(page)
    buttons = page.get_by_role("button", name=STUDENT_BUTTON_PATTERN)
    count = buttons.count()
    print(f"  Found {count} student buttons after scroll")
    students = []
    seen = set()
    unparsed = 0
    for i in range(count):
        btn_name = get_button_name(buttons.nth(i))
        if i < 3:
            print(f"  [debug] button {i} raw name: {btn_name!r}")
        first, last = parse_student_name(btn_name)
        if not first:
            unparsed += 1
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
    if unparsed:
        print(f"  [warn] {unparsed} buttons did not parse into a name")

    students.sort(key=lambda s: (s["first_name"].lower(), s["last_name"].lower()))
    print(f"Collected {len(students)} students")
    students = students[:TEST_LIMIT]
    print(f"TEST MODE: keeping first {len(students)} students")
    return students


def enter_search(page, query):
    """Fill the search box and wait for results."""
    search_box = page.get_by_role("textbox", name="Search for Students")
    search_box.fill("")
    page.wait_for_timeout(500)
    search_box.fill(query)
    page.wait_for_timeout(3000)


def ensure_search(page, query):
    """Make sure the students page is open and the search box contains `query`."""
    if OPERATION36_STUDENTS_URL not in page.url:
        page.goto(OPERATION36_STUDENTS_URL, wait_until="networkidle")
        page.wait_for_timeout(2000)
    search_box = page.get_by_role("textbox", name="Search for Students")
    if search_box.input_value() != query:
        enter_search(page, query)


def _absolutize(href):
    if not href:
        return ""
    if href.startswith("http"):
        return href
    return f"https://operation36golf.com{href}"


def extract_popup_url(page, label):
    """Read the href for a popup action (e.g., 'View Profile') without navigating."""
    for role in ("link", "button"):
        locator = page.get_by_role(role, name=label)
        if locator.count() == 0:
            continue
        first = locator.first
        href = first.get_attribute("href")
        if href:
            return _absolutize(href)
        href = first.evaluate(
            "el => el.closest('a')?.getAttribute('href') "
            "|| el.querySelector('a')?.getAttribute('href') || ''"
        )
        if href:
            return _absolutize(href)
    return ""


def close_popup(page):
    try:
        page.keyboard.press("Escape")
        page.wait_for_timeout(300)
    except Exception:
        pass


def capture_student_urls(page, student_button, debug=False):
    """Open popup once, read member and family hrefs in one shot, close popup.
    Falls back to click-and-navigate if href extraction fails."""
    student_button.click()
    try:
        page.get_by_role("button", name="View Profile").first.wait_for(
            state="visible", timeout=5000
        )
    except Exception:
        page.wait_for_timeout(1500)

    if debug:
        for link in page.locator("a").all()[:10]:
            try:
                print(
                    f"  [debug popup link] href={link.get_attribute('href')!r} "
                    f"text={(link.text_content() or '').strip()!r}"
                )
            except Exception:
                pass

    member_url = extract_popup_url(page, "View Profile")
    family_url = extract_popup_url(page, "View Family Page")

    if member_url and family_url:
        close_popup(page)
        return member_url, family_url

    # Fallback: click-and-navigate for whichever URL we couldn't read directly
    if not member_url:
        page.get_by_role("button", name="View Profile").click()
        page.wait_for_load_state("networkidle", timeout=60000)
        page.wait_for_timeout(1500)
        member_url = page.url
        page.go_back(timeout=60000)
        page.wait_for_load_state("networkidle", timeout=60000)
        page.wait_for_timeout(2000)
        if not family_url:
            student_button.click()
            page.wait_for_timeout(1500)

    if not family_url:
        page.get_by_role("button", name="View Family Page").click()
        page.wait_for_load_state("networkidle", timeout=60000)
        page.wait_for_timeout(1500)
        family_url = page.url
        page.go_back(timeout=60000)
        page.wait_for_load_state("networkidle", timeout=60000)
        page.wait_for_timeout(2000)
    else:
        close_popup(page)

    return member_url, family_url


def find_button_for(page, first_name, last_name, fallback_index):
    """Return the student button locator matching the full name, or the fallback index."""
    buttons = page.get_by_role("button", name=STUDENT_BUTTON_PATTERN)
    count = buttons.count()
    for bi in range(count):
        bname = get_button_name(buttons.nth(bi))
        bfn, bln = parse_student_name(bname)
        if bfn.lower() == first_name.lower() and bln.lower() == last_name.lower():
            return buttons.nth(bi)
    if count > fallback_index:
        return buttons.nth(fallback_index)
    return None


def search_and_capture_urls(page, students):
    """For each unique first name, search and capture member/family URLs for all matches."""
    name_counts = Counter(s["first_name"] for s in students)
    processed_names = set()

    for i, stu in enumerate(students):
        first_name = stu["first_name"]
        if first_name in processed_names:
            continue

        expected_count = name_counts[first_name]
        matching_indices = [j for j, s in enumerate(students) if s["first_name"] == first_name]
        print(f"\n  [{i+1}/{len(students)}] Searching: {first_name} (expect {expected_count} result(s))")

        ensure_search(page, first_name)
        buttons = page.get_by_role("button", name=STUDENT_BUTTON_PATTERN)
        result_count = buttons.count()
        print(f"    Found {result_count} search result(s)")

        for k in range(min(result_count, expected_count)):
            btn_name = get_button_name(buttons.nth(k))
            fn, ln = parse_student_name(btn_name)

            idx = None
            for mi in matching_indices:
                if (students[mi]["last_name"].lower() == ln.lower()
                        and not students[mi]["member_url"]):
                    idx = mi
                    break
            if idx is None:
                idx = matching_indices[k] if k < len(matching_indices) else matching_indices[0]

            target = find_button_for(page, fn, ln, k)
            if target is None:
                print(f"    [{k+1}/{expected_count}] {fn} {ln} — no button found, skipping")
                continue

            debug = (i == 0 and k == 0)
            member_url, family_url = capture_student_urls(page, target, debug=debug)
            students[idx]["member_url"] = member_url
            students[idx]["family_url"] = family_url
            print(
                f"    [{k+1}/{expected_count}] {fn} {ln} "
                f"member -> {member_url} | family -> {family_url}"
            )

            buttons = page.get_by_role("button", name=STUDENT_BUTTON_PATTERN)

        processed_names.add(first_name)

    return students


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


def main():
    email = os.getenv("OPERATION36_EMAIL")
    password = os.getenv("OPERATION36_PASSWORD")
    if not email or not password:
        print("Error: Set OPERATION36_EMAIL and OPERATION36_PASSWORD in .env")
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(viewport={"width": 1280, "height": 900})
        page = context.new_page()

        login(page)
        page.wait_for_timeout(2000)

        print("Collecting students...")
        students = collect_students(page)
        for s in students:
            print(f"  {s['first_name']} {s['last_name']}")

        print(f"\nSearching students to capture URLs...")
        students = search_and_capture_urls(page, students)

        export_players_csv(students)
        browser.close()


if __name__ == "__main__":
    main()
