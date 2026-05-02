"""
CoachNow contacts scraper.
Logs in, navigates to /contacts, scrolls to load all contacts,
extracts contact info, writes coachnow_players.csv, then iterates
each contact to capture lesson history.

Output:
  - output/coachnow_players.csv (first_name, last_name, email, phone)
  - output/lesson_history.csv (first_name, last_name, lesson_name, date)

Usage:  python scraper.py
"""

import os
import re
import csv
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
import config

load_dotenv()

PLAYERS_OUTPUT = "output/coachnow_players.csv"
LESSONS_OUTPUT = "output/lesson_history.csv"


def login(page):
    """Log into CoachNow."""
    page.goto(config.LOGIN_URL, wait_until="networkidle")
    page.get_by_role(
        config.LOGIN_EMAIL_ROLE[0], name=config.LOGIN_EMAIL_ROLE[1]["name"]
    ).fill(os.getenv("COACHNOW_EMAIL"))
    page.get_by_role(
        config.LOGIN_PASSWORD_ROLE[0], name=config.LOGIN_PASSWORD_ROLE[1]["name"]
    ).fill(os.getenv("COACHNOW_PASSWORD"))
    page.get_by_role(
        config.LOGIN_SUBMIT_ROLE[0],
        name=config.LOGIN_SUBMIT_ROLE[1]["name"],
        exact=config.LOGIN_SUBMIT_ROLE[1]["exact"],
    ).click()
    page.wait_for_load_state("networkidle")


def scroll_to_load_all_contacts(page):
    """Scroll the page to load all contact cards.
    Scrolls the last View button into view — works whether the list lives
    on the window or inside an inner scroll container."""
    buttons = page.get_by_role("button", name="View")
    prev_count = 0
    stable_rounds = 0
    while True:
        count = buttons.count()
        if count > 0:
            try:
                buttons.nth(count - 1).scroll_into_view_if_needed()
            except Exception:
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(3000)
        current_count = buttons.count()
        print(f"  Scrolling... {current_count} contacts loaded")
        if current_count == prev_count:
            stable_rounds += 1
            if stable_rounds >= 5:
                break
        else:
            stable_rounds = 0
        prev_count = current_count
    print(f"  Done scrolling. Total contacts: {current_count}")
    page.evaluate("window.scrollTo(0, 0)")
    page.wait_for_timeout(1000)


def navigate_to_contacts(page):
    """Navigate to contacts page and load all contacts."""
    page.goto(config.CONTACTS_URL, wait_until="networkidle")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(5000)
    try:
        page.get_by_role("button", name="View").first.wait_for(timeout=15000)
    except Exception:
        print("No contacts found on page.")
        return False
    print("Loading all contacts...")
    scroll_to_load_all_contacts(page)
    return True


def collect_contacts(page):
    """Scroll to load all contacts, extract first_name, last_name, email, phone."""
    if not navigate_to_contacts(page):
        return []

    num_contacts = page.get_by_role("button", name="View").count()
    print(f"Total contacts found: {num_contacts}")

    email_links = page.get_by_role("link").filter(has_text=re.compile(r"@"))
    phone_links = page.get_by_role("link").filter(has_text=re.compile(r"^\+"))

    emails = [
        email_links.nth(i).text_content().strip()
        for i in range(email_links.count())
    ]
    phones = [
        phone_links.nth(i).text_content().strip()
        for i in range(phone_links.count())
    ]

    body_text = page.inner_text("body")
    contacts_section = (
        body_text.split("Add Contacts")[-1]
        if "Add Contacts" in body_text
        else body_text
    )

    contact_blocks = re.split(r"\nView\n", contacts_section)
    contact_blocks = [b.strip() for b in contact_blocks if b.strip()]
    contact_blocks = [
        b for b in contact_blocks
        if "No Email" in b or "@" in b or "No Phone" in b
    ]

    print(f"Parsed {len(contact_blocks)} contact blocks")

    contacts = []
    seen = set()
    email_idx = 0
    phone_idx = 0

    for block in contact_blocks:
        lines = [l.strip() for l in block.split("\n") if l.strip()]
        if not lines:
            continue

        name = ""
        for line in lines:
            if re.match(r"^[A-Z]{1,3}$", line):
                continue
            if line in ("No Email", "No Phone") or "@" in line or line.startswith("+"):
                continue
            name = line
            break

        parts = name.split(" ", 1)
        first_name = parts[0] if parts else ""
        last_name = parts[1] if len(parts) > 1 else ""
        if not first_name:
            continue

        key = (first_name.lower(), last_name.lower())
        if key in seen:
            continue
        seen.add(key)

        email = ""
        if "No Email" not in block and email_idx < len(emails):
            email = emails[email_idx]
            email_idx += 1

        phone = ""
        if "No Phone" not in block and phone_idx < len(phones):
            phone = phones[phone_idx]
            phone_idx += 1

        contacts.append({
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "phone": phone,
        })
        print(f"  Found: {first_name} {last_name} | {email} | {phone}")

    print(f"Collected {len(contacts)} contacts")
    return contacts


def get_lesson_histories(page, contacts):
    """For each contact, click View, then History, extract individual lessons."""
    all_lessons = []

    for i, contact in enumerate(contacts):
        name = f"{contact['first_name']} {contact['last_name']}".strip()
        print(f"  [{i+1}/{len(contacts)}] Getting lessons for: {name}...")

        view_button = page.get_by_role("button", name="View").nth(i)
        view_button.scroll_into_view_if_needed()
        view_button.click()
        page.wait_for_timeout(1500)

        try:
            page.get_by_role("button", name="History").wait_for(timeout=5000)
        except Exception:
            print(f"    No lesson panel for {name}")
            try:
                page.get_by_role("img", name="close").click()
                page.wait_for_timeout(500)
            except Exception:
                page.keyboard.press("Escape")
                page.wait_for_timeout(500)
            continue

        page.get_by_role("button", name="History").click()
        page.wait_for_timeout(1500)

        lesson_count_before = len(all_lessons)

        try:
            close_btn = page.get_by_role("img", name="close")
            modal = close_btn.locator(
                "xpath=ancestor::div[contains(@class, 'modal') or contains(@class, 'popup') or contains(@class, 'dialog') or position()=1]"
            ).first

            modal_text = ""
            try:
                modal_text = modal.text_content()
            except Exception:
                pass

            if not modal_text:
                modal_text = close_btn.locator("xpath=../..").text_content()

            valid_months = "JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC"
            matches = re.findall(
                rf"({valid_months})(\d{{1,2}})((?:(?!(?:{valid_months})\d).)+)",
                modal_text,
            )

            for month, day, lesson_name in matches:
                lesson_name = lesson_name.strip()
                if lesson_name and lesson_name not in (
                    "Upcoming", "History", "View", "Edit", "Delete", "",
                ):
                    all_lessons.append({
                        "first_name": contact["first_name"],
                        "last_name": contact["last_name"],
                        "lesson_name": lesson_name,
                        "date": f"{month} {day}",
                    })

        except Exception as e:
            print(f"    Error extracting lessons: {e}")

        lessons_found = len(all_lessons) - lesson_count_before
        print(f"    Found {lessons_found} lessons")

        try:
            page.get_by_role("img", name="close").click()
            page.wait_for_timeout(500)
        except Exception:
            page.keyboard.press("Escape")
            page.wait_for_timeout(500)

    return all_lessons


def export_players_csv(contacts):
    """Write contacts to coachnow_players.csv."""
    os.makedirs("output", exist_ok=True)
    with open(PLAYERS_OUTPUT, "w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["first_name", "last_name", "email", "phone"]
        )
        writer.writeheader()
        writer.writerows(contacts)
    print(f"Exported {len(contacts)} contacts to {PLAYERS_OUTPUT}")


def export_lessons_csv(lessons):
    """Write lesson history to lesson_history.csv."""
    os.makedirs("output", exist_ok=True)
    with open(LESSONS_OUTPUT, "w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["first_name", "last_name", "lesson_name", "date"]
        )
        writer.writeheader()
        writer.writerows(lessons)
    print(f"Exported {len(lessons)} lessons to {LESSONS_OUTPUT}")


def main():
    email = os.getenv("COACHNOW_EMAIL")
    password = os.getenv("COACHNOW_PASSWORD")
    if not email or not password:
        print("Error: Set COACHNOW_EMAIL and COACHNOW_PASSWORD in .env")
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(viewport={"width": 1280, "height": 900})
        page = context.new_page()

        login(page)
        page.wait_for_timeout(3000)

        print("Collecting contacts...")
        contacts = collect_contacts(page)
        export_players_csv(contacts)

        lessons = []
        if contacts:
            print("\nGetting lesson histories...")
            lessons = get_lesson_histories(page, contacts)

        export_lessons_csv(lessons)
        browser.close()


if __name__ == "__main__":
    main()
