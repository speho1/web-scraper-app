"""
CoachNow contact scraper.
Logs in, navigates to /contacts, scrolls to load all contacts,
extracts contact info and lesson history, and exports to two CSVs:
  - output/contacts.csv: firstnamelastname, email, first_name, last_name, phone
  - output/lesson_history.csv: firstnamelastname, lesson_name, date

Usage:  python scraper.py
"""

import os
import re
import csv
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
import config

load_dotenv()


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


def scroll_to_load_all(page):
    """Scroll down repeatedly until all contacts are loaded."""
    prev_count = 0
    stable_rounds = 0
    while True:
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(3000)
        current_count = page.get_by_role("button", name="View").count()
        print(f"  Scrolling... {current_count} contacts loaded")
        if current_count == prev_count:
            stable_rounds += 1
            # Wait for 3 stable rounds to be sure everything is loaded
            if stable_rounds >= 3:
                break
        else:
            stable_rounds = 0
        prev_count = current_count
    # Scroll back to top
    page.evaluate("window.scrollTo(0, 0)")
    page.wait_for_timeout(1000)


def scrape_contacts(page):
    """Scrape all contacts from the contacts page."""
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(5000)

    all_contacts = []

    # Wait for View buttons to appear (one per contact)
    try:
        page.get_by_role("button", name="View").first.wait_for(timeout=15000)
    except Exception:
        print("No contacts found on page.")
        return all_contacts

    # Scroll to load all contacts
    print("Loading all contacts...")
    scroll_to_load_all(page)

    num_contacts = page.get_by_role("button", name="View").count()
    print(f"Total contacts found: {num_contacts}")

    # Get all email links and phone links
    email_links = page.get_by_role("link").filter(has_text=re.compile(r"@"))
    phone_links = page.get_by_role("link").filter(has_text=re.compile(r"^\+"))

    emails = []
    for i in range(email_links.count()):
        emails.append(email_links.nth(i).text_content().strip())

    phones = []
    for i in range(phone_links.count()):
        phones.append(phone_links.nth(i).text_content().strip())

    # Parse contact names from visible text
    body_text = page.inner_text("body")
    contacts_section = body_text.split("Add Contacts")[-1] if "Add Contacts" in body_text else body_text

    # Split by "View" to get each contact's text block
    # Use regex to split on standalone "View" lines
    contact_blocks = re.split(r"\nView\n", contacts_section)
    contact_blocks = [b.strip() for b in contact_blocks if b.strip()]

    # The last block may contain non-contact footer text — only keep blocks
    # that contain email-like text or "No Email"
    contact_blocks = [b for b in contact_blocks if "No Email" in b or "@" in b or "No Phone" in b]

    print(f"Parsed {len(contact_blocks)} contact blocks")

    email_idx = 0
    phone_idx = 0

    for block in contact_blocks:
        lines = [l.strip() for l in block.split("\n") if l.strip()]
        if not lines:
            continue

        # First meaningful line is the name (skip initials like "AS", "AF")
        # Initials are 1-3 uppercase-only chars with no spaces
        name = ""
        for line in lines:
            # Skip lines that are just uppercase initials (1-3 chars, all caps)
            if re.match(r"^[A-Z]{1,3}$", line):
                continue
            # Skip known non-name lines
            if line in ("No Email", "No Phone") or "@" in line or line.startswith("+"):
                continue
            name = line
            break

        parts = name.split(" ", 1)
        first_name = parts[0] if parts else ""
        last_name = parts[1] if len(parts) > 1 else ""

        # Build unique identifier
        firstnamelastname = f"{first_name}{last_name}".lower().replace(" ", "")

        # Check for email
        has_email = "No Email" not in block
        email = ""
        if has_email and email_idx < len(emails):
            email = emails[email_idx]
            email_idx += 1

        # Check for phone
        has_phone = "No Phone" not in block
        phone = ""
        if has_phone and phone_idx < len(phones):
            phone = phones[phone_idx]
            phone_idx += 1

        all_contacts.append({
            "firstnamelastname": firstnamelastname,
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "phone": phone,
        })

        print(f"  Found: {first_name} {last_name} | {email} | {phone}")

    return all_contacts


def get_lesson_histories(page, contacts):
    """For each contact, click View, then History, extract individual lessons."""
    all_lessons = []

    for i, contact in enumerate(contacts):
        name = f"{contact['first_name']} {contact['last_name']}".strip()
        firstnamelastname = contact["firstnamelastname"]
        print(f"  [{i+1}/{len(contacts)}] Getting lessons for: {name}...")

        # Scroll the View button into view and click it
        view_button = page.get_by_role("button", name="View").nth(i)
        view_button.scroll_into_view_if_needed()
        view_button.click()
        page.wait_for_timeout(1500)

        # Check if History button exists
        try:
            page.get_by_role("button", name="History").wait_for(timeout=5000)
        except Exception:
            print(f"    No lesson panel for {name}")
            # Close whatever opened
            try:
                page.get_by_role("img", name="close").click()
                page.wait_for_timeout(500)
            except Exception:
                page.keyboard.press("Escape")
                page.wait_for_timeout(500)
            continue

        page.get_by_role("button", name="History").click()
        page.wait_for_timeout(1500)

        # Capture the text content of the history panel BEFORE looking for lessons
        # Take a snapshot of what's visible in the popover/modal
        # Only look at text that appeared AFTER clicking History
        lesson_count_before = len(all_lessons)

        try:
            # Get the close button's parent container — that's the modal/popover
            close_btn = page.get_by_role("img", name="close")
            # Navigate up to find the modal container
            modal = close_btn.locator("xpath=ancestor::div[contains(@class, 'modal') or contains(@class, 'popup') or contains(@class, 'dialog') or position()=1]").first

            # Try to get text from the modal area
            # If we can't find a specific modal, get all visible text near the History button
            modal_text = ""
            try:
                modal_text = modal.text_content()
            except Exception:
                pass

            if not modal_text:
                # Fallback: get text from the area around the History button
                modal_text = close_btn.locator("xpath=../..").text_content()

            # Parse lesson entries from modal text
            # Look for patterns like "MAR19Adult Member Private" or "MAR 19 Adult Member Private"
            # Only match valid month abbreviations to avoid false positives
            valid_months = "JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC"
            matches = re.findall(rf"({valid_months})(\d{{1,2}})((?:(?!(?:{valid_months})\d).)+)", modal_text)

            for month, day, lesson_name in matches:
                lesson_name = lesson_name.strip()
                # Filter out non-lesson text (like "Upcoming", "History", button labels)
                if lesson_name and lesson_name not in ("Upcoming", "History", "View", "Edit", "Delete", ""):
                    all_lessons.append({
                        "firstnamelastname": firstnamelastname,
                        "lesson_name": lesson_name,
                        "date": f"{month} {day}",
                    })

        except Exception as e:
            print(f"    Error extracting lessons: {e}")

        lessons_found = len(all_lessons) - lesson_count_before
        print(f"    Found {lessons_found} lessons")

        # Close the popover/modal
        try:
            page.get_by_role("img", name="close").click()
            page.wait_for_timeout(500)
        except Exception:
            page.keyboard.press("Escape")
            page.wait_for_timeout(500)

    return all_lessons


def export_csvs(contacts, lessons):
    """Write contacts and lesson history to separate CSVs."""
    os.makedirs("output", exist_ok=True)

    with open("output/contacts.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "firstnamelastname", "email", "first_name", "last_name", "phone"
        ])
        writer.writeheader()
        writer.writerows(contacts)
    print(f"Exported {len(contacts)} contacts to output/contacts.csv")

    with open("output/lesson_history.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "firstnamelastname", "lesson_name", "date"
        ])
        writer.writeheader()
        writer.writerows(lessons)
    print(f"Exported {len(lessons)} lessons to output/lesson_history.csv")


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
        page.goto(config.CONTACTS_URL, wait_until="networkidle")

        print("Scraping contacts...")
        contacts = scrape_contacts(page)

        lessons = []
        if contacts:
            print(f"\nGetting lesson histories...")
            lessons = get_lesson_histories(page, contacts)

        export_csvs(contacts, lessons)
        browser.close()


if __name__ == "__main__":
    main()
