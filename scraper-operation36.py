"""
Operation 36 enrollments scraper.
Logs into Operation 36, iterates through all family cards,
clicks View Details for each, clicks Enrollments tab, and extracts enrollment data.

Output: output/operation36_enrollments.csv

Usage:  python scraper-operation36.py
"""

import os
import re
import csv
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
import config

load_dotenv()


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


def scroll_to_load_all_families(page):
    """Scroll the infinite scroll container to load all family rows."""
    container = page.locator(".infinite-scroll-component").first
    prev_count = 0
    stable_rounds = 0
    while True:
        # Scroll down incrementally to trigger infinite scroll loading
        container.evaluate("el => el.scrollBy(0, 1000)")
        page.wait_for_timeout(1000)
        # Also jump to bottom to ensure we trigger the load threshold
        container.evaluate("el => el.scrollTop = el.scrollHeight")
        page.wait_for_timeout(3000)
        current_count = page.locator(".FamiliesTable_row__AQCjl").count()
        print(f"  Scrolling... {current_count} families loaded")
        if current_count == prev_count:
            stable_rounds += 1
            if stable_rounds >= 5:
                break
        else:
            stable_rounds = 0
        prev_count = current_count
    print(f"  Done scrolling. Total families: {current_count}")
    # Scroll back to top
    container.evaluate("el => el.scrollTop = 0")
    page.wait_for_timeout(1000)


def get_family_info(page):
    """Get all family names from the table rows."""
    rows = page.locator(".FamiliesTable_row__AQCjl")
    count = rows.count()
    families = []
    for i in range(count):
        row = rows.nth(i)
        name_el = row.locator("h5")
        name = name_el.text_content().strip() if name_el.count() > 0 else f"Family_{i}"
        families.append(name)
    return families


def navigate_to_families(page):
    """Navigate to families page and load all families."""
    page.goto(config.OPERATION36_FAMILIES_URL, wait_until="networkidle")
    page.wait_for_timeout(3000)

    # Wait for table content
    page.locator("#tableContent").wait_for(timeout=15000)
    page.wait_for_timeout(2000)

    # Scroll to load all
    scroll_to_load_all_families(page)


def click_view_details(page, family_index):
    """Click View Details for a specific family row and return the URL."""
    rows = page.locator(".FamiliesTable_row__AQCjl")
    row = rows.nth(family_index)
    row.scroll_into_view_if_needed()
    page.wait_for_timeout(500)

    # Click the "View Details" button inside this row
    view_btn = row.locator("button.btn-primary")
    view_btn.click()
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(2000)
    return page.url


def get_enrollments(page, family_name):
    """Click Enrollments tab and extract enrollment data."""
    enrollments = []

    try:
        page.get_by_role("button", name="Enrollments").wait_for(timeout=5000)
        page.get_by_role("button", name="Enrollments").click()
        page.wait_for_timeout(2000)
    except Exception as e:
        print(f"    No Enrollments button: {e}")
        return enrollments

    # Dump visible text to parse enrollments
    body_text = page.inner_text("body")

    # Find the enrollments section after the header row
    if "Payment Status\nStatus" in body_text:
        enroll_section = body_text.split("Payment Status\nStatus")[-1]
    elif "Enrollments" in body_text:
        parts = body_text.split("Enrollments")
        enroll_section = parts[-1] if len(parts) > 1 else body_text
    else:
        enroll_section = body_text

    # Parse enrollment entries
    # Format: number\n\nstudent_name\nprogram_name\npackage_and_amount\npayment_status\nstatus
    lines = [l.strip() for l in enroll_section.strip().split("\n")]

    i = 0
    while i < len(lines):
        # Skip empty lines and look for a standalone number (the row index)
        if not lines[i] or not re.match(r"^\d+$", lines[i]):
            i += 1
            continue

        # Found a number — next non-empty lines are the enrollment fields
        i += 1
        # Skip any empty lines after the number
        while i < len(lines) and not lines[i]:
            i += 1

        # Need at least 5 lines: student, program, package, payment, status
        remaining = []
        while i < len(lines) and len(remaining) < 5:
            if lines[i]:
                remaining.append(lines[i])
            i += 1

        if len(remaining) >= 5:
            student_name = remaining[0]
            program_name = remaining[1]
            package_and_amount = remaining[2]
            payment_status = remaining[3]
            status = remaining[4]

            enrollments.append({
                "student_name": student_name,
                "program_name": program_name,
                "package_and_amount": package_and_amount,
                "payment_status": payment_status,
                "status": status,
            })

    print(f"    Parsed {len(enrollments)} enrollments")
    return enrollments


def scrape_all_enrollments(page):
    """Iterate over all families and collect their enrollments."""
    navigate_to_families(page)
    families = get_family_info(page)
    print(f"Found {len(families)} families:")
    for f in families:
        print(f"  - {f}")

    all_enrollments = []
    for i, family_name in enumerate(families):
        family_id = family_name.lower().replace(" ", "")
        print(f"\n  [{i+1}/{len(families)}] {family_name}")

        # Go back to families page if not the first
        if i > 0:
            navigate_to_families(page)

        # Click View Details
        family_url = click_view_details(page, i)
        print(f"    URL: {family_url}")

        # Get enrollments
        enrollments = get_enrollments(page, family_name)
        for enr in enrollments:
            enr["family_identifier"] = family_id
            enr["family_url"] = family_url
            all_enrollments.append(enr)
        print(f"    Found {len(enrollments)} enrollments")

    return all_enrollments


def export_csv(enrollments):
    """Write enrollments to CSV."""
    os.makedirs("output", exist_ok=True)
    with open(config.OPERATION36_ENROLLMENTS_OUTPUT, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "family_identifier",
            "family_url",
            "program_name",
            "student_name",
            "package_and_amount",
            "payment_status",
            "status",
        ])
        writer.writeheader()
        writer.writerows(enrollments)
    print(f"\nExported {len(enrollments)} enrollments to {config.OPERATION36_ENROLLMENTS_OUTPUT}")


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

        print("Scraping enrollments...")
        enrollments = scrape_all_enrollments(page)
        export_csv(enrollments)

        browser.close()


if __name__ == "__main__":
    main()
