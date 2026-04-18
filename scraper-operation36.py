"""
Operation 36 enrollments scraper.
Logs into Operation 36, iterates through all family cards,
clicks View Details for each, clicks Enrollments tab, and extracts enrollment data.

Output: output/operation36_families.csv, output/operation36_enrollments.csv

Usage:  python scraper-operation36.py
"""

import os
import re
import csv
from collections import Counter
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
        rows = page.locator(".FamiliesTable_row__AQCjl")
        count = rows.count()
        if count > 0:
            rows.nth(count - 1).scroll_into_view_if_needed()
        page.wait_for_timeout(3000)
        current_count = rows.count()
        print(f"  Scrolling... {current_count} families loaded")
        if current_count == prev_count:
            stable_rounds += 1
            if stable_rounds >= 5:
                break
        else:
            stable_rounds = 0
        prev_count = current_count
    print(f"  Done scrolling. Total families: {current_count}")
    container.evaluate("el => el.scrollTop = 0")
    page.wait_for_timeout(1000)


def navigate_to_families(page):
    """Navigate to families page and load all families."""
    page.goto(config.OPERATION36_FAMILIES_URL, wait_until="networkidle")
    page.wait_for_timeout(3000)
    page.locator("#tableContent").wait_for(timeout=15000)
    page.wait_for_timeout(2000)
    scroll_to_load_all_families(page)


def collect_families(page):
    """First pass: scroll to load all families, extract name and email."""
    navigate_to_families(page)
    rows = page.locator(".FamiliesTable_row__AQCjl")
    count = rows.count()
    families = []
    for i in range(count):
        row = rows.nth(i)
        name_el = row.locator("h5")
        name = name_el.text_content().strip() if name_el.count() > 0 else f"Family_{i}"

        # Email is inside FamiliesTable_familyAdmin__o03AK span
        email = ""
        admin_div = row.locator(".FamiliesTable_familyAdmin__o03AK")
        if admin_div.count() > 0:
            spans = admin_div.locator("span")
            for j in range(spans.count()):
                text = spans.nth(j).text_content().strip()
                if "@" in text:
                    email = text
                    break

        families.append({"family_name": name, "email": email, "family_url": ""})

    # Sort alphabetically by family_name
    families.sort(key=lambda f: f["family_name"].lower())
    print(f"Collected {len(families)} families")
    return families


def search_and_capture_urls(page, families):
    """Second pass: search each family by name, click View Details, capture URL."""
    # Count duplicates so we know how many results to expect
    name_counts = Counter(f["family_name"] for f in families)

    # Group families by name for processing duplicates together
    processed_names = set()

    for i, fam in enumerate(families):
        name = fam["family_name"]
        if name in processed_names:
            continue  # Already handled as part of a duplicate group

        expected_count = name_counts[name]
        print(f"\n  [{i+1}/{len(families)}] Searching: {name} (expect {expected_count} result(s))")

        # Clear search bar and type family name
        search_box = page.get_by_role("textbox", name="Search for Families")
        search_box.fill("")
        page.wait_for_timeout(500)
        search_box.fill(name)
        page.wait_for_timeout(3000)

        # Wait for results to load
        rows = page.locator(".FamiliesTable_row__AQCjl")
        result_count = rows.count()
        print(f"    Found {result_count} search result(s)")

        # Get all families with this name (in sorted order)
        matching_indices = [j for j, f in enumerate(families) if f["family_name"] == name]

        for k in range(min(result_count, expected_count)):
            row = rows.nth(k)
            view_btn = row.locator("button.btn-primary")
            view_btn.click()
            page.wait_for_load_state("networkidle", timeout=60000)
            page.wait_for_timeout(1500)

            family_url = page.url
            idx = matching_indices[k] if k < len(matching_indices) else matching_indices[0]
            families[idx]["family_url"] = family_url
            print(f"    [{k+1}/{expected_count}] {name} -> {family_url}")

            # Go back to search results
            page.go_back(timeout=60000)
            page.wait_for_timeout(2000)

        processed_names.add(name)

    return families


def export_families_csv(families):
    """Write families to CSV."""
    os.makedirs("output", exist_ok=True)
    family_csv = "output/operation36_families.csv"
    with open(family_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["family_name", "email", "family_url"])
        writer.writeheader()
        writer.writerows(families)
    print(f"\nExported {len(families)} families to {family_csv}")


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

    body_text = page.inner_text("body")

    if "Payment Status\nStatus" in body_text:
        enroll_section = body_text.split("Payment Status\nStatus")[-1]
    elif "Enrollments" in body_text:
        parts = body_text.split("Enrollments")
        enroll_section = parts[-1] if len(parts) > 1 else body_text
    else:
        enroll_section = body_text

    lines = [l.strip() for l in enroll_section.strip().split("\n")]

    i = 0
    while i < len(lines):
        if not lines[i] or not re.match(r"^\d+$", lines[i]):
            i += 1
            continue

        i += 1
        while i < len(lines) and not lines[i]:
            i += 1

        remaining = []
        while i < len(lines) and len(remaining) < 5:
            if lines[i]:
                remaining.append(lines[i])
            i += 1

        if len(remaining) >= 5:
            enrollments.append({
                "student_name": remaining[0],
                "program_name": remaining[1],
                "package_and_amount": remaining[2],
                "payment_status": remaining[3],
                "status": remaining[4],
            })

    print(f"    Parsed {len(enrollments)} enrollments")
    return enrollments


def scrape_all_enrollments(page):
    """Collect families, get URLs via search, then scrape enrollments."""
    # Phase 1: collect all family names and emails
    print("Phase 1: Collecting family names and emails...")
    families = collect_families(page)
    for f in families:
        print(f"  {f['family_name']} | {f['email']}")

    # Phase 2: search each family to get URLs
    print(f"\nPhase 2: Searching families to capture URLs...")
    families = search_and_capture_urls(page, families)

    # Save families CSV
    export_families_csv(families)

    # Phase 3: visit each family's enrollment page
    print(f"\nPhase 3: Scraping enrollments for {len(families)} families...")
    all_enrollments = []
    for i, fam in enumerate(families):
        family_name = fam["family_name"]
        family_id = family_name.lower().replace(" ", "")
        print(f"\n  [{i+1}/{len(families)}] {family_name}")

        if not fam["family_url"]:
            print(f"    No URL found, skipping")
            continue

        enroll_url = fam["family_url"].rstrip("/") + "/enrollments"
        page.goto(enroll_url, wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(2000)
        print(f"    URL: {enroll_url}")

        enrollments = get_enrollments(page, family_name)
        for enr in enrollments:
            enr["family_identifier"] = family_id
            enr["family_url"] = enroll_url
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
