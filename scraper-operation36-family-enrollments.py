"""
Operation 36 enrollments scraper.
Reads family URLs from output/operation36_families.csv, logs into Operation 36,
visits each family's enrollments page, and extracts enrollment data.

Input:  output/operation36_families.csv (family_name, email, family_url)
Output: output/operation36_enrollments.csv

Usage:  python scraper-operation36-enrollments.py
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


def load_families_csv():
    """Read families from CSV."""
    families_csv = "output/operation36_families.csv"
    if not os.path.exists(families_csv):
        print(f"Error: {families_csv} not found. Run scraper-operation36-families.py first.")
        return []

    families = []
    with open(families_csv, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            families.append(row)

    print(f"Loaded {len(families)} families from {families_csv}")
    return families


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


def scrape_enrollments(page, families):
    """Visit each family's enrollment page and extract data."""
    all_enrollments = []
    for i, fam in enumerate(families):
        family_name = fam["family_name"]
        print(f"\n  [{i+1}/{len(families)}] {family_name}")

        if not fam.get("family_id"):
            print(f"    No family ID found, skipping")
            continue

        enroll_url = f"https://operation36golf.com/members/families/{fam['family_id']}/enrollments"
        page.goto(enroll_url, wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(2000)
        print(f"    URL: {enroll_url}")

        enrollments = get_enrollments(page, family_name)
        for enr in enrollments:
            enr["family_name"] = family_name
            enr["family_url"] = enroll_url
            all_enrollments.append(enr)
        print(f"    Found {len(enrollments)} enrollments")

    return all_enrollments


def export_csv(enrollments):
    """Write enrollments to CSV."""
    os.makedirs("output", exist_ok=True)
    with open(config.OPERATION36_ENROLLMENTS_OUTPUT, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "family_name",
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

    families = load_families_csv()
    if not families:
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(viewport={"width": 1280, "height": 900})
        page = context.new_page()

        login(page)
        page.wait_for_timeout(2000)

        print("Scraping enrollments...")
        enrollments = scrape_enrollments(page, families)
        export_csv(enrollments)

        browser.close()


if __name__ == "__main__":
    main()
