"""
Operation 36 members scraper.
Reads families from output/operation36_families.csv, logs into Operation 36,
visits each family's enrollments page, and captures member (student) profile URLs.

Input:  output/operation36_families.csv (family_name, email, phone, family_id)
Output: output/operation36_members.csv (family_name, family_id, student_name, profile_id, profile_url)

Usage:  python scraper-operation36-members.py
"""

import os
import re
import csv
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
import config

load_dotenv()

MEMBERS_OUTPUT = "output/operation36_members.csv"


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


def get_members(page, family_name):
    """Find student links inside enrollment banners and extract profile URLs."""
    members = []
    seen_ids = set()

    banners = page.get_by_role("banner")
    banner_count = banners.count()
    if banner_count == 0:
        print(f"    No banners found on page")
        return members

    for i in range(banner_count):
        links = banners.nth(i).get_by_role("link")
        for j in range(links.count()):
            link = links.nth(j)
            href = link.get_attribute("href") or ""
            match = re.search(r"/profile/(\d+)", href)
            if not match:
                continue

            profile_id = match.group(1)
            if profile_id in seen_ids:
                continue
            seen_ids.add(profile_id)

            student_name = (link.text_content() or "").strip()
            profile_url = href if href.startswith("http") else f"https://operation36golf.com{href}"

            members.append({
                "student_name": student_name,
                "profile_id": profile_id,
                "profile_url": profile_url,
            })

    print(f"    Parsed {len(members)} members")
    return members


def scrape_members(page, families):
    """Visit each family's enrollments page and extract member profile URLs."""
    all_members = []
    for i, fam in enumerate(families):
        family_name = fam["family_name"]
        print(f"\n  [{i+1}/{len(families)}] {family_name}")

        family_id = fam.get("family_id", "")
        if not family_id:
            print(f"    No family ID found, skipping")
            continue

        enroll_url = f"https://operation36golf.com/members/families/{family_id}/enrollments"
        page.goto(enroll_url, wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(2000)
        print(f"    URL: {enroll_url}")

        members = get_members(page, family_name)
        for m in members:
            m["family_name"] = family_name
            m["family_id"] = family_id
            all_members.append(m)
        print(f"    Found {len(members)} members")

    return all_members


def export_csv(members):
    """Write members to CSV."""
    os.makedirs("output", exist_ok=True)
    with open(MEMBERS_OUTPUT, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "family_name",
            "family_id",
            "student_name",
            "profile_id",
            "profile_url",
        ])
        writer.writeheader()
        writer.writerows(members)
    print(f"\nExported {len(members)} members to {MEMBERS_OUTPUT}")


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

        print("Scraping members...")
        members = scrape_members(page, families)
        export_csv(members)

        browser.close()


if __name__ == "__main__":
    main()
