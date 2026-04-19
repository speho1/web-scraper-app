"""
Operation 36 families scraper.
Logs into Operation 36, scrolls to load all families, extracts names and emails,
then searches each family to capture their URL.

Output: output/operation36_families.csv (family_name, email, phone, family_url)

Usage:  python scraper-operation36-families.py
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
    """Scroll to load all families, extract name and email."""
    navigate_to_families(page)
    rows = page.locator(".FamiliesTable_row__AQCjl")
    count = rows.count()
    families = []
    for i in range(count):
        row = rows.nth(i)
        name_el = row.locator("h5")
        name = name_el.text_content().strip() if name_el.count() > 0 else f"Family_{i}"
        # Strip "Family" suffix (e.g., "Drew Family" -> "Drew")
        name = re.sub(r"\s*Family$", "", name).strip()

        email = ""
        phone = ""
        admin_div = row.locator(".FamiliesTable_familyAdmin__o03AK")
        if admin_div.count() > 0:
            spans = admin_div.locator("span")
            for j in range(spans.count()):
                text = spans.nth(j).text_content().strip()
                if "@" in text:
                    email = text
                elif not phone and re.search(r"\d", text) and len(re.findall(r"\d", text)) >= 7:
                    phone = text

        families.append({"family_name": name, "email": email, "phone": phone, "family_id": ""})

    families.sort(key=lambda f: f["family_name"].lower())
    print(f"Collected {len(families)} families")
    return families


def search_and_capture_urls(page, families):
    """Search each family by name, click View Details, capture URL."""
    name_counts = Counter(f["family_name"] for f in families)
    processed_names = set()

    for i, fam in enumerate(families):
        name = fam["family_name"]
        if name in processed_names:
            continue

        expected_count = name_counts[name]
        print(f"\n  [{i+1}/{len(families)}] Searching: {name} (expect {expected_count} result(s))")

        search_box = page.get_by_role("textbox", name="Search for Families")
        search_box.fill("")
        page.wait_for_timeout(500)
        search_box.fill(name)
        page.wait_for_timeout(3000)

        rows = page.locator(".FamiliesTable_row__AQCjl")
        result_count = rows.count()
        print(f"    Found {result_count} search result(s)")

        matching_indices = [j for j, f in enumerate(families) if f["family_name"] == name]

        for k in range(min(result_count, expected_count)):
            row = rows.nth(k)
            view_btn = row.locator("button.btn-primary")
            view_btn.click()
            page.wait_for_load_state("networkidle", timeout=60000)
            page.wait_for_timeout(1500)

            family_url = page.url
            # Extract family ID from URL (e.g., /members/families/165704)
            match = re.search(r"/families/(\d+)", family_url)
            family_id = match.group(1) if match else ""
            idx = matching_indices[k] if k < len(matching_indices) else matching_indices[0]
            families[idx]["family_id"] = family_id
            print(f"    [{k+1}/{expected_count}] {name} -> {family_id}")

            page.go_back(timeout=60000)
            page.wait_for_timeout(2000)

        processed_names.add(name)

    return families


def export_families_csv(families):
    """Write families to CSV."""
    os.makedirs("output", exist_ok=True)
    family_csv = "output/operation36_families.csv"
    with open(family_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["family_name", "email", "phone", "family_id"])
        writer.writeheader()
        writer.writerows(families)
    print(f"\nExported {len(families)} families to {family_csv}")


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

        print("Collecting families...")
        families = collect_families(page)
        for f in families:
            print(f"  {f['family_name']} | {f['email']} | {f['phone']}")

        print(f"\nSearching families to capture URLs...")
        families = search_and_capture_urls(page, families)

        export_families_csv(families)
        browser.close()


if __name__ == "__main__":
    main()
