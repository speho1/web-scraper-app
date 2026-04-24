"""
Operation 36 users export script.
Logs into Operation 36, navigates to the students page, and clicks the
"Export Members CSV" button to download the exported file.

Output: output/operation36_users_export.xlsx

Usage: python scraper-operation36-users-export.py
"""

import os
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
import config

load_dotenv()

STUDENTS_URL = "https://operation36golf.com/members/students"
EXPORT_OUTPUT = "output/operation36_users_export.xlsx"


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


def export_users(page):
    """Navigate to students page and trigger the export download."""
    page.goto(STUDENTS_URL, wait_until="networkidle", timeout=60000)
    page.wait_for_timeout(2000)

    os.makedirs("output", exist_ok=True)

    page.get_by_role("button", name="Export Members CSV").click()

    download_link = page.get_by_role("link", name="Download CSV")
    download_link.wait_for(timeout=120000)

    with page.expect_download(timeout=120000) as download_info:
        download_link.click()
    download = download_info.value

    download.save_as(os.path.join(os.getcwd(), EXPORT_OUTPUT))
    print(f"Saved export to {EXPORT_OUTPUT}")


def main():
    email = os.getenv("OPERATION36_EMAIL")
    password = os.getenv("OPERATION36_PASSWORD")
    if not email or not password:
        print("Error: Set OPERATION36_EMAIL and OPERATION36_PASSWORD in .env")
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            accept_downloads=True,
        )
        page = context.new_page()

        login(page)
        page.wait_for_timeout(2000)

        print("Exporting users...")
        export_users(page)

        browser.close()


if __name__ == "__main__":
    main()
