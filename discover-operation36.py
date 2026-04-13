"""
Operation 36 discovery tool.
Launches a headed browser, logs into Operation 36, navigates to the
families page, and pauses for you to explore enrollment selectors.

Usage:  python discover-operation36.py
"""

import os
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
import config

load_dotenv()


def main():
    email = os.getenv("OPERATION36_EMAIL")
    password = os.getenv("OPERATION36_PASSWORD")
    if not email or not password:
        print("Error: Set OPERATION36_EMAIL and OPERATION36_PASSWORD in .env")
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=500)
        context = browser.new_context(viewport={"width": 1280, "height": 900})
        page = context.new_page()

        # Login
        page.goto(config.OPERATION36_LOGIN_URL, wait_until="networkidle")
        page.fill(config.OPERATION36_LOGIN_EMAIL_SELECTOR, email)
        page.fill(config.OPERATION36_LOGIN_PASSWORD_SELECTOR, password)
        page.get_by_role(
            config.OPERATION36_LOGIN_SUBMIT_ROLE[0],
            name=config.OPERATION36_LOGIN_SUBMIT_ROLE[1]["name"],
        ).click()
        page.wait_for_load_state("networkidle")

        # Navigate to families page
        page.goto(config.OPERATION36_FAMILIES_URL, wait_until="networkidle")
        page.wait_for_timeout(3000)

        print("Logged in and on the families page.")
        print("Playwright Inspector opening.")
        print()
        print("Explore the enrollment cards to find selectors for:")
        print("  - program_name")
        print("  - student_name")
        print("  - package_and_amount")
        print("  - payment_status")
        print("  - status")

        page.pause()

        browser.close()


if __name__ == "__main__":
    main()
