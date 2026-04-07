"""
Selector discovery tool.
Launches a headed browser, logs into CoachNow, navigates to /contacts,
and opens the Playwright Inspector so you can find CSS selectors for contacts.

Usage:  python discover.py
"""

import os
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from config import LOGIN_URL, CONTACTS_URL, LOGIN_EMAIL_ROLE, LOGIN_PASSWORD_ROLE, LOGIN_SUBMIT_ROLE

load_dotenv()


def main():
    email = os.getenv("COACHNOW_EMAIL")
    password = os.getenv("COACHNOW_PASSWORD")
    if not email or not password:
        print("Error: Set COACHNOW_EMAIL and COACHNOW_PASSWORD in .env")
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=500)
        context = browser.new_context(viewport={"width": 1280, "height": 900})
        page = context.new_page()

        # Go to login page, fill credentials, and pause for manual sign-in
        page.goto(LOGIN_URL, wait_until="networkidle")
        page.get_by_role(LOGIN_EMAIL_ROLE[0], name=LOGIN_EMAIL_ROLE[1]["name"]).fill(email)
        page.get_by_role(LOGIN_PASSWORD_ROLE[0], name=LOGIN_PASSWORD_ROLE[1]["name"]).fill(password)

        page.get_by_role(LOGIN_SUBMIT_ROLE[0], name=LOGIN_SUBMIT_ROLE[1]["name"], exact=LOGIN_SUBMIT_ROLE[1]["exact"]).click()
        page.wait_for_load_state("networkidle")

        # Navigate to contacts after manual login
        page.goto(CONTACTS_URL, wait_until="networkidle")

        print("Logged in and on /contacts page.")
        print("Playwright Inspector opening — use 'Explore' mode to find selectors for:")
        print("  1. A contact list item (the row/card for one contact)")
        print("  2. The contact name element")
        print("  3. Click into a contact, then find: email, phone, lesson history")
        print("Close the browser when done.")

        page.pause()  # Opens the Playwright Inspector GUI

        browser.close()


if __name__ == "__main__":
    main()
