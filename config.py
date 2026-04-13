# URLs
LOGIN_URL = "https://app.coachnow.io/login"
CONTACTS_URL = "https://app.coachnow.io/contacts"

# Login page locators (using Playwright role-based locators)
# These are used as arguments to page.get_by_role(), not CSS selectors
LOGIN_EMAIL_ROLE = ("textbox", {"name": "E-mail"})
LOGIN_PASSWORD_ROLE = ("textbox", {"name": "Password"})
LOGIN_SUBMIT_ROLE = ("button", {"name": "Sign In", "exact": True})

# Contacts page — data is visible directly on each contact card
# Email and phone are <a> link elements; name is plain text
# Lesson history is behind a button on each card (needs click — TBD what it opens)
PAGINATION_NEXT_BUTTON = None            # set to selector string if paginated, None for infinite scroll

# Output
CONTACTS_OUTPUT = "output/contacts.csv"
LESSONS_OUTPUT = "output/lesson_history.csv"

# ---------- Operation 36 ----------
OPERATION36_LOGIN_URL = "https://operation36golf.com/log-in"
OPERATION36_FAMILIES_URL = "https://operation36golf.com/members/families"

# Operation 36 login — uses CSS selectors (input[name=...]), not role-based
OPERATION36_LOGIN_EMAIL_SELECTOR = 'input[name="email"]'
OPERATION36_LOGIN_PASSWORD_SELECTOR = 'input[name="password"]'
OPERATION36_LOGIN_SUBMIT_ROLE = ("button", {"name": "Log In"})

# Operation 36 output
OPERATION36_ENROLLMENTS_OUTPUT = "output/operation36_enrollments.csv"
