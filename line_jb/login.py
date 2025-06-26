from instagrapi import Client
from instagrapi.exceptions import LoginRequired
import os
from dotenv import load_dotenv

load_dotenv()

USERNAME = os.getenv("IG_USERNAME")
PASSWORD = os.getenv("IG_PASSWORD")

def get_logged_in_client(session_file: str = "session.json") -> Client:
    """
    Returns an authenticated Client:
    - Tries to load an existing session.
    - Logs in if needed.
    - Handles automatic re-login if session is expired.
    """
    cl = Client()

    if os.path.exists(session_file):
        try:
            cl.load_settings(session_file)
            cl.login(USERNAME, PASSWORD)  # Verifies session
            return cl
        except Exception as e:
            print(f"Session invalid or expired, trying again... ({e})")
            cl.login(USERNAME, PASSWORD)

    # Always dump settings after successful login
    cl.login(USERNAME, PASSWORD)
    cl.dump_settings(session_file)
    return cl

if __name__ == "__main__":
    # Enables running this script directly for testing
    client = get_logged_in_client()
    print(f"Logged in as: {client.username}")

