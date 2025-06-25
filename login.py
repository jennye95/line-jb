from instagrapi import Client
import os

# TODO: Move these to environment variables for security
USERNAME = "jennyyefanpage"
PASSWORD = "i<3jenny"

def get_logged_in_client(session_file: str = "session.json") -> Client:
    """
    Returns a logged-in Client.
    Tries to load session settings first,
    otherwise logs in with USERNAME/PASSWORD.
    """
    cl = Client()
    if os.path.exists(session_file):
        cl.load_settings(session_file)

    try:
        cl.login(USERNAME, PASSWORD)
    except Exception as e:
        # If login fails (e.g., first run), save settings
        print(f"Login failed with settings, trying fresh login. Error: {e}")
        cl.login(USERNAME, PASSWORD)

    # Always dump settings after successful login
    cl.dump_settings(session_file)
    return cl

if __name__ == "__main__":
    # Enables running this script directly for testing
    client = get_logged_in_client()
    print(f"Logged in as: {client.username}")

