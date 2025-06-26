import os
import streamlit as st
from line_jb.data_ingestion.search import fetch_posts_by_hashtag
from line_jb.data_ingestion.nyc_open_data import fetch_parks_events, insert_parks_events

if __name__ == "__main__":
#    st.title("Instagram Hashtag Explorer 🔍")
#    hashtag = st.text_input("Enter a hashtag (without #):")
#
#    if st.button("Search"):
#        posts = fetch_posts_by_hashtag(hashtag)
#        for post in posts:
#            st.image(post.thumbnail_url)  # Show post images
#            st.write(post.caption or "")
    
	# Get events
    events = fetch_parks_events()

    # Build path for the database
    db_file = os.path.join(os.path.dirname(__file__), 'db/local.db')
    db_file = os.path.abspath(db_file)

    # Insert events
    insert_parks_events(events, db_file=db_file)

    print(f"Inserted {len(events)} NYC Parks events into {db_file}.")
