import streamlit as st
from search import fetch_posts_by_hashtag

if __name__ == "__main__":
    st.title("Instagram Hashtag Explorer 🔍")
    hashtag = st.text_input("Enter a hashtag (without #):")

    if st.button("Search"):
        posts = fetch_posts_by_hashtag(hashtag)
        for post in posts:
            st.image(post.thumbnail_url)  # Show post images
            st.write(post.caption or "")
