# line-jb
we're gonna get sum 🥯

Project Idea: Real-Time Trend / Event & Location Analytics

What it does:
    Ingests social media posts based on searchs and hashtags to gauge excitement or concerns.

	check for subway delays affecting event attendance.

	Avoid streets with sanitation or noise complaints

	Finds concerts, fairs, special activities nearby

    Extracts metadata: captions, timestamps, location, hashtags, mentions.

    Processes the data to find trending topics, hashtag clusters, or peak posting times in NYC or your chosen area.

    Visualizes the insights on a simple dashboard or exports to CSV for analysis.

    Applies NLP to detect sentiment or categorize posts (event vs. casual vs. promo).

Why it’s cool and useful:
	Users aware of transit issues or neighborhood complaints can avoid unpleasant experiences, improving overall satisfaction with city life and events.

	Many event apps just list events. Our project can stand out by combining event data + live community issues + transit conditions, offering a richer, actionable snapshot of urban life.:
	
    Flex data pipelines skills: extraction, transformation, and loading (ETL).

    Play with music, art, events, and see what’s trending in real-time on Instagram.

    It’s interview-ready: shows skills in API integration, data cleaning, NLP, and visualization.

    It has tons of room to grow: add Twitter, TikTok, or NYC open datasets later.

## System Requirements

Before installing Python dependencies, make sure the following system packages are installed:

- PROJ: Required for pyproj and geopandas

On macOS (Homebrew):
brew install proj

On Ubuntu/Debian:
sudo apt-get install libproj-dev

Then install the Python dependencies:
pip install -e .[dev]
