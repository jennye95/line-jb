-- NYC Parks Events
CREATE TABLE IF NOT EXISTS nyc_parks_events (
    id SERIAL PRIMARY KEY,
    event_name VARCHAR(255),
    location VARCHAR(255),
    date_and_time TIMESTAMP,
    borough VARCHAR(100),
    location_type VARCHAR(100),
    group_name_partner VARCHAR(255),
    event_type VARCHAR(100),
    category VARCHAR(100),
    attendance VARCHAR(100),
    audience VARCHAR(100),
    source VARCHAR(255),
    CONSTRAINT unique_event UNIQUE(event_name, date_and_time, location)
);

-- Placeholder for future tables
-- e.g., NYC 311 Service Requests
CREATE TABLE IF NOT EXISTS nyc_311_requests (
    id SERIAL PRIMARY KEY,
    complaint_type VARCHAR(255),
    status VARCHAR(100),
    created_date TIMESTAMP,
    latitude DECIMAL(9, 6),
    longitude DECIMAL(9, 6)
);

-- Placeholder for social media posts
CREATE TABLE IF NOT EXISTS social_posts (
    id SERIAL PRIMARY KEY,
    platform VARCHAR(50),
    post_id VARCHAR(100),
    content TEXT,
    created_at TIMESTAMP,
    latitude DECIMAL(9, 6),
    longitude DECIMAL(9, 6)
);

