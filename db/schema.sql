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

-- NYC Permitted Events (Historical)
CREATE TABLE IF NOT EXISTS permitted_events_historical (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	event_id INTEGER,
	event_name TEXT,
	start_date_time TEXT,
	end_date_time TEXT,
	event_agency TEXT,
	event_type TEXT,
	event_borough TEXT,
	event_location TEXT,
	event_street_side TEXT,
	street_closure_type TEXT,
	community_board TEXT,
	police_precinct TEXT,
	UNIQUE(event_id)
);

-- NYC Permitted Events

-- NYC 311 Service Requests

-- NYC 311 Resolution Satisfaction Survey Responses

-- LinkNYC Kiosk Status Records

-- NYC Sidewalk Management Database (Violations)

-- NYC Parks & Recreation Forestry Tree Points Record

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

