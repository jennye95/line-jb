-- NYC Parks Events
CREATE TABLE IF NOT EXISTS nyc_parks_events (
    id SERIAL PRIMARY KEY,
    event_name TEXT,
    location TEXT,
    date_and_time TEXT,
    borough TEXT,
    location_type TEXT,
    group_name_partner TEXT,
    event_type TEXT,
    category TEXT,
    attendance TEXT,
    audience TEXT,
    source TEXT,
    CONSTRAINT unique_event UNIQUE(event_name, date_and_time, location)
);

-- NYC Permitted Events (Historical)
CREATE TABLE IF NOT EXISTS nyc_permitted_events_historical (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	event_id INTEGER UNIQUE,
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
	police_precinct TEXT
);

-- NYC Permitted Events (Occurring Within the Next Month)
CREATE TABLE IF NOT EXISTS nyc_permitted_events_future (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	event_id INTEGER UNIQUE,
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
	police_precinct TEXT
);

-- NYC 311 Service Requests
CREATE TABLE IF NOT EXISTS nyc_311_requests (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	unique_key TEXT UNIQUE,
	created_date TEXT,
	closed_date TEXT,
	agency TEXT,
	agency_name TEXT,
	complaint_type TEXT,
	descriptor TEXT,
	location_type TEXT,
	incident_zip TEXT,
	incident_address TEXT,
	street_name TEXT,
	city TEXT,
	status TEXT,
	due_date TEXT,
	resolution_description TEXT,
	resolution_action_updated_date TEXT,
	borough TEXT,
	latitude REAL,
	longitude REAL
);

-- NYC 311 Resolution Satisfaction Survey Responses
CREATE TABLE IF NOT EXISTS nyc_311_resolutions (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	unique_key TEXT UNIQUE,
	agency TEXT,
	agency_name TEXT,
	complaint_type TEXT,
	descriptor TEXT,
	borough TEXT,
	resolution_description TEXT,
	year INTEGER,
	month INTEGER,
	overall_satisfaction TEXT,
	dissatisfaction_reason TEXT
);

-- LinkNYC Kiosk Status Records
CREATE TABLE IF NOT EXISTS linknyc_status (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	generated_on TEXT,
	site_id TEXT UNIQUE,
	status TEXT,
	kiosk_type TEXT,
	ppt_id TEXT,
	address TEXT,
	city TEXT,
	state TEXT,
	zip TEXT,
	boro TEXT,
	latitude REAL,
	longitude REAL,
	cross_street_1 TEXT,
	cross_street_2 TEXT,
	corner TEXT,
	community_board TEXT,
	council_district TEXT,
	census_tract TEXT,
	nta TEXT,
	bbl TEXT,
	bin TEXT,
	install_date TEXT,
	active_date TEXT,
	wifi_status TEXT,
	wifi_status_date TEXT,
	tablet_status TEXT,
	tablet_status_date TEXT,
	phone_status TEXT,
	phone_status_date TEXT
);

-- NYC Sidewalk Management Database (Sidewalk Violations)
CREATE TABLE IF NOT EXISTS nyc_sidewalk_status (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	broken TEXT,
	cb INTEGER,
	certi_date TEXT,
	contract TEXT,
	entrydate TEXT,
	flag TEXT,
	frstname TEXT,
	grace_pd INTEGER,
	hardware TEXT,
	house_num TEXT,
	integrity TEXT,
	onfrtocode TEXT,
	onstname TEXT,
	other_def TEXT,
	patchwork TEXT,
	post_date TEXT,
	slope TEXT,
	sq_feet INTEGER,
	sw_missing TEXT,
	swv_number INTEGER,
	tostname TEXT,
	trip_haz TEXT,
	undermined TEXT,
	vdismissdate TEXT,
	violationid INTEGER,
	vissuedate TEXT,
	bblid INTEGER UNIQUE
);

-- NYC Parks & Recreation Forestry Tree Points Record
CREATE TABLE IF NOT EXISTS tree_points (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	objectid INTEGER UNIQUE,
	dbh INTEGER,
	tpstructure TEXT,
	tpcondition TEXT,
	stumpdiameter TEXT,
	plantingspaceglobalid TEXT,
	geometry TEXT,
	globalid TEXT,
	genusspecies TEXT,
	createddate TEXT,
	updateddate TEXT,
	planteddate TEXT,
	riskrating TEXT,
	riskratingdate TEXT,
	location TEXT
);

