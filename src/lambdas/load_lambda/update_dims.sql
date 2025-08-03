

/*
=========================================
    LOAD auction_status_dim
=========================================
*/
INSERT INTO auction_status_dim(status)
SELECT DISTINCT TRIM(LOWER(auction_status))
FROM staging
WHERE auction_status IS NOT NULL
ON CONFLICT(status) DO NOTHING;


/*
=========================================
    LOAD reserve_status_dim
=========================================
*/
INSERT INTO reserve_status_dim(status)
SELECT DISTINCT TRIM(LOWER(reserve_status))
FROM staging
WHERE reserve_status IS NOT NULL
ON CONFLICT(status) DO NOTHING;

/*
=========================================
    LOAD body_style_dim
=========================================
*/
INSERT INTO body_style_dim(body_style)
SELECT DISTINCT TRIM(LOWER(body_style))
FROM staging
WHERE body_style IS NOT NULL
ON CONFLICT(body_style) DO NOTHING;

/*
=========================================
    LOAD seller_type_dim
=========================================
*/
INSERT INTO seller_type_dim(seller_type)
SELECT DISTINCT TRIM(LOWER(seller_type))
FROM staging
WHERE seller_type IS NOT NULL
ON CONFLICT(seller_type) DO NOTHING;

/*
=========================================
    LOAD drivetrain_dim
=========================================
*/
INSERT INTO drivetrain_dim(drivetrain)
SELECT DISTINCT TRIM(UPPER(drivetrain))
FROM staging
WHERE drivetrain IS NOT NULL
ON CONFLICT(drivetrain) DO NOTHING;

/*
=========================================
    LOAD transmission_dim
=========================================
*/
INSERT INTO transmission_dim(transmission)
SELECT DISTINCT TRIM(LOWER(transmission_type)) AS transmission
FROM staging
WHERE transmission_type IS NOT NULL
ORDER BY transmission ASC
ON CONFLICT(transmission) DO NOTHING;


/*
==================================================================
    LOAD city_dim
    - TODO: remove state names/abbr from from Canadian city names 
==================================================================
*/

INSERT INTO city_dim(city_name, state_id)
SELECT DISTINCT TRIM(s.city) AS city, sd.id
FROM staging s
LEFT JOIN state_dim sd 
	ON s.title_state=sd.state_abbr OR s.title_state=sd.state
WHERE s.city IS NOT NULL
ORDER BY city ASC
ON CONFLICT(city_name,state_id) DO NOTHING;



/*
==================================================================
    LOAD vehicle_make_dim
==================================================================
*/
INSERT INTO vehicle_make_dim(make)
SELECT DISTINCT TRIM(make) AS make
FROM staging
WHERE make IS NOT NULL
ORDER BY make ASC
ON CONFLICT(make) DO NOTHING;

/*
==================================================================
    LOAD vehicle_model_dim
==================================================================
*/
INSERT INTO vehicle_model_dim(model,make_id)
SELECT DISTINCT TRIM(s.model) AS model,vmd.id
FROM staging s
LEFT JOIN vehicle_make_dim vmd
	ON TRIM(s.make)=TRIM(vmd.make)
WHERE model IS NOT NULL
ORDER BY model ASC
ON CONFLICT(model,make_id) DO NOTHING;


/*
==================================================================
    LOAD vehicle_dim
==================================================================
*/
INSERT INTO vehicle_dim(vin,auction_id,make_id,model_id,body_style_id,manufacture_year,mileage,engine,
    transmission_id,gear_count,drivetrain_id,exterior_color,interior_color,title_status, title_state,
	equipment_count, mod_count,flaw_count,service_count,included_items_count)
SELECT 
	TRIM(s.vin) AS vin,
	s.auction_id,
	make_dim.id AS make_id,
	model_dim.id AS model_id,
	bsd.id AS body_style_id,
	s.manufacture_year,
	s.mileage,
	s.engine,
	td.id AS transmission_id,
	s.gears,
	dd.id as drivetrain_id,
    s.exterior_color,
    s.interior_color,
	s.title_status_cleaned,
	s.title_state,
	s.equipment_count,
	s.mod_count,
	s.flaw_count,
	s.service_count,
	s.included_items_count
	
FROM staging s
LEFT JOIN vehicle_make_dim make_dim
	ON TRIM(s.make)=make_dim.make
LEFT JOIN vehicle_model_dim model_dim
	ON TRIM(s.model)=model_dim.model AND make_dim.id=model_dim.make_id
LEFT JOIN body_style_dim bsd
	ON TRIM(LOWER(s.body_style))=bsd.body_style
LEFT JOIN transmission_dim td
	ON TRIM(LOWER(s.transmission_type))=td.transmission
LEFT JOIN drivetrain_dim dd
	ON TRIM(UPPER(s.drivetrain))=dd.drivetrain
ON CONFLICT(vin,auction_id) 
DO UPDATE SET
	make_id = EXCLUDED.make_id,
	model_id = EXCLUDED.model_id,
	body_style_id = EXCLUDED.body_style_id,
	manufacture_year = EXCLUDED.manufacture_year,
	engine = EXCLUDED.engine,
	transmission_id = EXCLUDED.transmission_id,
	gear_count = EXCLUDED.gear_count,
	drivetrain_id = EXCLUDED.drivetrain_id,
    mileage = EXCLUDED.mileage,
    exterior_color = EXCLUDED.exterior_color,
    interior_color = EXCLUDED.interior_color,
	title_status = EXCLUDED.title_status,
	title_state = EXCLUDED.title_state,
	equipment_count = EXCLUDED.equipment_count,
	mod_count = EXCLUDED.mod_count,
	flaw_count = EXCLUDED.flaw_count,
	service_count = EXCLUDED.service_count,
	included_items_count = EXCLUDED.included_items_count;


/*
==================================================================
    LOAD auction_fact
==================================================================
*/
INSERT INTO auction_fact
SELECT 
	s.auction_id,
	TO_TIMESTAMP(s.auction_date / 1000)::timestamptz AS auction_time,
	vd.vehicle_id,
	asd.id AS auction_status,
	rsd.id AS reserve_status,
	sd.id AS auction_state, 
	cd.id AS auction_city,
	std.id AS seller_type,
	s.view_count,
	s.watcher_count,
	s.bid_count,
	s.max_bid,
	s.min_bid,
	s.mean_bid,
	s.median_bid,
	s.bid_range,
	s.bids,
	s.highlight_count,
    s.video_count,
	s.auction_title,
	s.auction_subtitle,
	s.auction_url
FROM staging s
LEFT JOIN vehicle_dim vd 
	ON TRIM(s.vin)=vd.vin AND s.auction_id=vd.auction_id
LEFT JOIN auction_status_dim asd
	ON TRIM(LOWER(s.auction_status))=asd.status
LEFT JOIN reserve_status_dim rsd
	ON TRIM(LOWER(s.reserve_status))=rsd.status
LEFT JOIN state_dim sd
	ON TRIM(UPPER(s.title_state))=sd.state_abbr
LEFT JOIN city_dim CD
	ON TRIM(s.city)=cd.city_name AND sd.id=cd.state_id
LEFT JOIN seller_type_dim std
	ON TRIM(LOWER(s.seller_type))=std.seller_type
WHERE s.auction_id IS NOT NULL
ON CONFLICT(auction_id) DO NOTHING; 