-- set up the context
USE ROLE accountadmin;
USE WAREHOUSE compute_wh;
USE DATABASE yelp;
USE SCHEMA public;

-- load the ZIP code data
CREATE OR REPLACE TABLE zipcode
(
    zip varchar,
    type varchar,
    decommissioned int,
    primary_city varchar,
    acceptable_cities varchar,
    unacceptable_cities varchar,
    state varchar,
    county varchar,
    timezone varchar,
    area_codes varchar,
    world_region varchar,
    country varchar,
    latitude decimal,
    longitude decimal,
    irs_estimated_population int
);

COPY INTO zipcode
FROM @stage_yelp_s3/zip_code_database.csv.gz
FILE_FORMAT = format_yelp_csv;

SELECT * FROM zipcode LIMIT 3;

-- load the cumulative COVID19 cases data
CREATE OR REPLACE TABLE covidcases
(
    date timestamp,
    county varchar,
    state varchar,
    zip varchar,
    cumulative_cases int,
    cumulative_deaths int
);

COPY INTO covidcases
FROM @stage_yelp_s3/us_counties_covid.csv.gz
FILE_FORMAT = format_yelp_csv;

SELECT * FROM covidcases LIMIT 10;

-- load US state abbreviations
CREATE OR REPLACE TABLE abbrv
(
    state varchar,
    abbreviation varchar
);

COPY INTO abbrv
FROM @stage_yelp_s3/us_state_abbreviation.csv.gz
FILE_FORMAT = format_yelp_csv;

SELECT * FROM abbrv;


SELECT * FROM zipcode LIMIT 100;


SELECT * FROM covidcases order by date desc limit 100;


SELECT DISTINCT state from abbrv;


--------view of states to be considered analysis------------

CREATE VIEW states AS
SELECT CASE WHEN state = 'AB' THEN 'AL' ELSE state END AS state, count(*) AS businesses
FROM business
GROUP BY CASE WHEN state = 'AB' THEN 'AL' ELSE state END
HAVING businesses >= 100 
ORDER BY businesses DESC;


-------- state, category, and year level ----------
SELECT c.category_name, YEAR(review_date) AS year, COUNT(DISTINCT b.business_id) AS businesses, AVG(r.stars) AS stars
FROM review r
LEFT JOIN business b ON r.business_id = b.business_id
LEFT JOIN category c ON b.business_id = c.business_id
WHERE b.state IN (SELECT state from states)
GROUP BY c.category_name, YEAR(review_date)
HAVING COUNT(DISTINCT b.business_id) >= 100
ORDER BY category_name ASC, Year ASC;


---------percent drop in businesses and stars pre vs post covid pre(2018,2019) and post(2020,2021)
SELECT category_name,
       pre_covid_businesses,
       post_covid_businesses,
       pre_covid_reviews,
       post_covid_reviews,
       pre_covid_ratings,
       post_covid_ratings,
       ROUND((post_covid_businesses - pre_covid_businesses)*100/pre_covid_businesses,2) AS percent_change_businesses,
       ROUND((post_covid_reviews - pre_covid_reviews)*100/pre_covid_reviews,2) AS percent_change_reviews,
       post_covid_ratings - pre_covid_ratings AS change_ratings
FROM       
(SELECT c.category_name,
       COUNT(DISTINCT CASE WHEN YEAR(review_date) IN (2018,2019) THEN b.business_id END) AS pre_covid_businesses,
       COUNT(DISTINCT CASE WHEN YEAR(review_date) IN (2020,2021) THEN b.business_id END) AS post_covid_businesses,
       COUNT(DISTINCT CASE WHEN YEAR(review_date) IN (2018,2019) THEN r.review_id END) AS pre_covid_reviews,
       COUNT(DISTINCT CASE WHEN YEAR(review_date) IN (2020,2021) THEN r.review_id END) AS post_covid_reviews,
       ROUND(AVG(CASE WHEN YEAR(review_date) IN (2018,2019) THEN r.stars END),2) AS pre_covid_ratings,
       ROUND(AVG(CASE WHEN YEAR(review_date) IN (2020,2021) THEN r.stars END),2) AS post_covid_ratings
FROM review r
LEFT JOIN business b ON r.business_id = b.business_id
LEFT JOIN category c ON b.business_id = c.business_id
WHERE b.state IN (SELECT state from states)
GROUP BY c.category_name
HAVING COUNT(DISTINCT CASE WHEN YEAR(review_date) IN (2018,2019) THEN b.business_id END) >= 100
ORDER BY category_name ASC) t;




