
use role accountadmin;
CREATE ROLE IF NOT EXISTS QDRANT_CONSUMER_ROLE;
grant role QDRANT_consumer_role to role accountadmin;
create warehouse if not exists xsmall_wh;
GRANT USAGE ON WAREHOUSE XSMALL_WH to role QDRANT_CONSUMER_ROLE;
GRANT USAGE ON WAREHOUSE LARGE_WH to role QDRANT_CONSUMER_ROLE;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE QDRANT_CONSUMER_ROLE;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE QDRANT_CONSUMER_ROLE;
GRANT ROLE QDRANT_CONSUMER_ROLE to USER PLAKHANPAL;
SELECT SYSTEM$ENABLE_BEHAVIOR_CHANGE_BUNDLE('2024_04');
grant imported privileges on database snowflake to application DISTRIBUTED_QDRANT_ON_SPCS;


use role QDRANT_consumer_role;
create database if not exists QDRANT_consumer_db;
use database QDRANT_consumer_db;
create schema if not exists yelp_reviews_sch;
use schema yelp_reviews_sch;
use warehouse large_wh;
create or replace table QDRANT_consumer_db.yelp_reviews_sch.yelp_reviews as select * from YELP_REVIEWS.YELP_REVIEWS_SCH.YELP_BUSINESS_REVIEWS_FOR_ANALYSIS;

select * from QDRANT_consumer_db.yelp_reviews_sch.yelp_reviews limit 10;

GRANT USAGE ON DATABASE QDRANT_consumer_db to APPLICATION DISTRIBUTED_QDRANT_ON_SPCS;
GRANT USAGE ON SCHEMA QDRANT_consumer_db.yelp_reviews_sch to APPLICATION DISTRIBUTED_QDRANT_ON_SPCS;
GRANT SELECT ON TABLE QDRANT_consumer_db.yelp_reviews_sch.yelp_reviews to APPLICATION DISTRIBUTED_QDRANT_ON_SPCS;
GRANT CREATE TABLE ON SCHEMA QDRANT_consumer_db.yelp_reviews_sch TO APPLICATION DISTRIBUTED_QDRANT_ON_SPCS;
GRANT CREATE STAGE ON SCHEMA QDRANT_consumer_db.yelp_reviews_sch TO APPLICATION DISTRIBUTED_QDRANT_ON_SPCS;
GRANT CREATE FILE FORMAT ON SCHEMA QDRANT_consumer_db.yelp_reviews_sch TO APPLICATION DISTRIBUTED_QDRANT_ON_SPCS;
GRANT CREATE VIEW ON SCHEMA QDRANT_consumer_db.yelp_reviews_sch TO APPLICATION DISTRIBUTED_QDRANT_ON_SPCS;