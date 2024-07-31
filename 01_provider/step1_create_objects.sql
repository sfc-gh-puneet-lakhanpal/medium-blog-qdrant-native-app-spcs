USE ROLE qdrant_provider_role;
CREATE OR REPLACE WAREHOUSE qdrant_provider_wh WITH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 180
  AUTO_RESUME = true
  INITIALLY_SUSPENDED = false;

CREATE DATABASE IF NOT EXISTS qdrant_provider_db;
use database qdrant_provider_db;
CREATE schema if not exists qdrant_provider_schema;
use schema qdrant_provider_schema;
CREATE IMAGE REPOSITORY if not exists qdrant_provider_image_repo;
show image repositories;