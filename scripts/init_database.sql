-- WARNING: This script will drop and recreate the 'DataWarehouse' database.
-- Ensure backups are taken before executing!

-- Drop the database if it exists
DROP DATABASE IF EXISTS DataWarehouse;

-- Create the database
CREATE DATABASE DataWarehouse;

-- Switch to the new database
USE DataWarehouse;



-- For educational or organizational purposes, we can simulate schemas like:
CREATE TABLE bronze_example (
    id INT PRIMARY KEY,
    data VARCHAR(100)
);

CREATE TABLE silver_example (
    id INT PRIMARY KEY,
    data VARCHAR(100)
);

CREATE TABLE gold_example (
    id INT PRIMARY KEY,
    data VARCHAR(100)
);

