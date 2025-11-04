-- ==============================================
-- CAPSTONE PROJECT: CREDITCARD_CAPSTONE DATABASE
-- Author: Brett Thalacker
-- Purpose: create database, build tables, and load cleaned data
-- ==============================================

-- drop creditcard_capstone database if it already exists to start clean
DROP DATABASE IF EXISTS creditcard_capstone; -- deletes creditcard_capstone

-- create the main database that will hold all project tables
CREATE DATABASE creditcard_capstone; -- create the database creditcard_capstone

-- switch to the creditcard_capstone database so all commands after this use it automatically
USE creditcard_capstone; -- make creditcard_capstone the default schema for the rest of this script

-- ==============================================
-- CREATE TABLE: CDW_SAPP_CUSTOMERS
-- ==============================================

-- create a new table to store customer information
CREATE TABLE cdw_sapp_customers ( -- create a new table called cdw_sapp_customers
    FIRST_NAME VARCHAR(50) NOT NULL, -- customer’s first name which is required
    MIDDLE_NAME VARCHAR(50), -- customer’s middle name, optional since not everyone has one
    LAST_NAME VARCHAR(50) NOT NULL, -- customer’s last name which is required
    SSN VARCHAR(11) PRIMARY KEY NOT NULL, -- social security number used as a unique identifier (primary key) which is required
    CREDIT_CARD_NO VARCHAR(16) UNIQUE NOT NULL, -- credit card number for each customer, unique to prevent duplicates which is required
    APT_NO VARCHAR(10), -- apartment or unit number, optional
    STREET_NAME VARCHAR(100) NOT NULL, -- street name for the customer’s address which is required
    CUST_CITY VARCHAR(50) NOT NULL, -- city where the customer lives which is required
    CUST_STATE VARCHAR(50) NOT NULL, -- state where the customer lives which is required
    CUST_COUNTRY VARCHAR(50) NOT NULL, -- country for customer location which is required
    CUST_ZIP VARCHAR(10) NOT NULL, -- zip code as text so leading zeros don’t get removed which is required
    CUST_PHONE VARCHAR(15), -- customer phone number stored as text to keep consistent formatting
    CUST_EMAIL VARCHAR(100), -- customer email address
    Customer_ID VARCHAR(100) -- customer id field used for reference (not primary key)
);

-- ==============================================
-- CREATE TABLE: CDW_SAPP_TRANSACTION
-- ==============================================

-- create a new table to store all customer transactions
CREATE TABLE cdw_sapp_transaction ( -- create a new table called cdw_sapp_transaction
    TRANSACTION_ID INT NOT NULL, -- unique transaction number for each record which is required
    `DAY` INT NOT NULL, -- day value for when the transaction happened which is required
    `MONTH` INT NOT NULL, -- month value for the transaction date which is required
    `YEAR` INT NOT NULL, -- year value for the transaction date which is required
    CREDIT_CARD_NO VARCHAR(16) NOT NULL, -- credit card number used in the transaction which is required
    CUST_SSN VARCHAR(11) NOT NULL, -- ssn that connects this transaction back to a customer in cdw_sapp_customers which is required
    BRANCH_CODE VARCHAR(10) NOT NULL, -- code for which branch processed this transaction which is required
    TRANSACTION_TYPE VARCHAR(50) NOT NULL, -- type of transaction (shopping, healthcare, education, etc.) which is required
    TRANSACTION_VALUE_OLD DECIMAL(10, 2) NOT NULL, -- old column from original data that does not exist in cleaned version (will be dropped later) which is required
    TRANSACTION_AMOUNT DECIMAL(10, 2) NOT NULL, -- actual transaction amount in dollars which is required
    MERCHANT_CATEGORY VARCHAR(50) NOT NULL, -- category of the merchant for this transaction which is required
    MERCHANT_LOCATION VARCHAR(100) NOT NULL, -- city or location where the merchant is based which is required
    PAYMENT_METHOD VARCHAR(50) NOT NULL, -- payment method (credit, online, etc.) which is required
    IS_ONLINE BOOLEAN NOT NULL, -- whether the transaction was done online (true or false) which is required
    FRAUDULENT BOOLEAN NOT NULL, -- whether this transaction was flagged as fraudulent which is required
    `Time` TIME -- time of the transaction (as initially designed; will be corrected later in the walkthrough)
);

-- ==============================================
-- LOAD DATA INTO CDW_SAPP_CUSTOMERS
-- ==============================================

-- load all customer records from the cleaned csv file into cdw_sapp_customers
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cdw_sapp_customer_cleaned.csv' -- path to the csv file being imported
INTO TABLE cdw_sapp_customers -- specify which table to load data into
FIELDS TERMINATED BY ','  -- each column value in the csv is separated by a comma
OPTIONALLY ENCLOSED BY '"'  -- keep text values inside quotes as single entries
LINES TERMINATED BY '\r\n'  -- rows in csv end with windows-style newline
IGNORE 1 ROWS  -- skip header row
(FIRST_NAME, MIDDLE_NAME, LAST_NAME, SSN, CREDIT_CARD_NO, APT_NO, STREET_NAME,
 CUST_CITY, CUST_STATE, CUST_COUNTRY, CUST_ZIP, CUST_PHONE, CUST_EMAIL, Customer_ID); -- list of columns in order matching the csv file

-- ==============================================
-- LOAD DATA INTO CDW_SAPP_TRANSACTION (INITIAL ATTEMPT)
-- note: to allow “run all” and also let the initial attempt demonstrate the failure,
-- we temporarily relax strict mode so the bad rows produce warnings instead of stopping the script.
-- if you want to reproduce a hard error line-by-line, restore strict mode before running this block.
-- ==============================================

SET @old_sql_mode := @@SESSION.sql_mode; -- remember current session sql_mode
SET SESSION sql_mode := REPLACE(@@SESSION.sql_mode, 'STRICT_TRANS_TABLES', ''); -- allow warnings instead of errors for this load
SET SESSION sql_mode := REPLACE(@@SESSION.sql_mode, 'STRICT_ALL_TABLES', ''); -- ensure non-strict behavior for the initial load

-- try to load the transaction data into cdw_sapp_transaction (this will generate warnings in non-strict mode)
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cdw_sapp_transaction_cleaned.csv' -- path to the cleaned transaction csv file
INTO TABLE cdw_sapp_transaction -- specify which table to load data into
FIELDS TERMINATED BY ','  -- each column value in the csv is separated by a comma
OPTIONALLY ENCLOSED BY '"'  -- keep text values inside quotes as single entries
LINES TERMINATED BY '\r\n'  -- rows in csv end with windows-style newline
IGNORE 1 ROWS  -- skip header row
(TRANSACTION_ID, `DAY`, `MONTH`, `YEAR`, CREDIT_CARD_NO, CUST_SSN, BRANCH_CODE, 
 TRANSACTION_TYPE, TRANSACTION_VALUE_OLD, TRANSACTION_AMOUNT, MERCHANT_CATEGORY, 
 MERCHANT_LOCATION, PAYMENT_METHOD, IS_ONLINE, FRAUDULENT, `TIME`); -- list of columns in order matching the csv file

-- restore original strictness before applying fixes, so downstream queries behave normally
SET SESSION sql_mode := @old_sql_mode; -- restore the original session mode

-- ==============================================
-- FIX TABLE STRUCTURE (REMOVE MISSING COLUMN)
-- ==============================================

ALTER TABLE cdw_sapp_transaction -- drop extra column so csv columns align with table
DROP COLUMN TRANSACTION_VALUE_OLD; -- prevents values like 'clothing' from shifting into transaction_amount

-- ==============================================
-- FIX DATA TYPE MISMATCH FOR TIME FIELD
-- ==============================================

ALTER TABLE cdw_sapp_transaction -- change time to text so csv can load full date and time
MODIFY COLUMN `Time` VARCHAR(20); -- allows values like 10/14/2025 6:24 to load as-is

TRUNCATE TABLE cdw_sapp_transaction; -- clear rows from the initial attempt so the corrected reload does not append

-- reload transactions after fixes (now the column list matches the csv order exactly)
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cdw_sapp_transaction_cleaned.csv' -- path to transaction csv after schema fixes
INTO TABLE cdw_sapp_transaction -- table now matches csv exactly
FIELDS TERMINATED BY ','  -- csv is comma separated
OPTIONALLY ENCLOSED BY '"'  -- allow quoted fields
LINES TERMINATED BY '\r\n'  -- windows newline
IGNORE 1 ROWS  -- skip header
(TRANSACTION_ID, `DAY`, `MONTH`, `YEAR`, CREDIT_CARD_NO, CUST_SSN, BRANCH_CODE,
 TRANSACTION_TYPE, TRANSACTION_AMOUNT, MERCHANT_CATEGORY, MERCHANT_LOCATION,
 PAYMENT_METHOD, IS_ONLINE, FRAUDULENT, `TIME`); -- column list matches csv order

UPDATE cdw_sapp_transaction -- convert text date and time into mysql datetime
SET `Time` = STR_TO_DATE(`Time`, '%m/%d/%Y %k:%i'); -- becomes 'YYYY-MM-DD HH:MM:SS'

ALTER TABLE cdw_sapp_transaction -- convert time column to DATETIME for accurate time-based analysis
MODIFY COLUMN `Time` DATETIME; -- enables use of functions like HOUR(), MINUTE(), and time-based grouping in future queries

-- ==============================================
-- VERIFY DATA LOAD SUCCESS
-- ==============================================

SELECT * FROM cdw_sapp_customers; -- quick validation of customer load
SELECT * FROM cdw_sapp_transaction; -- quick validation of transaction load

-- ==============================================
-- Functional Requirement 3.6
-- ==============================================

-- Compute total transactions per year+month and include the month name as a string for excel
SELECT
    YEAR, -- extract the year of each transaction
    MONTH, -- extract the month (numeric) of each transaction
    MONTHNAME(STR_TO_DATE(CONCAT(YEAR, '-', LPAD(MONTH, 2, '0'), '-01'), '%Y-%m-%d')) AS Month_Name, -- derive a proper month name from year+month
    COUNT(TRANSACTION_ID) AS total_transactions -- count total transactions for each year+month
FROM cdw_sapp_transaction -- pulls data from the main transaction table
GROUP BY YEAR, MONTH -- group by both year and month to aggregate correctly
ORDER BY total_transactions DESC -- sort from highest to lowest
LIMIT 3; -- show top 3 months with most transactions

-- ==============================================
-- Functional Requirement 3.7
-- ==============================================

-- Retrieve the top 10 customers with the highest total transaction amount
SELECT
    c.FIRST_NAME, -- extracts customer first name from the customers table
    c.LAST_NAME, -- extracts customer last name from the customers table
    SUM(t.TRANSACTION_AMOUNT) AS total_spent -- use the sum aggregation to calculate total spent per customer
FROM cdw_sapp_transaction t -- pulls data from the main transaction table as the data source with an alias of "t"
JOIN cdw_sapp_customers c -- join the customers table with alias "c"
ON t.CUST_SSN = c.SSN -- link tables on ssn
GROUP BY c.FIRST_NAME, c.LAST_NAME, t.CUST_SSN -- group by customer first name, last name, and ssn
ORDER BY total_spent DESC -- sort the total spent largest to smallest
LIMIT 10; -- return only the top 10 customers

-- ==============================================
-- Functional Requirement 3.8
-- ==============================================

-- Retrieve how many transactions are flagged as fraudulent
SELECT
    COUNT(*) AS total_fraudulent_transactions -- counts the total number of fraud cases
FROM cdw_sapp_transaction -- pulls data from the main transaction table
WHERE FRAUDULENT = 1; -- only include rows that are marked as fraudulent (1)

-- step 2: show the total number of fraudulent cases vs non-fraudulent
SELECT
    SUM(CASE WHEN FRAUDULENT = 1 THEN 1 ELSE 0 END) AS Fraudulent, -- count how many rows have fraudulent = 1 (fraud transactions)
    SUM(CASE WHEN FRAUDULENT = 0 THEN 1 ELSE 0 END) AS Non_Fraudulent -- count how many rows have fraudulent = 0 (non-fraud transactions)
FROM cdw_sapp_transaction; -- pulls data from the main transaction table

-- ==============================================
-- Functional Requirement 3.9
-- ==============================================

-- Return fraudulent and non-fraudulent rates in decimal form for excel percentage formatting
SELECT
  ROUND(SUM(CASE WHEN FRAUDULENT = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 4) AS Fraudulent_Rate, -- percent of all transactions that are fraud; NULLIF(COUNT(*),0) prevents divide by zero
  ROUND(1 - (SUM(CASE WHEN FRAUDULENT = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)), 4) AS Non_Fraudulent_Rate -- percent of all transactions that are not fraud; NULLIF(COUNT(*),0) prevents divide by zero
FROM cdw_sapp_transaction; -- use data from transaction table  

-- ==============================================
-- Functional Requirement 3.10
-- ==============================================

-- Compare online fraudulent transactions to total transactions for visual analysis
SELECT
    COUNT(*) AS Online_Fraudulent_Count, -- total transactions that are both online and fraudulent
    (SELECT COUNT(*) FROM cdw_sapp_transaction) AS Total_Transactions -- grand total of all transactions
FROM cdw_sapp_transaction -- pulls data from the main transaction table
WHERE IS_ONLINE = 1 AND FRAUDULENT = 1; -- filter for online + fraudulent

-- ==============================================
-- Functional Requirement 3.11
-- ==============================================

-- List all customers with more than 7 fraudulent transactions
SELECT CONCAT(c.FIRST_NAME, ' ', c.LAST_NAME) AS Customer_Name, -- combine the customer's first name and last name into one name (this will be so the chart will show the full name)
       COUNT(t.TRANSACTION_ID) AS Fraudulent_Count -- total number of fraudulent transactions per customer
FROM cdw_sapp_transaction t -- pulls data from the main transaction table with alias "t"
JOIN cdw_sapp_customers c -- join to customer table, with alias "c" for name lookup
	ON t.CUST_SSN = c.SSN -- match customer ssn between tables
WHERE t.FRAUDULENT = 1 -- include only transactions marked as fraudulent
GROUP BY t.CUST_SSN, c.FIRST_NAME, c.LAST_NAME -- group by each customer's SSN, first name, and last name
HAVING COUNT(t.TRANSACTION_ID) > 7 -- show only those with more than 7 fraudulent transactions
ORDER BY Fraudulent_Count DESC; -- rank results from most to least fraudulent activity

-- ==============================================
-- Functional Requirement 4.1
-- ==============================================

-- Identify the time of day when most fraudulent transactions occur
SELECT
    HOUR(Time) AS Transaction_Hour, -- extract the hour (0–23) from the Time column
    COUNT(*) AS Fraudulent_Count -- count how many fraudulent transactions occurred in each hour
FROM cdw_sapp_transaction -- pulls data from the main transaction table
WHERE Fraudulent = 1 -- include only fraudulent transactions
GROUP BY HOUR(Time) -- group results by each hour of the day
ORDER BY Fraudulent_Count DESC; -- rank hours by number of frauds, highest first

-- ==============================================
-- Functional Requirement 4.2
-- ==============================================

-- Step 1: count how many fraudulent transactions occur on each day of the week
SELECT
    DAYNAME(CONCAT(YEAR, '-', MONTH, '-', DAY)) AS Fraud_Day, -- converts date parts into a full date and extracts weekday name
    COUNT(*) AS Fraudulent_Count -- counts how many fraudulent transactions occurred on each weekday
FROM cdw_sapp_transaction -- pulls data from the main transaction table
WHERE Fraudulent = 1 -- filters only fraudulent transactions
GROUP BY Fraud_Day -- groups results by day of week
ORDER BY Fraudulent_Count DESC; -- sorts so the most frequent day appears first

-- Step 2: find which month has the highest percentage of fraudulent transactions
SELECT
    MONTHNAME(STR_TO_DATE(CONCAT('2024-', LPAD(MONTH, 2, '0'), '-01'), '%Y-%m-%d')) AS Month_Name, -- builds a full date like '2024-01-01' so monthname() recognizes it
    COUNT(CASE WHEN Fraudulent = 1 THEN 1 END) AS Fraudulent_Count, -- counts only fraudulent transactions
    COUNT(*) AS Total_Transactions, -- total transactions for that month
  ROUND((COUNT(CASE WHEN Fraudulent = 1 THEN 1 END) / COUNT(*)), 4) AS Fraudulent_Percentage -- calculates % fraud per month
FROM cdw_sapp_transaction -- pulls data from the main transaction table
GROUP BY MONTH, Month_Name -- groups by month name
ORDER BY CAST(MONTH AS UNSIGNED); -- ensures months appear jan–dec in correct order

-- ==============================================
-- Functional Requirement 4.3
-- ==============================================

-- Compare average transaction amounts for fraudulent vs non-fraudulent transactions
SELECT
    Transaction_Type, -- the fraud label created in the subquery ('Fraudulent' or 'Non-Fraudulent')
    ROUND(AVG(TRANSACTION_AMOUNT), 2) AS Avg_Transaction_Amount -- calculates the average transaction value for each type, rounded to 2 decimals
FROM (
    SELECT
        CASE
            WHEN Fraudulent = 1 THEN 'Fraudulent' -- label transactions as 'Fraudulent' when the fraudulent flag equals 1
            ELSE 'Non-Fraudulent' -- label all remaining transactions as 'Non-Fraudulent'
        END AS Transaction_Type, -- creates a temporary alias for use in the main query
        TRANSACTION_AMOUNT -- selects the amount column used for averaging
    FROM cdw_sapp_transaction -- pulls data from the main transaction table
    WHERE TRANSACTION_AMOUNT IS NOT NULL -- filters out any null or missing values that could affect the average
) AS sub -- creates a subquery (temporary dataset) named 'sub' for cleaner grouping
GROUP BY Transaction_Type; -- groups by the fraud label to return one row per type

-- ==============================================
-- Functional Requirement 4.4
-- ==============================================

-- Identify total and fraudulent transactions over $4000
SELECT
    COUNT(*) AS Total_Transactions_Over_4000, -- total transactions greater than $4000
    SUM(CASE WHEN Fraudulent = 1 THEN 1 ELSE 0 END) AS Fraudulent_Transactions_Over_4000 -- fraudulent subset count
FROM cdw_sapp_transaction -- pulls data from the main transaction table
WHERE TRANSACTION_AMOUNT > 4000; -- filters only high-value transactions