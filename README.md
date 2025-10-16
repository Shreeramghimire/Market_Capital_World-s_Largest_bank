# Market_Capital_World-s_Largest_bank

## Largest Banks Market Capitalization ETL Project
### Project Overview
This project implements an automated ETL (Extract, Transform, Load) pipeline to compile and analyze the top 10 largest banks in the world ranked by market capitalization. The system extracts data from Wikipedia, transforms currency values using exchange rates, and loads the processed data into both CSV files and SQLite databases.

### Key Features
Web Scraping: Automatically extracts bank data from Wikipedia tables

Currency Conversion: Converts market capitalization from USD to GBP, EUR, and INR

Data Storage: Saves processed data to CSV files and SQLite databases

Logging System: Comprehensive logging of all ETL stages

Query Interface: Executes analytical queries on the database

Quarterly Automation: Designed for regular financial quarter execution

### Technical Stack
Python 3 with pandas, BeautifulSoup, requests, sqlite3

Web Scraping from archived Wikipedia pages

SQLite Database for data persistence

CSV Integration for exchange rates and output

### Data Processing Pipeline
Extract: Scrapes top 10 banks by market cap from Wikipedia

Transform:

Cleans market cap values (removes newline characters)

Converts USD values to GBP, EUR, INR using exchange rates

Rounds values to 2 decimal places

Load: Stores data in CSV format and SQLite database

Analyze: Runs verification queries and generates reports

### Output Files
Largest_banks_data.csv: Processed data with multiple currency values

Banks.db: SQLite database with complete bank information

code_log.txt: Detailed execution log with timestamps

exchange_rate.csv: Currency conversion rates

### Use Case
Financial analysts can use this automated system to generate quarterly reports on global bank rankings, track market capitalization trends, and perform currency-based comparative analysis.

### Project Structure
Modular ETL functions including:

extract(): Data scraping from web

transform(): Currency conversion and cleaning

load_to_csv() & load_to_db(): Data storage

run_queries(): Data analysis and verification

log_progress(): Execution tracking

This project demonstrates end-to-end data engineering skills including web scraping, data transformation, database management, and automation.

