# coal_study

This project involves parsing and scraping text data on climate change and coal energy from ProQuest Congressional and ProQuest Newspaper indexes utilizing the Selenium and Beautiful Soup packages for Python. 

Included in this project are four main component files: **coal_query.sql**, **congress_parser.py**, **congress_scraper.py**, and **news_parser.py**

The script **coal_query.sql** contains the PostgreSQL queries used to create the tables and views needed for storage and analysis of text data.

The script **congress_parser.py** parses text data from ProQuest Congressional and inserts the transcript date of publication and url of Congressional hearings into a PostgreSQL local host. 

The script **congress_scraper.py** uses the table created by congress_parser.py to retrieve the url; scrapes the associated webpage text data from ProQuest Congressional; and inserts the document title, committee, text, and url into a PostgreSQL local host. (Note: To be able to run, this script requires ChromeDriver to be installed in its local directory. For more information, see: https://chromedriver.chromium.org/).

The script **news_parser.py** parses text data from ProQuest Newspaper and inserts the newspaper article ID and text into a PostgreSQL local host.
