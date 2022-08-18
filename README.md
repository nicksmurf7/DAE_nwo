# DAE Case Study for NWO.ai
This repository contains code that will scrape the the list of "Top Rated English Movies by Genre" from imdb.com.

# Read before running program:
A locally running PostgreSQL database is necessary for this program to work. 

Moreover, before running the program, a few of the credentials need to be changed. Firstly, in 'main.py',
the name of the database, the name of the user and the password need to be changed to match that of your local host.

Apologies for any log level warnings that might show up. 

# To run the program:
'python3 main.py'
OR
'chmod u+x run.sh' THEN './run.sh'

# Schema Design Pattern:
STAR schema because a single, large table is used to store both numerical and categorical data. 

# Extensions
The functionality to scrape each synopsis is written, but to limit runtime and workload I refrained from adding
each synopsis to the database.
In the same light,ratings and reviews of each movie could be scraped in order to perform sentiment analyis. 
A lot more preprocessing could have been done to expedite future analysis on this dataset. For example, as it stands,
there are only strings in this database. To perform analysis, it is likely that features such as rating and release year
should be translated to numerical data. 