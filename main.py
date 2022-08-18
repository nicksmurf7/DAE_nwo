import scraper
import load
import os
import psycopg2

# Given URL from DAE Case Study specification
base_url = 'https://www.imdb.com/feature/genre?ref_=fn_asr_ge'


def main():
    # Create directory to hold data locally
    os.mkdir('csvs')
    # Get data from the list of "Top Rated English Movies by Genre" 
    scraper.extract_links(base_url)
    # Establish a connection to the db
    conn = psycopg2.connect(
        host = "localhost",
        database = "nwo_imdb",
        user = "postgres",
        password = "newpassword")
    print("Connection to PostgreSQL created", "\n")

    # Creates cursor to communicate with Postgres
    cur = conn.cursor()
    table_name = "movie_table"
    spark = load.load_Spark()
    print("Initialized spark")
    try:
        df = load.loadSchema(spark)
        print("DF loaded with Schema")
    except:
        print("DF could not load with Schema")
        cur.close()
        conn.close()
        return 0
    df = df.fillna('NA')

    # Load table into PostgreSQL
    try:
        load.load_table(cur)
        conn.commit()
        print("Created table")
    except:
        print("Error creating table")
        cur.close()
        conn.close()
        return 0
    # Write SQL to populate table
    query, seq = load.write_sql(df, cur)
    print("SQL written")    
    # Execute SQL
    try:
        q = cur.mogrify(query, seq).decode('utf8')
        cur.execute(q)
        print("Data inserted into PostgreSQL")
    except:
        print("Data could not be inserted into PostgreSQL")
        cur.close()
        conn.close()
        return 0
    # Validate table being filled
    load.validate_db(cur)
    cur.close()
    print("Changes commited to database")
    conn.commit()
    print("Closing connection")
    conn.close()
    print("Done!")


if __name__ == "__main__":
    main()

