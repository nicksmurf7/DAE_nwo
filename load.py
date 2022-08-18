import scraper
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType, TimestampType, ShortType, DateType
from pyspark.sql.functions import col


# Initializes spark session
def load_Spark():

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("simple etl job") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("FATAL")
    return spark

# Loads proper schema into spark session
def loadSchema(spark):
    schema = StructType([
        StructField("title", StringType(), True),
        StructField("rating", StringType(), True),
        StructField("year", StringType(), True),
        StructField("genre_1", StringType(), True),
        StructField("genre_2", StringType(), True),
        StructField("genre_3", StringType(), True),
        StructField("director", StringType(), True),
        StructField("cast_1", StringType(), True),
        StructField("cast_2", StringType(), True),
        StructField("cast_3", StringType(), True),
        StructField("cast_4", StringType(), True),
    ])
    path = ['csvs/1.csv',
            'csvs/2.csv',
            'csvs/3.csv',
            'csvs/4.csv',
            'csvs/5.csv',
            'csvs/6.csv',
            'csvs/7.csv',
            'csvs/8.csv',
            'csvs/9.csv',
            'csvs/10.csv',
            'csvs/11.csv',
            'csvs/12.csv',
            'csvs/13.csv',
            'csvs/14.csv',
            'csvs/15.csv',
            'csvs/16.csv',
            'csvs/17.csv',
            'csvs/18.csv',
            'csvs/19.csv',
            'csvs/20.csv',
            'csvs/21.csv',
            'csvs/22.csv',
            'csvs/23.csv',
            'csvs/24.csv']
    df = spark.read.options(header=True).csv(path)
    return df

# Data processing (Add more as more features are collected)
def clean_data(df):
    res = df.fillna('NA')
    return res

# Create table in PostgreSQL if it does not already exist
def load_table(cursor):
    sql_create_table = "CREATE TABLE IF NOT EXISTS movie_table (title VARCHAR(500) NOT NULL,rating VARCHAR(500) NOT NULL,year VARCHAR(500) NOT NULL,genre_1 VARCHAR(500) NOT NULL,genre_2 VARCHAR(500) NOT NULL, genre_3 VARCHAR(500) NOT NULL,director VARCHAR(500) NOT NULL,cast_1 VARCHAR(500) NOT NULL,cast_2 VARCHAR(500) NOT NULL, cast_3 VARCHAR(500) NOT NULL, cast_4 VARCHAR(500) NOT NULL);"
    try:
        cursor.execute(sql_create_table)
        print("Created table in PostgreSQL", "\n")
    except:
        print("Something went wrong when creating the table", "\n")

# Write the SQL needed to insert data into table, executed in main
def write_sql(df, cursor):
    movies_seq = [tuple(x) for x in df.collect()]
    records_list_template = ','.join(['%s'] * len(movies_seq))
    insert_query = "INSERT INTO movie_table (title, rating, year, genre_1, genre_2, genre_3, director, cast_1, cast_2, cast_3, cast_4) VALUES {}".format(records_list_template)
    
    print("Inserting data into PostgreSQL...", "\n")
    return insert_query, movies_seq


# Simple test function to see if table is populated
def validate_db(cursor):
    postgreSQL_select_Query = "SELECT title, year, rating FROM movie_table"
    cursor.execute(postgreSQL_select_Query)
    movies_records = cursor.fetchmany(2)
    print("Printing 2 rows")
    for row in movies_records:
        print("Title = ", row[0])
        print("Year = ", row[1])
        print("Rating  = ", row[2], "\n")
