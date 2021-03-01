# Week 9: PySpark Lecture Starter Files

Throughout this week we will cover higher level data engineering concepts.
Our main focus for the week will be:

1. What does ETL stand for?
2. How do we create an ETL pipeline using Pandas?
3. How do we create an ETL pipeline using PySpark
    - What is PySpark
    - How do we use it
    - What are the components of Spark?
    - How do we get up and running with pySpark and Google Cloud Platform (GCP)

## To Get Started:
- Verify Your Java Install
    - `java -version`
    - `javac -version`

- Verify Needed Environment Variables
    - JAVA_HOME
        - `windows`: `echo %JAVA_HOME%`
        - `mac/linux`: `echo $JAVA_HOME`

    - SPARK_HOME
        - `windows`:`echo %SPARK_HOME%`
        - `mac/linux`: `echo $SPARK_HOME`

    - HADOOP_HOME
        - `windows`:`echo %HADOOP_HOME%`
        - `mac/linux`:`echo $HADOOP_HOME`

    Once Verified:
    1. `pip install -r requirements.txt`
    2. Create Spotify Project
        - [Spotify-App-Dashboard](https://developer.spotify.com/dashboard/applications)
        - Create `.env` file
    3. Get Spotify Project Creds
    4. Place the Spotify Project Creds into `.env` file
    5. Create Database
        - Create `pandas_etl` Database in PGAdmin4
        - Create tables using the `CREATE TABLE` query in `pandas_etl.sql` (paste into PGAdmin4)