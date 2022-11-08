from sqlite3 import Cursor
import pymysql
import config
import pandas as pd
from sqlalchemy import create_engine

cities = ["indianapolis", "louisville", "pittsburgh", "chicago", "detroit", "cincinnati"]

def mysql_get_mydb():
    try:
        database = pymysql.connect(
            host=config.HOST,
            user=config.USER,
            passwd=config.PASSWD,
            db=config.DB
        )
    finally:
        return database

def create_tables(city):
    db = mysql_get_mydb()
    cursor = db.cursor()

    try:
        with cursor:
            create_zillow = """
                CREATE TABLE zillow_properties_"""+city+"""(
                    ID INT AUTO_INCREMENT PRIMARY KEY,
                    id VARCHAR(500) NOT NULL,
                    address VARCHAR(500) NOT NULL,
                    beds    VARCHAR(500) NOT NULL,
                    baths   VARCHAR(500)  NOT NULL,
                    area    VARCHAR(500) NOT NULL,
                    price   VARCHAR(500) NOT NULL,
                    zestimate   VARCHAR(500) NOT NULL,
                    best_deal   VARCHAR(500) NOT NULL
                )
            """

            cursor.execute(create_zillow)
            db.commit()
    except pymysql.err.OperationalError as e:
        print(e)
        
def add_zillow(df, city):
    db = mysql_get_mydb()
    cursor = db.cursor()

    conn = create_engine("mysql+mysqldb://" +config.USER + ":" + config.PASSWD + "@" + config.HOST + "/" + config.DB)

    table_name = "zillow_properties_"+city

    df.astype(str).to_sql(name=table_name, con=conn, if_exists='append')


    print("Dataframe created successfully.")
    sql = "INSERT INTO zillow_properties_"+city+"(id, address, beds, baths, area, price, zestimate, best_deal) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

    with cursor:
        cursor.execute(sql, (df["id"], df["address"], df["beds"], df["baths"], df["area"], df["price"], df["zestimate"], df["best_deal"]))
        db.commit()