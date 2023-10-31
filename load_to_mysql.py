from sqlalchemy import create_engine
import pymysql
import pandas as pd

df= pd.read_csv("financial_dataset.csv",nrows=10000)


sqlEngine = create_engine('mysql+pymysql://subrat:3aajqi9tfwuwJucBQu4Y@stylicart.cadvwxskvyn3.us-east-1.rds.amazonaws.com/test', pool_recycle=3600)

dbConnection = sqlEngine.connect()
tableName="financial"

try:
    
    df.to_sql(tableName,dbConnection,if_exists='replace')

except ValueError as vx:

    print(vx)

except Exception as ex:   

    print(ex)

else:

    print("Table %s created successfully."%tableName);   

finally:

    dbConnection.close()