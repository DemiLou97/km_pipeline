
import pandas as pd 
import glob
import warnings
warnings.filterwarnings('ignore')

from sqlalchemy import create_engine


# Credentials to database connection
hostname="172.22.114.8"
dbname="key_metrics"
uname="t"
pwd="z"

# Create SQLAlchemy engine to connect to MySQL Database

engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
				.format(host=hostname, db=dbname, user=uname, pw=pwd))



data=glob.glob('/mnt/d/ubuntu/test/key_metrics/*')

for x in data:
    
    #create name to database
    
    db_name=x.split('/')[-1].split('.xls')[0].split('.20')[0].replace('.','_')
    
    #read dataframe
    tmp=pd.read_excel(x)

    tmp.columns=[ x.replace('# of', '').strip().upper().replace(' ', '_').replace('(', '').replace(')', '').replace('/', '_').replace('）','').replace('.','_') 
        
                    for x in tmp.columns]

    
    # Convert dataframe to sql table                          
         
    tmp.to_sql(db_name, engine, index=False)
    
    print(db_name)

    print(tmp.columns)








    
