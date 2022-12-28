
### clean data   
       
# input : data download

# output : data cleaned

def laz_biz(path):
    
    import pandas as pd
    import glob
    import warnings
    warnings.filterwarnings('ignore')
    
    path=glob.glob(path)
    
    columns=['Product Name','Seller SKU','SKU ID','SKU Visitors','SKU Views',
    'Visitor Value','Add to Cart Visitors','Add to Cart Units','Add to Cart CR',
    'Wishlist Visitors','Wishlists','Buyers','Orders','Units Sold','Revenue',
    'Conversion Rate','Revenue per Buyer', 'Product ID', 'B_Date']

    container_aff=[]
    check_existing_date=[]
    for x in path:
        tmp=pd.read_excel(x, header=0, )
        date=tmp.columns[0].split(':')[-1].split('_')[-1]

        ## avoid duplicated files
        if date not in check_existing_date:
            tmp=pd.read_excel(x, header=5)
            tmp=tmp[tmp['Seller SKU']!='-']
            tmp['B_Date']=[date]*tmp.shape[0]
            check_existing_date.append(date)
            tmp=tmp.rename(columns={'Product/SKU Visitors': 'SKU Visitors',
            'Product/SKU Views': 'SKU Views',
            'Add to Cart Conversion Rate': 'Add to Cart CR',
            'Add to Cart Rate': 'Add to Cart CR',})

            if tmp.shape[1]==19:
                tmp['Product ID']=''

            tmp=tmp[columns]
            tmp['Revenue']=tmp['Revenue'].astype(int)
            container_aff.append(tmp)

    business_01=pd.concat(container_aff)
    business_01=business_01.rename(columns={'B_Date': 'Date'})
    business_01.columns=[x.upper() for x in business_01.columns]
    business_01=business_01.sort_values('DATE')
    
    laz_biz=business_01
    
    Date=business_01['DATE'].max()
    
    p='/mnt/d/ubuntu/DataPipeline/Data/platform_lazada/business_advisor/output/laz_biz_'+Date+'.xlsx'
    
    laz_biz.to_excel(p,index=False)
    
    return laz_biz
    
#############################################################################################

def shp_overall(path):
    
    import pandas as pd
    import glob
    import warnings
    warnings.filterwarnings('ignore')
    
    path=glob.glob(path)
    
    tmp=[]
    for x in path:
        y=pd.read_excel(x, sheet_name='Placed Order', dtype=str, skiprows=3)
        tmp.append(y)
    data=pd.concat(tmp)
    data.columns=[x.replace('# of', '').strip().upper().replace(' ', '_').replace('(', '').replace(')', '').replace('(', '') for x in data.columns]
    
    shp_overall=data
    
    Date=data['DATE'].max()
    
    p='/mnt/d/ubuntu/DataPipeline/Data/platform_shopee/business_advisor/output/shp_overall_'+Date+'.xlsx'
    
    shp_overall.to_excel(p,index=False)
    
    return shp_overall

def shp_biz(path):
    
    import pandas as pd
    import glob
    import warnings
    warnings.filterwarnings('ignore')
    
    path=glob.glob(path)
    
    container=[]
    dates=[]
    column=[]
    # column_default=['date', 'Product', 'Item ID', 'Parent SKU', 'Sales (Placed Orders） (VND)',
    #         'Units (Placed Orders)', 'Product Visitors', 'Product Page Views', ]

    number_column=[]
    for x in path:
        tmp=pd.read_excel(x, header=0)
        # print(f'==> column: {number_column.append(tmp.shape[1])}')
        number_column.append(tmp.shape[1])
        date=x.split('/')[-1].split('_')[-1].split('.')[0]
        tmp['date']=[date]*tmp.shape[0]
        ## check columns
        s=''
        for x in tmp.columns:
            s+='_'+x
        column.append(s)

        ## just get some columns
        # tmp=tmp[column_default]
        container.append(tmp)
        
    business_01=pd.concat(container)

    ## convert columm to datetime
    business_01['date'] = pd.to_datetime(business_01['date'], format="%Y%m%d")
    tmp=[int(str(x).replace(".", '')) for x in business_01['Sales (Placed Orders） (VND)'].tolist()]
    business_01['Sales (Placed Orders） (VND)']=tmp
    business_01=business_01.sort_values('date')
    business_01.columns=[x.strip().upper().replace(' ', '_').replace('(', '').replace(')', '').replace('(', '') for x in business_01.columns]
    
    shp_biz=business_01
    
    Date=business_01['DATE'].max()
    
    p='/mnt/d/ubuntu/DataPipeline/Data/platform_shopee/business_advisor/output/shp_biz_'+Date+'.xlsx'
    
    shp_biz.to_excel(p,index=False)
    
    return shp_biz

#############################################################################################


def tiki_seller_center(path):
    
    import pandas as pd
    import glob
    import warnings
    warnings.filterwarnings('ignore')
    
    path=glob.glob(path)

    tmp=[]
    for x in path:
        y=pd.read_excel(x, dtype=str) 
        date=x.split('.')[-2]
        date=date[:4]+'-'+date[4:6]+'-'+date[6:]
        y['DATE']=date
        tmp.append(y)
    seller_center=pd.concat(tmp).sort_values('DATE')
    seller_center.columns=[x.upper() for x in seller_center.columns]
    
    tiki_seller_center=seller_center
    
    Date=seller_center['DATE'].max()
    
    p='/mnt/d/ubuntu/DataPipeline/Data/platform_tiki/business_advisor/output/tiki_seller_center_'+Date+'.xlsx'
    
    tiki_seller_center.to_excel(p,index=False)
    
    return tiki_seller_center
    
def read_business_advisor(path):
    try:
        df_business_advisor=pd.read_excel(open(path, 'rb'), sheet_name='DATA', dtype=str) 
    except:
        df_business_advisor=pd.read_excel(open(path, 'rb'), engine='pyxlsb', sheet_name='DATA', dtype=str) 

    ## 1.c business advisor
    df_business_advisor['confirmed_amount']=[float(x) for x in df_business_advisor['confirmed_amount'].tolist()]
    df_business_advisor['confirmed_order']=[int(x) for x in df_business_advisor['confirmed_order'].tolist()]
    df_business_advisor['pdp_view']=[float(x) for x in df_business_advisor['pdp_view'].tolist()]
    df_business_advisor['date']=[str(x).replace(' thg ', '_').replace(', ', '_') for x in df_business_advisor['date'].tolist()]
    
    ## visitor not support
    df_business_advisor['product_name']=[str(x).strip().lower().replace('.', '') for x in df_business_advisor['product_name'].tolist()]
    # df_business_advisor=df_business_advisor[['date', 'sku', 'product_name', 'confirmed_amount', 'confirmed_order', 'pdp_view']]
    return df_business_advisor


def tiki_msb(path):
    
    import pandas as pd
    import glob
    import warnings
    warnings.filterwarnings('ignore')
    
    path=glob.glob(path)
    
    tmp_df=[]
    for x in path:
        tmp=read_business_advisor(x)
        tmp.columns=[str(x).strip() for x in tmp.columns]
        tmp=tmp[['date', 'CMMF', 'product_name', 
            'confirmed_amount', 'confirmed_quantity', 'confirmed_order', 
        ]]
        tmp_df.append(tmp)

    df_business_advisor=pd.concat(tmp_df)
    df_business_advisor.columns=['DATE', 'CMMF', 'B_PRODUCT_NAME', 'B_GMV', 'B_UNIT_SOLD', 'B_ORDER']
    
    tmp=[]
    for x in df_business_advisor['DATE'].tolist():
        x=x.replace('-', '_').replace(' 00:00:00', '')
        z=x.split('_')

        y=''
        ## month
        if len(z[1])==1:
            y='0'+z[1]
        else:
            y=z[1]
        z.remove(z[1])

        ## year
        for t in z:
            if len(t)==4:
                y=t+'-'+y
                z.remove(t)

        ## date
        if len(z[0])==1:
            y=y+'-0'+z[0]
        else:
            y+='-'+z[0]

        tmp.append(y)

    df_business_advisor['DATE']=tmp
    df_business_advisor.sort_values('DATE')
    
    tiki_msb=df_business_advisor
    
    Date=df_business_advisor['DATE'].max()
    
    p='/mnt/d/ubuntu/DataPipeline/Data/platform_tiki/business_advisor/output/tiki_msb_'+Date+'.xlsx'
    
    tiki_msb.to_excel(p,index=False)
    
    return tiki_msb


#############################################################################################
### push data to database

# input : data cleaned 

# output : data cleaned change column name : push/concat into database 

def update_db(df,table_name):

    from sqlalchemy import create_engine
   
    # Credentials to database connection
        ### server z
    hostname="172.22.114.8"
    dbname="z"    
    uname="lou"                                     ### user in mysql at server z 
    pwd="z"

    # Create SQLAlchemy engine to connect to MySQL Database

    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                    .format(host=hostname, db=dbname, user=uname, pw=pwd))


        ### localhost
    hostname="localhost"
    dbname="z"     ### input from user
    uname="z"                                     ### create in mysql : local / server z
    pwd="z"



    # Create SQLAlchemy engine to connect to MySQL Database

    engine2 = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                    .format(host=hostname, db=dbname, user=uname, pw=pwd))

    # change name of columns 

    df.columns=[ x.replace('# of', '').strip().upper().replace(' ', '_').replace('(', '').replace(')', '').replace('/', '_').replace('）','').replace('.','_') 
        
                    for x in df.columns]




    # Convert dataframe to sql table

    # server z
    # df.to_sql(table_name, engine, index=False, )  
    
    # localhost
    df.to_sql(table_name, engine2, index=False, )
    
    #if_exists='append'
    
    #if_exists='append'