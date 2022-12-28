import pandas as pd

import glob  

import os 

import shutil

path=glob.glob('/mnt/d/ubuntu/DataPipeline/download/*xls*')

for x in path :
    
    ### lazada
    if 'Business Advisor - Product - Performance' in x:
        # get date
        tmp_date=pd.read_excel(x, header=0, )
        date=tmp_date.columns[0].split(':')[-1].split('_')[-1]
        # rename
        na='/mnt/d/ubuntu/DataPipeline/download/laz_biz_'+date+'.xls'
        os.rename(x,na)
        # move 
        des='/mnt/d/ubuntu/DataPipeline/Data/platform_lazada/business_advisor/raw/laz_biz_'+date+'.xls'
        shutil.move(na,des)
        
    ### shopee overall    
    elif 'tefal_vn_official.shopee' in x:
        # get date
        date=x.split('.')[-2]
        # date=tmp[:4]+'-'+tmp[4:6]+'-'+tmp[6:8]+'_'+tmp[9:13]+'-'+tmp[13:15]+'-'+tmp[15:17]
        # rename
        na='/mnt/d/ubuntu/DataPipeline/download/shp_biz_overall_'+date+'.xlsx'
        os.rename(x,na)
        # move 
        des='/mnt/d/ubuntu/DataPipeline/Data/platform_shopee/business_advisor/raw/overall/shp_biz_overall_'+date+'.xlsx'
        shutil.move(na,des) 
         
    ### shopee product
    elif 'export_report.parentskudetail' in x:
        # get date
        date=x.split('.')[-2].replace('_','-')
        # date=tmp[:4]+'-'+tmp[4:6]+'-'+tmp[6:8]+'_'+tmp[9:13]+'-'+tmp[13:15]+'-'+tmp[15:17]
        # rename
        na='/mnt/d/ubuntu/DataPipeline/download/shp_biz_product_'+date+'.xlsx'
        os.rename(x,na)
        # move 
        des='/mnt/d/ubuntu/DataPipeline/Data/platform_shopee/business_advisor/raw/product/shp_biz_product_'+date+'.xlsx'
        shutil.move(na,des)
        
    ### tiki msb
    elif 'SKU performance' in x:
        # get date
        date=(x.split(' ')[-1]).split('.xlsx')[0].replace('_','-')
        #date=tmp[:4]+'-'+tmp[4:6]+'-'+tmp[6:8]+'_'+tmp[9:13]+'-'+tmp[13:15]+'-'+tmp[15:17]
        # rename
        na='/mnt/d/ubuntu/DataPipeline/download/tiki_biz_msb_'+date+'.xlsx'
        os.rename(x,na)
        # move 
        des='/mnt/d/ubuntu/DataPipeline/Data/platform_tiki/business_advisor/raw/msb/tiki_biz_msb_'+date+'.xlsx'
        shutil.move(na,des)
        
    ### tiki seller centre
    elif 'Tefal.Official.Store' in x:
        date=x.split('.')[-2]
        # date=tmp[:4]+'-'+tmp[4:6]+'-'+tmp[6:8]
        # rename
        na='/mnt/d/ubuntu/DataPipeline/download/tiki_biz_seller-center_'+date+'.xlsx'   
        os.rename(x,na)
        # move 
        des='/mnt/d/ubuntu/DataPipeline/Data/platform_tiki/business_advisor/raw/seller center/tiki_biz_seller-center_'+date+'.xlsx'
    else: 
        # get date
        tmp=(x.split('download/')[-1]).split('.xls')[0]
        date=tmp[4:8]+tmp[2:4]+tmp[:2]
        # rename
        na='/mnt/d/ubuntu/DataPipeline/download/laz_biz_'+date+'.xls'
        os.rename(x,na)
        # move 
        des='/mnt/d/ubuntu/DataPipeline/Data/platform_lazada/business_advisor/raw/laz_biz_'+date+'.xls'
        shutil.move(na,des)
        
    
    
    
    
    