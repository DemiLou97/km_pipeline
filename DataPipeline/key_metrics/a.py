
import argparse

from func import*

### main

if __name__== "__main__":
    pass

parser = argparse.ArgumentParser()

parser.add_argument("--tag", help="laz_biz , shp_biz , shp_overall , tiki_msb , tiki_seller_center ")    

args = parser.parse_args()

tag_name=str(args.tag)

print(tag_name)

#processing 

### lazada 
if tag_name=="laz_biz":
    
    ## path 
    path_data='/mnt/d/ubuntu/DataPipeline/Data/platform_lazada/business_advisor/raw/*'
    
    ## clean data
    laz_biz=laz_biz(path_data)
    
    ## push to database
    update_db(laz_biz,tag_name)
    
### shopee overall
elif tag_name=="shp_biz":
    
    ## path
    path_data='/mnt/d/ubuntu/DataPipeline/Data/platform_shopee/business_advisor/raw/overall/*'
    
    ## clean data
    shp_biz=shp_biz(path_data)
    
    ## push to database
    update_db(shp_biz,tag_name)

### shopee product
elif tag_name =="shp_overall":
    
    ## path
    path_data='/mnt/d/ubuntu/DataPipeline/Data/platform_shopee/business_advisor/raw/product/*'
    
    ## clean data
    shp_overall=shp_overall(path_data)
    
    ## push to database
    
    update_db(shp_overall,tag_name)

### tiki msb
elif tag_name=="tiki_msb":
    
    ## path 
    path_data='/mnt/d/ubuntu/DataPipeline/Data/platform_tiki/business_advisor/raw/msb/*'
    
    ## clean data
    tiki_msb=tiki_msb(path_data)
    
    ## push to database
    update_db(tiki_msb,tag_name)

### tiki seller centre
elif tag_name=="tiki_seller_center":
    
    ## path
    path_data='/mnt/d/ubuntu/DataPipeline/Data/platform_tiki/business_advisor/raw/seller center/*'
    
    ## clean data
    tiki_seller_center=tiki_seller_center(path_data)
    
    ## push to database
    update_db(tiki_seller_center,tag_name)

else : 
    pass 


