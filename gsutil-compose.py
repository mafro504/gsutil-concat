##########################################################################################
###### PLEASE UPDATE ACCORDINGLY #########################################################
##########################################################################################

#CONSTANTS. PLEASE UPDATE TO MATCH THE DIRECTORY LOCATION AND FILE NAME OF YOUR FILES IN GCS
MY_PROJECT = 'twttr-mia-amp-samba-dev' #uncomment the relevant project name as needed
#MY_PROJECT = 'twttr-mia-pulsar-prod'
BUCKET_ = 'sambadata' #uncomment the relevant bucket as needed
#BUCKET_ = 'pulsardata'
VENDOR_ = 'Samba' #uncomment the relevant bucket as needed
#VENDOR_ = 'Pulsar'
SHOWNAME_ = 'XXXXXXXXX' #for pulsar, enter campaign name here
QUARTER_ = '2021Q1'
FILETYPE_ = 'controltest' #update to controltest, earned, cohort1, cohort2, cohort3, or cohort4 as needed
ADVERTISER_ = 'XXX'
START_FILE = 'YYYYMMDD' 
END_FILE = 'YYYYMMDD'

##########################################################################################
##########################################################################################
##########################################################################################

# imports
import numpy as np
import pandas as pd
from pandas import Series, DataFrame

from datetime import datetime
from datetime import timedelta

from google.cloud import bigquery
import gcsfs

import time

# import dask.dataframe as dd
import subprocess



# setup BigQuery client to connect to project
client = bigquery.Client(project=MY_PROJECT)


#combine files in GCP bucket based on above study variables

bashCommand = "gsutil du gs://{bucket_}/{show_}/{quart_}/{vendor_}_Data_{adv_}_{start_file_}_{end_file_}_{type_}_*.csv | wc -l".format( bucket_ = BUCKET_, 
                                                                                                                                        show_ = SHOWNAME_, 
                                                                                                                                        quart_ = QUARTER_,
                                                                                                                                        adv_ = ADVERTISER_,
                                                                                                                                        vendor_ = VENDOR_,
                                                                                                                                        start_file_ = START_FILE,
                                                                                                                                        end_file_ = END_FILE,
                                                                                                                                        type_ = FILETYPE_)


process = subprocess.Popen(bashCommand, 
                           stdout=subprocess.PIPE, 
                           shell=True)
output, error = process.communicate()
output = output.decode('utf-8')
output = output.strip()



total_shards = int(output) 
print ("total files to combine is",total_shards)
if total_shards > 1:

    bashCommand = "gsutil compose gs://{bucket_}/{show_}/{quart_}/{vendor_}_Data_{adv_}_{start_file_}_{end_file_}_{type_}_000000000000.csv \
                                  gs://{bucket_}/{show_}/{quart_}/{vendor_}_Data_{adv_}_{start_file_}_{end_file_}_{type_}_000000000001.csv \
                                  gs://{bucket_}/{show_}/{quart_}/{vendor_}_Data_{adv_}_{start_file_}_{end_file_}_{type_}.csv".format(  bucket_ = BUCKET_, 
                                                                                                                                        show_ = SHOWNAME_, 
                                                                                                                                        adv_ = ADVERTISER_,
                                                                                                                                        quart_ = QUARTER_,
                                                                                                                                        vendor_ = VENDOR_,
                                                                                                                                        start_file_ = START_FILE,
                                                                                                                                        end_file_ = END_FILE,
                                                                                                                                        type_ = FILETYPE_)

    subprocess.call(bashCommand, 
                               stdout=subprocess.PIPE, 
                               shell=True)




    for i in range(2,total_shards):
        bashCommand = "gsutil compose gs://{bucket_}/{show_}/{quart_}/{vendor_}_Data_{adv_}_{start_file_}_{end_file_}_{type_}.csv \
        gs://{bucket_}/{show_}/{quart_}/{vendor_}_Data_{adv_}_{start_file_}_{end_file_}_{type_}_{partition_}.csv \
        gs://{bucket_}/{show_}/{quart_}/{vendor_}_Data_{adv_}_{start_file_}_{end_file_}_{type_}.csv".format(  bucket_ = BUCKET_, 
                                                                                                              show_ = SHOWNAME_, 
                                                                                                              adv_ = ADVERTISER_,
                                                                                                              quart_ = QUARTER_,
                                                                                                              vendor_ = VENDOR_,
                                                                                                              start_file_ = START_FILE,
                                                                                                              end_file_ = END_FILE,
                                                                                                              type_ = FILETYPE_,
                                                                                                              partition_ = str(i).zfill(12))
                                                                                            

        subprocess.call(bashCommand, 
                        stdout=subprocess.PIPE, 
                        shell=True)
