# This is the MapReduce Programming Assignment Submited By:
# Member 1: Nikhil Dhiman (nikhildhiman3644@gmail.com)
# Member 2: Rohan kulkarni (rohan.kulkarni951@gmail.com)
# Member 3: Mahesh Kumar Patra (maheshkumarpatra5@gmail.com)

# To run this program:-
# Create a table in HBase with name as- 'trip_record' and column family as- 'trip_details'
# This input files name should start with 'yellow_tripdata_'
# This python script can be run simply by typing: 'python batch_ingest.py' 
# Note- Both this file and csv data should be on same directory

import happybase as hb
import os

#Creating connection to HBase using Thrift Server
conn = hb.Connection('localhost')

def openConnectionHbase():
    #Function open connecttion 
    conn.open()

def closeConnectionHbase():
    #Function to close connecttion
    conn.close()

def setHbaseTableName(name):
    #Set connection to table
    openConnectionHbase()
    table = conn.table(name)
    closeConnectionHbase()
    return table

def insertBatchData(filename):
    #Start batch insert
    print("******Started bulk import********")
    #Open file
    selectedFile = open(filename, "r")
    #Open habse table 
    tableName = 'trip_record'
    table = setHbaseTableName(tableName)
    #Open hbase connection
    openConnectionHbase()
    #set row count to zero
    rowCount = 0
    #set batch set to 4000
    with table.batch(batch_size=4000) as tab:
        #Insert data row by row
        for line in selectedFile:
            if rowCount!=0:
                row = line.strip().split(",")
                tab.put(row[0]+"_"+row[1]+"_"+row[2],  { "trip_details:VendorID": row[0], "trip_details:tpep_pickup_datetime": row[1], "trip_details:tpep_dropoff_datetime": row[2], "trip_details:passenger_count": row[3], "trip_details:trip_distance": row[4], "trip_details:RatecodeID": row[5], "trip_details:store_and_fwd_flag": row[6], "trip_details:PULocationID": row[7], "trip_details:DOLocationID": row[8], "trip_details:payment_type": row[9], "trip_details:fare_amount": row[10], "trip_details:extra": row[11], "trip_details:mta_tax": row[12], "trip_details:tip_amount": row[13], "trip_details:tolls_amount": row[14], "trip_details:improvement_surcharge": row[15], "trip_details:total_amount": row[16], "trip_details:congestion_surcharge": row[17], "trip_details:airport_fee": row[18]})
            rowCount+=1
        #Close file    
        selectedFile.close()
        print("******Completed bulk import********")
        #Close connection
        closeConnectionHbase()

#Read all files starting with 'yellow_tripdata_'
for filename in os.listdir():
     if filename.startswith("yellow_tripdata_"):
            print('Loading Data from file:'+filename)
            insertBatchData(filename)
