import sys
import os
import csv
import math
import datetime
import zipfile

import pymysql

import pandas as pd
from sqlalchemy import create_engine
from dateutil import tz

from dotenv import load_dotenv


# TODO
# Find proper way (via decode) to avoid ï»¿ at beginning of data
# Find proper way to conver date string with timezone like +00 to local time
# We should assume that time in NYC file is EST, in SanFrancisco file is Pacific time, right?
#
#

load_dotenv()

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pwd = os.getenv('DB_PWD')
db_name = os.getenv('DB_NAME')


filename = sys.argv[1] #filename - make it to handle a directory too

table_name = "911" #move table_name in .env?

# do more index like party, town, status
#dropCountyIndexSql = "drop index county_idx on " + table_name
#createCountyIndexSql = "create index county_idx on " + table_name + "(county)"

print("Connecting to", db_host, db_name)

connection = pymysql.connect(host=db_host, user=db_user, password=db_pwd, db=db_name)

#cursor = connection.cursor(buffered=True)
cursor = connection.cursor()

engine = create_engine("mysql+pymysql://"+db_user+":"+db_pwd+"@"+db_host+"/"+db_name  
                      .format(user=db_user, pw=db_pwd, db=db_name))

print("Processing", filename, str(datetime.datetime.now().time()))

time_1 = datetime.datetime.now()

inData = pd.read_csv(filename, index_col=False, encoding='iso8859_15')

# need to remove first record or how does it become the header?
inData.drop(index=inData.index[0], axis=0, inplace=True)

#print (inData.dtypes)
inData.fillna("", inplace=True)

# dataframe column names need to be the same as DB

inData['dataset_name'] = filename
inData['county'] = None

# These 'settings' can be override by data in dataset
call_type = None
if filename.find('service')  >= 0 or filename.find('srvc') >= 0:
  call_type = 'call for service'
elif filename.find('911') >= 0:
  call_type = '911'

inData['call_type'] = call_type


agency = None
if filename.find('police') >= 0 or filename.find('pd') >= 0:
  agency = 'police'
elif filename.find('fire') >= 0 or filename.find('fd') >= 0:
  agency = 'fire'
elif filename.find('ems') >= 0:
  agency = 'ems'
# what if more than one agency in filename? I can do appending..?

inData['agency'] = agency
  

# processing 55 datasets from R911 Inventory of Datasets in June 2022

# make pandas changes in function? Not a big improvement as changes are different for each file
# common code is outside of the if / elif statements

if(filename == "AZ_Chandler_calls_for_service_2021.csv"):

  inData.rename(
    {
    'id': 'id_in_dataset',
    'call_place_name': 'incident_location',
    'call_city': 'incident_city',
    'call_how_received': 'how_received',
    'call_reported_as': 'event_type',
    'call_closed_as': 'event_type_final',
    'call_cleared_by': 'disposition',
    'call_on_scene_date_time': 'on_scene_datetime',
    'call_response_time': 'response_time_sec',
    }, 
  axis=1, inplace=True)

  inData['call_datetime'] = inData['call_received_date'].astype(str) + " " + inData['call_received_time'].astype(str)
  
  #inData['call_datetime'] = inData['call_datetime'].astype('datetime64[ns]') 
  inData['call_datetime'] = pd.to_datetime(inData['call_datetime'], format='%m/%d/%Y %H:%M:%S', errors='coerce') 
  inData['on_scene_datetime'] = pd.to_datetime(inData['on_scene_datetime'], format='%m/%d/%Y %H:%M', errors='coerce') 
  
  #pd.to_datetime(
  
  inData.drop(inplace=True, columns=['agency', 'call_number', 'call_year','call_received_date',
  'call_received_time','call_received_day', 'call_address', 'call_district', 'call_beat', 'call_latitude', 'call_longitude',
  'call_dispatched_date_time', 'call_enroute_date_time', 'call_911_date_time', 'call_dispatch_to_enroute_time', 
  'call_rec_to_queue_time','call_queue_time', 'call_travel_time','call_received_date_time','call_911_to_rec_time'])
  
elif(filename == "AZ_Glendale_Police_Calls_for_Service.csv"):

  inData.rename(
    {
    'ï»¿OBJECTID': 'id_in_dataset',
    'Radio_Code': 'event_type',
	'Call_Priority': 'call_priority',
	'Call_Disposition': 'disposition',
	'Zip_Code': 'zip_code'
    }, 
  axis=1, inplace=True)
  
  inData['call_datetime'] = pd.to_datetime(inData['Date_Call_Received'], format='%m/%d/%Y %H:%M:%S', errors='coerce') 
    
  inData.drop(inplace=True, columns=['DateLoaded', 'Event_Number', 'Date_Call_Received', 'Day_Of_Week','Address',
  'Beat','Council_District', 'Council_Member', 'Patrol_Division'])
  
elif(filename == "AZ_Mesa_Police_Computer_Aided_Dispatch_Events_2021.csv"):

  inData.rename(
    {
    'ï»¿Event Number': 'id_in_dataset',
	'Event Type Code': 'event_code',
    'Event Type Description': 'event_type',
	'Call Priority': 'call_priority',
	'Response Time Seconds': 'response_time_sec'
    }, 
  axis=1, inplace=True)
  
  inData['call_datetime'] = pd.to_datetime(inData['Creation Datetime'], format='%m/%d/%Y %H:%M') 
    
  inData.drop(inplace=True, columns=['Creation Month', 'Creation Year','Creation Datetime', 'Event Address', 'Street Number','Street Direction',
  'Street Name','Street Type', 'Latitude', 'Longitude', 'Dispatch Group','Division','Response Time Minutes','Location 1'])
    
elif(filename == "AZ_Phoenix_calls-for-service-fire_calls-for-service-2021_calls_for_service.csv"):

  inData.rename(
    {
    'ï»¿INCIDENT': 'id_in_dataset',
	'NATURE_CODE': 'event_code',
    'NATURE_TEXT': 'event_type',
	'CATEGORY': 'event_category'
    }, 
  axis=1, inplace=True)
  
  inData['call_datetime'] = pd.to_datetime(inData['REPORTED'], format='%m/%d/%Y %I:%M:%S %p', errors='coerce')
  inData['closed_datetime'] = pd.to_datetime(inData['CLOSED'], format='%m/%d/%Y %I:%M:%S %p', errors='coerce') 

  inData.drop(inplace=True, columns=['REPORTED','CLOSED','INCIDENT_ADDRESS'])

elif(filename == "AZ_Phoenix_callsforsrvc2021.csv"):

  inData.rename(
    {
    'INCIDENT_NUM': 'id_in_dataset',
	'DISP_CODE': 'disposition_code',
    'DISPOSITION': 'disposition',
	'FINAL_RADIO_CODE': 'event_code',
	'FINAL_CALL_TYPE': 'event_type_final'
    }, 
  axis=1, inplace=True)
  
  inData['call_datetime'] = pd.to_datetime(inData['CALL_RECEIVED'], format='%m/%d/%Y %I:%M:%S%p', errors='coerce')

  inData.drop(inplace=True, columns=['CALL_RECEIVED','HUNDREDBLOCKADDR','GRID'])

elif(filename == "AZ_Scottsdale_Police_Calls_for_Service_2021.csv"):
# problem with timezone
  inData.rename(
    {
    'ï»¿Common_Event_ID': 'id_in_dataset',
	'Call_Type': 'event_code',
	'CFS_English': 'event_type',
	'Call_Type_English': 'event_category'
    }, 
  axis=1, inplace=True)
  
  inData['call_datetime'] = pd.to_datetime(inData['Create_Date']) 
  #problem with timezone, input has +00 which seems to say it is UTC time .dt.tz_convert(tz.tzlocal())

  inData.drop(inplace=True, columns=['CFS_Number','Create_Date','Street_Directional_Prefix','Street_Name','Street_Type',
  'Cross_Street_1','Cross_Street_2','Call_Sub_Type','Call_Sub_Type_English','CFS_Type'])
  
elif(filename == "AZ_Tucson_Police_Calls_for_Service_-_2021_-_Open_Data.csv"):

  inData.rename(
    {
    'call_id': 'id_in_dataset',
	'city': 'incident_city',
	'zip': 'zip_code',
	'NEIGHBORHD': 'incident_city_neighborhood',
	'CAPRIORITY': 'call_priority',
	'NATURECODE': 'event_code',
	'NatureCodeDesc': 'event_type',
	'HOWRECEIVE': 'how_received',
	'CSDISPOSIT': 'disposition_code',
	'DispositionCodeDesc': 'disposition',
	'WARD': 'incident_location'
    }, 
  axis=1, inplace=True)
  
  inData['call_datetime'] = pd.to_datetime(inData['ACTDATETIME']) 
  #problem with timezone, input has +00 which seems to say it is UTC time .dt.tz_convert(tz.tzlocal())

  inData.drop(inplace=True, columns=['ï»¿OBJECTID','X','Y','case_id','acci_id','agency',
  'ADDRESS_PUBLIC','state','emdivision','ACTDATE','ACTTIME','ACTDATETIME','ACT_MONTH','ACT_YEAR',
  'ACT_DOW','ACT_TIME','LOC_STATUS','NHA_NAME','DIVISION','DIVISION_NO','DIVSECT','TRSQ','City_geo','ADDRESS_100BLK'])

elif(filename == "CA_Sacramento_Fire_Department_911_Call_Response.csv"):

  inData.rename(
    {
    'OBJECTID': 'id_in_dataset',
	'Incident_Description': 'event_category',
	'Incident_Type': 'event_type',
	'Incident_Reference': 'report',
	'Responding_Station': 'agency_precinct'
    }, 
  axis=1, inplace=True)
  
  inData['call_datetime'] = pd.to_datetime(inData['Date']) 
  #problem with timezone, input has +00 which seems to say it is UTC time .dt.tz_convert(tz.tzlocal())

  inData.drop(inplace=True, columns=['ï»¿X','Y','Date','Address','Property_Use','Longitude','Latitude','Parcel_Number','Land_Use_Code'])
  
elif(filename == "CA_SanDiego_fd_incidents_2021_datasd_v1.csv"):

  inData.rename(
    {
    'incident_number': 'id_in_dataset',
	'agency_type': 'agency',
	'call_category': 'event_category',
	'address_city': 'incident_city',
	'problem': 'event_type',
	'address_zip': 'zip_code'
    }, 
  axis=1, inplace=True)
  
  inData['call_datetime'] = pd.to_datetime(inData['date_response']) 

  inData.drop(inplace=True, columns=['address_state','jurisdiction','date_response','day_response','month_response','year_response'])
  
elif(filename == "CA_SanDiego_pd_calls_for_service_2021_datasd.csv"):

  inData.rename(
    {
    'incident_num': 'id_in_dataset',
	'city': 'incident_city',
	'zip': 'zip_code',
	'priority': 'call_priority',
	'disposition': 'disposition_code'
    }, 
  axis=1, inplace=True)
  
  inData['call_datetime'] = pd.to_datetime(inData['date_time']) 
  #problem with timezone, input has +00 which seems to say it is UTC time .dt.tz_convert(tz.tzlocal())

  inData.drop(inplace=True, columns=['date_time','day_of_week','address_number_primary','address_dir_primary','address_road_primary',
  'address_sfx_primary','address_dir_intersecting','address_road_intersecting','address_sfx_intersecting','beat'])
  
elif(filename == "CA_SanFrancisco_Fire_Department_Calls_for_Service_2021.csv"):

  inData.rename(
    {
    'ï»¿Call Number': 'id_in_dataset',
	'On Scene DtTm': 'on_scene_datetime',
	'Call Final Disposition': 'disposition_final',
	'City': 'incident_city',
	'Zipcode of Incident': 'zip_code',
	'Priority': 'call_priority',
	'Final Priority': 'call_priority_final',
	'Supervisor District': 'agency_precinct',
	'Neighborhooods - Analysis Boundaries': 'incident_city_neighborhood'
    }, 
  axis=1, inplace=True)
  
  inData['call_datetime'] = pd.to_datetime(inData['Received DtTm']) 
  #problem with timezone, input has +00 which seems to say it is UTC time .dt.tz_convert(tz.tzlocal())

  inData.drop(inplace=True, columns=['Unit ID','Incident Number','Call Date','Watch Date','Entry DtTm',
  'Dispatch DtTm','Response DtTm','Transport DtTm','Hospital DtTm','Available DtTm','Address',
  'Battalion','Station Area','Box','ALS Unit','Number of Alarms','Unit Type','Unit sequence in call dispatch',
  'Fire Prevention District','RowID','case_location', 'Call Type','Received DtTm','Original Priority','Call Type Group'])
  
elif(filename == "CA_SanFrancisco_Law_Enforcement_Dispatched_Calls_for_Service__Closed.csv"):

  inData.rename(
    {
    'ï»¿cad_number': 'id_in_dataset',
	'onscene_datetime': 'on_scene_datetime',
	'close_datetime': 'closed_datetime',
	'call_type_original': 'event_code',
	'call_type_original_desc': 'event_type',
	'call_type_final_desc': 'event_type_final',
	'priority_original': 'call_priority',
	'priority_final': 'call_priority_final',
	'analysis_neighborhood': 'incident_city_neighborhood',
	'police_district': 'agency_precinct',
	'pd_incident_report': 'report'
    }, 
  axis=1, inplace=True)
  
  inData['call_datetime'] = pd.to_datetime(inData['received_datetime']) 
  #problem with timezone, input has +00 which seems to say it is UTC time .dt.tz_convert(tz.tzlocal())

  inData.drop(inplace=True, columns=['dup_cad_number','entry_datetime','dispatch_datetime','enroute_datetime','received_datetime',
  'call_type_original_notes','call_type_final','call_type_final_notes','onview_flag','sensitive_call','intersection_name',
  'intersection_id','intersection_point','supervisor_district','data_as_of','data_updated_at','data_loaded_at','source_filename'])
  
elif(filename == "CA_SanJose_policecalls2021.csv"):

  inData.rename(
    {
    'CALL_NUMBER': 'id_in_dataset',
	'CALLTYPE_CODE': 'event_code',
	'CALL_TYPE': 'event_type',
	'PRIORITY': 'call_priority',
	'FINAL_DISPO': 'disposition_final',
	'COMMON_PLACE_NAME': 'incident_city_neighborhood',
	'CITY': 'incident_city'
    }, 
  axis=1, inplace=True)
  
  inData['call_datetime'] = pd.to_datetime(inData['START_DATE']) 

  inData.drop(inplace=True, columns=['CDTS','EID','START_DATE','REPORT_DATE','OFFENSE_DATE','OFFENSE_TIME',
  'FINAL_DISPO_CODE','ADDRESS','STATE'])
  
elif(filename == "CA_SanJose_sanjosefireincidentdata_2021.csv"):

  inData.rename(
    {
    'Incident_No': 'id_in_dataset',
	'Unit_On_Scene_TimeStamp': 'on_scene_datetime',
	'Cleared_TimeStamp': 'closed_datetime',
	'Final_Incident_Type': 'event_type_final',
	'Priority': 'call_priority',
	'Final_Incident_Category': 'event_category',
	'Station': 'agency_precinct'
    }, 
  axis=1, inplace=True)
  
  inData['call_datetime'] = pd.to_datetime(inData['Date_Time_Of_Event']) 

  inData.drop(inplace=True, columns=['Date_Time_Of_Event','Dispatched_Time','Unit_On_The_Way_Time',
  'On_Scene_Unit','Unit_Count','Near_X','Near_Y','Battalion'])
  
elif(filename == "FL_Gainesville_Crime_Responses.csv"):
# these perhaps are not calls at all

  inData.rename(
    {
    'ID': 'id_in_dataset',
	'Offense Date': 'call_datetime',
	# there is a report_date in file but it is later than offense_date
	'Incident Type': 'event_type',
	'City': 'incident_city'
    }, 
  axis=1, inplace=True)
  
  #inData['call_datetime'] = pd.to_datetime(inData['Date_Time_Of_Event']) 

  inData.drop(inplace=True, columns=['Report Date','Report Hour of Day','Report Day of Week',
  'Offense Hour of Day','Offense Day of Week','State','Address','Latitude','Longitude','Location'])
  
elif(filename == "FL_Gainesville_Fire_Rescue_Responses.csv"):

  inData.rename(
    {
    'Master Incident Number': 'id_in_dataset',
	'Response Type': 'agency',
	'Location Name': 'incident_location'
    }, 
  axis=1, inplace=True)

  inData['call_datetime'] = pd.to_datetime(inData['Response Date']) 
  
  inData.drop(inplace=True, columns=['Response Hour of Day','Response Day of Week','Responding Unit',
  'Longitude','Latitude','Location','Response Date'])

elif(filename == "FL_StPete_Police_Calls.csv"):

  inData.rename(
    {
    'ID': 'id_in_dataset',
	'Type of Event': 'event_type',
	'Event Subtype': 'event_category', # questionable
	'Classification': 'disposition', # questionable
	'Neighborhood Name': 'incident_city_neighborhood'
    }, 
  axis=1, inplace=True)

  inData['call_datetime'] = pd.to_datetime(inData['Crime date']) 
  
  inData.drop(inplace=True, columns=['Event Number','Event Case Number','Display Address','Crime date','Crime Time',
  'Longitude','Latitude','Location','Submit an anonymous tip','Council District','Event Subtype - Type of Event'])

else:
  print("Not ready to process", filename)
  exit()


#pd.reset_option('max_columns')
#pd.set_option('max_columns', None) # doesn't work
print(inData.head(10))
#print(inData[['0', '1']].head(10)) doesn't work

a = filename.split("_")
inData['state'] = a[0]
inData['city'] = a[1]

print("Max date", inData['call_datetime'].max())
print("Min date", inData['call_datetime'].min())

rows = "{:,}".format(len(inData))
print("Loaded", rows , "rows")

tdelta = datetime.datetime.now() - time_1
print("Source data loaded in", tdelta)

#exit()

time_2 = datetime.datetime.now()

row_inserted = inData.to_sql(table_name, con = engine, if_exists = 'append', chunksize = 50000, index=False)
# method='multi' is slower
# it doesn't return the number of rows inserted

#print("Data loaded in DB", row_inserted, str(datetime.datetime.now().time()))
tdelta = datetime.datetime.now() - time_2
print("Data loaded in DB in", tdelta)

#print("Records inserted:", cursor.rowcount) without buffered cursor it returns -1
# buffered cursor is available with mysql connector not in pymysql

connection.commit()

connection.close()

print("Done", str(datetime.datetime.now().time()))
