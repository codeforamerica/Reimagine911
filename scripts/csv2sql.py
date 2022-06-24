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

# it should first unzip the all-counties file
# then unzip all county files
# then process each county file like below

filename = sys.argv[1] #"AZ_Chandler_calls_for_service_2021.csv"

table_name = "911"

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
  

# For Tucson
if(filename == "AZ.Tuscon.2021.Calls.csv"):
  inData.drop(inplace=True, columns=['OBJECTID', 'zip', 'ACTDATE', 'ACTTIME', 'ACT_MONTH', 'ACT_YEAR', 'ACT_DOW', 'ACT_TIME', 'LOC_STATUS', 'WARD', 
  'DIVISION', 'DIVSECT', 'TRSQ', 'City_geo', 'ADDRESS_100BLK'])

  inData.rename(
    {
    'X': 'incident_coord_x', 
    'Y': 'incident_coord_y', 
    'case_id': 'report_id',
    'acci_id': 'incident_id',
    'agency': 'entity_dispatched_type',
    'ADDRESS_PUBLIC': 'incident_address', # concatenate multiple source fields?
    'city': 'municipality',
    'NEIGHBORHD': 'incident_location',
    'emdivision': 'incident_location_2',
    'ACTDATETIME': 'call_received_time',
    'CAPRIORITY': 'call_priority',
    'NATURECODE': 'call_type',
    'NatureCodeDesc': 'call_description',
    'HOWRECEIVE': 'call_method',
    'CSDISPOSIT': 'disposition_code',
    'DispositionCodeDesc': 'disposition_description',
    'NHA_NAME': 'incident_location_3',
    'DIVISION_NO': 'district'
    }, 
  axis=1, inplace=True)

  inData['municipality'] = 'Tucson'
  inData['county'] = 'Pima'
  inData['state'] = 'AZ'

elif(filename == "CA.SanDiego.2021.Calls.csv"):

  inData["incident_address"] = inData['address_number_primary'].astype(str) + " " + inData['address_dir_primary'] \
  + " " + inData['address_road_primary'] + " " + inData['address_sfx_primary']
  
  inData["incident_location"] = inData['address_dir_intersecting'] \
  + " " + inData['address_road_intersecting'] + " " + inData['address_sfx_intersecting']

  inData.drop(inplace=True, columns=['day_of_week', 'address_number_primary', 'address_dir_primary', 'address_road_primary',
  'address_sfx_primary', 'address_dir_intersecting', 'address_road_intersecting', 'address_sfx_intersecting'])

  inData.rename(
    {
    'incident_num': 'incident_id',
    'date_time': 'call_received_time',
    'disposition': 'disposition_code',
    'priority': 'call_priority'
    }, 
  axis=1, inplace=True)

  inData['municipality'] = 'San Diego'
  inData['county'] = 'San Diego'
  inData['state'] = 'CA'

elif(filename == "CA.Sacramento.2019.Calls.csv"):

  inData.rename(
    {
    'X': 'incident_coord_x', 
    'Y': 'incident_coord_y',
    'Record_ID': 'original_record_id',
    'Call_Type': 'call_type',
    'Description': 'call_description',
    'Reporting_Officer': 'call_taker',
    'Unit_ID': 'unit_dispatched_id',
    'Report_Created': 'report_created',
    'Location': 'incident_address',
    'Police_District': 'district',
    'Occurence_Date': 'incident_datetime',
    'Received_Date': 'call_received_time',
    'Dispatch_Date': 'dispatch_datetime',
    'Enroute_Date': 'enroute_datetime',
    'At_Scene_Date': 'at_scene_datetime',
    'Clear_Date': 'clear_datetime'
    }, 
  axis=1, inplace=True)

  inData.drop(inplace=True, columns=['OBJECTID', 'Grid', 'X_Coordinate', 'Y_Coordinate', 'Day_of_Week'])

  inData['municipality'] = 'Sacramento'
  inData['county'] = 'Pima'
  inData['state'] = 'CA'

elif(filename == "MD.Baltimore.2021.Calls.csv"): # 2020 and older have 1.4 million records

  inData.rename(
    {
    'callDateTime': 'call_received_time', 
    'priority': 'call_priority',
    'description': 'call_description',
    'callNumber': 'call_id', 
    'location': 'incident_address', 
    'Neighborhood': 'incident_location', 
    'PoliceDistrict': 'district', 
    'SheriffDistricts': 'district_2', 
    'Community_Statistical_Areas': 'incident_location_2'
    }, 
  axis=1, inplace=True)

  inData.drop(inplace=True, columns=['OBJECTID', 'callKey', 'incidentLocation', 'PolicePost', 'CouncilDistrict', 
  'Census_Tracts', 'VRIZones', 'ZIPCode', 'NeedsSync', 'IsDeleted'])

  inData['municipality'] = 'Baltimore'
  #inData['county'] = '' Wikipedia says is independent, not in a county
  inData['state'] = 'MD'

elif(filename == "AZ.Mesa.Calls.csv"):

  inData.rename(
    {
    'Event Number': 'call_id', 
    'Creation Datetime': 'call_received_time',
    'Event Type Code': 'call_type',
    'Event Type Description': 'call_description',
    'Call Priority': 'call_priority',
    'Event Address': 'incident_address',
    'Longitude': 'incident_coord_x',
    'Latitude': 'incident_coord_y',
    'Dispatch Group': 'unit_dispatched_id',
    'Division': 'incident_location',
    'Response Time Seconds': 'response_time'
    }, 
  axis=1, inplace=True)

  inData.drop(inplace=True, columns=['Creation Month', 'Creation Year', 'Street Number', 'Street Direction', 'Street Name', 
  'Street Type', 'Response Time Minutes', 'Location 1'])
  
  inData['municipality'] = 'Mesa'
  inData['county'] = 'Maricopa'
  inData['state'] = 'AZ'

elif(filename == "CT.Hartford.Calls.csv"):

  inData.rename(
    {
    'APNO': 'call_id',   
    'Address': 'incident_address',
    'Neighborhood': 'incident_location',
    'Priority': 'call_priority',
    'Call_Initiated_By': 'call_initiated_by'
    }, 
  axis=1, inplace=True)

  inData['call_received_time'] = inData['Date'].astype(str) + " " + inData['Time_24HR'].astype(str) #need to put a : in between time number
  inData['call_description'] = inData['Description_1'] + " " + inData['Description_2']
  inData["geom"].replace("(),","").str.split(' ', expand=True)
  #x_y = geom.str.split(' ', expand=True)
  #inData['incident_coord_x'] = x_y[0]
  #inData['incident_coord_y'] = x_y[1]

  inData.drop(inplace=True, columns=['Date', 'Time_24HR', 'Description_1', 'Description_2']) #, 'geom'])
  
  inData['municipality'] = 'Hartford'
  inData['county'] = ''
  inData['state'] = 'CT'

elif(filename == "NYPD_Calls_for_Service__Year_to_Date_.csv"):

  inData.rename(
    {
    'CAD_EVNT_ID': 'call_id',
    'CREATE_DATE': 'call_date',
    'NYPD_PCT_CD': 'district',
    'BORO_NM': 'incident_location',
    'PATRL_BORO_NM': 'beat',
    'RADIO_CODE': 'call_type',
    'TYP_DESC': 'call_description',
    'ADD_TS': 'call_received_time',
    'DISP_TS': 'dispatch_datetime',
    'ARRIVD_TS': 'at_scene_datetime',
    'CLOSNG_TS': 'clear_datetime',
    'Latitude': 'incident_coord_x',
    'Longitude': 'incident_coord_y'
    }, 
  axis=1, inplace=True)

  inData['incident_datetime'] = inData['INCIDENT_DATE'].astype(str) + " " + inData['INCIDENT_TIME'].astype(str) #need to put a : in between time number
  
  inData.drop(inplace=True, columns=['INCIDENT_DATE', 'INCIDENT_TIME', 'GEO_CD_X', 'GEO_CD_Y'])
  
  inData['municipality'] = 'New York'
  inData['county'] = ''
  inData['state'] = 'NY'

  print(inData['call_date'].max())
  print(inData['call_date'].min())

  # this file has more than 6M records, from 1/1/21 to 12/31/21

# processing 55 datasets from R911 Inventory of Datasets in June 2022

elif(filename == "AZ_Chandler_calls_for_service_2021.csv"):

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
  
else:
  print("Not ready to process", filename)
  exit()

pd.reset_option('max_columns')
#pd.set_option('max_columns', None)
print(inData.head(10))
#print(inData[['0', '1', '2','3','4','5','6']].head(10))

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

#print("Data loaded in DB", row_inserted, str(datetime.datetime.now().time()))
tdelta = datetime.datetime.now() - time_2
print("Data loaded in DB in", tdelta)

#print("Records inserted:", cursor.rowcount) without buffered cursor it returns -1
# buffered cursor is available with mysql connector not in pymysql

connection.commit()

connection.close()

print("Done", str(datetime.datetime.now().time()))
