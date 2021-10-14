import time
from datetime import date, timedelta, datetime
import mysql.connector
from selenium.webdriver.common.action_chains import ActionChains
from mysql.connector import Error, errorcode
import csv
import pandas as pd
import os
import glob
import numpy as np
import requests

def jao_special(driver, tomorrow, cursor, connection, path_to_docs, logf):
	today = date.today()
	day = today.strftime("%d")
	month = today.strftime("%m")
	year = today.strftime("%Y")
	dayt = tomorrow.strftime("%d")
	
	#insert into db
	jao_insert_query = """INSERT IGNORE INTO aukcija (direction, date, hour, offered_capacity, total_requested, total_allocated, auction_price, ATC, no_of_partis, awarded_partis, sink, source)
													VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """ 													
		
	data_jao = pd.read_excel(path_to_docs + 'JAO_marketdata_export_'+day+'-'+month+'-'+year + '.xlsx', engine='openpyxl')
	df = data_jao[['Border','Market period start','TimeTable','OfferedCapacity (MW)','Total requested capacity (MW)','Total allocated capacity (MW)','Price (€/MWH)','ATC (MW)','Number of participants', 'Awarded participants']]

	df.Border = df.Border.replace({"-":""}, regex=True)							
	df['Market period start'] = pd.to_datetime(df['Market period start'] ,errors = 'coerce',format = '%d/%m/%Y').dt.strftime('%Y-%m-%d')
	df.TimeTable = df.TimeTable.str[6:8]
	df.TimeTable = pd.to_numeric(df.TimeTable)	


	if str(tomorrow) == '2021-03-28':	
		df.loc[(df1['TimeTable'] == 24) , 'TimeTable'] = 0
		df.loc[(df1['TimeTable'] > 2) , 'TimeTable'] = df1['TimeTable'] + 1
		df.loc[(df1['TimeTable'] == 0) , 'TimeTable'] = 3
	
		
	conditions = [
		(df['Border'] == "ATHU"),
		(df['Border'] == "HUAT"),
		(df['Border'] == "HUHR"),
		(df['Border'] == "HRHU"),
		(df['Border'] == "RSHR"),
		(df['Border'] == "HRRS"),
		(df['Border'] == "ATCZ"),
		(df['Border'] == "CZAT"),
		(df['Border'] == "BGGR"),
		(df['Border'] == "GRBG"),
		(df['Border'] == "RSBG"),
		(df['Border'] == "BGRS")
		]

	# create a list of the values we want to assign for each condition
	sources = ['APG', 'MAVIR', 'MAVIR', 'HROTE', 'EMS', 'HROTE', 'APG', 'CEPS', 'BG-ESO', 'HTSO', 'EMS', 'BG-ESO']

	sinks = ['MAVIR', 'APG', 'HROTE', 'MAVIR', 'HROTE', 'EMS', 'CEPS', 'APG', 'HTSO', 'BG-ESO', 'BG-ESO', 'EMS']

	# create a new column and use np.select to assign values to it using our lists as arguments
	df['sink'] = np.select(conditions, sinks)
	df['source'] = np.select(conditions, sources)

	cursor.executemany(jao_insert_query, df.values.tolist())
	print(df)
	os.remove(path_to_docs + 'JAO_marketdata_export_'+day+'-'+month+'-'+year + '.xlsx')
	try:
		connection.commit()				
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records JAO inserted successfully into aukcija table \n")

	except:
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records JAO  \n")
		pass

