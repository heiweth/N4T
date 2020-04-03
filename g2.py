from datetime import date, timedelta, datetime
import time
from datetime import date, timedelta, datetime
import csv
import mysql.connector
from mysql.connector import Error, errorcode
import pandas as pd
import os
import math

def greece(driver, year, month, day, tomorrow, cursor, connection, path_to_docs, logf):
	driver.get("http://www.enexgroup.gr/en/market/electricity-day-ahead-market/results/")
	driver.find_element_by_xpath('//a[@href="fileadmin/./user_upload/reports/DayAheadSchedulingResults/'+year+month+day+'_DayAheadSchedulingResults_01.xls"]').click()
	time.sleep(8)
	
	#Grcka
#	greece_insert_query = """INSERT IGNORE  INTO berza (area, date, hour, price) 
#				                     VALUES ('Greece',%s,%s,%s) """
	greece_insert_query = """INSERT IGNORE  INTO berza (area, date, hour, price) 
				                     VALUES ('ENEX',%s,%s,%s) """
		
	grcka_df = pd.read_excel(path_to_docs + year+month+day+"_DayAheadSchedulingResults_01.xls", usecols='A:Z')
	prices_grcka = grcka_df[grcka_df['Day Ahead Scheduling'] == 'SMP'].values.tolist()[0]
	del prices_grcka[0]
	start_hour = grcka_df.at[0,'Day Ahead Scheduling'] - timedelta(hours=1)
	to_delete = []
	for i in range(len(prices_grcka)-1,-1,-1):
		if math.isnan(prices_grcka[i]) : to_delete.append(i)
	for j in to_delete: del prices_grcka[j]	
	end_hour = start_hour + timedelta(hours=len(prices_grcka))	 
	#end_hour = start_hour + timedelta(hours=24)
	hour = start_hour
	hours = []
	while hour < end_hour:
		hours.append(hour)
		hour += timedelta(hours=1)

	datum = [x.strftime('%Y-%m-%d') for x in hours]
	sati = [str(int(x.strftime('%H'))+1) for x in hours]

	data_grcka = {"Date": datum, "Hour": sati, "Price": prices_grcka}	
	greece_df = pd.DataFrame(data_grcka)
	cursor.executemany(greece_insert_query, greece_df.values.tolist())

	try:
		connection.commit()				
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records Greece inserted successfully into berza table \n")
		os.remove(path_to_docs + year+month+day+"_DayAheadSchedulingResults_01.xls")
	except mysql.connector.Error as error:
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records Greece {} \n".format(error))
		pass
		
		
def austria(driver, year, tomorrow, cursor, connection, path_to_docs, logf):
	driver.get("https://www.exaa.at/en/marketdata/historical-data")
	driver.find_element_by_xpath('//a[@href="/download/history/DSHistory'+year+'.xls"]').click()
	time.sleep(8)
	
	#Austrija
#	austria_insert_query = """INSERT IGNORE INTO berza (area, date, hour, price, volume)
#														VALUES ('Austria',%s,%s,%s,%s) """ 
	austria_insert_query = """INSERT IGNORE INTO berza (area, date, hour, price, volume)
														VALUES ('EXAA',%s,%s,%s,%s) """ 
		
													
	price_austria = pd.read_excel(path_to_docs + "dshistory" + year + ".xls", sheet_name="Price AT (EUR)", usecols='A:Y',skiprows=1)
	volume_austria = pd.read_excel(path_to_docs + "dshistory" + year + ".xls", sheet_name="Volume AT (MWh)", usecols='A:Y',skiprows=1)
	price_austria["Delivery Date"] = price_austria.loc[:,"Delivery Date"].dt.strftime('%Y-%m-%d')
	volume_austria["Delivery Date"] = volume_austria.loc[:,"Delivery Date"].dt.strftime('%Y-%m-%d')
	price_austria_1 = price_austria[price_austria["Delivery Date"] == str(tomorrow)]
	volume_austria_1 = volume_austria[volume_austria["Delivery Date"] == str(tomorrow)]
	price_values = price_austria_1.iloc[0,1:]
	volume_values = volume_austria_1.iloc[0,1:]
	data = {"Date":str(tomorrow), "Hour":range(1,25),"Price":price_values.values.tolist(), "Volume":volume_values.values.tolist()}
	austria_df = pd.DataFrame(data)		
	cursor.executemany(austria_insert_query, austria_df.values.tolist())

	try:
		connection.commit()				
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records Austria inserted successfully into berza table \n")
		os.remove(path_to_docs + "dshistory" + year + ".xls")

	except mysql.connector.Error as error:
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records Austria {} \n".format(error))
		pass

