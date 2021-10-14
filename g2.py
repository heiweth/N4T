import time
from datetime import date, timedelta, datetime
import csv
import mysql.connector
from mysql.connector import Error, errorcode
import pandas as pd
import os
import math
import glob
import numpy as np

def greece(driver, tomorrow, cursor, connection, path_to_docs, logf):
	day = tomorrow.strftime("%d")
	month = tomorrow.strftime("%m")
	year = tomorrow.strftime("%Y")
	
	driver.get("https://www.enexgroup.gr/web/guest/markets-publications-el-day-ahead-market")
	time.sleep(8)
	try:
		driver.find_element_by_xpath('//*[@id="_eucookiespolicy_okButton"]/span').click()
		time.sleep(3)
	except:
		pass
	items = driver.find_elements_by_tag_name("a")
	for item in items:
		if year+month+day+'_EL-DAM_ResultsSummary_EN_v01.xlsx' in item.text:
			item.click()
			time.sleep(8)
			break

	#insert to db
	greece_insert_query = """INSERT IGNORE  INTO berza (area, date, hour, price) 
				                     VALUES ('SMP',%s,%s,%s) """
	
	grcka_df = pd.read_excel(path_to_docs + year+month+day+'_EL-DAM_ResultsSummary_EN_v01.xlsx', usecols='A:Y', engine='openpyxl')
	os.remove(path_to_docs + year+month+day+'_EL-DAM_ResultsSummary_EN_v01.xlsx')
	
	prices_grcka = grcka_df.iloc[5,:].values.tolist()
	prices_first = grcka_df.iloc[5,1:3].values.tolist()
	prices_second = grcka_df.iloc[5,3:].values.tolist()

	greece_df = pd.DataFrame({"Date": str(tomorrow), "Hour": [x for x in range(1,25)], "Price": prices_grcka[1:]})
	print(greece_df)
	
	if str(tomorrow) == '2021-03-28':
#		greece_df = greece_df.replace(r'NaN',np.nan, regex=True)
#		greece_df1 = greece_df.dropna()
		greece_df.drop(greece_df['Hour'] == 24)

		greece_df.loc[(greece_df['Hour'] > 2) , 'Hour'] = greece_df['Hour'] + 1
		greece_df = greece_df.append({'Date': str(tomorrow), "Hour":3, "Price":0}, ignore_index=True)
#		greece_df = pd.DataFrame({"Date": str(tomorrow), "Hour": [1,2], "Price": prices_first})
#		greece_df = greece_df.append({"Date": str(tomorrow), "Hour": 3, "Price": 0}, ignore_index=True)
#		greece_df = greece_df.append({"Date": str(tomorrow), "Hour": [x for x in range(3,24)], "Price": prices_second}, ignore_index=True)
	print(greece_df)		
	
	cursor.executemany(greece_insert_query, greece_df.values.tolist())

	try:
		connection.commit()				
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records SMP inserted successfully into berza table \n")

	except:
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records SMP \n")
		pass
	



def austria(driver, tomorrow, cursor, connection, path_to_docs, logf):
	year = tomorrow.strftime("%Y")

	driver.get("https://www.exaa.at/en/marketdata/historical-data")
	driver.find_element_by_xpath('//a[@href="/download/history/DSHistory'+year+'.xls"]').click()
	time.sleep(8)
	
	#Austrija
	austria_insert_query = """INSERT IGNORE INTO berza (area, date, hour, price, volume)
														VALUES ('EXAA',%s,%s,%s,%s) """ 
		
													
	price_austria = pd.read_excel(path_to_docs + "dshistory" + year + ".xls", sheet_name="Price AT (EUR)", usecols='A:Y',skiprows=1)
	volume_austria = pd.read_excel(path_to_docs + "dshistory" + year + ".xls", sheet_name="Volume AT (MWh)", usecols='A:Y',skiprows=1)

	os.remove(path_to_docs + "dshistory" + year + ".xls")	
	price_austria["Delivery Date"] = price_austria.loc[:,"Delivery Date"].dt.strftime('%Y-%m-%d')
	volume_austria["Delivery Date"] = volume_austria.loc[:,"Delivery Date"].dt.strftime('%Y-%m-%d')
	price_austria_1 = price_austria[price_austria["Delivery Date"] == str(tomorrow)]
	volume_austria_1 = volume_austria[volume_austria["Delivery Date"] == str(tomorrow)]
	price_values = price_austria_1.iloc[0,1:]
	volume_values = volume_austria_1.iloc[0,1:]
	data = {"Date":str(tomorrow), "Hour":range(1,25),"Price":price_values.values.tolist(), "Volume":volume_values.values.tolist()}
	austria_df = pd.DataFrame(data)
	
	if str(tomorrow) == '2021-03-28':
		austria_df = austria_df.replace(np.nan,0)	

	cursor.executemany(austria_insert_query, austria_df.values.tolist())

	try:
		connection.commit()				
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records Austria inserted successfully into berza table \n")
	except:
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records Austria \n")
		pass

