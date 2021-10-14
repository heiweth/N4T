import time
from datetime import date, timedelta, datetime
import csv
from selenium.webdriver.common.action_chains import ActionChains
import mysql.connector
from mysql.connector import Error, errorcode
import pandas as pd
import os
import numpy as np
 
balkan_insert_query = """INSERT IGNORE INTO berza (area, date, hour, price, volume) 
						                  VALUES (%s,%s,%s,%s,%s) """ 

def balkan_query(tomorrow, cursor, connection, path_to_docs, logf):
	balkan_insert_query = """INSERT IGNORE INTO berza (area, date, hour, price, volume) 
						                  VALUES (%s,%s,%s,%s,%s) """  
	bal = ["ibex", "sepex", "cropex"]

	for b in bal:
		try:
			data_b = pd.read_csv(path_to_docs + b + "_" + str(tomorrow) + ".csv")
			cursor.executemany(balkan_insert_query, data_b.values.tolist())
			connection.commit()				
			logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records " + b + " inserted successfully into berza table \n")
			os.remove(path_to_docs + b + "_" + str(tomorrow) + ".csv")

		except mysql.connector.Error as error:
			logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records "+ b +" area {} \n".format(error))
			pass
		except:
			logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records "+ b +" area \n")
			pass

def croatia(driver, tomorrow, cursor, connection, path_to_docs, logf):
	#cropex
	day = tomorrow.strftime("%d")
	driver.get("https://www.cropex.hr/en/market-data/day-ahead-market/day-ahead-market-results.html")
	time.sleep(7)
	driver.find_element_by_class_name("accept").click()
	time.sleep(3)
#	driver.find_element_by_id("datepicker").click()
	driver.find_element_by_class_name('ui-datepicker-trigger').click()
	date_list = driver.find_elements_by_xpath("//a[@class='ui-state-default']")
	try:
		[x.click() for x in date_list if x.text == day]
		time.sleep(7)
	except:
		pass
		
	table = driver.find_element_by_id("tableVolumePrice").text
	table = table.split("\n")
	table_cropex = [x.split() for x in table[7:]]
	try:
		df_cropex = pd.DataFrame(table_cropex, columns=["Hour", "Price-4", "Volume-4", "Price-3", "Volume-3",
	"Price-2", "Volume-2", "Price-1", "Volume-1", "Price", "Volume"])
		df_cropex1 = df_cropex.drop(columns=["Price-4", "Volume-4", "Price-3", "Volume-3","Price-2", "Volume-2", "Price-1", "Volume-1"])
		
	except:
		pass
	
	df_cropex1.insert(0,"Area","CROPEX")
	df_cropex1.insert(1,"Date",str(tomorrow))
	df_cropex1["Price"] = df_cropex1["Price"].astype(float)
	df_cropex1["Volume"].replace({",":""}, inplace=True)
	if str(tomorrow) == '2021-03-28':
		df_cropex1["Price"] = df_cropex1["Price"].replace(np.nan,0)
		df_cropex1["Volume"] = df_cropex1["Volume"].replace(np.nan,0)

	if str(tomorrow) == '2021-03-29' or str(tomorrow) == '2021-03-30' or str(tomorrow) == '2021-03-31' or str(tomorrow) == '2021-04-01': 
		new_price = float(df_cropex.loc[2,"Price-1"])
		new_volume = float(df_cropex.loc[2,"Volume-1"])
		df_cropex1["Price"] = df_cropex1["Price"].replace(np.nan,new_price)
		df_cropex1["Volume"] = df_cropex1["Volume"].replace(np.nan,new_volume)
					

	df_cropex1 = df_cropex1.dropna()


	#insert to db		
	try:
		cursor.executemany(balkan_insert_query, df_cropex1.values.tolist())
		connection.commit()				
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records CROPEX inserted successfully into berza table \n")

	except mysql.connector.Error as error:
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records CROPEX area {} \n".format(error))
		pass
	except:
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records CROPEX area \n")
		pass
	
def seepex(driver, tomorrow, cursor, connection, path_to_docs, logf):
	#seepex
	driver.get("http://seepex-spot.rs/en/market-data/day-ahead-auction/"+str(tomorrow)+"/RS")
	time.sleep(10)
	xp = "//*[@id='tab_fr']/table[2]"
	hours_tbl = driver.find_element_by_xpath(xp).text
#	hours_tbl = driver.find_element_by_class_name("hours")
	hours_tbl = hours_tbl.split("\n")
	seepex_tbl = [x.split() for x in hours_tbl[1:]]
	seepex_price = [seepex_tbl[i] for i in range(0, len(seepex_tbl),2)]
	seepex_volume = [seepex_tbl[i] for i in range(1, len(seepex_tbl),2)]	

	df_price = pd.DataFrame(seepex_price)
	df_volume = pd.DataFrame(seepex_volume)
#	if str(tomorrow) == '2021-03-28' and df_price
	print(df_price)
	print(df_volume)
	if str(tomorrow) == '2021-03-28':
		df_price[10] = df_price[10].replace(r'N/A',0, regex=True)
		df_volume[7] = df_volume[7].replace(r'N/A', 0, regex=True)
	else:
		df_price = df_price.replace(r'N/A',np.nan, regex=True) 
		df_volume = df_volume.replace(r'N/A', np.nan, regex=True)

	df_seepex = pd.DataFrame({"Area":"SEEPEX", "Date":str(tomorrow), "Hour": df_price[2].astype(int).astype(str), "Price":df_price[10], "Volume":df_volume[7]})
	df_seepex = df_seepex.dropna()
	df_seepex.Price = df_seepex.Price.replace({',':'.'}, regex=True)
	df_seepex.Volume = df_seepex.Volume.replace({',':'.'}, regex=True)


	#insert to db
	try:
		cursor.executemany(balkan_insert_query, df_seepex.values.tolist())
		connection.commit()				
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records SEEPEX inserted successfully into berza table \n")

	except mysql.connector.Error as error:
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records SEEPEX area {} \n".format(error))
		pass
	except:
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records SEEPEX area \n")
		pass


def bulgaria(driver, tomorrow, cursor, connection, path_to_docs, logf):
	day = tomorrow.strftime("%d")
	#ibex
#	driver.get("http://www.ibex.bg/en/market-data/dam/prices-and-volumes/")
	driver.get("https://ibex.bg/markets/dam/dam-market-segment/")
	time.sleep(5)
	driver.find_element_by_id("cn-accept-cookie").click()
	
#	calc_tbl = driver.find_elements_by_class_name("calculations-table")
#	calculations = calc_tbl[2].text
	calculations = driver.find_element_by_id("wpdtSimpleTable-33").text
	calc1 = calculations.split("\n")
	price = []
	volume = []
	for row in calc1:
		row_data = row.split()
		if len(row_data) == 11:		
			price.append(row_data[-1])
		elif len(row_data) == 8:
			volume.append(row_data[-1])

		
	if len(price) == 25:
		price.remove('-')
		volume.remove("-")	

	df_ibex = pd.DataFrame({"Area": "IBEX", "Date": str(tomorrow), "Hour": [x for x in range(1,25)], "Price": price, "Volume" : volume})
	if str(tomorrow) == '2021-03-28':
		df_ibex.Price = df_ibex.Price.replace(r'-',0, regex=True) 
		df_ibex.Volume = df_ibex.Volume.replace(r'-',0, regex=True)
	else:	
		df_ibex.Price = df_ibex.Price.replace(r'-',np.nan, regex=True) 
		df_ibex.Volume = df_ibex.Volume.replace(r'-',np.nan, regex=True)
		df_ibex = df_ibex.dropna()
		print(df_ibex)
	#insert to db
	try:
		cursor.executemany(balkan_insert_query, df_ibex.values.tolist())
		connection.commit()				
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records IBEX inserted successfully into berza table \n")

	except mysql.connector.Error as error:
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records IBEX area {} \n".format(error))
		pass
	except:
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records IBEX area \n")
		pass
