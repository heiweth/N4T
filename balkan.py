import time
from datetime import date, timedelta, datetime
import csv
import mysql.connector
from mysql.connector import Error, errorcode
import pandas as pd
import os

def balkan_query(tomorrow, cursor, connection, path_to_docs, logf):
	balkan_insert_query = """INSERT IGNORE INTO berza (area, date, hour, price, volume) 
						                  VALUES (%s,%s,%s,%s,%s) """  
	bal = ["sepex", "ibex", "cropex"]

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

def croatia(driver, tomorrow, path_to_docs):
	#cropex
	driver.get("https://www.cropex.hr/en/market-data/day-ahead-market/day-ahead-market-results.html")
	time.sleep(10)
	driver.find_element_by_class_name("accept").click()
	time.sleep(3)
	table = driver.find_element_by_id("tableVolumePrice").text
	table = table.split("\n")

	with open(path_to_docs + 'cropex_'+str(tomorrow)+'.csv', 'w') as hr:
		writer = csv.writer(hr)
		writer.writerow(["Area","Date", "Hour", "Price", "Volume"])
		hour = 0
		for row in range(7,len(table)):
			values = table[row].split()
			price = values[-2]
			volume = values[-1].replace(",","")
			#area = 'Croatia'
			area = "CROPEX"
			hour = hour + 1
			writer.writerow([area, str(tomorrow), hour, str(price), str(volume)])
			
	hr.close()
	
	
def seepex(driver, tomorrow, path_to_docs):
	#seepex
	driver.get("http://seepex-spot.rs/en/market-data/day-ahead-auction/"+str(tomorrow)+"/RS")
	time.sleep(10)
	hours_tbl = driver.find_element_by_class_name("list.hours.responsive").text
	hours_tbl = hours_tbl.split("\n")
	with open(path_to_docs + 'sepex_'+str(tomorrow)+'.csv', 'w') as sr:
		writer = csv.writer(sr)
		writer.writerow(["Area","Date", "Hour", "Price", "Volume"])
		hour = 0
		for row in range(1,len(hours_tbl),2):
			prices = hours_tbl[row].split()
			price = str(prices[-1]).replace(",",".")
			volumes = hours_tbl[row+1].split()
			volume = str(volumes[-1]).replace(",",".")
			#area = "Serbia"
			area = "SEEPEX"
			hour = hour + 1
			writer.writerow([area, str(tomorrow), hour, str(price), str(volume)])
			
	sr.close()

time.sleep(5)

def bulgaria(driver, day, tomorrow, path_to_docs):
	#ibex
	driver.get("http://www.ibex.bg/en/market-data/dam/prices-and-volumes/")
	time.sleep(10)
	calc_tbl = driver.find_elements_by_class_name("calculations-table")
	calculations = calc_tbl[2].text
	calculations = calculations.split("\n")
	with open(path_to_docs + 'ibex_'+str(tomorrow)+'.csv', 'w') as bg:
		writer = csv.writer(bg)
		writer.writerow(["Area","Date", "Hour", "Price", "Volume"])
		for i in range(1,len(calculations), 17): 
			price = calculations[i+15]
			volume = calculations[i+16]
			#area = "Bulgaria"
			area = "IBEX"
			hour = calculations[i][-2:]
			writer.writerow([area, str(tomorrow), int(hour), str(price), str(volume)])
			
	bg.close()

	

