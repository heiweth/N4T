import time
import os
import pandas as pd
from datetime import date, timedelta, datetime
import csv
from mysql.connector import Error, errorcode
import logging
import numpy as np
import sys
from pytz import timezone
import mysql

def bih2(driver, tomorrow, cursor, connection, path_to_docs, logf):
	month_slo = tomorrow.strftime("%B")
	day = tomorrow.strftime("%d")
	utc = datetime.combine(tomorrow, datetime.min.time())
	UTCzone = timezone('UTC')
	utc = UTCzone.localize(utc)
	print(utc)
	utcts = utc.timestamp()
	print(str(utcts).split(".")[0])
	driver.get("https://www.nosbih.ba/bs/kapaciteti/rezultati-aukcija/")
	time.sleep(3)
	driver.find_element_by_id("datepicker").click()
	xpath = "//*[@data-date='"+str(utcts).split(".")[0]+"000']"
	disabled = driver.find_elements_by_class_name('disabled') 
	date2select = driver.find_element_by_xpath(xpath)
	if date2select in disabled:
		sys.exit()
	else:
		try:
			date2select.click()
		except:
			pass

	driver.find_element_by_id("selectDateRegionButton").click()
	time.sleep(10)
	
	if driver.find_element_by_id("auctionTable").text == 'Nema podataka!':
		sys.exit()
	
	table = driver.find_element_by_id("auctionTable").text.split('\n')
	table_data = [x.split() for x in table[1:-1]]
	print(table_data)
	df_nosbih = pd.DataFrame(columns=["Hour","Date", "Direction", "ATC", "Total Offered", "Total requested capacity", "Total allocated capacity", "Price", "No parties"])
	for row in table_data:
		df_nosbih = df_nosbih.append({"Hour":int(row[0].split(":")[0])+1,
																	"Date":str(tomorrow), 
																	"Direction":row[3].replace("-",""), 
																	"ATC":row[4], 
																	"Total Offered":row[4], 
																	"Total requested capacity":row[5], 
																	"Total allocated capacity":row[6], 
																	"Price":row[7], 
																	"No parties":row[9]}, ignore_index=True)


	print(df_nosbih)
	
	if str(tomorrow) == '2021-03-28':
		df_nosbih = df_nosbih.append({"Hour":3,"Date":str(tomorrow), "Direction":"RSBA", "ATC":0, "Total Offered":0, "Total requested capacity":0, "Total allocated capacity":0, "Price":0, "No parties":0}, ignore_index=True)
		df_nosbih = df_nosbih.append({"Hour":3,"Date":str(tomorrow), "Direction":"BARS", "ATC":0, "Total Offered":0, "Total requested capacity":0, "Total allocated capacity":0, "Price":0, "No parties":0}, ignore_index=True)
		
	conditions = [
		(df_nosbih['Direction'] == "RSBA"),
		(df_nosbih['Direction'] == "BARS")
		]

	# create a list of the values we want to assign for each condition
	sources = ['EMS', 'NOS']

	sinks = ['NOS', 'EMS']

	df_nosbih['sink'] = np.select(conditions, sinks)
	df_nosbih['source'] = np.select(conditions, sources)


	bih_insert_query = """INSERT IGNORE INTO aukcija (hour, date, direction, ATC, offered_capacity, total_requested, total_allocated, auction_price, no_of_partis, sink, source)
												VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """ 
	try:
		cursor.executemany(bih_insert_query, df_nosbih.values.tolist())
		connection.commit()
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records NOSBIH inserted successfully into aukcija table \n")				
		
	except mysql.connector.Error as error:
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert record NOSBIH {} \n".format(error))	
		pass

	
	

