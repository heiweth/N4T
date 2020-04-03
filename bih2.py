import time
import os
import pandas as pd
from datetime import date, timedelta, datetime
import csv
from mysql.connector import Error, errorcode
import logging

def bih2(driver, day, tomorrow, cursor, connection, path_to_docs, logf):
	month_slo = tomorrow.strftime("%B")
	year = tomorrow.strftime("%Y")
	driver.get("https://www.nosbih.ba/bh/partneri/rezultati-aukcija/29")
	selected = []
	time.sleep(10)
	driver.find_element_by_id("selectDan").click()
	time.sleep(5)
	els = driver.find_elements_by_class_name("switch")[0].text
#	if month_slo not in str(els):
	if month_slo not in str(els):
		next = driver.find_elements_by_class_name("next")
		next[0].click()
			
	time.sleep(3)
	dates = driver.find_elements_by_class_name('day')
	old_dates = driver.find_elements_by_class_name('day.old')
	for d in dates:
		if d not in old_dates and str(d.text) == str(int(day)): 
				selected.append(d)
	try:
		[x.click() for x in selected]
	except:
		pass
	
	
	time.sleep(5)
	driver.find_element_by_class_name("btn.btn-default.nos-btn-filter").click()
	time.sleep(5)
	table = driver.find_element_by_class_name("table.table-condensed.table-responsive.table-striped.nos-table").text
	table = table.split("\n")

	with open(path_to_docs + "bih_"+str(tomorrow)+".csv", 'w') as tbl:
		writer = csv.writer(tbl)
		writer.writerow(["Hour","Direction", "ATC", "Total requested capacity", "Total allocated capacity", "Price", "No Offers", "No parties"])
		[writer.writerow(table[row].split()) for row in range(1,len(table))]
	tbl.close()

	time.sleep(10)
	
	try:
		bih_insert_query = """INSERT IGNORE INTO aukcija (hour, date, direction, ATC, offered_capacity, total_requested, total_allocated,
												 auction_price, no_of_partis)
												VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) """ 			
					
		bih_df = pd.read_csv(path_to_docs + "bih_" + str(tomorrow) + ".csv")
		bih_df.insert(1,"Date",str(tomorrow))
		hours = bih_df["Hour"].values.tolist()
		hours = [str(int(x.split("h")[0])+1) for x in hours]
		bih_df["Hour"] = hours
		bih_df["Direction"] = bih_df["Direction"].str.replace("-","")		
		bih_1 = bih_df.drop(columns=["No Offers"])
		bih_1.insert(4,"Total Offered",bih_1["ATC"].values.tolist())
		cursor.executemany(bih_insert_query, bih_1.values.tolist())

		connection.commit()				
		os.remove(path_to_docs + "bih_" + str(tomorrow) + ".csv")
		
	except mysql.connector.Error as error:
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert record NOSBIH {} \n".format(error))	
		pass

	
	

