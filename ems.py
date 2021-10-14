from selenium import webdriver
import time
import os
import pandas as pd
from selenium.webdriver.common.action_chains import ActionChains
from datetime import date, timedelta, datetime
import mysql.connector
from mysql.connector import Error, errorcode
import logging
import numpy as np

def ems(driver, day_to_insert, cursor, connection, path_to_docs, logf):
	#date today
	month = day_to_insert.strftime("%B")
	mont = day_to_insert.strftime("%m")
	mon = int(mont) - 1
	year = day_to_insert.strftime("%Y")
	day = day_to_insert.strftime("%d")

	driver.get("https://transparency.ems.rs/auction/daily/resdailyauc.php?lang=en")
	time.sleep(7)

	#select date
	driver.switch_to_frame(driver.find_element_by_tag_name("iframe"))
	driver.find_element_by_id("date").click()
	time.sleep(3)
	mm = driver.find_element_by_class_name("datepicker--nav-title").text.split(",")[0]
	mbr = datetime.strptime(mm, '%B').strftime('%m')
	if mbr < mont: driver.find_element_by_xpath("//div[@data-action='next']").click()
	elif mbr > mont: driver.find_element_by_xpath("//div[@data-action='prev']").click()
	driver.find_element_by_xpath("//div[@data-date='"+str(int(day))+"' and @data-month='"+str(mon)+"' and @data-year='"+year+"']").click()
	time.sleep(10)

	#open download page
	sel_down_button = driver.find_element_by_xpath("//i[(contains(@class,'fa-download'))]")
#	actions = ActionChains(driver)
#	actions.move_to_element(sel_down_button).perform()
	sel_down_button.click()
	time.sleep(10)

	#select all borders
#	switch_btn = driver.find_element_by_xpath("//*[@class='switch']/label[1]")
#	time.sleep(5)
#	switch_btn.click()
#	time.sleep(5)

	#select csv format
	driver.find_element_by_xpath("//*[@class='format']/div/label[1]").click()
	time.sleep(5)

	#change separator if needed
#	separator_button = driver.find_element_by_xpath("//*[@id='decimal_separator']/div/label[1]")
#	actions.move_to_element(separator_button).perform()
#	separator_button.click()

	#download data
	driver.find_element_by_id("btn_download").click()
	time.sleep(10)

#insert to db	
	ems_insert_query = """INSERT IGNORE INTO aukcija (direction, date, hour, offered_capacity, total_requested, total_allocated, auction_price, sink, source)
													VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) """ 
													
	ems = pd.read_csv(path_to_docs + 'ALLOCATION_RESULTS__'+year+mont+day+'.csv',skiprows=1,sep=',')
	print(ems)
	conditions = [
		(ems['direction'] == "RSHU"),
		(ems['direction'] == "HURS"),
		(ems['direction'] == "RSMK"),
		(ems['direction'] == "MKRS"),
		(ems['direction'] == "RSRO"),
		(ems['direction'] == "RORS")
		]

	# create a list of the values we want to assign for each condition
	sources = ['EMS', 'MAVIR', 'EMS', 'MEPSO', 'EMS', 'TEL']

	sinks = ['MAVIR', 'EMS', 'MEPSO', 'EMS', 'TEL', 'EMS']

	ems['sink'] = np.select(conditions, sinks)
	ems['source'] = np.select(conditions, sources)
	
	if ems.shape[0] < 144 :
		ems.loc[(ems['hour'] > 2) , 'hour'] = ems['hour'] + 1
		print(ems)
	
		ems = ems.append({'direction':"RSHU", 'date':day_to_insert, 'hour': 3, 'ATC':0, 'total_requested':0, 'total_allocated':0, 'auction_price':0, 'sink':'MAVIR', 'source':'EMS'}, ignore_index=True)
		ems = ems.append({'direction':"HURS", 'date':day_to_insert, 'hour': 3, 'ATC':0, 'total_requested':0, 'total_allocated':0, 'auction_price':0, 'sink':'EMS', 'source':'MAVIR'}, ignore_index=True)
		ems = ems.append({'direction':"RSMK", 'date':day_to_insert, 'hour': 3, 'ATC':0, 'total_requested':0, 'total_allocated':0, 'auction_price':0, 'sink':'MEPSO', 'source':'EMS'}, ignore_index=True)
		ems = ems.append({'direction':"MKRS", 'date':day_to_insert, 'hour': 3, 'ATC':0, 'total_requested':0, 'total_allocated':0, 'auction_price':0, 'sink':'EMS', 'source':'MEPSO'}, ignore_index=True)
		ems = ems.append({'direction':"RSRO", 'date':day_to_insert, 'hour': 3, 'ATC':0, 'total_requested':0, 'total_allocated':0, 'auction_price':0, 'sink':'TEL', 'source':'EMS'}, ignore_index=True)
		ems = ems.append({'direction':"RORS", 'date':day_to_insert, 'hour': 3, 'ATC':0, 'total_requested':0, 'total_allocated':0, 'auction_price':0, 'sink':'EMS', 'source':'TEL'}, ignore_index=True)								

			
	cursor.executemany(ems_insert_query, ems.values.tolist())

	os.remove(path_to_docs + 'ALLOCATION_RESULTS__'+year+mont+day+'.csv')
	try:
		connection.commit()				
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records EMS inserted successfully into aukcija table \n")

	except mysql.connector.Error as error:
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records EMS {} \n".format(error))
		pass
	except:
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records EMS \n")
		pass
