import time
from selenium.webdriver.common.action_chains import ActionChains
from mysql.connector import Error, errorcode
import csv
import mysql.connector
import pandas as pd
import logging
from datetime import date, timedelta, datetime
import glob
import os

def germany(driver, tomorrow, cursor, connection, path_to_docs, logf):
#	today = date.today() + timedelta(days=-1)
#	tomorrow = today + timedelta(days=1)

	
	ids = ["DE-LU", "BE", "CH", "DK1", "DK2", "FI", "FR", "GB", "NL", "NO1", "NO2", "NO3", "NO4", "NO5", "SE1", "SE2", "SE3", "SE4"]
	dict_ids = {"AT": "EXAA", "BE": "Belgium", "CH": "Switzerland", "DE-LU": "EEX","FR": "France", "GB": "Great Britain",
				"NL": "Netherlands", "DK1": "Denmark1", "DK2": "Denmark2", "FI": "Finland", "NO1": "Norway1",
				"NO2": "Norway2", "NO3": "Norway3",	"NO4": "Norway4", "NO5": "Norway5",
				"SE1": "Sweden1", "SE2": "Sweden2", "SE3": "Sweden3", "SE4": "Sweden4"}
	
	ids_2 = ["PHELIX"]

	#nemacka
	driver.get("https://www.epexspot.com/en/market-data")
	time.sleep(5)
	try:
		driver.find_element_by_class_name("icon-chevron-right").click()
	except:
		print("Market data already chosen")
	time.sleep(2)
	try:
#		driver.find_element_by_xpath('//*[@id="popup-buttons"]/button[1]').click()
		driver.find_element_by_class_name("eu-cookie-compliance-save-preferences-button").click()
	except:
		print("Cookies already accepted")
	time.sleep(2)
	try:
		driver.find_element_by_id("edit-filters-data-mode-table").click()
	except:
		print("Table view already selected")
	time.sleep(2)
#	driver.find_element_by_xpath('//select[(contains(@id,"edit-filters-trading-date"))]/option[@value="'+str(today)+'"]').click()

	driver.find_element_by_xpath('//*[(contains(@id,"edit-filters-trading-date"))]').click()
# select day tomorrow
	driver.find_element_by_xpath('//td[(contains(@class,"ui-datepicker-today"))]/a').click()
#	driver.find_element_by_xpath('//td[(contains(@class," "))]/a').click()
	time.sleep(2)
	
#//*[@id="ui-datepicker-div"]/table/tbody/tr[2]/td[2]/a
#<td class=" " data-handler="selectDay" data-event="click" data-month="4" data-year="2021"><a class="ui-state-default" href="#">3</a></td>
#//*[@id="ui-datepicker-div"]/table/tbody/tr[2]/td[2]	
	
# select day today
#	driver.find_element_by_xpath('//td/a[(contains(@class,"ui-state-default"))]').click()
	
	german_insert_query = """INSERT IGNORE INTO berza (area, date, hour, buy_volume, sell_volume, volume, price) VALUES (%s,%s,%s,%s,%s,%s,%s) """  
	#German 2 area	
	german2_insert_query = """INSERT IGNORE INTO berza (area, date, hour, price) 
					                   VALUES (%s,%s,%s,%s) """  
	
	xp = "//input[@value='AT']"
	element = driver.find_element_by_xpath(xp)
	element.click()
	time.sleep(2)

	for key in dict_ids.keys():
		xp = "//input[@value='"+key+"']"
		element = driver.find_element_by_xpath(xp)
#		actions = ActionChains(driver)
#		actions.move_to_element(element).perform()
		element.click()
		time.sleep(2)
		
		list_table = driver.find_element_by_class_name("js-table-values").text.split('\n')
		list_table = [x.split() for x in list_table[12:] ]
		df_key = pd.DataFrame(list_table, columns=["Buy Volume", "Sell Volume", "Volume", "Price"])
		df_key["Buy Volume"] = df_key["Buy Volume"].str.replace(",","")
		df_key["Sell Volume"] = df_key["Sell Volume"].str.replace(",","")
		df_key["Volume"] = df_key["Volume"].str.replace(",","")
		
#		df_key["Price"].replace({",":""}, inplace=True)
		df_key.insert(0,"Area", dict_ids[key])
		df_key.insert(1, "Date", str(tomorrow))
		if str(tomorrow) == '2021-03-28':
			df_key.insert(2, "Hour", [1,2] + [x for x in range(4,25)])
			df_key = df_key.append({"Area": dict_ids[key], "Date": str(tomorrow), "Hour":3, "Buy Volume":0, "Sell Volume":0, "Volume":0, "Price":0}, ignore_index=True)
		else:
			df_key.insert(2, "Hour", [x for x in range(1,25)])
		print(df_key.values.tolist())

		try:
			cursor.executemany(german_insert_query, df_key.values.tolist())
			connection.commit()				
			logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records "+ key + " inserted successfully into berza table \n")

		except mysql.connector.Error as error:
			logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records"+ key +" area {} \n".format(error))
			pass
		except:
			logging.exception("message")
			pass	

	for area in ids_2:	
		xp = "//*[@value='"+area+"']"
		element = driver.find_element_by_xpath(xp)
		actions = ActionChains(driver)
		actions.move_to_element(element).perform()
		element.click()
		time.sleep(3)
		wholetbl = driver.find_element_by_class_name("js-table-values").text.split("\n")
		if len(wholetbl) > 26 :
			df_elix  = pd.DataFrame({"Area": area, "Date": wholetbl[0], "Hour":range(1,25), "Price": wholetbl[3:27]})
			for x in range(1,25):
				if str(tomorrow) == '2021-03-28' and x==3:
					df_elix = df_elix.append({"Area": area, "Date": wholetbl[27], "Hour":x, "Price": 0}, ignore_index=True)	
				elif str(tomorrow) == '2021-03-28' and x>3:
					df_elix = df_elix.append({"Area": area, "Date": wholetbl[27], "Hour":x, "Price": wholetbl[28+x]}, ignore_index=True)	
				else:
					df_elix = df_elix.append({"Area": area, "Date": wholetbl[27], "Hour":x, "Price": wholetbl[29+x]}, ignore_index=True)
		else:
			first_date = datetime.strptime(wholetbl[0], '%d.%m.%Y').strftime('%Y-%m-%d')
			df_elix = pd.DataFrame({"Area":area,"Date": wholetbl[0],"Hour":range(1,25),"Price": wholetbl[2:26]})
		
		df_elix["Date"] = pd.to_datetime(df_elix['Date'], format='%d.%m.%Y').dt.strftime('%Y-%m-%d')

		try:
			cursor.executemany(german2_insert_query, df_elix.values.tolist())
			connection.commit()		 
			logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records "+area + " inserted successfully into berza table \n")

		except mysql.connector.Error as error:
			logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records"+ area +" area {} \n".format(error))
			pass	



