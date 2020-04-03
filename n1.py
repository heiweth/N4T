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

def germany(driver, today, tomorrow, cursor, connection, path_to_docs, logf):
	ids = ["DE-LU","BE","CH", "FR", "GB", "NL"]
	dict_ids = {"DE-LU":"EEX","BE":"Belgium","CH":"Switzerland", "FR":"France", "GB":"Great Britain", "NL":"Netherlands"}
	
	ids_2 = ["ELIX","PHELIX"]

	#nemacka
	driver.get("https://www.epexspot.com/en/market-data")
	time.sleep(5)
	try:
		driver.find_element_by_class_name("icon-chevron-right").click()
	except ElementClickInterceptedException:
		print("Market data already chosen")
	time.sleep(2)
	try:
		driver.find_element_by_xpath('//*[@id="popup-buttons"]/button[1]').click()
	except ElementClickInterceptedException:
		print("Cookies already accepted")
	time.sleep(2)
	try:
		driver.find_element_by_id("edit-filters-data-mode-table").click()
	except ElementClickInterceptedException:
		print("Table view already selected")
	time.sleep(2)
	driver.find_element_by_xpath("//option[@value='"+str(today)+"']").click()
	
	for key in ids:	
		xp = "//*[@value='"+key+"']"
		time.sleep(2)
		element = driver.find_element_by_xpath(xp)
		actions = ActionChains(driver)
		actions.move_to_element(element).perform()
		element.click()

		dd = driver.find_element_by_tag_name('h2').text.split('>')[-1]
		time.sleep(2)
		with open(path_to_docs + "epex_"+key+"_"+str(tomorrow)+".csv", 'w') as file:
			hour = 0
			writer = csv.writer(file)
			writer.writerow(["Area", "Date", "Hour", "Price", "Volume", "Buy Volume", "Sell Volume"])
			list = driver.find_element_by_class_name("js-table-values").text.split('\n')
			for row in range(5,len(list)):
				values = list[row].split()
				hour = hour + 1
				buy_volume_epex = float(values[0].replace(",",""))
				sell_volume_epex = float(values[1].replace(",",""))
				volume_epex = float(values[2].replace(",",""))
				prices_epex = float(values[3].replace(",",""))
					
				writer.writerow([dict_ids[key], str(tomorrow), str(hour), str(prices_epex), str(volume_epex), str(buy_volume_epex), str(sell_volume_epex)])
		
		file.close()

	german_insert_query = """INSERT IGNORE INTO berza (area, date, hour, price, volume, buy_volume, sell_volume) 
						                   VALUES (%s,%s,%s,%s,%s,%s,%s) """  
	we = ["DE-LU","BE","CH", "FR", "GB", "NL"]

	for i in we:
		try:
			data = pd.read_csv(path_to_docs + "epex_" + i +"_"+ str(tomorrow) + ".csv")
			cursor.executemany(german_insert_query, data.values.tolist())
			connection.commit()				
			logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records "+ i + "inserted successfully into berza table \n")
			os.remove(path_to_docs + "epex_" + i +"_"+ str(tomorrow) + ".csv")

		except mysql.connector.Error as error:
			logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records"+ i +" area {} \n".format(error))
			pass
		except:
			logging.exception("message")
			pass	

	for area in ids_2:	
		xp = "//*[@value='"+area+"']"
		time.sleep(2)
		element = driver.find_element_by_xpath(xp)
		actions = ActionChains(driver)
		actions.move_to_element(element).perform()
		element.click()
		time.sleep(3)
		wholetbl = driver.find_element_by_class_name("js-table-values").text
		wholetbl = wholetbl.split("\n")
		if len(wholetbl) > 26 :
			first_date = datetime.strptime(wholetbl[0], '%d.%m.%Y').strftime('%Y-%m-%d')
			first_tbl = pd.DataFrame({"Area":area,"Date": first_date,"Hour":range(1,25),"Price": wholetbl[3:27]})
			first_tbl.to_csv(path_to_docs + "epex_"+area+"_"+first_date+".csv", index=False)
			second_date = datetime.strptime(wholetbl[27], '%d.%m.%Y').strftime('%Y-%m-%d')
			second_tbl = pd.DataFrame({"Area":area,"Date": second_date,"Hour":range(1,25),"Price": wholetbl[30:54]})
			second_tbl.to_csv(path_to_docs + "epex_"+area+"_"+second_date+".csv", index=False)
		else:
			first_date = datetime.strptime(wholetbl[0], '%d.%m.%Y').strftime('%Y-%m-%d')
			first_tbl = pd.DataFrame({"Area":area,"Date": first_date,"Hour":range(1,25),"Price": wholetbl[2:26]})
			first_tbl.to_csv(path_to_docs + "epex_"+area+"_"+first_date+".csv", index=False)


	#German 2 area	
	german2_insert_query = """INSERT IGNORE INTO berza (area, date, hour, price) 
					                   VALUES (%s,%s,%s,%s) """  
	xs = ["ELIX", "PHELIX"]
	for x in xs:
		filesx = glob.glob(path_to_docs + "epex_" + x +"*.csv")
		suxs = 0
		for filex in filesx:
			data_x = pd.read_csv(filex)
			try:
				cursor.executemany(german2_insert_query, data_x.values.tolist())
				connection.commit()		 
				logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records "+ x + " inserted successfully into berza table \n")
				suxs = suxs +1
				
			except mysql.connector.Error as error:
				logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records"+ x +" area {} \n".format(error))
				pass	

		if suxs == 2:
			to_remove = glob.glob(path_to_docs + "epex_"+x+"*.csv")
			for f in to_remove: os.remove(f)

