import time
from datetime import date, timedelta, datetime
from selenium.webdriver.common.action_chains import ActionChains
import mysql.connector
from mysql.connector import Error, errorcode
import csv
import pandas as pd
import os

def ukraine(driver, kursna, today, day, tomorrow, cursor, connection, path_to_docs, logf):
	#kurs
	try:
		driver.get("https://bank.gov.ua/markets/exchangerates?date="+kursna+"&period=daily")
		driver.find_element_by_xpath("//a[@href='/locale?_locale=en']").click()
		list = driver.find_element_by_id("exchangeRates").text
		list = list.split('\n')
		kurs = [x.split()[-1] for x in list if 'EUR' in x]
		kurs = float(kurs[0])
	except:
		driver.get("https://www.xe.com/currencytables/?from=UAH&date="+str(today))
		try:
			driver.find_element_by_class_name("privacy-basic-button.privacy-basic-button-submit").click()
		except:
			pass
		table = driver.find_element_by_id("historicalRateTbl").text
		table = table.split("\n")
		kurs = [str(row.split()[3]) for row in table if "EUR" in row]
		kurs = float(kurs[0])

	#ukrajna:
	driver.get("https://www.oree.com.ua/index.php/control/results_mo/DAM")
	time.sleep(5)

	#select language
	driver.find_element_by_id('dropdownMenuLink').click()
	time.sleep(5)
	driver.find_element_by_xpath("//a[@href='/?lang=english']").click()
	time.sleep(5)

	market_data = driver.find_element_by_class_name('view_all_link')
	actions = ActionChains(driver)
	actions.move_to_element(market_data).perform()
	market_data.click()
	time.sleep(5)

	#pick date
	driver.find_element_by_class_name('datepicker').click()
	time.sleep(5)
	items = driver.find_elements_by_class_name('day')
	old = driver.find_elements_by_class_name('old.day')
	sel = []
	for im in items:
		if im not in old and str(im.text) == str(int(day)):
			sel.append(im)

	try:
		[x.click() for x in sel]
	except:
		pass
		#driver.find_elements_by_class_name('active.day').click()
	#select table view
	driver.find_elements_by_class_name("tab-trade-res")[2].click()
	time.sleep(5)

	area_1 = driver.find_element_by_xpath("//*[@id='pxs_table_res']/tbody/tr[1]/td[8]").text

	with open(path_to_docs + area_1 + "_"+str(tomorrow)+".csv", 'w') as file:
		writer = csv.writer(file)
		writer.writerow(["Area", "Date", "Hour", "Price", "Sell Volume", "Purchase volume"])
		table = driver.find_elements_by_class_name("ranges-info")
		for element in table:
			row = str(element.text).split()
			hour = str(int(row[0].split(":")[0]))
			price = str(float(row[1])/kurs)
			sell = str(float(row[2]))
			purchase = str(float(row[3]))
			writer.writerow([area_1, str(tomorrow), hour,price,sell,purchase])
	file.close()


	driver.find_element_by_xpath("//*[@id='pxs_table_res']/tbody/tr[3]").click()
	time.sleep(5)
	area_2 = driver.find_element_by_xpath("//*[@id='pxs_table_res']/tbody/tr[3]/td[8]").text

	with open(path_to_docs + area_2 + "_"+str(tomorrow)+".csv", 'w') as file_2:
		writer = csv.writer(file_2)
		writer.writerow(["Area", "Date", "Hour", "Price", "Sell Volume", "Purchase volume"])
		table_2 = driver.find_elements_by_class_name("ranges-info")
		for elem in table_2:
			row = str(elem.text).split()
			hour = str(int(row[0].split(":")[0]))
			price = str(float(row[1]))
			sell = str(float(row[2]))
			purchase = str(float(row[3]))
			writer.writerow([area_2, str(tomorrow), hour,price,sell,purchase])
	file_2.close()

	time.sleep(10)


	ukrajna_insert_query = """INSERT IGNORE INTO berza (area, date, hour, price, volume, buy_volume) 
								                   VALUES (%s,%s,%s,%s,%s,%s) """  
	ua = ["IPS","BEI"]
	for u in ua:
		try:
			data_u = pd.read_csv(path_to_docs + "UA-" + u +"_"+ str(tomorrow) + ".csv")
			cursor.executemany(ukrajna_insert_query, data_u.values.tolist())	
			connection.commit()				
			logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records Ukrajna inserted successfully into aukcija table \n")
			os.remove(path_to_docs + "UA-" + u +"_"+ str(tomorrow) + ".csv")

		except mysql.connector.Error as error:
			logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records Ukrajna area {} \n".format(error))
			pass


