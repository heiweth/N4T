import time
from datetime import date, timedelta, datetime
from selenium.webdriver.common.action_chains import ActionChains
import mysql.connector
import csv
import pandas as pd
import os

def ukraine(driver, tomorrow, cursor, connection, path_to_docs, logf):
	#date
	today = date.today()
	day = tomorrow.strftime("%d")
	before = today + timedelta(days=-2)
	kursna = today.strftime("%d.%m.%Y")
	day_after = today + timedelta(days=2)
	
	#kurs
	try:
		driver.get("https://bank.gov.ua/markets/exchangerates?date="+kursna+"&period=daily")
		driver.find_element_by_xpath("//a[@href='/locale?_locale=en']").click()
		list_kurs = driver.find_element_by_id("exchangeRates").text
		list_kurs = list_kurs.split('\n')
		kurs = [x.split()[-1] for x in list_kurs if 'EUR' in x]
		kurs = float(kurs[0])
	except:
#		driver.get("https://www.xe.com/currencytables/?from=UAH&date="+str(before))
		driver.get("https://www.xe.com/currencytables/?from=UAH")
		time.sleep(3)
		try:
			driver.find_element_by_xpath('//*[@id="__next"]/div[3]/section/div[2]/button[2]').click()
		except:
			pass

		table = driver.find_element_by_xpath('//*[@id="table-section"]/section/div[2]/div/table').text
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
	driver.get("https://www.oree.com.ua/index.php/control/results_mo/DAM")
#	market_data = driver.find_element_by_class_name('view_all_link')
#	actions = ActionChains(driver)
#	actions.move_to_element(market_data).perform()
#	market_data.click()
	time.sleep(5)

	#pick date
	driver.find_element_by_class_name('datepicker').click()
	time.sleep(5)
	items = driver.find_elements_by_class_name('day')
	print([x.text for x in items])
	old = driver.find_elements_by_class_name('old')
	print([x.text for x in items])
	sel = []
	for im in items:
		if im not in old and str(im.text) == str(int(day)):
			sel.append(im)

	try:
		[x.click() for x in sel]
	except:
		pass

	driver.find_elements_by_class_name("tab-trade-res")[2].click()
	time.sleep(5)

	area_1 = driver.find_element_by_xpath("//*[@id='pxs_table_res']/tbody/tr[1]/td[8]").text

	table = driver.find_elements_by_class_name("ranges-info")
	table1 = [(x.text).split() for x in table]
	pd_table1 = pd.DataFrame(table1, columns=["Hour", "Price", "Sell Volume", "Purchase volume", "Declared sales", "Declared purchase" ])
	pd_table1.insert(0, "Area", area_1)
	pd_table1["Price"] = pd_table1["Price"].astype(float) / kurs

	pd_bilans1 = pd_table1.drop(columns=["Sell Volume", "Purchase volume", "Declared sales", "Declared purchase"])
	pivot_table1 = pd_bilans1.pivot_table(index="Area", columns="Hour", values="Price")

	pd_table1.insert(1, "Date", str(tomorrow))
	pd_table1["Datetime"] = pd_table1["Date"] + " " + pd_table1["Hour"]
	pd_table1["Datetime"].replace({str(tomorrow)+" 24:00": str(day_after)+" 00:00"}, inplace=True)
	pd_table1["Datetime"] = pd.to_datetime(pd_table1['Datetime'], format='%Y-%m-%d %H:%M')

	pd_table1["Datetime"] = pd_table1["Datetime"] - timedelta(hours=2)
	pd_table1["Date"] = pd_table1["Datetime"].dt.date
	pd_table1["Hour"] = pd_table1["Datetime"].dt.strftime("%H")
	pd_table1["Hour"] = pd_table1["Hour"].astype(int) + 1
	pd_table11 = pd_table1.drop(columns=["Datetime", "Declared sales", "Declared purchase"])

	if str(tomorrow) == '2021-03-28' :
		pd_table11.loc[(pd_table11['Hour'] > 1) , 'Hour'] = pd_table11['Hour'] + 1
		pd_table11 = pd_table11.append({'Area':area_1, "Date":str(tomorrow), "Hour":2, "Price":0, "Sell Volume": 0, "Purchase volume":0}, ignore_index=True)

	driver.find_element_by_xpath("//*[@id='pxs_table_res']/tbody/tr[3]").click()
	time.sleep(5)

	area_2 = driver.find_element_by_xpath("//*[@id='pxs_table_res']/tbody/tr[3]/td[8]").text

	table = driver.find_elements_by_class_name("ranges-info")
	table2 = [(x.text).split() for x in table]
	pd_table2 = pd.DataFrame(table2, columns=["Hour", "Price", "Sell Volume", "Purchase volume", "Declared sales", "Declared purchase" ])
	pd_table2.insert(0, "Area", area_2)
	pd_table2["Price"] = pd_table2["Price"].astype(float) / kurs

	pd_bilans2 = pd_table2.drop(columns=["Sell Volume", "Purchase volume", "Declared sales", "Declared purchase"])
	pivot_table2 = pd_bilans2.pivot_table(index="Area", columns="Hour", values="Price")

	pd_table2.insert(1, "Date", str(tomorrow))
	pd_table2["Datetime"] = pd_table2["Date"] + " " + pd_table2["Hour"]
	pd_table2["Datetime"].replace({str(tomorrow)+" 24:00": str(day_after)+" 00:00"}, inplace=True)
	pd_table2["Datetime"] = pd.to_datetime(pd_table2['Datetime'], format='%Y-%m-%d %H:%M')

	pd_table2["Datetime"] = pd_table2["Datetime"] - timedelta(hours=2)
	pd_table2["Date"] = pd_table2["Datetime"].dt.date
	pd_table2["Hour"] = pd_table2["Datetime"].dt.strftime("%H")
	pd_table2["Hour"] = pd_table2["Hour"].astype(int) + 1
	pd_table22 = pd_table2.drop(columns=["Datetime", "Declared sales", "Declared purchase"])

	if str(tomorrow) == '2021-03-28' :
		pd_table22.loc[(pd_table22['Hour'] > 1) , 'Hour'] = pd_table22['Hour'] + 1
		pd_table22 = pd_table22.append({'Area':area_2, "Date":str(tomorrow), "Hour":2, "Price":0, "Sell Volume": 0, "Purchase volume":0}, ignore_index=True)

#	if area_1 == "UA-BEI":
#		pivot_table1.to_csv(path_to_docs + "bilans_UABEI" + "_"+str(tomorrow)+".csv")
#	else:
#		pivot_table2.to_csv(path_to_docs + "bilans_UABEI" + "_"+str(tomorrow)+".csv")


	ukrajna_insert_query = """INSERT IGNORE INTO berza (area, date, hour, price, volume, buy_volume) 
								                   VALUES (%s,%s,%s,%s,%s,%s) """  
	try:
		cursor.executemany(ukrajna_insert_query, pd_table11.values.tolist())
		connection.commit()
		cursor.executemany(ukrajna_insert_query, pd_table22.values.tolist())	
		connection.commit()				
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records Ukrajna inserted successfully into berza table \n")

	except:
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records Ukrajna area \n")
		pass


