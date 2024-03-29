import time
from datetime import date, timedelta, datetime
from mysql.connector import Error, errorcode
import csv
import pandas as pd
import os

def turkey(driver, tomorrow, cursor, connection, path_to_docs, logf):
	today = date.today()
	month = tomorrow.strftime("%B")
	day = tomorrow.strftime("%d")
	before = today + timedelta(days=-2)
	
	#kurs
	driver.get("https://www.xe.com/currencytables/?from=TRY")
	time.sleep(3)
	try:
		driver.find_element_by_xpath('//*[@id="__next"]/div[3]/section/div[2]/button[2]').click()
	except:
		pass

	table = driver.find_element_by_xpath('//*[@id="table-section"]/section/div[2]/div/table').text
	table = table.split("\n")
	kurs = [str(row.split()[2]) for row in table if "EUR" in row]
	kurs = float(kurs[0])

	driver.get("https://rapor.epias.com.tr/rapor/xhtml/ptfSmfListeleme.xhtml")
	time.sleep(10)

	#buttons TR, EN, Display
	driver.find_element_by_xpath('//*[@id="languageForm:j_idt22"]/div[2]/span').click()
#	driver.find_element_by_xpath('//*[@id="languageForm:j_idt29"]/div[2]/span').click()
	time.sleep(5)

	#chose date
	driver.find_element_by_xpath('//*[@id="PmumBI:baslangicTarih"]/button/span[1]').click()
	time.sleep(3)
	month_pick = driver.find_element_by_class_name("ui-datepicker-month").text
	if month_pick != month:
		driver.find_element_by_xpath('//*[@id="ui-datepicker-div"]/div/a[2]').click()
		time.sleep(3)
	dates = driver.find_elements_by_xpath("//td[@data-handler='selectDay']")
	[x.click() for x in dates if str(x.text) == str(int(day))]
	time.sleep(3)

	driver.find_element_by_xpath('//*[@id="PmumBI:bitisTarih"]/button/span[1]').click()
	time.sleep(3)
	month_pick = driver.find_element_by_class_name("ui-datepicker-month").text
	if month_pick != month:
		driver.find_element_by_xpath('//*[@id="ui-datepicker-div"]/div/a[2]').click()
		time.sleep(3)
	dates = driver.find_elements_by_xpath("//td[@data-handler='selectDay']")
	[x.click() for x in dates if str(x.text) == str(int(day))]
	time.sleep(3)
	driver.find_element_by_xpath('//*[@id="PmumBI:j_idt38"]').click()
	time.sleep(3)

	#download
	driver.find_element_by_xpath("//*[@id='PmumBI:ptfSmfList_paginator_bottom']/a[7]").click()
	time.sleep(10)

	#create new cv with corrected datetime and currency
	turska_df = pd.read_csv(path_to_docs + "ptf-smf.csv")
	os.remove(path_to_docs + "ptf-smf.csv")
	date_turska = turska_df["Date"].values.tolist()
	turska_df["Date"] = [(datetime.strptime(x, "%d.%m.%y %H:%M") - timedelta(hours=2)).strftime('%Y-%m-%d %H') for x in date_turska]
	price_list = turska_df['MCP '].values.tolist()
	turska_df['MCP '] = [float(x)*kurs for x in price_list]
	split_date = turska_df["Date"].str.split()		
	data = split_date.tolist()
	datae = [[data[i][0],str(int(data[i][1])+1)] for i in range(len(data))]
	names = ["Date","Hour"]
	turkey_df = pd.DataFrame(datae, columns=names)
	turkey_df.insert(2,'Price ', turska_df['MCP '])

	try:				
		#Turska
		turkey_insert_query = """INSERT IGNORE INTO berza (area, date, hour, price) 
						                   VALUES ('PMUM',%s,%s,%s) """ 						
		cursor.executemany(turkey_insert_query, turkey_df.values.tolist())

		connection.commit()				
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records PMUM inserted successfully into berza table \n")
	
	except:
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records Turkey \n")
		pass
