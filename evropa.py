import time
from datetime import date, timedelta, datetime
import mysql.connector
from mysql.connector import Error, errorcode
import csv
import pandas as pd
import os
import glob

today = date.today()
tomorrow = today + timedelta(days=1)
month = tomorrow.strftime("%B")

def slovakia(driver, tomorrow, cursor, connection, path_to_docs, logf):
	#slovacka
	driver.get("https://www.okte.sk/en/short-term-market/published-information/total-stm-results-cz-sk-hu-ro/#date="+str(tomorrow))
	time.sleep(5)
	driver.find_element_by_class_name('dismiss').click()
	button_okte = driver.find_element_by_id('ContentPlaceHolderDefault_BaseMasterContent_SubPageContent_SubPageContentForms_TotalMMCResults_9_btnExportXls')
	button_okte.click()
	time.sleep(5)
	button_daily = driver.find_element_by_xpath('//a[@href="/en/short-term-market/published-information/daily-stm-results/"]').click()
	time.sleep(5)
	button_view = driver.find_element_by_id("ext-gen21").click()
	time.sleep(5)
	button_view = driver.find_element_by_id("ext-gen64").click()
	time.sleep(5)
	
	#insert into db
#	slovacka_insert_query = """INSERT IGNORE INTO berza (area, date, hour, price, volume) 
#													VALUES ('Slovakia',%s,%s,%s,%s) """ 
	slovacka_insert_query = """INSERT IGNORE INTO berza (area, date, hour, price, volume) 
													VALUES ('OKTE',%s,%s,%s,%s) """ 

									
	skmr = pd.read_excel(path_to_docs + 'MMCResults_' + str(tomorrow) + '.xls')
	sk = pd.read_excel(path_to_docs + 'DailyResultsDM_' + str(tomorrow) + '.xls', usecols="A:D",names=["Date","Hour","Price","Volume"])
	succ = 0

	try:
		cursor.executemany(slovacka_insert_query, sk.values.tolist())
		connection.commit()				
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records Slovakia inserted successfully into berza table \n")
		os.remove(path_to_docs + 'DailyResultsDM_' + str(tomorrow) + '.xls')
		succ = succ + 1
	except mysql.connector.Error as error:
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records Slovakia {} \n".format(error))
		pass

	dictm = {"CZ": "Czech", "HU": "Hungary", "RO": "Romania"}
	mpd_insert_query = """INSERT IGNORE INTO berza (area, date, hour, price) 
													VALUES (%s,%s,%s,%s) """
	for m in dictm.keys():
		mpd = skmr[["Period","Price "+m+" (EUR/MWh)"]]
		mpd.insert(0,"Area",dictm[m])
		mpd.insert(1,"Date", str(tomorrow))
		try:
			cursor.executemany(mpd_insert_query, mpd.values.tolist())		
			connection.commit()				
			logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records "+m+" inserted successfully into berza table \n")
			succ += 1		
		except mysql.connector.Error as error:
			logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records "+m+" {} \n".format(error))
			pass

	if succ == 4:		
		os.remove(path_to_docs + 'MMCResults_' + str(tomorrow) + '.xls')
	

def slovenia(driver, month_slo, tomorrow, cursor, connection, path_to_docs, logf):
	#slovenija
	driver.get("https://www.bsp-southpool.com/trading-data.html")
	time.sleep(5)
	driver.find_element_by_class_name("cookiebar__button").click()
	driver.find_element_by_xpath('//a[@title="Download MarketResultsAuction.xlsx"]').click()
	time.sleep(5)
	
	#insert into db
	slovenija_insert_query = """INSERT IGNORE INTO berza (area, date, hour, price, volume) 
						                  VALUES ('Slovenia',%s,%s,%s,%s) """ 
	
	hours_list = [i for i in range(1,25)]
	slo = pd.read_excel(path_to_docs + 'MarketResultsAuction.xlsx', sheet_name=month_slo, skiprows=1, usecols="A,D:AA",names=['Date'] + hours_list)
	slo_1 = slo.dropna()
	slo_1 = slo_1.drop(34)
	slo_1['Date'] = slo_1.loc[:,'Date'].dt.strftime('%Y-%m-%d')
	slo_2 = slo_1[slo_1.Date == str(tomorrow)]
	slo_price = slo_2.iloc[0,1:]
	slo_volume = slo_2.iloc[1,1:]
	data_slo = {"Date":str(tomorrow) , "Hour":hours_list,"Price":slo_price.values.tolist(), "Volume":slo_volume.values.tolist()}
	data_s = pd.DataFrame(data_slo)
	cursor.executemany(slovenija_insert_query, data_s.values.tolist())

	try:
		connection.commit()				
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records Slovenia inserted successfully into berza table \n")
		os.remove(path_to_docs + 'MarketResultsAuction.xlsx')

	except mysql.connector.Error as error:
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records Slovenia {} \n".format(error))
		pass


def seecao(driver, day, month_slo, year, cursor, connection, path_to_docs, logf):
	#SEE-CAO
	driver.get("http://seecao.com/daily-results-download")
	time.sleep(5)
	driver.find_element_by_xpath("//*[@id='node-931']/div/div/div[2]/div[1]/span/a").click()
	time.sleep(8)

	#insert into db	
	seecao_insert_query = """INSERT IGNORE INTO aukcija (direction, date, hour, offered_capacity, total_requested, total_allocated,
													 auction_price, no_of_partis, awarded_partis)
													VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) """ 

	see_cao = glob.glob(path_to_docs + 'Results*Daily Auctions ' + month_slo + ' ' + year + '*.xlsx')
	df =  pd.read_excel(see_cao[0], sheet_name=day + ' ' + month_slo,  header = None)
	data = pd.read_excel(see_cao[0], sheet_name=day + ' ' + month_slo, skiprows=1, usecols="A:H")
	data = data.dropna()
	for row in range(0,df.shape[0],28):
		if 'Auction ID' in df.iat[row,0] :
			direction  = df.iat[row,0].split()[-1].split("-")[0]
			data_direction = data.loc[row:row+25]
			data_direction["Date"] = pd.to_datetime(data_direction['Date'], errors='coerce')
			data_direction["Date"] = data_direction.loc[:,"Date"].dt.strftime('%Y-%m-%d') 
			data_direction.insert(0,"Direction",direction)
			data_direction["Time"] = data_direction.loc[:,"Time"].str[8:10]
			data_direction["Time"] = pd.to_numeric(data_direction["Time"])
			data_direction.loc[row+25,"Time"] = 24
			h = ["Direction","Date","Time","Offered Capacity [MW]","Total Requested Capacity [MW]","Total Allocated Capacity [MW]","Auction Clearing Price [EUR/MWh]","Number of Auction Participants","Number of Successful Participants"]
			cursor.executemany(seecao_insert_query, data_direction.values.tolist())

	try:
		connection.commit()				
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records SEE CAO inserted successfully into aukcija table \n")
		os.remove(see_cao[0])	
	except mysql.connector.Error as error:
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records SEE CAO {} \n".format(error))
		pass
		
def jao(driver, day, cursor, connection, path_to_docs, logf):
	#JAO EU
	driver.get("https://www.jao.eu/marketdata/export/overview")
	time.sleep(5)

	#drop-down lists
	multi_buttons = driver.find_elements_by_class_name("multi-drop-down-button")
	#type
	multi_buttons[1].click()
	driver.find_element_by_xpath("//*[@id='theBody']/div[7]/table/tr[1]/td[1]/input").click()
	multi_buttons[1].click()
	time.sleep(3)

	#border
	multi_buttons[0].click()
	driver.find_element_by_id("select_all_8").click()
	multi_buttons[0].click()
	time.sleep(3)

	#start date
	driver.find_element_by_xpath("//*[@id='pool']/div[1]/div[6]/button").click()
	month_to_pick = driver.find_elements_by_xpath("//td[(contains(@class,'dp_caption'))]")
	if month not in month_to_pick[0].text:
		next_btn = driver.find_elements_by_xpath("//td[(contains(@class,'dp_next'))]")
		next_btn[0].click()
		time.sleep(3)
	day_to_pick = driver.find_elements_by_xpath("//td[not(contains(@class,'dp_not_in_month')) and text()='"+day+"']")
	day_to_pick[0].click()
	time.sleep(3)

	#end date
	driver.find_element_by_xpath("//*[@id='pool']/div[1]/div[7]/button").click()
	month_to_pick = driver.find_elements_by_xpath("//td[(contains(@class,'dp_caption'))]")
	if month not in month_to_pick[1].text:
		next_btn = driver.find_elements_by_xpath("//td[(contains(@class,'dp_next'))]")
		next_btn[1].click()
		time.sleep(3)
	day_to_pick = driver.find_elements_by_xpath("//td[not(contains(@class,'dp_not_in_month')) and text()='"+day+"']")
	day_to_pick[1].click()
	time.sleep(3)

	#apply filters
	driver.find_element_by_xpath("//*[@id='pool']/div[1]/div[13]/div/div").click()
	time.sleep(3)

	#export data
	driver.find_element_by_xpath("//*[@id='pool']/div[4]/div[17]/div/div").click()

	time.sleep(10)
	
	#insert into db
	jao_insert_query = """INSERT IGNORE INTO aukcija (direction, date, hour, offered_capacity, total_requested, total_allocated,
												auction_price, ATC, no_of_partis, awarded_partis)
													VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """ 													
		
	data_jao = pd.read_excel(path_to_docs + 'AR_Export.xlsx')
	df = data_jao[['Border','Day','TimeTable','OfferedCapacity','Total requested capacity','Total allocated capacity','Price','ATC',
										'Number of participants','Awarded participants']]
	df["Border"] = df["Border"].str.replace("-","")									
	df['TimeTable'] = df.loc[:,'TimeTable'].str[6:8]
	df['TimeTable'] = pd.to_numeric(df.loc[:,'TimeTable'])
	df1 = df.fillna("")
	new = df1['OfferedCapacity'].values.tolist()
	newoc = []
	for x in new:
		if 'For' in x :
			newoc.append("")
		else: newoc.append(x)
	df1["OfferedCapacity"] = newoc	
	#df_2 = df_1[~df_1['OfferedCapacity'].str.contains('For',na=False)]
	cursor.executemany(jao_insert_query, df1.values.tolist())

	try:
		connection.commit()				
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records JAO inserted successfully into aukcija table \n")
		os.remove(path_to_docs + 'AR_Export.xlsx')

	except mysql.connector.Error as error:
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records JAO {} \n".format(error))
		pass


