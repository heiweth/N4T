import time
from datetime import date, timedelta, datetime
import mysql.connector
from selenium.webdriver.common.action_chains import ActionChains
from mysql.connector import Error, errorcode
import csv
import pandas as pd
import os
import glob
import numpy as np
import requests


def poland(driver, tomorrow, cursor, connection, path_to_docs, logf):
	dayt = tomorrow.strftime('%d')
	mont = tomorrow.strftime('%M')
	yeart = tomorrow.strftime('%Y')

	driver.get('https://tge.pl/electricity-dam?dateShow='+dayt+'-'+mont+'-'+yeart+'&dateAction=')
	time.sleep(5)

	

def hungary(driver, tomorrow, cursor, connection, path_to_docs, logf):
	driver.get("https://hupx.hu/en/market-data/dam/weekly-data")
	time.sleep(5)
	try:
		driver.find_element_by_id('accept-cookie').click()
	except:
		pass
	driver.find_element_by_xpath('//a[@title="Select date"]').click()
	driver.find_element_by_xpath('//span[@aria-label="'+tomorrow.strftime("%B")+' '+ tomorrow.strftime("%-d") + ', '+tomorrow.strftime("%Y")+'"]').click()
	driver.find_element_by_class_name('fa-refresh').click()
	time.sleep(3)
	pndv = driver.find_element_by_xpath('//*[@id="main-content"]/div[2]/div/div/div[5]/div/div/table').text.split('\n')
	price = []
	volume = []
	hour = []
	for i in range(2, len(pndv)-1, 2):
		priceRow = pndv[i].split()
		hour.append(priceRow[0][1:])
		volumeRow = pndv[i+1].split()
		price.append(float(priceRow[-1]))
		volume.append(volumeRow[-1].replace(",",""))
	
	dateAhead = pndv[1].split()[-1]
	dateA = datetime.strptime(dateAhead+'/'+tomorrow.strftime("%Y"), "%d/%m/%Y")
	df_hungary = pd.DataFrame({"Area":"HUPX", "Date":dateA, "Hour": hour, "Price":price, "Volume":volume})
	
	#insert into db
	hungary_insert_query = """INSERT IGNORE INTO berza (area, date, hour, price, volume) 
													VALUES (%s,%s,%s,%s,%s) """ 

	try:
		cursor.executemany(hungary_insert_query, df_hungary.values.tolist())
		connection.commit()				
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records Hungary inserted successfully into berza table \n")

	except:
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records Hungary \n")
		pass													
	
def czech(driver, tomorrow, cursor, connection, path_to_docs, logf):	
	year = tomorrow.strftime("%Y")
	month = tomorrow.strftime("%m")
	dday = tomorrow.strftime("%d")
	driver.get("https://www.ote-cr.cz/en/short-term-markets/electricity/day-ahead-market?date="+str(tomorrow))
	time.sleep(5)
	try:
		driver.find_element_by_id("optOutCookieBttnAgree").click()
	except:
		pass
		
	driver.find_element_by_xpath('//a[@href="/pubweb/attachments/01/'+year+'/month'+month+'/day'+dday+'/DM_'+dday+'_'+month+'_'+year+'_EN.xls"]').click()
	time.sleep(8)
	xls_cz = pd.ExcelFile(path_to_docs + 'DM_'+dday+'_'+month+'_'+year+'_EN.xls')
	cz_df = xls_cz.parse(sheet_name='Day-Ahead Market CZ Results', skiprows=[1,2,3,4,5], usecols="A:C", names=["Hour", "Price", "Volume"], nrows=24)
	cz_df.insert(0,"Area","OTE")
	cz_df.insert(1,"Date",str(tomorrow))
	
	#insert into db
	czech_insert_query = """INSERT IGNORE INTO berza (area, date, hour, price, volume) 
													VALUES (%s,%s,%s,%s,%s) """ 

	os.remove(path_to_docs + 'DM_'+dday+'_'+month+'_'+year+'_EN.xls')
	
	try:
		cursor.executemany(czech_insert_query, cz_df.values.tolist())
		connection.commit()				
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records Czech inserted successfully into berza table \n")

	except:
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records Czech \n")
		pass	

	
def romania(driver, tomorrow, cursor, connection, path_to_docs, logf):
	driver.get("https://www.opcom.ro/pp/grafice_ip/raportPIPsiVolumTranzactionat.php?lang=en")
	time.sleep(5)
	dday = driver.find_element_by_xpath('//input[@name="day"]')
	dmonth = driver.find_element_by_xpath('//input[@name="month"]')
	dyear = driver.find_element_by_xpath('//input[@name="year"]')
	driver.execute_script('arguments[0].value = arguments[1]', dday, tomorrow.strftime('%d'))
	driver.execute_script('arguments[0].value = arguments[1]', dmonth, tomorrow.strftime('%m'))
	driver.execute_script('arguments[0].value = arguments[1]', dyear, tomorrow.strftime('%Y'))
	driver.find_element_by_xpath('//input[@value="Refresh"]').click()
	time.sleep(5)
	ro_tbl = driver.find_element_by_class_name('border_table').text.split('\n')
	price = []
	volume = []
	hour = []
	for i in range(1,len(ro_tbl),6):
		hour.append(ro_tbl[i+1])
		price.append(ro_tbl[i+2])
		volume.append(ro_tbl[i+3].replace(",",""))
		
	ro_df = pd.DataFrame({'area':'OPCOM', 'date': str(tomorrow), 'hour': hour, 'price': price,
'volume': volume})

	#insert into db
	romania_insert_query = """INSERT IGNORE INTO berza (area, date, hour, price, volume) 
													VALUES (%s,%s,%s,%s,%s) """ 

	try:
		cursor.executemany(romania_insert_query, ro_df.values.tolist())
		connection.commit()				
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records Romania inserted successfully into berza table \n")

	except:
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records Romania \n")
		pass
	
def slovakia(driver, tomorrow, cursor, connection, path_to_docs, logf):
#	#slovacka
#	driver.get("https://www.okte.sk/en/short-term-market/published-information/total-stm-results-cz-sk-hu-ro/#date="+str(tomorrow))
#	time.sleep(5)
	totalResponse = requests.get("https://isot.okte.sk/api/v1/dam/results?deliveryDayFrom="+str(tomorrow)+"&deliveryDayTo="+str(tomorrow))
	
	slovakiaResponse = requests.get("https://isot.okte.sk/api/v1/dam/results/detail?deliveryDay="+str(tomorrow))

	#insert into db
	slovacka_insert_query = """INSERT IGNORE INTO berza (area, date, hour, price, buy_volume, sell_volume) 
													VALUES (%s,%s,%s,%s,%s,%s) """ 

	priceSlovakia = []
	volumeBuySlovakia = []
	volumeSellSlovakia = []
	period = []
	
	for i in range(len(slovakiaResponse.json())):
		priceSlovakia.append(slovakiaResponse.json()[i]['price'])
		volumeBuySlovakia.append(slovakiaResponse.json()[i]['purchaseSuccessfulVolume'])
		volumeSellSlovakia.append(slovakiaResponse.json()[i]['saleSuccessfulVolume'])
		period.append(slovakiaResponse.json()[i]['period'])
		
	slovakiaDF = pd.DataFrame({'area':'OKTE', 'date': str(tomorrow), 'hour': period, 'price': priceSlovakia,
														 'buy_volume': volumeBuySlovakia, 'sell_volume': volumeSellSlovakia})
	slovakiaDF = slovakiaDF[['area', 'date', 'hour', 'price', 'buy_volume', 'sell_volume']]

#	if slovakiaDF.shape[0] < 24:
	if str(tomorrow) == '2021-03-28':
		slovakiaDF.loc[(slovakiaDF['hour'] > 2) , 'hour'] = slovakiaDF['hour'] + 1
		slovakiaDF = slovakiaDF.append({'area':'OKTE', 'date': str(tomorrow), 'hour': 3, 'price': 0, 'buy_volume': 0, 'sell_volume': 0}, ignore_index=True)
	
	try:
		cursor.executemany(slovacka_insert_query, slovakiaDF.values.tolist())
		connection.commit()				
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records Slovakia inserted successfully into berza table \n")

	except:
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records Slovakia \n")
		pass

#	dictm = {"CZ": "OTE", "HU": "HUPX", "RO": "OPCOM"}
#	mpd_insert_query = """INSERT IGNORE INTO berza (area, date, hour, price) 
#													VALUES (%s,%s,%s,%s) """
													
#	period = []
#	priceRO = []
#	priceHU = []
#	priceCZ = []
#	for i in range(len(totalResponse.json())):
#		priceRO.append(totalResponse.json()[i]['priceRo'])
#		priceHU.append(totalResponse.json()[i]['priceHu'])
#		priceCZ.append(totalResponse.json()[i]['priceCz'])
#		period.append(totalResponse.json()[i]['period'])
#														
#	HU = pd.DataFrame({'area':"HUPX", "date": str(tomorrow), "hour": period, "price": priceHU})
#	RO = pd.DataFrame({'area':"OPCOM", "date": str(tomorrow), "hour": period, "price": priceRO})
#	CZ = pd.DataFrame({'area':"OTE", "date": str(tomorrow), "hour": period, "price": priceCZ})

#	if str(tomorrow) == '2021-03-28':
#		HU.loc[(HU['hour'] > 2) , 'hour'] = HU['hour'] + 1
#		HU = HU.append({'area':'HUPX', 'date': str(tomorrow), 'hour': 3, 'price': 0}, ignore_index=True)
#
#	if str(tomorrow) == '2021-03-28':
#		RO.loc[(RO['hour'] > 2) , 'hour'] = RO['hour'] + 1
#		RO = RO.append({'area':'OPCOM', 'date': str(tomorrow), 'hour': 3, 'price': 0}, ignore_index=True)
#		
#	if str(tomorrow) == '2021-03-28':
#		CZ.loc[(CZ['hour'] > 2) , 'hour'] = CZ['hour'] + 1
#		CZ = CZ.append({'area':'OTE', 'date': str(tomorrow), 'hour': 3, 'price': 0}, ignore_index=True)
#	
#	for region in [HU, RO, CZ]:
#		try:
#			cursor.executemany(mpd_insert_query, region.values.tolist())		
#			connection.commit()		
#			logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records  inserted successfully into berza table \n")
#		except:
#			logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records \n")
#			pass



def slovenia(driver, tomorrow, cursor, connection, path_to_docs, logf):
	month_slo = month_slo = tomorrow.strftime("%B")
	
	#slovenija
	driver.get("https://www.bsp-southpool.com/trading-data.html")
	time.sleep(5)
	driver.find_element_by_class_name("cookiebar__button").click()
	driver.find_element_by_xpath('//a[@title="Download MarketResultsAuction.xlsx"]').click()
	time.sleep(5)
	
	#insert into db
	slovenija_insert_query = """INSERT IGNORE INTO berza (area, date, hour, price, volume) 
						                  VALUES ('SP',%s,%s,%s,%s) """ 
	
	hours_list = [i for i in range(1,25)]	
	try:
		if str(tomorrow) == '2021-03-28':
			hours_special = [i for i in range(4,25)]
			slo = pd.read_excel(path_to_docs + 'MarketResultsAuction.xlsx', sheet_name=month_slo, skiprows=1, usecols="A,D:Z",names=['Date', '1', '2'] + hours_special, engine='openpyxl')
		else:
			slo = pd.read_excel(path_to_docs + 'MarketResultsAuction.xlsx', sheet_name=month_slo, skiprows=1, usecols="A,D:AA",names=['Date'] + hours_list, engine='openpyxl')
	finally:
		os.remove(path_to_docs + 'MarketResultsAuction.xlsx')

	slo_1 = slo.dropna()
	slo_1 = slo_1.drop(34)
	slo_1['Date'] = pd.to_datetime(slo_1['Date'] ,errors = 'coerce',format = '%Y-%m-%d').dt.strftime('%Y-%m-%d')
#	slo_1['Date'] = slo_1.loc[:,'Date'].dt.strftime('%Y-%m-%d')#
	slo_2 = slo_1[slo_1.Date == str(tomorrow)]
#	slo_2 = slo_1[slo_1.Date == '2020-03-29']
	if str(tomorrow) == '2021-03-28':
		slo_2.insert(3, "3", [0, 0], True)
	print(slo_2)
	slo_price = slo_2.iloc[0,1:]
	slo_volume = slo_2.iloc[1,1:]
	data_slo = {"Date":str(tomorrow) , "Hour":hours_list,"Price":slo_price.values.tolist(), "Volume":slo_volume.values.tolist()}
	data_s = pd.DataFrame(data_slo)
	cursor.executemany(slovenija_insert_query, data_s.values.tolist())

	try:
		connection.commit()				
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records Slovenia inserted successfully into berza table \n")

	except:
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records Slovenia \n")
		pass


def seecao(driver, tomorrow, cursor, connection, path_to_docs, logf):
	month_slo = tomorrow.strftime("%B")
	day = tomorrow.strftime("%d")
	year = tomorrow.strftime("%Y")
	
	#SEE-CAO
	driver.get("http://seecao.com/daily-results-download")
	time.sleep(5)
	driver.find_element_by_xpath("//*[@id='node-931']/div/div/div[2]/div[1]/span/a").click()
	time.sleep(8)

	#insert into db	
	seecao_insert_query = """INSERT IGNORE INTO aukcija (direction, date, hour, offered_capacity, total_requested, total_allocated, auction_price, no_of_partis, awarded_partis, sink, source)
													VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """ 

	see_cao = glob.glob(path_to_docs + 'Results*Daily Auctions *' + month_slo + ' *' + year + '*.xlsx')
#	see_cao = glob.glob(path_to_docs + 'Results - Daily Auctions September  2021.xlsx')
	try:
		df =  pd.read_excel(see_cao[0], sheet_name=day + ' ' + month_slo,  header = None, engine='openpyxl')
		data_seecao = pd.read_excel(see_cao[0], sheet_name=day + ' ' + month_slo, skiprows=1, usecols="A:H", engine='openpyxl')
#		df =  pd.read_excel(see_cao[0], sheet_name='29 March',  header = None, engine='openpyxl')
#		data = pd.read_excel(see_cao[0], sheet_name='29 March', skiprows=1, usecols="A:H", engine='openpyxl')


	finally:
		os.remove(see_cao[0])
		print("hello")

	data_seecao = data_seecao.dropna()

	if str(tomorrow) == '2021-03-28':
		date_range = 24
		nrow = 23
	else:
		date_range = 25
		nrow = 24
	
	for row in range(0,df.shape[0],date_range):
		if 'Auction ID' in df.iat[row,0] :
			#direction  = df.iat[row,0].split()[-1].split("-")[0]
			direction = df.iat[row+1,0].split("-")[0]
			print(direction)
			print(row)
			data_direction = pd.DataFrame(columns=df.loc[0,:], data = df.loc[row+1:row+nrow].values.tolist())

			data_direction["Auction ID"] = direction
			data_direction["Date"] = pd.to_datetime(data_direction['Date'], errors='coerce', dayfirst=True)
			data_direction["Date"] = data_direction.loc[:,"Date"].dt.strftime('%Y-%m-%d') 
			data_direction["Time"] = data_direction.loc[:,"Time"].str[8:10]
			data_direction["Time"] = pd.to_numeric(data_direction["Time"])
			data_direction = data_direction.drop(columns=["Congestion Income [EUR]", "Number of Auction Bids"])
			data_direction.loc[23,"Time"] = 24

			print(data_direction)
			if str(tomorrow) == '2021-03-28':
				to_append = [direction, str(tomorrow), 3, 0, 0, 0, 0, 0, 0]
				a_series = pd.Series(to_append, index = data_direction.columns)
				data_direction = data_direction.append(a_series, ignore_index=True)

			conditions = [
					(data_direction['Auction ID'] == "ALGR"),
					(data_direction['Auction ID'] == "ALME"),
					(data_direction['Auction ID'] == "ALXK"),
					(data_direction['Auction ID'] == "BAHR"),
					(data_direction['Auction ID'] == "BAME"),
					(data_direction['Auction ID'] == "GRAL"),
					(data_direction['Auction ID'] == "GRMK"),
					(data_direction['Auction ID'] == "GRTR"),
					(data_direction['Auction ID'] == "HRBA"),
					(data_direction['Auction ID'] == "ITME"),
					(data_direction['Auction ID'] == "MEAL"),
					(data_direction['Auction ID'] == "MEBA"),
					(data_direction['Auction ID'] == "MEIT"),
					(data_direction['Auction ID'] == "MEXK"),
					(data_direction['Auction ID'] == "MKGR"),
					(data_direction['Auction ID'] == "MKXK"),
					(data_direction['Auction ID'] == "TRGR"),
					(data_direction['Auction ID'] == "XKAL"),
					(data_direction['Auction ID'] == "XKME"),
					(data_direction['Auction ID'] == "XKMK")
					]
			
			# create a list of the values we want to assign for each condition
			sources = ['OST', 'OST', 'OST', 'NOS', 'NOS', 'HTSO', 'HTSO', 'HTSO', 'HROTE', 'TERNA', 'EPCG', 'EPCG', 'EPCG', 'EPCG', 'MEPSO', 'MEPSO', 'TEIAS', 'KOSTT', 'KOSTT', 'KOSTT']

			sinks = ['HTSO', 'EPCG', 'KOSTT', 'HROTE', 'EPCG', 'OST', 'MEPSO', 'TEIAS', 'NOS', 'EPCG', 'OST', 'NOS', 'TERNA', 'KOSTT', 'HTSO', 'KOSTT', 'HTSO', 'OST', 'EPCG', 'MEPSO']

			# create a new column and use np.select to assign values to it using our lists as arguments
			data_direction['sink'] = np.select(conditions, sinks)
			data_direction['source'] = np.select(conditions, sources)

			h = ["Auction ID","Date","Time","Offered Capacity [MW]","Total Requested Capacity [MW]","Total Allocated Capacity [MW]","Auction Clearing Price [EUR/MWh]","Number of Auction Participants","Number of Successful Participants", "Sink", "Source"]
			
			print(data_direction.values.tolist())
			cursor.executemany(seecao_insert_query, data_direction.values.tolist())

	try:
		connection.commit()				
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records SEE CAO inserted successfully into aukcija table \n")	
	except:
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records SEE CAO \n")
		pass
		

def jao(driver, tomorrow, cursor, connection, path_to_docs, logf):
	today = date.today()
	day = today.strftime("%d")
	month = today.strftime("%m")
	year = today.strftime("%Y")
	dayt = tomorrow.strftime("%d")
	
	#JAO EU
	driver.get("https://www.jao.eu/auctions#/export")
	time.sleep(5)
	
	driver.find_element_by_class_name("agree-button").click()
	time.sleep(2)
	
	#type
	types = driver.find_elements_by_xpath("//*[@class='auctionsSidebar__radioLabel']")
	try:
		[x.click() for x in types if x.text == 'Daily']
	except:
		pass
		
#	#borders' group
#	driver.find_element_by_css_selector('.reactSelect__value-container').click()
#	driver.find_element_by_css_selector('.reactSelect__menu').click()

#"
	#borders
	for border in ["BG-RS","HU-HR","HR-HU","HR-RS","RS-BG","RS-HR"]:	
		driver.find_element_by_css_selector('.reactSelect__value-container--is-multi').click()
		options = driver.find_elements_by_css_selector('.reactSelect__option')
		try: 
			[x.click() for x in options if x.text == border]
			time.sleep(1)
		except: pass

		
	#start date	
	driver.find_element_by_css_selector('.auctionsSidebar__field--dateRange.-first').click()
	mon = driver.find_element_by_class_name('ui-datepicker-month').text
	print(mon)
	print(month)
#	if mon != month:
#		driver.find_element_by_class_name('ui-datepicker-next').click()
		 
	dates = driver.find_elements_by_class_name('ui-state-default')
	try:
		[x.click() for x in dates if x.text == str(int(dayt))]
	except:
		pass
	
	#end date
	driver.find_elements_by_css_selector('.auctionsSidebar__field--dateRange')[1].click()
	dates = driver.find_elements_by_class_name('ui-state-default')
	try:
		[x.click() for x in dates if x.text == str(int(dayt))]
	except:
		pass
	
	driver.find_element_by_class_name('auctionsSidebar__btn--100').click()
	time.sleep(5)
	driver.find_element_by_css_selector('.auctionsSidebar__btn.btn.btn--default').click()
	
	buttons = driver.find_elements_by_css_selector('button.btn.btn--primary')
	try:
		[x.click() for x in buttons if x.text == 'Export auctions']
	except:
		pass
		
	time.sleep(5)	
	#insert into db
	jao_insert_query = """INSERT IGNORE INTO aukcija (direction, date, hour, offered_capacity, total_requested, total_allocated, auction_price, ATC, no_of_partis, awarded_partis, sink, source)
													VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """ 													
		

	jao_file = glob.glob(path_to_docs + 'JAO_marketdata_export_*'+day+'*-'+month+'*-'+year + '*.xlsx')
#	data_jao = pd.read_excel(path_to_docs + 'JAO_marketdata_export_'+day+'-'+month+'-'+year + '.xlsx', engine='openpyxl')
#	data_jao = pd.read_excel(path_to_docs + 'JAO_marketdata_export_13-09-2021.xlsx', engine='openpyxl')

	data_jao = pd.read_excel(jao_file[0], engine='openpyxl')
	df = data_jao[['Border','Market period start','TimeTable','OfferedCapacity (MW)','Total requested capacity (MW)','Total allocated capacity (MW)','Price (€/MWH)','ATC (MW)','Number of participants', 'Awarded participants']]

	df.Border = df.Border.replace({"-":""}, regex=True)							
	df['Market period start'] = pd.to_datetime(df['Market period start'] ,errors = 'coerce',format = '%d/%m/%Y').dt.strftime('%Y-%m-%d')
	df.TimeTable = df.TimeTable.str[6:8]
	df.TimeTable = pd.to_numeric(df.TimeTable)	


	if str(tomorrow) == '2021-03-28':	
		df.loc[(df1['TimeTable'] == 24) , 'TimeTable'] = 0
		df.loc[(df1['TimeTable'] > 2) , 'TimeTable'] = df1['TimeTable'] + 1
		df.loc[(df1['TimeTable'] == 0) , 'TimeTable'] = 3
	
		
	conditions = [
		(df['Border'] == "HUHR"),
		(df['Border'] == "HRHU"),
		(df['Border'] == "RSHR"),
		(df['Border'] == "HRRS"),
		(df['Border'] == "RSBG"),
		(df['Border'] == "BGRS")
		]

	# create a list of the values we want to assign for each condition
	sources = ['MAVIR', 'HROTE', 'EMS', 'HROTE', 'EMS', 'BG-ESO']

	sinks = ['HROTE', 'MAVIR', 'HROTE', 'EMS', 'BG-ESO', 'EMS']

	# create a new column and use np.select to assign values to it using our lists as arguments
	df['sink'] = np.select(conditions, sinks)
	df['source'] = np.select(conditions, sources)

	cursor.executemany(jao_insert_query, df.values.tolist())
	print(df)
#	os.remove(path_to_docs + 'JAO_marketdata_export_'+day+'-'+month+'-'+year + '.xlsx')
	os.remove(jao_file[0])
	try:
		connection.commit()				
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records JAO inserted successfully into aukcija table \n")

	except:
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records JAO  \n")
		pass
		
