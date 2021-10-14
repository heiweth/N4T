from selenium import webdriver
import time
import os
import pandas as pd
from selenium.webdriver.common.action_chains import ActionChains
from datetime import date, timedelta, datetime
from mysql.connector import Error, errorcode
import logging
import mysql.connector as mariadb

months = {"January":"Januar", "February":"Februar", "March":"Mart", "April":"April", "May":"Maj","June":"Jun", "July":"Jul", "August":"Avgust", "September":"Septembar", "October":"Oktobar", "November":"Novembar", "December":"Decembar"}

def cges(driver, tomorrow, cursor, connection, path_to_docs, logf):
	month_slo = tomorrow.strftime("%B")
	year = tomorrow.strftime("%Y")
	day = tomorrow.strftime("%d")
	mon = tomorrow.strftime("%m")
	#get data

	driver.get("http://www.cges.me/trziste/aukcije-kapaciteta")
	ime = 'Rezultati_za_sajt_'+months[month_slo]+'_'+year+'.xlsx'
#	ime = 'Rezultati_za_sajt_Septembar.xlsx'
	driver.find_element_by_xpath('//a[@href="/images/'+ime+'"]').click()
	time.sleep(2)

	#create dfs
#	df = pd.read_excel(path_to_docs + ime, na_filter=False, sheet_name=day+"."+mon+".")
	dfs = pd.read_excel(path_to_docs + ime, na_filter=False, sheet_name=None)
	for key in dfs.keys():
		df = dfs[key]
		for idx, row in enumerate(df.itertuples(index=False)):
			if "Aukcija za dan" in row: 
				cges_df = df.iloc[idx:,]
				break
		
		cges_np = cges_df.to_numpy()		
		df_rs = pd.DataFrame(columns=["date", "direction","hour","ATC", "offered_capacity", "total_requested", "total_allocated", "auction_price"])  
		df_me = pd.DataFrame(columns=["date", "direction","hour","ATC", "offered_capacity", "total_requested", "total_allocated", "auction_price"])  

		for row in cges_np:
			new_row = list(filter(lambda x: x==0 or x, row))
			if "Aukcija za dan" in new_row:
				datum = str(new_row[1])
			elif "smjer" in new_row:
				direction = "".join(list(filter(None, row))[1].split("->"))
			elif "ATC" in new_row:
				if direction == "MERS":
					ATCme = offered_capacity_me = new_row[1:]
				elif direction == "RSME":
					ATCrs = offered_capacity_rs = new_row[1:]
			elif "zahtijevani kap." in new_row:
				if direction == "MERS":
					total_requested_me = new_row[1:]
				elif direction == "RSME":
					total_requested_rs = new_row[1:]
			elif "dodijeljeni kap." in new_row:
				if direction == "MERS":
					total_allocated_me = new_row[1:]
				elif direction == "RSME":	
					total_allocated_rs = new_row[1:]
			elif "cijena [€/MW]" in new_row:
				new_r = [float(x) for x in new_row[1:]]
				if direction == "MERS":
					auction_price_me = new_r
				elif direction == "RSME":
					auction_price_rs = new_r
	

#		
		#dfs
		df_rs = pd.DataFrame({"date": str(datum), "direction": "RSME", "hour": [x for x in range(1,25)], "ATC": ATCrs, "offered_capacity":offered_capacity_rs, "total_requested":total_requested_rs, "total_allocated": total_allocated_rs, "auction_price": auction_price_rs })

		df_me = pd.DataFrame({"date": str(datum), "direction": "MERS", "hour": [x for x in range(1,25)], "ATC": ATCme, "offered_capacity":offered_capacity_me, "total_requested":total_requested_me, "total_allocated": total_allocated_me, "auction_price": auction_price_me })

	#insert to db		
		cges_insert_query = """INSERT IGNORE INTO aukcija (date, direction, hour, ATC, offered_capacity, total_requested, total_allocated, auction_price)
																VALUES (%s,%s,%s,%s,%s,%s,%s,%s) """ 			
		try:
			cursor.executemany(cges_insert_query, df_rs.values.tolist())	
			cursor.executemany(cges_insert_query, df_me.values.tolist())	
			connection.commit()				
			logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records CGES inserted successfully into aukcija table \n")
		except mysql.connector.Error as error:
			logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records CGES border {} \n".format(error))
			pass		
		
		
	os.remove(path_to_docs + ime)	
