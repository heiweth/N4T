import time
from datetime import date, timedelta, datetime
import csv
from selenium.common.exceptions import NoSuchElementException
import mysql.connector
from mysql.connector import Error, errorcode
import pandas as pd
import os
import logging
 
months = {"January":"gennaio", "February":"febbraio", "March":"marzo", "April":"aprile", "May":"maggio","June":"giugno", "July":"luglio", "August":"agosto", "September":"settembre", "October":"ottobre", "November":"novembre", "December":"dicembre"}

def italy(driver, day, month, tomorrow, cursor, connection, path_to_docs, logf): 
	driver.get("http://www.mercatoelettrico.org/En/Tools/Accessodati.aspx?ReturnUrl=%2fEn%2fEsiti%2fMGP%2fEsitiMGP.aspx")
	time.sleep(3)
	driver.find_element_by_id("ContentPlaceHolder1_CBAccetto1").click()
	driver.find_element_by_id("ContentPlaceHolder1_CBAccetto2").click()
	driver.find_element_by_id("ContentPlaceHolder1_Button1").click()

	driver.find_element_by_xpath("//a[contains(@title,'"+str(int(day))+" " + months[month]+"')]").click()

	volume_head = ["Hour", "Italy", "Austria", "Central-northern Italy", "Corsica AC",	"Corsica",	"Central-southern Italy",	"France",	"Greece",	"Northern Italy",	"Rossano",	"Sardegna",	"Sicilia",	"Slovenia",	"Southern Italy",	"Switzerland",	"Slovenia Coupling",	"France Coupling",	"Austria Coupling",	"Malta",	"Montenegro",	"Hour_"]

	price_head = ["Hour", "Italy", "Italy (unconstrained)", "Austria", "Central-northern Italy", "Corsica AC",	"Corsica",	"Central-southern Italy",	"France",	"Greece",	"Northern Italy",	"Rossano",	"Sardegna",	"Sicilia",	"Slovenia",	"Southern Italy",	"Switzerland",	"Slovenia Coupling",	"France Coupling",	"Austria Coupling",	"Malta",	"Montenegro",	"Hour_"]

	pit = ["Italy", "Central-northern Italy", "Corsica AC",	"Corsica",	"Central-southern Italy",	"Northern Italy",	"Rossano",	"Sardegna",	"Sicilia",	"Southern Italy",	"Malta",	"Montenegro"]


	driver.get("http://www.mercatoelettrico.org/En/Esiti/MGP/TabellaEsitiMGPPrezzi.aspx")
	time.sleep(10)
	price = driver.find_element_by_id("gvFabbisogno").text
	price = price.split('\n')
	price_list = []
	for row in range(1,len(price)):
		price_list.append(price[row].split())

	price_pd = pd.DataFrame(price_list, columns=price_head)
	price_pd = price_pd.drop(columns=["Italy (unconstrained)", "Hour_", "Austria", "France",	"Greece", "Slovenia", "Switzerland",	"Slovenia Coupling",	"France Coupling",	"Austria Coupling"])
	price_pd.to_csv(path_to_docs + "Italy_price_"+str(tomorrow)+".csv")

	time.sleep(3)

	driver.get("http://www.mercatoelettrico.org/En/Esiti/MGP/TabellaEsitiMGPQuantita.aspx")
	time.sleep(10)

	purchased_vol = driver.find_element_by_xpath("//table[contains(@id,'GridQuantit')]").text
	purchased_vol = purchased_vol.split('\n')
	pur_vol = []
	for row in range(1,len(purchased_vol)):
		pur_vol.append(purchased_vol[row].split())

	pur_vol_pd = pd.DataFrame(pur_vol, columns=volume_head)
	pur_vol_pd = pur_vol_pd.drop(columns=["Hour_", "Austria", "France",	"Greece", "Slovenia", "Switzerland",	"Slovenia Coupling",	"France Coupling",	"Austria Coupling"])
	pur_vol_pd.to_csv(path_to_docs + "Italy_purchased_volume_"+str(tomorrow)+".csv")

	time.sleep(3)
	driver.find_element_by_id("LinkButton2").click()
	time.sleep(3)
	sold_vol = driver.find_element_by_id("GridQuantitaVendute").text
	sold_vol = sold_vol.split('\n')
	sell_vol = []
	for row in range(1,len(purchased_vol)):
		sell_vol.append(sold_vol[row].split())

	sold_vol_pd = pd.DataFrame(sell_vol, columns=volume_head)
	sold_vol_pd = sold_vol_pd.drop(columns=["Hour_", "Austria", "France",	"Greece", "Slovenia", "Switzerland",	"Slovenia Coupling",	"France Coupling",	"Austria Coupling"])
	sold_vol_pd.to_csv(path_to_docs + "Italy_sold_volume_"+str(tomorrow)+".csv")


	#insert to db
	italy_insert_query = """INSERT IGNORE INTO berza (area, date, hour, price, volume, buy_volume, sell_volume) 
					                   VALUES (%s,%s,%s,%s,%s,%s,%s) """  

	pit = ["Italy", "Central-northern Italy", "Corsica AC",	"Corsica",	"Central-southern Italy",	"Northern Italy",	"Rossano",	"Sardegna",	"Sicilia",	"Southern Italy",	"Malta",	"Montenegro"]

	price_it = pd.read_csv(path_to_docs + "Italy_price_"+ str(tomorrow) + ".csv")
	purchased_it = pd.read_csv(path_to_docs + "Italy_purchased_volume_"+ str(tomorrow) + ".csv")
	sold_it = pd.read_csv(path_to_docs + "Italy_sold_volume_"+ str(tomorrow) + ".csv")

	success = 0	
	for t in pit:
		price_it[t] = price_it[t].str.replace(".","").str.replace(",",".")
		purchased_it[t] = purchased_it[t].str.replace(".","").str.replace(",",".")
		sold_it[t] = sold_it[t].str.replace(".","").str.replace(",",".")
		volume_pd = pd.concat([purchased_it[t], sold_it[t]], axis=1)
		new_pd = pd.concat([price_it["Hour"], price_it[t], purchased_it[t], sold_it[t]], axis=1)
		new_pd.insert(0,"Area",t)
		new_pd.insert(1,"Date",str(tomorrow))
		new_pd.insert(4,"Volume", volume_pd[[t, t]].max(axis=1))		

		try:		
			cursor.executemany(italy_insert_query, new_pd.values.tolist())
			connection.commit()				
			logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + str(cursor.rowcount) + " Records " + t + " inserted successfully into berza table \n")
			success = success + 1
		except mysql.connector.Error as error:
			logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " Failed to insert records "+ t +" area {} \n".format(error))
			pass

	if success == 12 :
		os.remove(path_to_docs + "Italy_price_"+ str(tomorrow) + ".csv")
		os.remove(path_to_docs + "Italy_purchased_volume_"+ str(tomorrow) + ".csv")
		os.remove(path_to_docs + "Italy_sold_volume_"+ str(tomorrow) + ".csv")

