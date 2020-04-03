from selenium import webdriver
import time
from datetime import date, timedelta, datetime
import csv
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import ElementClickInterceptedException
import pandas as pd
import mysql.connector as mariadb
from mysql.connector import Error, errorcode
import glob
import os
import sys
import logging

from u2 import ukraine
from n1 import germany
from bih2 import bih2
from t2 import turkey
from i2 import italy
from balkan import *
from g2 import *
from evropa import *

path_to_scripts = "/home/sanja/N4T/"
path_to_docs = "/home/sanja/N4T_docs/"


#setup driver
options = webdriver.ChromeOptions()
prefs = {"download.default_directory" : path_to_docs}
options.add_experimental_option("prefs",prefs)

driver = webdriver.Chrome(chrome_options=options)

#date today
today = date.today()
tomorrow = today + timedelta(days=1)
day = tomorrow.strftime("%d")
month = tomorrow.strftime("%m")
year = tomorrow.strftime("%Y")
date_t = tomorrow.strftime("%d.%m.%Y")
kursna = today.strftime("%d.%m.%Y")
month_slo = tomorrow.strftime("%B")


#setup logging
logf = open(path_to_scripts+str(tomorrow)+".log", "w+")
logging.basicConfig( filename=path_to_scripts+str(tomorrow)+".log", filemode='w', level=logging.WARNING,
										format= '%(asctime)s - %(levelname)s - %(message)s')
										
#open MySQL connection 
try:	
	connection = mariadb.connect(host='localhost', database='N4T', user='root', password='network')
	cursor = connection.cursor()
except mysql.connector.Error as error:
	logging.exception("message")

try:
	jao(driver, day, cursor, connection, path_to_docs, logf)
except:
	logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " An error occured with JAO data \n")
	logging.exception("message")
	pass

try:
	ukraine(driver, kursna, today, day, tomorrow, cursor, connection, path_to_docs, logf)
except:
	logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " An error occured with OREE data \n")
	logging.exception("message")
	pass

try:
	bih2(driver, day, tomorrow, cursor, connection, path_to_docs, logf)	
except:
	logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " An error occured with NOSBIH data \n")
	logging.exception("message")
	pass

try:
	greece(driver, year, month, day, tomorrow, cursor, connection, path_to_docs, logf)
except:
	logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " An error occured with ENEX data \n")
	logging.exception("message")
	pass
	
try:
	germany(driver, today, tomorrow, cursor, connection, path_to_docs, logf)
except:
	logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " An error occured with EPEX data \n")
	logging.exception("message")
	pass
	
try:
	austria(driver, year, tomorrow, cursor, connection, path_to_docs, logf)
except:
	logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " An error occured with EXAA data \n")
	logging.exception("message")
	pass
	
try:
	turkey(driver, today, month_slo, day, tomorrow, cursor, connection, path_to_docs, logf)
except:
	logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " An error occured with EPIAS data \n")
	logging.exception("message")
	pass

try:
	italy(driver, day, month_slo, tomorrow, cursor, connection, path_to_docs, logf)
except:
	logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " An error occured with Italy data \n")
	logging.exception("message")
	pass

try:	
	croatia(driver, tomorrow, path_to_docs)
except:
	logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " An error occured with CROPEX data \n")
	logging.exception("message")
	pass

try:
	seepex(driver, tomorrow, path_to_docs)
except:
	logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " An error occured with SEEPEX data \n")
	logging.exception("message")
	pass

try:
	bulgaria(driver, day, tomorrow, path_to_docs)
except:
	logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " An error occured with IBEX data \n")
	logging.exception("message")
	pass

try:
	balkan_query(tomorrow, cursor, connection, path_to_docs, logf)
except:
	logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " An error occured with IBEX data \n")
	logging.exception("message")
	pass

try:
	slovakia(driver, tomorrow, cursor, connection, path_to_docs, logf)
except:
	logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " An error occured with OKTA data \n")
	logging.exception("message")
	pass

try:
	slovenia(driver, month_slo, tomorrow, cursor, connection, path_to_docs, logf)
except:
	logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " An error occured with Slovenia data \n")
	logging.exception("message")
	pass

try:
	seecao(driver, day, month_slo, year, cursor, connection, path_to_docs, logf)
except:
	logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " An error occured with SEE-CAO data \n")
	logging.exception("message")
	pass


#close webdriver
driver.quit()

#close MySQL connection
try:
	cursor.close()
finally:
	if (connection.is_connected()):
		connection.close()
		logf.write("MySQL connection is closed")
		
import sql2excel
