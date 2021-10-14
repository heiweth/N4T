from selenium import webdriver
import mysql.connector as mariadb
from datetime import date, timedelta, datetime
import logging

from u2 import ukraine
from n1 import germany
from bih2 import bih2
from t2 import turkey
from i2 import italy
from balkan import *
from g2 import *
from evropa import *
from ems import ems
from cges import cges

path_to_docs = "/home/cene-admin/n4t_docs/"
path_to_logs = "/home/cene-admin/n4t_logs/"


#setup driver
options = webdriver.ChromeOptions()
prefs = {"download.default_directory" : path_to_docs}
options.add_experimental_option("prefs",prefs)
options.add_argument("--remote-debugging-port=9222")  # this
#options.add_argument("--disable-gpu")

driver = webdriver.Chrome('/usr/bin/chromedriver', options=options)
driver.maximize_window()

#date today
today = date.today()
tomorrow = today + timedelta(days=1)
before = today + timedelta(days=-1)


#setup logging
logf = open(path_to_logs+str(tomorrow)+".log", "w+")
loge = open(path_to_logs+"successful"+str(tomorrow)+".log", "w+")
logging.basicConfig( filename=path_to_logs+str(tomorrow)+".log", filemode='w', level=logging.WARNING,
										format= '%(asctime)s - %(levelname)s - %(message)s')

#open MySQL connection
try:
	connection = mariadb.connect(host='192.168.100.145', database='cene', user='cene', password='Babastamena22')
	cursor = connection.cursor()
except mysql.connector.Error as error:
	logging.exception("message")

#data_to_browse = [cges, jao, ukraine, seepex, croatia, bulgaria, turkey, germany, austria, italy, slovakia, slovenia, seecao, ems, bih2, greece]

data2browse = {ems:["RSMK","MKRS","RSRO","RORS","RSHU","HURS"],bih2:["BARS","RSBA"],
				jao:["ATHU","HUAT","HRHU","HUHR","BGGR","GRBG","HRRS","RSHR","BGRS","RSBG","GRIT","ITGR","ATCZ",
				"CZAT", "DE(TenneT)CZ", "CZDE(TenneT)"], hungary: ['HUPX'],
				seecao:["BAHR","HRBA","MEBA","BAME","ALME","MEAL","ALGR","GRAL","GRTR","TRGR","MKGR","GRMK"],
				ukraine:["UA-BEI", "UA-IPS"], seepex: "SEEPEX", croatia: "CROPEX", turkey: "PMUM",
				germany: "EEX", greece: "SMP", italy: ["BRINDISI", "PUN"], romania: "OPCOM", slovenia: "SP",
				slovakia: "OKTE", czech:"OTE", bulgaria: "IBEX"}
data2log = {ems: "EMS", bih2: "NOSBIH", jao: "JAO", seecao: "SEECAO", ukraine: "UA-BEI and UA-IPS", 
				seepex: "SEEPEX", croatia: "CROPEX", bulgaria: "IBEX", turkey: "PMUM", hungary: 'HUPX',
				germany: "EEX", greece: "SMP", italy: "BRINDISI and PUN", slovenia: "SP",romania: "OPCOM", 
				slovakia: "OKTE", czech:"OTE",  cges: "RSME and MERS"}
				
for dtb in data2browse.keys():
#for dtb in [germany]:
	try:
		dtb(driver, tomorrow, cursor, connection, path_to_docs, logf)
		loge.write(data2log[dtb] + "\n")
	except:
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + " An error occured with "+data2log[dtb]+" data \n")
		logging.exception("message")
		pass

#close webdriver
driver.quit()

#fill source and sink columns
TSOS = [("EMS","RS"),("NOS","BA"),("HROTE","HR"),("MEPSO","MK"),("EPCG","ME"),("OST","AL"),("HTSO","GR"),("TEIAS","TR"), ("APG","AT"), ("BG-ESO","BG"),("MAVIR","HU"),("TEL","RO"),("TERNA", "IT"), ("CEPS", "CZ"),
("Swissgrid", "CH"), ("ELES", "SI"), ("ELIA", "BE"), ("RTE", "FR"), ("PSE", "PL"), ("SEPS", "SK"), 
("Statnett", "NO")]

#for TSO in TSOS:
#	query_source = "update aukcija set source='%s' where date='%s' and direction like '%s';" % (TSO[0], tomorrow, TSO[1] + "%")
#	query_sink = "update aukcija set sink='%s' where date='%s' and direction like '%s';" % (TSO[0], tomorrow, "%" + TSO[1])
		
#	cursor.execute(query_source)
#	cursor.execute(query_sink)
	
#	connection.commit()



#close MySQL connection
try:
	cursor.close()
finally:
	if (connection.is_connected()):
		connection.close()
		logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + "MySQL connection is closed\n")
		
import sql2excel
logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + "Market file done\n")
import sql4templates
logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + "Templates done\n")
import sql6bilans
import sql8bilans
logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + "Bilans file done\n")
import excel2smb
logf.write((datetime.now()).strftime("%Y-%m-%d %H:%M:%S") + "Export done\n")		
logf.close()
