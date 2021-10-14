from urllib.request import build_opener
from smb.SMBHandler import SMBHandler
from datetime import date
import glob
from datetime import date, timedelta, datetime

today = date.today()
tomorrow = today + timedelta(days=1)

userID = 'cene'
password = 'Babastamena22'
server_ip = '192.168.100.1'
today = date.today()

files_fh = glob.glob('/home/cene-admin/n4t_templates/*.xls*')

templates = glob.glob('/home/cene-admin/n4t_templates/*template*.xls*')
bilans = '/home/cene-admin/n4t_templates/Bilans_' + str(tomorrow) + '.xlsx'
market = '/home/cene-admin/n4t_templates/Market_2020.xlsx'
bilansY = '/home/cene-admin/n4t_templates/Bilans_' + str(today) + '.xlsx'

director = build_opener(SMBHandler)

for filename in [bilans,bilansY, market]:
	print(filename)
	try:
		file_fh = open(filename, 'rb')
	except:
		continue
	name = filename.split('/')[-1]
	try:
		fh = director.open('smb://cene:Babastamena22@192.168.100.1/2!Trading/CENE - BERZE I CBC/' + name, data = file_fh)
	except:
		na = name.split('.')[0]
		fh = director.open('smb://cene:Babastamena22@192.168.100.1/2!Trading/CENE - BERZE I CBC/' + na + str(today) +'.xlsx', data = file_fh)

# Reading from fh will only return an empty string
	file_fh.close()

for filename in templates:
	file_fh = open(filename, 'rb')
	print(file_fh)
	name = filename.split('/')[-1]
	try:
		fh = director.open('smb://cene:Babastamena22@192.168.100.1/2!Trading/CENE - BERZE I CBC/' + name, data = file_fh)
	except:
		na = name.split('.')[0]
		fh = director.open('smb://cene:Babastamena22@192.168.100.1/2!Trading/CENE - BERZE I CBC/' + na + str(today) +'.xlsx', data = file_fh)

# Reading from fh will only return an empty string
	file_fh.close()
