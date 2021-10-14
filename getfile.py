import tempfile
from smb.SMBConnection import SMBConnection
from nmb.NetBIOS import NetBIOS

userID = 'cene'
password = 'Babastamena22'
server_ip = '192.168.100.1'


conn = SMBConnection(userID, password, "cene", "file1", use_ntlm_v2 = True)
assert conn.connect(server_ip, 139)
if conn:
    print ("successfull",conn)
else:
    print ("failed to connect")
    
files = conn.listPath("2!Trading/1! - TRADING", "/") 
for item in files:
    print (item.filename)        
file_obj = tempfile.NamedTemporaryFile()
#file_attributes, filesize = conn.retrieveFile('\\\\file1\\2!Trading\\1! - TRADING\\', 'Berze.xlsx', file_obj)
#file_attributes, filesize = conn.retrieveFile('\\\\file1\\2!Trading\\CENE - BERZE I CBC\\Market_2020.xlsx','Market_2020.xlsx', file_obj)


