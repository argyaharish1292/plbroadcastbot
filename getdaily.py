import pysftp
import datetime
import os
import pandas as pd
import sqlalchemy as db
import pymysql

def sftp_get4g():
   #setting file name variable
   curr_day = datetime.datetime.now()
   delta = datetime.timedelta(days = 2) #gap is set to 2 days (subject of change)
   target = curr_day - delta
   filename = "RAW_DATA_4G_DAILY_SUMBAGSEL-"+target.strftime("%Y"+"-"+"%m"+"-"+"%d")+".csv"

   #ignoring hostkeys
   cnopts = pysftp.CnOpts()
   cnopts.hostkeys = None

   #get the file
   try:
      with pysftp.Connection('host', username='user', password='password#', cnopts=cnopts) as sftp:
         with sftp.cd('output/RAW_DATA/DAILY_MONITORING'):             # temporarily chdir to public
            sftp.get(filename)  #get filename 
   except Exception as error:
      print ('There seems to be a problem on conection to the host, please try again')

def sftp_get4g_prev():
   #setting file name variable
   curr_day = datetime.datetime.now()
   delta = datetime.timedelta(days = 3) #gap is set to 2 days (subject of change)
   target = curr_day - delta
   filename = "RAW_DATA_4G_DAILY_SUMBAGSEL-"+target.strftime("%Y"+"-"+"%m"+"-"+"%d")+".csv"

   #ignoring hostkeys
   cnopts = pysftp.CnOpts()
   cnopts.hostkeys = None

   #get the file
   try:
      with pysftp.Connection('host', username='user', password='password#', cnopts=cnopts) as sftp:
         with sftp.cd('output/RAW_DATA/DAILY_MONITORING'):             # temporarily chdir to public
            sftp.get(filename)  #get filename 
   except Exception as error:
      print ('There seems to be a problem on conection to the host, please try again')

def sftp_get3g():
   #setting file name variable
   curr_day = datetime.datetime.now()
   delta = datetime.timedelta(days = 2) #gap is set to 2 days (subject of change)
   target = curr_day - delta
   filename = "RAW_DATA_3G_DAILY_SUMBAGSEL-"+target.strftime("%Y"+"-"+"%m"+"-"+"%d")+".csv"

   #ignoring hostkeys
   cnopts = pysftp.CnOpts()
   cnopts.hostkeys = None

   #get the file
   try:
      with pysftp.Connection('host', username='user', password='password#', cnopts=cnopts) as sftp:
         with sftp.cd('output/RAW_DATA/DAILY_MONITORING'):             # temporarily chdir to public
            sftp.get(filename)  #get filename 
   except Exception as error:
      print ('There seems to be a problem on conection to the host, please try again')

 
def export_sql4g():
   curr_day = datetime.datetime.now()
   delta = datetime.timedelta(days = 2) #gap is set to 2 days (subject of change)
   target = curr_day - delta
   filename = "RAW_DATA_4G_DAILY_SUMBAGSEL-"+target.strftime("%Y"+"-"+"%m"+"-"+"%d")+".csv"
   try:
      data = pd.read_csv(filename)
      df = pd.DataFrame(data)
      engine = db.create_engine('mysql+pymysql://user:pass@host/db')
      df.to_sql(name='4g_ftp', con=engine, if_exists='append', index=False)
   except IOError:
      print ("Targeted file with the name {} not found".format(filename))

def export_sql4g_prev():
   curr_day = datetime.datetime.now()
   delta = datetime.timedelta(days = 3) #3 because it the day before the gap, gap is set to 2 days (subject of change)
   target = curr_day - delta
   filename = "RAW_DATA_4G_DAILY_SUMBAGSEL-"+target.strftime("%Y"+"-"+"%m"+"-"+"%d")+".csv"
   try:
      data = pd.read_csv(filename)
      df = pd.DataFrame(data)
      engine = db.create_engine('mysql+pymysql://user:password@host:3306/db')
      df.to_sql(name='4g_ftp', con=engine, if_exists='append', index=False)
   except IOError:
      print ("Targeted file with the name "+filename+" is not found")

def export_sql3g():
   curr_day = datetime.datetime.now()
   delta = datetime.timedelta(days = 2) #gap is set to 2 days (subject of change)
   target = curr_day - delta
   filename = "RAW_DATA_3G_DAILY_SUMBAGSEL-"+target.strftime("%Y"+"-"+"%m"+"-"+"%d")+".csv"
   try:
      data = pd.read_csv(filename)
      df = pd.DataFrame(data)
      engine = db.create_engine('mysql+pymysql://user:password@host:3306/db')
      df.to_sql(name='3g_ftp', con=engine, if_exists='append', index=False)
   except IOError:
      print ("Targeted file with the name "+filename+" is not found")

def export_sql3g_prev():
   curr_day = datetime.datetime.now()
   delta = datetime.timedelta(days = 10) #gap is set to 2 days (subject of change)
   target = curr_day - delta
   filename = "RAW_DATA_3G_DAILY_SUMBAGSEL-"+target.strftime("%Y"+"-"+"%m"+"-"+"%d")+".csv"
   try:
      data = pd.read_csv(filename)
      df = pd.DataFrame(data)
      engine = db.create_engine('mysql+pymysql://user:password@host:3306/db')
      df.to_sql(name='3g_ftp', con=engine, if_exists='append', index=False)
   except IOError:
      print ("Targeted file with the name "+filename+" is not found")

def mysql_conn():
   mysql_conn = pymysql.connect(host='host',
               user ='userr',
               password = 'password',
               db='db',
               charset='utf8',
               cursorclass=pymysql.cursors.DictCursor)
   return(mysql_conn)

def sftp_hour4g():
   #setting file name variable
   curr_day = datetime.datetime.now()
   filename = "RAW_TWAMP_HUAWEI_HOURLY_SUMBAGSEL-"+curr_day.strftime("%Y"+"-"+"%m"+"-"+"%d")+".csv"

   #ignoring hostkeys
   cnopts = pysftp.CnOpts()
   cnopts.hostkeys = None

   #get the file
   try:
      with pysftp.Connection('host', username='user', password='password', cnopts=cnopts) as sftp:
         with sftp.cd('output/RAW_DATA/NATION_DAILY'):             # temporarily chdir to public
            sftp.get(filename)  #get filename 
   except Exception as error:
      print ('There seems to be a problem on conection to the host, please try again')

def export_sql_hr4g_v1():
   curr_day = datetime.datetime.now()
   filename = "RAW_TWAMP_HUAWEI_HOURLY_SUMBAGSEL-"+curr_day.strftime("%Y"+"-"+"%m"+"-"+"%d")+".csv"
   try:
      conn = mysql_conn()
      cursor = conn.cursor()
      data = pd.read_csv(filename)
      df_temp = pd.DataFrame(data)
      df = df_temp.rename({'Date':'date','Time':'time','Name' : 'region','Source Device Name' : 'source_ne','Source Device Interface IP' : 'source_ip','Source Device Interface Name' : 'source_name','Target Device Name' : 'ne_name','Target Device IP' : 'ne_ip','AVG Two way Packet Loss Ratio(%)' : 'avg_pl','Max Two way Packet Loss Ratio(%)' : 'max_pl','Min Two way Packet Loss Ratio(%)' : 'min_pl','AVG Two way Delay(us)' : 'avg_lat','AVG Two way Jitter(us)' : 'avg_jitt','Max Two way Delay(us)' : 'max_lat','Max Two way Jitter(us)' : 'max_jitt','Min Two way Delay(us)' : 'min_lat','Min Two way Jitter(us)' : 'min_jitt','AVG MOS' : 'avg_mos'}, axis=1)
      cols = "`,`".join([str(i) for i in df.columns.tolist()])
      for i,row in df.iterrows():
         sql="INSERT IGNORE INTO `tbl_hourly` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
         cursor.execute(sql, tuple(row))
         conn.commit()
   except IOError:
      print ("Targeted file with the name "+filename+" is not found")

def export_sql_hr4g_v2():
   curr_day = datetime.datetime.now()
   filename = "RAW_TWAMP_HUAWEI_HOURLY_SUMBAGSEL-"+curr_day.strftime("%Y"+"-"+"%m"+"-"+"%d")+".csv"
   try:
      data = pd.read_csv(filename)
      df_temp = pd.DataFrame(data)
      df = df_temp.rename({'Date':'date','Time':'time','Name' : 'region','Source Device Name' : 'source_ne','Source Device Interface IP' : 'source_ip','Source Device Interface Name' : 'source_name','Target Device Name' : 'ne_name','Target Device IP' : 'ne_ip','AVG Two way Packet Loss Ratio(%)' : 'avg_pl','Max Two way Packet Loss Ratio(%)' : 'max_pl','Min Two way Packet Loss Ratio(%)' : 'min_pl','AVG Two way Delay(us)' : 'avg_lat','AVG Two way Jitter(us)' : 'avg_jitt','Max Two way Delay(us)' : 'max_lat','Max Two way Jitter(us)' : 'max_jitt','Min Two way Delay(us)' : 'min_lat','Min Two way Jitter(us)' : 'min_jitt','AVG MOS' : 'avg_mos'}, axis=1)
      df['avg_pl']=pd.to_numeric(df['avg_pl'], errors='coerce')
      df['max_pl']=pd.to_numeric(df['max_pl'], errors='coerce')
      df['min_pl']=pd.to_numeric(df['min_pl'], errors='coerce')
      engine = db.create_engine('mysql+pymysql://user:pass@host:3306/db')
      df.to_sql(name='temp_table', con=engine, if_exists='append', index=False)
      with engine.begin() as cnx:
         insert_sql = "INSERT IGNORE INTO tbl_hourly (SELECT * FROM temp_table)"
         cnx.execute(insert_sql)
      engine.dispose()
   except IOError:
      print ("Targeted file with the name "+filename+" is not found")

def sftp_hour4g_prev():
   #setting file name variable
   curr_day = datetime.datetime.now()
   delta = datetime.timedelta(days = 1) #gap is set to 2 days (subject of change)
   target = curr_day - delta
   filename = "RAW_TWAMP_HUAWEI_HOURLY_SUMBAGSEL-"+target.strftime("%Y"+"-"+"%m"+"-"+"%d")+".csv"

   #ignoring hostkeys
   cnopts = pysftp.CnOpts()
   cnopts.hostkeys = None

   #get the file
   try:
      with pysftp.Connection('host', username='user', password='pass', cnopts=cnopts) as sftp:
         with sftp.cd('output/RAW_DATA/NATION_DAILY'):             # temporarily chdir to public
            sftp.get(filename)  #get filename 
   except Exception as error:
      print ('There seems to be a problem on conection to the host, please try again')

def export_sql_hr4g_prev2():
   curr_day = datetime.datetime.now()
   delta = datetime.timedelta(days = 1) #gap is set to 2 days (subject of change)
   target = curr_day - delta
   filename = "RAW_TWAMP_HUAWEI_HOURLY_SUMBAGSEL-"+target.strftime("%Y"+"-"+"%m"+"-"+"%d")+".csv"
   try:
      data = pd.read_csv(filename)
      df_temp = pd.DataFrame(data)
      df = df_temp.rename({'Date':'date','Time':'time','Name' : 'region','Source Device Name' : 'source_ne','Source Device Interface IP' : 'source_ip','Source Device Interface Name' : 'source_name','Target Device Name' : 'ne_name','Target Device IP' : 'ne_ip','AVG Two way Packet Loss Ratio(%)' : 'avg_pl','Max Two way Packet Loss Ratio(%)' : 'max_pl','Min Two way Packet Loss Ratio(%)' : 'min_pl','AVG Two way Delay(us)' : 'avg_lat','AVG Two way Jitter(us)' : 'avg_jitt','Max Two way Delay(us)' : 'max_lat','Max Two way Jitter(us)' : 'max_jitt','Min Two way Delay(us)' : 'min_lat','Min Two way Jitter(us)' : 'min_jitt','AVG MOS' : 'avg_mos'}, axis=1)
      df['avg_pl']=pd.to_numeric(df['avg_pl'], errors='coerce')
      df['max_pl']=pd.to_numeric(df['max_pl'], errors='coerce')
      df['min_pl']=pd.to_numeric(df['min_pl'], errors='coerce')
      engine = db.create_engine('mysql+pymysql://user:pass@host:3306/db',pool_size=20, max_overflow=20)
      df.to_sql(name='temp_table', con=engine, if_exists='append', index=False)
      with engine.begin() as cnx:
         insert_sql = "INSERT IGNORE INTO tbl_hourly (SELECT * FROM temp_table)"
         cnx.execute(insert_sql)
      engine.dispose()
   except IOError:
      print ("Targeted file with the name "+filename+" is not found")
