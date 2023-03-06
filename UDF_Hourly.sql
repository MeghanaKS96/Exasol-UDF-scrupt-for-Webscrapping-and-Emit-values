
CREATE OR REPLACE PYTHON SCALAR SCRIPT OMEI.get_data_TempSoilHourly(stat INT, ft VARCHAR(25)) EMITS(Measurement TIMESTAMP,station_id DECIMAL,quality DECIMAL,temperature_soil_2cm_degree_celsius DECIMAL,temperature_soil_5cm_degree_celsius DECIMAL,temperature_soil_10cm_degree_celsius DECIMAL,temperature_soil_20cm_degree_celsius DECIMAL,temperature_soil_50cm_degree_celsius DECIMAL,temperature_soil_100cm_degree_celsius DECIMAL,interval_min DECIMAL)AS 
import pandas as pd
import requests
import zipfile
import io 
from io import BytesIO
from datetime import datetime as dt
import re
def generate_sql_for_import_spec(import_spec):
  if not "STATION" in import_spec.parameters:
    raise ValueError('Mandatory parameter station is missing')
  elif not "FOLDERTYPE"  in import_spec.parameters:
    raise ValueError('Mandatory parameter foldertype is missing')
  return "SELECT OMEI.get_data_TempSoilHourly("+import_spec.parameters["STATION"]+",'"+import_spec.parameters["FOLDERTYPE"]+"')"
def run(ctx):
 stationid = ''
 targetfile =''
 foldertype=str(ctx[1])
 
 for i in range(5-len(str(ctx[0]))):
    stationid = stationid +'0'
 stationid = stationid + str(ctx[0])   
 
 zip_file_url = '' --Insert URL
 
 if(foldertype=='hist'):
    url=zip_file_url+'historical/'
 elif (foldertype=='akt'):
    url=zip_file_url+'recent/'

 filename = ''+stationid.  -- Insert File name
 req = requests.get(url)
 my_dict = re.findall('(?<=<a href=")[^"]*', str(req.content))
 for x in my_dict:
    if(filename in x ):
     new_url = url + x
    

 req1 = requests.get(new_url)
 z = zipfile.ZipFile(io.BytesIO(req1.content))
 for file in z.filelist:
  if file.filename.startswith(''): --Insert file name
   targetfile= file.filename
 csv_file = z.open(targetfile)
 dataq = pd.read_csv(csv_file,delimiter= ';',header=1)
 for index, row in dataq.iterrows():
    converted_date = str(row[1])
    years = converted_date[0:4]
    months = converted_date[4:6]
    days = converted_date[6:8]
    hours = converted_date[8:10]
    minutes = converted_date[10:12]

    date_time = years + '-' + months + '-' + days +  ' ' + hours 
    date_time_obj = dt.strptime(date_time, '%Y-%m-%d %H')
    intervalmin = 60
    ctx.emit(date_time_obj,row[0],row[2],row[3],row[4],row[5],row[6],row[7],row[8],intervalmin)
    
/
