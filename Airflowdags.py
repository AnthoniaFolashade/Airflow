#Importing the Python Libraries to be used
import datetime as dt
import csv
import requests
import os
from datetime import datetime
import requests.exceptions as requests_exceptions
import requests
import json
import logging
import re
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os


# Specifying the project directories
BaseDir = os.getcwd()
RawFiles = os.path.join(BaseDir, "Raw")
Staging = os.path.join(BaseDir, "Staging")
StarSchema = os.path.join(BaseDir, "StarSchema")

os.makedirs(BaseDir, exist_ok=True)
os.makedirs(RawFiles, exist_ok=True)
os.makedirs(Staging, exist_ok=True)
os.makedirs(StarSchema, exist_ok=True)


# Cleaning log file and extracting relevant data
def clean_and_extract_log():
    #Get the log files
    filenames = os.listdir(RawFiles)

    # Defining input and output files
    for files in filenames:
        input_file = os.path.join(RawFiles, files)
        short_output_file = os.path.join(Staging, 'Outputshort.txt')
        long_output_file = os.path.join(Staging, 'Outputlong.txt')

    # Defining the expected number of columns for short and long lines
        short_columns = 14
        long_columns = 18

    # Open output files for writing     
        with open(short_output_file, 'a') as short_file, open(long_output_file, 'a') as long_file:
            with open(input_file, 'r') as file:
                for line in file: 
                    if not line.startswith("#"):
                        split_line = line.split(" ")
                        if len(split_line) == short_columns:
                            short_file.write(line)
                        elif len(split_line) == long_columns:
                            long_file.write(line)
                        else:
                            pass
                            #print("Invalid Line:", line)

# Function to Build Fact Tables
def build_fact_tables():
    input_short_file = os.path.join(Staging, 'Outputshort.txt')
    input_long_file = os.path.join(Staging, 'Outputlong.txt')
    output_fact_file = os.path.join(Staging, 'OutFact1.txt')

    with open(output_fact_file, 'w') as output_file:
        output_file.write("Date,Time,File,IP,BrowserOS,Referrer,RequestStatus,ProcessingTime\n")

    with open(output_fact_file, 'a') as output_file:
        build_fact_from_file(input_short_file, output_file)
        build_fact_from_file(input_long_file, output_file)

def build_fact_from_file(input_file, output_file):
    with open(input_file, 'r') as file:
    
        for line in file:
            split_line = line.split(" ")
            browser_os = split_line[9].replace(",", "")
            file_info = split_line[4].replace(",", "")
            referer = split_line[11].replace(",", "")
            if len(split_line) == 14:
                out = f"{split_line[0]},{split_line[1]},{file_info},{split_line[8]},{browser_os},,{split_line[10]},{split_line[13]}"
            elif len(split_line) == 18:
                out = f"{split_line[0]},{split_line[1]},{file_info},{split_line[8]},{browser_os},{referer},{split_line[12]},{split_line[17]}"
            else:
                #print("Invalid line:", line)
                continue
            
            output_file.write(out)

# Function to extract Date
def extract_date():
    input_file = os.path.join(Staging, 'OutFact1.txt')
    unique_date = set()
    output_file = os.path.join(Staging, 'ExtractDate.txt')

    #Extract unique Date
    with open(input_file, 'r') as input_file:
        input_file_lines = input_file.readlines()
        for line in input_file_lines[1:]:
            split_line = line.split(",")
            date = split_line[0]
            unique_date.add(date)

    #Write the unique Date into ExtractDate file
    with open(output_file, 'a') as output_file:
        output_file.write("Date\n")
        for date in unique_date:        
            output_file.write(date + "\n")


# Function to extract Time
def extract_time():
    input_file = os.path.join(Staging, 'OutFact1.txt')
    unique_time = set()
    output_file = os.path.join(Staging, 'ExtractTime.txt')

    #Extract unique Time
    with open(input_file, 'r') as input_file:
        input_file_lines = input_file.readlines()
        for line in input_file_lines[1:]:
            split_line = line.split(",")
            time = split_line[1]
            unique_time.add(time)

    #Write the unique Time into ExtractTime file
    with open(output_file, 'a') as output_file:
        output_file.write("Time\n")
        for time in unique_time:        
            output_file.write(time + "\n")


# Function to extract File
def extract_file():
    input_file = os.path.join(Staging, 'OutFact1.txt')
    unique_file = set()
    output_file = os.path.join(Staging, 'ExtractFile.txt')

    #Extract unique File
    with open(input_file, 'r') as input_file:
        input_file_lines = input_file.readlines()
        for line in input_file_lines[1:]:
            split_line = line.split(",")
            file = split_line[2]
            unique_file.add(file)

    #Write the unique File
    with open(output_file, 'a') as output_file:
        output_file.write("File\n")
        for file in unique_file:        
            output_file.write(file + "\n")

# Function to extract File Info
def extract_file_info():
    input_file = os.path.join(Staging, 'OutFact1.txt')
    output_file = os.path.join(Staging, 'ExtractFileInfo.txt')

    #Extract file info
    with open(input_file, 'r') as input_file, \
        open(output_file, 'w') as output_file:
 
        lines = input_file.readlines()
        output_file.write("File,Name,Type\n")
 
        for i, line in enumerate(lines[1:]):
            line=line.strip()  
 
            try:
                if not line:  # Skip empty lines
                    continue
 
                if len(line)>3 and line[-3] == ".":
                    name = line[:-3]
                    type = line[-3:]
                    out="{},{},{}".format(line,name,type)
 
                    # Add newline character to all but the last line
                    if i < len(lines) - 2:
                        out += "\n"
                    output_file.write(out)
 
                elif len(line)>4 and line[-4] == ".":
                    name = line[:-4]
                    type = line[-4:]
                    out="{},{},{}".format(line,name,type)
 
                    # Add newline character to all but the last line
                    if i < len(lines) - 2:
                        out += "\n"
                    output_file.write(out)
 
                elif len(line)>5 and line[-5] == ".":
                    name = line[:-5]
                    type = line[-5:]
                    out="{},{},{}".format(line,name,type)
 
                    # Add newline character to all but the last line
                    if i < len(lines) - 2:
                        out += "\n"
                    output_file.write(out)
 
                else:
                    name = line
                    type = ""
 
                    if i == len(lines) - 2:
                        type = "/"
 
                    out="{},{},{}".format(line,name,type)
 
                    # Add newline character to all but the last line
                    if i < len(lines) - 2:
                        out += "\n"
                    output_file.write(out)
 
            except Exception as e:
                print("Error with line:", e)


# Function to extract IP addresses
def extract_ips():
    input_file = os.path.join(Staging, 'OutFact1.txt')
    unique_ips = set()
    output_file = os.path.join(Staging, 'ExtractIP.txt')

    #Extract unique IP addresses
    with open(input_file, 'r') as input_file:
        input_file_lines = input_file.readlines()
        for line in input_file_lines[1:]:
            split_line = line.split(",")
            ip = split_line[3]
            unique_ips.add(ip)

    #Write the unique IP addresses into ExtractIP file
    with open(output_file, 'a') as output_file:
        output_file.write("IP\n")
        for ip in unique_ips:        
            output_file.write(ip + "\n")


# Function to extract BrowserOS
def extract_browser_os():
    input_file = os.path.join(Staging, 'OutFact1.txt')
    unique_browseros = set()
    output_file = os.path.join(Staging, 'ExtractBrowserOS.txt')

    #Extract unique BrowserOS
    with open(input_file, 'r') as input_file:
        input_file_lines = input_file.readlines()
        for line in input_file_lines[1:]:
            split_line = line.split(",")
            browser_os = split_line[4]
            unique_browseros.add(browser_os)

    #Write the unique BrowserOS into ExtractBrowserOS file
    with open(output_file, 'a') as output_file:
        output_file.write("BrowserOS\n")
        for browser_os in unique_browseros:        
            output_file.write(browser_os + "\n")


# Function to extract Referrer
def extract_referrer():
    input_file = os.path.join(Staging, 'OutFact1.txt')
    unique_referrer = set()
    output_file = os.path.join(Staging, 'ExtractReferrer.txt')

    #Extract unique Referrer
    with open(input_file, 'r') as input_file:
        input_file_lines = input_file.readlines()
        for line in input_file_lines[1:]:
                split_line = line.split(",")
                referrer = split_line[5]
                unique_referrer.add(referrer)

    #Write the unique Referrer into ExtractReferrer file
    with open(output_file, 'a') as output_file:
        output_file.write("Referrer\n")
        for referrer in unique_referrer: 
            if referrer != "":     
                output_file.write(referrer + "\n")


# Function to extract RequestStatus
def extract_request_status():
    input_file = os.path.join(Staging, 'OutFact1.txt')
    unique_requeststatus = set()
    output_file = os.path.join(Staging, 'ExtractRequestStatus.txt')

    #Extract unique RequestStatus
    with open(input_file, 'r') as input_file:
        input_file_lines = input_file.readlines()
        for line in input_file_lines[1:]:
            split_line = line.split(",")
            request_status = split_line[6]
            unique_requeststatus.add(request_status)

    #Write the unique RequestStatus into ExtractRequestStatus file
    with open(output_file, 'a') as output_file:
        output_file.write("RequestStatus\n")
        for request_status in unique_requeststatus:        
            output_file.write(request_status + "\n")


# Function to extract ProcessingTime
def extract_processing_time():
    input_file = os.path.join(Staging, 'OutFact1.txt')
    unique_processingtime = set()
    output_file = os.path.join(Staging, 'ExtractProcessingTime.txt')

    #Extract unique ProcessingTime
    with open(input_file, 'r') as input_file:
        input_file_lines = input_file.readlines()
        for line in input_file_lines[1:]:
            split_line = line.split(",")
            processing_time = split_line[7]
            unique_processingtime.add(processing_time)

    #Write the unique ProcessingTime into ExtractProcessingTime file
    with open(output_file, 'a') as output_file:
        output_file.write("ProcessingTime\n")
        for processing_time in unique_processingtime:        
            output_file.write(processing_time)


# Function to create date dimension table
def create_date_dimension():
    input_file = os.path.join(Staging, 'ExtractDate.txt')
    output_file = os.path.join(StarSchema, 'DimDateTable.txt')

    with open(input_file, 'r') as input_file, open(output_file, 'w') as output_file:
        output_file.write("Date,Year,Month,Day,DayOfWeek\n")
        for line in input_file:
            line = line.strip()

            if line != 'Date':
                date_obj = dt.datetime.strptime(line, '%Y-%m-%d').date()
                day_of_week = date_obj.strftime("%A")
                output_file.write(f"{date_obj},{date_obj.year},{date_obj.month},{date_obj.day},{day_of_week}\n")


# Function to create Time dimension table
def create_time_dimension():
    input_file = os.path.join(Staging, 'ExtractTime.txt')
    output_file = os.path.join(StarSchema, 'DimTimeTable.txt')

    with open(input_file, 'r') as input_file, open(output_file, 'w') as output_file:
        output_file.write("time_id,full_time,hour,minute,second\n")
        for line in input_file:
            line = line.strip()

            # Time format is 'HH:MM:SS'
            if line !='Time':
                time_obj = dt.datetime.strptime(line, '%H:%M:%S').time()
                output_file.write(f"{time_obj},{time_obj},{time_obj.hour},{time_obj.minute},{time_obj.second}\n")


# Function to create the File dimension table
def create_file_dimension():
    input_file = os.path.join(Staging, 'ExtractFileInfo.txt')
    output_file = os.path.join(StarSchema, 'DimFileTable.txt')

    with open(input_file, 'r') as input_file, \
        open(output_file, 'w') as output_file:
 
        lines = input_file.readlines()
        output_file.write("File,Name,Type\n")
 
        for i, line in enumerate(lines[1:]):
            line=line.strip()
            split_line = line.split(",")
            file = split_line[0]
            type = split_line[2]  
 
            try:
                if not line:  # Skip empty lines
                    continue
 
                if "/darwin/image" in split_line[1].lower():
                    name = "/Darwin/Image"
                elif "/php" in split_line[1].lower():
                    name = "/phpMyAdmin"
                else:
                    name = split_line[1]
 
                out = "{},{},{}".format(file,name,type)
 
                # Add newline character to all but the last line
                if i < len(lines) - 2:
                    out += "\n"
                output_file.write(out)
               
            except Exception as e:
                print("Error with line:", e)


# Function to create the IP dimension table
def create_ip_dimension():
    input_file = os.path.join(Staging, 'ExtractIP.txt')
    output_file = os.path.join(StarSchema, 'DimIPTable.txt')

    with open(input_file, 'r') as input_file, open(output_file, 'w') as output_file:
        output_file.write("IP,Country Code,Country Name,City,Latitude,Longitude\n")

        for line in input_file:
            line = line.strip()
            request_url = f'https://geolocation-db.com/jsonp/{line}'

            try:
                response = requests.get(request_url)
                result = response.content.decode()

                # Clean the returned string so it contains only the dictionary data for the IP address
                result = result.split("(")[1].strip(")")
                result = json.loads(result)

                # Extract relevant location information
                country_code = result["country_code"]
                country_name = result["country_name"]
                city = result["city"]
                latitude = result["latitude"]
                longitude = result["longitude"]

                # Write location information to the output file
                output_file.write(f"{line},{country_code},{country_name},{city},{latitude},{longitude}\n")
           
            except Exception as e:
                print(f"Error getting location for IP {line}: {str(e)}")


# Function to create the Browser dimension table
def create_browser_dimension():
    input_file = os.path.join(Staging, 'ExtractBrowserOS.txt')
    output_file = os.path.join(StarSchema, 'DimBrowserTable.txt')

    with open(input_file, 'r') as input_file, open(output_file, 'w') as output_file:
        output_file.write("browser_id,browser_name\n")
        
        for line in input_file:
            line = line.strip()

            try:
                if not line:  # Skip empty lines
                    continue
                if "MSIE" in line:
                    browser = "MSIE"
                elif "Netscape" in line:
                    browser = "Netscape"
                elif "Firefox" in line:
                    browser = "Firefox"
                elif "msnbot" in line:
                   browser = "msnbot"
                elif "panscient.com" in line:
                    browser = "panscient.com"
                elif "Baiduspider" in line:
                    browser = "Baiduspider"
                elif "Yandex" in line:
                   browser = "Yandex"
                elif "Safari" in line:
                    browser = "Safari"
                elif "Sogou web spider" in line:
                    browser = "Sogou web spider"
                else:
                    browser = "Other"   
                    
                output_file.write(f"{line},{browser}\n")       

            except Exception as e:
                    print ("Error: ", str(e) )  


# Function to create the OS dimension table
def create_OS_dimension():
    input_file = os.path.join(Staging, 'ExtractBrowserOS.txt')
    output_file = os.path.join(StarSchema, 'DimOSTable.txt')

    with open(input_file, 'r') as input_file, open(output_file, 'w') as output_file:
        output_file.write("os_id,os_name\n")
        
        for line in input_file:
            line = line.strip()

            try:
                if line == 'BrowserOS':
                    continue
                if not line:  # Skip empty lines
                    continue
                if "Windows" in line:
                    OS = "Windows"
                elif "Macintosh" in line:
                    OS = "Macintosh"
                elif "bot" in line:
                    OS = "known robots"
                else:
                    OS = "Other"
                output_file.write(f"{line},{OS}\n")       

            except Exception as e:
                    print ("Error: ", str(e) )  


# Function to create the Referrer dimension table
def create_referrer_dimension():
    input_file = os.path.join(Staging, 'ExtractReferrer.txt')
    output_file = os.path.join(StarSchema, 'DimReferrerTable.txt')

    with open(input_file, 'r') as input_file, open(output_file, 'w') as output_file:
        output_file.write("referrer_id,referrer_url\n")
        for line in input_file:
            line = line.strip()

            split_line = re.split('[?;%]', line)
            referrer_url = split_line[0].strip() 
         
            try:
                if line == 'Referrer': #Skip header
                    continue
                if not line:  #Skip empty lines
                    continue
                output_file.write(f"{line},{referrer_url}\n")
            
            except Exception as e:
                    print ("Error: ", str(e) )  


# Function to create the RequestStatus dimension table
def create_request_status_dimension():
    input_file = os.path.join(Staging, 'ExtractRequestStatus.txt')
    output_file = os.path.join(StarSchema, 'DimRequestStatusTable.txt')

    with open(input_file, 'r') as input_file, open(output_file, 'w') as output_file:
        output_file.write("status_id,description,status_type\n")
        for line in input_file:
            line = line.strip()

            if not line:  # Skip empty lines
                continue
            if line == 'RequestStatus':
                continue
            statuscode = int(line)
            if 200 <= statuscode < 300:
                Type = "Successful"
                if statuscode == 200:
                    Description = "Ok"
                elif statuscode == 206:
                    Description = "Partial Content"
                else:
                    Description = "Other Successful"
            elif 300 <= statuscode < 400:
                Type = "Redirection"
                if statuscode == 302:
                    Description = "Found (or Moved Temporarily)"
                elif statuscode == 304:
                    Description = "Not Modified"
                else:
                    Description = "Other Redirection"
            elif 400 <= statuscode < 500:
                Type = "Client Error"
                if statuscode == 403:
                    Description = "Forbidden"
                elif statuscode == 404:
                    Description = "Not Found"
                elif statuscode == 405:
                    Description = "Method Not Allowed"
                elif statuscode == 406:
                    Description = "Not Acceptable"
                else:
                    Description = "Other Client Error"
            elif 500 <= statuscode < 600:
                Type = "Server Error"
                if statuscode == 500:
                    Description = "Internal Server Error"
                else:
                     Description = "Other Server Error"
            else:
                Description = "Unknown"
                Type = "Unknown"

            output_file.write(f"{line},{Description},{Type}\n")
            

# Function to create the ProcessingTime dimension table
def create_processing_time_dimension():
    input_file = os.path.join(Staging, 'ExtractProcessingTime.txt')
    output_file = os.path.join(StarSchema, 'DimProcessingTimeTable.txt')

    with open(input_file, 'r') as input_file, open(output_file, 'w') as output_file:
        output_file.write("processing_time_id,processing_time_bucket\n")
        for line in input_file:
            line = line.strip()

            
            if not line:  # Skip empty lines
                continue
            if line == 'ProcessingTime':
                continue
            Time = int(line)

            if Time <= 0:
                TimeBucket = "0"
            elif 0 < (Time /1000) <= 0.01:
                TimeBucket = "0 to 0.01"
            elif 0.01 < (Time /1000) <= 0.02:
                TimeBucket = "0.01 to 0.02"
            elif 0.02 < (Time /1000) <= 0.05:
                TimeBucket = "0.02 to 0.05"
            elif 0.05 < (Time /1000) <= 0.1:
                TimeBucket = "0.05 to 0.1"
            elif 0.1 < (Time /1000) <= 0.2:
                TimeBucket = "0.1 to 0.2"
            elif 0.2 < (Time /1000) <= 0.5:
                TimeBucket = "0.2 to 0.5"
            elif 0.5 < (Time /1000) <= 1:
                TimeBucket = "0.5 to 1"
            elif 1 < (Time /1000) <= 2:
                TimeBucket = "1 to 2"
            elif 2 < (Time /1000) <= 5:
                TimeBucket = "2 to 5"
            else:
                TimeBucket = "Above 5"

            output_file.write(f"{line},{TimeBucket}\n")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('ETL_Process', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
    clean_and_extract_task = PythonOperator(
        task_id='clean_and_extract_task',
        python_callable=clean_and_extract_log
    )

    build_fact_tables_task = PythonOperator(
        task_id='build_fact_tables_task',
        python_callable=build_fact_tables
    )

    extract_date_task = PythonOperator(
        task_id='extract_date_task',
        python_callable=extract_date
    )

    extract_time_task = PythonOperator(
        task_id='extract_time_task',
        python_callable=extract_time
    )

    extract_file_task = PythonOperator(
        task_id='extract_file_task',
        python_callable=extract_file
    )

    extract_file_info_task = PythonOperator(
        task_id='extract_file_info_task',
        python_callable=extract_file_info
    )

    extract_ips_task = PythonOperator(
        task_id='extract_ips_task',
        python_callable=extract_ips
    )

    extract_browser_os_task = PythonOperator(
        task_id='extract_browser_os_task',
        python_callable=extract_browser_os
    )

    extract_referrer_task = PythonOperator(
        task_id='extract_referrer_task',
        python_callable=extract_referrer
    )

    extract_request_status_task = PythonOperator(
        task_id='extract_request_status_task',
        python_callable=extract_request_status
    )

    extract_processing_time_task = PythonOperator(
        task_id='extract_processing_time_task',
        python_callable=extract_processing_time
    )

    create_date_dimension_task = PythonOperator(
        task_id='create_date_dimension_task',
        python_callable=create_date_dimension
    )

    create_time_dimension_task = PythonOperator(
        task_id='create_time_dimension_task',
        python_callable=create_time_dimension
    )

    create_file_dimension_task = PythonOperator(
        task_id='create_file_dimension_task',
        python_callable=create_file_dimension
    )

    create_ip_dimension_task = PythonOperator(
        task_id='create_ip_dimension_task',
        python_callable=create_ip_dimension
    )

    create_browser_dimension_task = PythonOperator(
        task_id='create_browser_dimension_task',
        python_callable=create_browser_dimension
    )

    create_OS_dimension_task = PythonOperator(
        task_id='create_OS_dimension_task',
        python_callable=create_OS_dimension
    )

    create_referrer_dimension_task = PythonOperator(
        task_id='create_referrer_dimension_task',
        python_callable=create_referrer_dimension
    )

    create_request_status_dimension_task = PythonOperator(
        task_id='create_request_status_dimension_task',
        python_callable=create_request_status_dimension
    )

    create_processing_time_dimension_task = PythonOperator(
        task_id='create_processing_time_dimension_task',
        python_callable=create_processing_time_dimension
    )

    # Define the task dependencies
    clean_and_extract_task >> build_fact_tables_task
    build_fact_tables_task >> [extract_date_task, extract_time_task, extract_file_task, extract_ips_task,
                               extract_browser_os_task, extract_referrer_task, extract_request_status_task,
                               extract_processing_time_task]
    extract_date_task >> create_date_dimension_task
    extract_time_task >> create_time_dimension_task
    extract_file_task >> extract_file_info_task
    extract_file_info_task >> create_file_dimension_task
    extract_ips_task >> create_ip_dimension_task
    extract_browser_os_task >> [create_browser_dimension_task,create_OS_dimension_task]
    extract_referrer_task >> create_referrer_dimension_task
    extract_request_status_task >> create_request_status_dimension_task
    extract_processing_time_task >> create_processing_time_dimension_task