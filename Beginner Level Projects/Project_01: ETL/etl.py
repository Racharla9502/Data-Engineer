import pandas as pd 
import glob
from datetime import datetime

# first step to create a log file, and the target file
log_file = 'etl_log.txt'
target_file = 'target_data.txt'

# then download the ibm "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/datasource.zip " data available link, this link is from the practice of the data engnieer project
# we can not directly download the file by using the link in vscode, insterd use this link "curl -O "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/datasource.zip"
# for unzipping use "unzip datasource.zip" command in the terminal, because the file name is datasource.zip

# here we will be performing 3 functions: for the ".csv", ".json", and ".xml" files, that are to be proceeded in the etl process for extraction
def extract_from_csv(file_to_process):
    df = pd.read_csv(file_to_process)
    return df

def extract_from_json(file_to_process):
    df = pd.read_json(file_to_process, lines=True)  # assuming the JSON file has a line-delimited format
    return df

def extract_from_xml(file_to_process):
    df = pd.read_xml(file_to_process)
    return df

# now create the extract function, this is the first step/process in the ETL
def extract():
    # now create an empty file to hold the extracted files
    extract_file = pd.DataFrame(columns=['car_model', 'year_of_manufacture', 'price', 'fuel'])

    # now we will be processing all the 3 different format files and stored/ extracted into the "extract_file"
    # for doing this install the module glob
    for csvfile in glob.glob("*.csv"):
        if csvfile != 'target_data.txt':
            df = extract_from_csv(csvfile)
            extract_file = pd.concat([extract_file, df], ignore_index=True)

    for jsonfile in glob.glob("*.json"):
        if jsonfile != 'target_data.txt':
            df = extract_from_json(jsonfile)
            extract_file = pd.concat([extract_file, df], ignore_index = True)

    for xmlfile in glob.glob("*.xml"):
        if xmlfile!= 'target_data.txt':
            df = extract_from_xml(xmlfile)
            extract_file = pd.concat([extract_file, df], ignore_index=True)

    return extract_file

# now create the transform function, this is the second step/process in the ETL
def transform(data):
    # Transform the values under the 'price' header such that they are rounded to 2 decimal places.
    data['price'] = data['price'].round(2)

    return data

# now create the load function, this is the third step/process in the ETL
def load(target_file, transformed_data):
    transformed_data.to_csv(target_file)

# now creat the log progress, this is the fourth step/process in the ETL
def log_progress(message):
    now = datetime.now()
    timestamp = now.strftime('%A, %d %B %Y %I:%M:%S:%f %p') #ex: "Sunday, 06 July 2025 02:25:30:123 PM"
    # keep only first 3 digits of microseconds (milliseconds)
    timestamp = timestamp.replace(now.strftime(':%f'), ':' + now.strftime('%f')[:3])
    with open(log_file, 'a') as log:
        log.write(f"{timestamp} - {message}\n")
    


# ETL Job starts here

log_progress("ETL Job Started")

log_progress("Extract phase Started")
extracted_data = extract()
log_progress("Extract phase Ended")

log_progress("Transform phase Started")
transformed_data = transform(extracted_data)
print("Transformed Data:")
print(transformed_data)
log_progress("Transform phase Ended")

log_progress("Load phase Started")
load(target_file, transformed_data)
log_progress("Load phase Ended")

log_progress("ETL Job Ended")
print("\n")

with open(log_file, 'a') as log:
    log.write("\n")