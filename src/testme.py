import csv
from dsmr_parser import telegram_specifications, obis_references
from dsmr_parser.clients import SerialReader, SERIAL_SETTINGS_V4
from dsmr_parser.parsers import TelegramParser

from dotenv import load_dotenv
load_dotenv()

import os

serial_reader = SerialReader(
    device=os.getenv('DEVICE_ADDRESS'),
    serial_settings=SERIAL_SETTINGS_V4,
    telegram_specification=telegram_specifications.V4
)

def store_data(dataset):
    datestr  = dataset[0].split('-')[0]
    fname = os.path.abspath("{0}/{1}.csv".format(os.getenv('OUTPUT_DIR'), datestr))

    headers = False
    if not os.path.isfile(fname):
        headers = {'datetime', 'tariff', 'electricity_used_total', 'electricity_delivered_total', 'gas_reading'}

    # writing to csv file  
    with open(fname, 'a') as csvfile:  
        # creating a csv writer object  
        csvwriter = csv.writer(csvfile)  
        if headers:
            # writing the fields  
            print('Printing the headers')
            csvwriter.writerow(headers)        
        # writing the data rows  
        csvwriter.writerow(dataset)


from pprint import pprint


# Todo:
# - Put try/catch around this
# - if failed, retry
# - If succeeded once, append to today's CSV, and exit. 
# - If day changes, create a new CSV

for telegram in serial_reader.read():
    message_datetime = telegram[obis_references.P1_MESSAGE_TIMESTAMP].value.strftime('%Y%m%d-%H:%I:%S')
    
    # Find out the active tariff
    active_tariff_str = telegram[obis_references.ELECTRICITY_ACTIVE_TARIFF]
    active_tariff = int(active_tariff_str.value)

    electricity_used_total = telegram[obis_references.ELECTRICITY_USED_TARIFF_ALL[active_tariff - 1]].value
    electricity_delivered_total = telegram[obis_references.ELECTRICITY_DELIVERED_TARIFF_ALL[active_tariff - 1]].value

    gas_reading = telegram[obis_references.HOURLY_GAS_METER_READING].value
    resultset = [message_datetime, active_tariff, str(electricity_used_total), str(electricity_delivered_total), str(gas_reading)]
    store_data(resultset)
    pprint(resultset)

