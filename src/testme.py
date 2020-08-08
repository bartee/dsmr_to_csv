from dsmr_parser import telegram_specifications, obis_references
from dsmr_parser.clients import SerialReader, SERIAL_SETTINGS_V4
from dsmr_parser.parsers import TelegramParser

serial_reader = SerialReader(
    device='/dev/serial/by-id/usb-FTDI_FT232R_USB_UART_A13KK398-if00-port0',
    serial_settings=SERIAL_SETTINGS_V4,
    telegram_specification=telegram_specifications.V4
)

headers = {'datetime', 'tariff', 'electricity_used_total', 'electricity_delivered_total', 'gas_reading'}

# Todo:
# - Put try/catch around this
# - if failed, retry
# - If succeeded once, append to today's CSV, and exit. 
# - If day changes, create a new CSV

for telegram in serial_reader.read():
    message_datetime = telegram[obis_references.P1_MESSAGE_TIMESTAMP].value.strftime('%Y%M%d-%H:%I:%S')
    
    # Find out the active tariff
    active_tariff_str = telegram[obis_references.ELECTRICITY_ACTIVE_TARIFF]
    active_tariff = int(active_tariff_str.value)

    electricity_used_total = telegram[obis_references.ELECTRICITY_USED_TARIFF_ALL[active_tariff - 1]].value
    electricity_delivered_total = telegram[obis_references.ELECTRICITY_DELIVERED_TARIFF_ALL[active_tariff - 1]].value

    gas_reading = telegram[obis_references.HOURLY_GAS_METER_READING].value
    resultset = [message_datetime, active_tariff, str(electricity_used_total), str(electricity_delivered_total), str(gas_reading)]

    pprint(resultset)

