#!/usr/bin/env python3

from bs4 import BeautifulSoup
from io import BytesIO
import os
import sys
import requests
import datetime
import pytz
import csv
import zipfile

import json
import pathlib

# Inputs
USERNAME            = os.getenv('USER')
PASSWORD            = os.getenv('PASSWORD')

# Outputs
CACHE_FILE          = 'cache.json'
CACHE_EXPIRY_HOURS  = 6
MISSING_READINGS    = 6

def get_gas_and_electricity_usage_report(username, password):

    # API Constants
    LOGIN_API           = 'https://apigprd.cloud.pge.com/myaccount/v1/login'
    SAML_API            = 'https://itiamping.cloud.pge.com/idp/startSSO.ping'
    SSO_API             = 'https://sso2.opower.com/sp/ACS.saml2'
    TOKEN_API           = 'https://pge.opower.com/ei/app/r/energy-usage-details'
    ACCOUNT_API         = 'https://pge.opower.com/ei/edge/apis/multi-account-v1/cws/pge/customers/current'
    EXPORT_API          = 'https://pge.opower.com/ei/edge/apis/DataBrowser-v1/cws/utilities/pge/customers/UUID/usage_export/download'

    # Report Constants
    EXPORT_OFFSET_DAYS              = 3
    CSV_ROW_OFFSET                  = 6
    CSV_ELECTRIC_COLUMN_DATESTAMP   = -6
    CSV_ELECTRIC_COLUMN_TIMESTAMP   = -5
    CSV_ELECTRIC_COLUMN_KWH         = -3
    CSV_GAS_COLUMN_DATESTAMP        = -5
    CSV_GAS_COLUMN_THERMS           = -4
    CSV_GAS_COLUMN_COST             = -2

    login_headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1 Safari/605.1.15'
    }
    login_request = requests.get(LOGIN_API, auth=(username, password), headers=login_headers)

    saml_params = {
        'PartnerSpId': 'sso.opower.com'
    }
    saml_request = requests.get(SAML_API, cookies=login_request.cookies, params=saml_params)
    saml_token = BeautifulSoup(saml_request.text, 'html.parser').find('input', {'name': 'SAMLResponse'}).get('value')
    
    sso_payload = {
        'RelayState': TOKEN_API,
        'SAMLResponse': saml_token
    }
    sso_request  = requests.post(SSO_API, data=sso_payload)
    open_token = BeautifulSoup(sso_request.text, 'html.parser').find('input', {'name': 'opentoken'}).get('value')

    token_payload = {
        'opentoken': open_token
    }
    token_request  = requests.post(TOKEN_API, data=token_payload, allow_redirects=False)

    account_request = requests.get(ACCOUNT_API, cookies=token_request.cookies)
    account_uuid = account_request.json()['uuid']

    now             = datetime.datetime.now(tz=pytz.timezone('UTC'))
    lag_from        = now - datetime.timedelta(days=EXPORT_OFFSET_DAYS)
    lag_from_str    = datetime.datetime(lag_from.year, lag_from.month, lag_from.day).strftime('%Y-%m-%d')
    now_str         = datetime.datetime(now.year, now.month, now.day, now.hour).strftime('%Y-%m-%d')
    export_params   = {
        'format':       'csv',
        'startDate':    lag_from_str,
        'endDate':      now_str
    }
    export_request = requests.get(EXPORT_API.replace('UUID', account_uuid), cookies=token_request.cookies, params=export_params)

    report = {
        'readings': {},
        'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    }
    zip_file = zipfile.ZipFile(BytesIO(export_request.content))
    for file_name in zip_file.namelist():
        if 'gas' in file_name:
            report['readings']['gas'] = {}
            file_content = zip_file.read(file_name)
            csv_content = file_content.decode('utf-8')
            csv_reader = csv.reader(csv_content.splitlines(), delimiter=',')
            line_count = 0
            for row in csv_reader:
                if line_count >= CSV_ROW_OFFSET:
                    datestamp = row[CSV_GAS_COLUMN_DATESTAMP]
                    report['readings']['gas'][datestamp] = {
                        'therms': row[CSV_GAS_COLUMN_THERMS],
                        'cost': row[CSV_GAS_COLUMN_COST].replace("$", "")
                    }
                    report['last_reported_gas'] = datestamp
                line_count += 1
        elif 'electric' in file_name:
            report['readings']['electric'] = {}
            file_content = zip_file.read(file_name)
            csv_content = file_content.decode('utf-8')
            csv_reader = csv.reader(csv_content.splitlines(), delimiter=',')
            line_count = 0
            for row in csv_reader:
                if line_count >= CSV_ROW_OFFSET:
                    datetimestamp = row[CSV_ELECTRIC_COLUMN_DATESTAMP] + ' ' + row[CSV_ELECTRIC_COLUMN_TIMESTAMP]
                    report['readings']['electric'][datetimestamp] = row[CSV_ELECTRIC_COLUMN_KWH]
                    report['last_reported_electric'] = datetimestamp
                line_count += 1
    
    return report

def update_cache(report, filename):
    json_object = json.dumps(report, indent = 4)
    with open(pathlib.Path(__file__).parent / filename, 'w') as outfile:
        outfile.write(json_object)

def read_cache(filename):
    try:
        with open(pathlib.Path(__file__).parent / filename, 'r') as openfile:
            return json.load(openfile)
    except FileNotFoundError:
        return {}

def get_or_update_cached_gas_and_electricity_usage_report(reading_dt_str, filename, cache_age):
    gas_and_electricity_usage_report = read_cache(filename)
    if 'timestamp' in gas_and_electricity_usage_report:
        cache_timestamp = datetime.datetime.strptime(gas_and_electricity_usage_report['timestamp'], '%Y-%m-%d %H:%M')
        if datetime.datetime.now() > cache_timestamp + datetime.timedelta(hours=cache_age):
            gas_and_electricity_usage_report = get_gas_and_electricity_usage_report(USERNAME, PASSWORD)
            update_cache(gas_and_electricity_usage_report, CACHE_FILE)
        elif reading_dt_str not in gas_and_electricity_usage_report['readings']['electric'] and reading_dt_str not in gas_and_electricity_usage_report['readings']['gas']:
            gas_and_electricity_usage_report = get_gas_and_electricity_usage_report(USERNAME, PASSWORD)
            update_cache(gas_and_electricity_usage_report, CACHE_FILE)
    else:
        gas_and_electricity_usage_report = get_gas_and_electricity_usage_report(USERNAME, PASSWORD)
        update_cache(gas_and_electricity_usage_report, CACHE_FILE)
    return gas_and_electricity_usage_report

def get_electric_use_kwh_reading(reading_dt):
    if reading_dt.minute < 15:
        floored_reading_dt_minute = 0
    elif reading_dt.minute < 30:
        floored_reading_dt_minute = 15
    elif reading_dt.minute < 45:
        floored_reading_dt_minute = 30
    else: 
        floored_reading_dt_minute = 45
    floored_reading_dt = datetime.datetime(reading_dt.year, reading_dt.month, reading_dt.day, reading_dt.hour, floored_reading_dt_minute)
    floored_reading_dt_str = floored_reading_dt.strftime('%Y-%m-%d %H:%M')
    gas_and_electricity_usage_report = get_or_update_cached_gas_and_electricity_usage_report(floored_reading_dt_str, CACHE_FILE, CACHE_EXPIRY_HOURS)
    for i in range(MISSING_READINGS):
        try:
            return gas_and_electricity_usage_report['readings']['electric'][floored_reading_dt_str]
        except KeyError:
            floored_reading_dt -= datetime.timedelta(minutes=15)
            floored_reading_dt_str = datetime.datetime(reading_dt.year, reading_dt.month, reading_dt.day, reading_dt.hour).strftime('%Y-%m-%d %H:%M')

def get_gas_use_therms_reading(reading_dt):
    reading_dt_str = reading_dt.strftime('%Y-%m-%d')
    gas_and_electricity_usage_report = get_or_update_cached_gas_and_electricity_usage_report(reading_dt_str, CACHE_FILE, CACHE_EXPIRY_HOURS)
    for i in range(MISSING_READINGS):
        try:
            return gas_and_electricity_usage_report['readings']['gas'][reading_dt_str]
        except KeyError:
            reading_dt -= datetime.timedelta(days=1)
            reading_dt_str = reading_dt.strftime('%Y-%m-%d')

reading_dt = datetime.datetime.now() - datetime.timedelta(days=1)
if sys.argv[-1] == 'gas':
    print(json.dumps(get_gas_use_therms_reading(reading_dt)))
else:
    print(get_electric_use_kwh_reading(reading_dt))
