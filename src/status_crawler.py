"""
version : v1.0.2

MIT License

Copyright (c) 2020 Dropper Lab

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import logging
from logging.handlers import RotatingFileHandler

import re
from urllib.request import urlopen
from bs4 import BeautifulSoup
import datetime

import json
import time

import pymysql

import mysql_status_property
import status_property
import mail_sender

logging.Formatter.converter = time.gmtime
logger = logging.getLogger(__name__)
fileHandler = RotatingFileHandler('./log/status_crawler.log', maxBytes=1024 * 1024 * 1024 * 9, backupCount=9)
fileHandler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)s] >> %(message)s'))
logger.addHandler(fileHandler)
logger.setLevel(logging.INFO)
logger.info('every package loaded and start logging')

logger.info(
    'mysql_status_property.hostname=' + str(mysql_status_property.hostname) + ' | mysql_status_property.user=' + str(
        mysql_status_property.user) + ' | mysql_status_property.password=' + str(
        mysql_status_property.password) + ' | mysql_status_property.database=' + str(
        mysql_status_property.database) + ' | mysql_status_property.charset=' + str(mysql_status_property.charset))
logger.info('status_property.region_dictionary=' + str(status_property.region_dictionary))


def insert_result(uid, data_list):
    logger.info('insert_result: function started')
    connection = pymysql.connect(host=mysql_status_property.hostname, user=mysql_status_property.user,
                                 password=mysql_status_property.password, db=mysql_status_property.database,
                                 charset=mysql_status_property.charset)
    logger.info('insert_result: database connection opened')
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    logger.info('insert_result: database cursor created')

    for data in data_list[1:]:
        cursor.execute(
            f"insert into status_{data['region']} values({uid}, {data_list[0]}, {data['increased']}, {data['increased_foreign']}, {data['increased_local']}, {data['certified']}, {data['isolated']}, {data['unisolated']}, {data['dead']}, {data['percentage']});")
        logger.info('insert_result: status_' + str(data['region']) + ' data inserted | data=' + str(data))

    connection.commit()
    logger.info('insert_result: database connection commited')
    connection.close()
    logger.info('insert_result: database connection closed')

    logger.info('insert_result: function ended')


def dump_result(uid, data):
    logger.info('dump_result: function started')

    with open('./status-data/k_covid19_status_' + str(uid) + '.json', 'w') as json_file:
        json.dump(data, json_file)
    logger.info(
        'dump_result: data dumped in status-data/k_covid19_status_' + str(uid) + '.json | data=' + str(data))

    logger.info('dump_result: function ended')


def get_status_data(target='', current_timestamp=0):
    logger.info('get_status_data: function started | target=' + target)

    downloaded_html = urlopen(target)
    logger.info('get_status_data: html downloaded')
    beautifulsoup_object = BeautifulSoup(downloaded_html, 'html.parser')
    logger.info('get_status_data: html parsed to beautifulsoup object')

    announced_time = ['2020',
                      re.findall('([0-9]+)[.]', beautifulsoup_object.findAll('p', class_='info')[0].text)[0],
                      re.findall('[.]([0-9]+)', beautifulsoup_object.findAll('p', class_='info')[0].text)[0],
                      re.findall('([0-9]+)시', beautifulsoup_object.findAll('p', class_='info')[0].text)[0]]
    logger.info('get_status_data: get announced time | announced_time=' + str(announced_time))

    datetime_object = datetime.datetime.strptime(str(announced_time), "['%Y', '%m', '%d', '%H']")
    logger.info('get_status_data: convert announced time to datetime object | datetime_object=' + str(datetime_object))
    announced_time_unix = int(time.mktime(datetime_object.timetuple())) - 32400
    logger.info(
        'get_status_data: convert datetime object to unix time | announced_time_unix=' + str(announced_time_unix))

    raw_table = beautifulsoup_object.findAll('tbody')
    logger.info('get_status_data: table picked out | raw_table=' + str(raw_table))
    raw_table_beautifulsoup_object = BeautifulSoup(str(raw_table[0]), 'html.parser')
    logger.info('get_status_data: convert raw table to beautifulsoup object | raw_table_beautifulsoup_object=' + str(
        raw_table_beautifulsoup_object))
    table_data_rows = raw_table_beautifulsoup_object.findAll('tr')
    logger.info('get_status_data: export table data from raw_table_beautifulsoup_object | table_data_rows=' + str(
        table_data_rows))

    status_data_list = [announced_time_unix]
    logger.info('get_status_data: declare status_data_list | status_data_list=' + str(status_data_list))

    convert_error_list = [0]
    database_error_list = [0]
    dictionary_error_list = [0]

    report_message = '* Dropper API Status Crawling Report *\n\n\n'
    report_level = 0

    if len(table_data_rows) == 0:
        if report_level < 3:
            report_level = 3
        logger.info('get_status_data: table_data_rows is empty')
        report_message += '- FATAL: table_data_rows is empty -\n\n\n'
        report_message += '\n'
        report_message += '\nThis report is about table_data_rows ' + str(table_data_rows)
        report_message += '\n'
        report_message += '\n\n\n\n\n'
    else:
        for table_data, index_no in zip(table_data_rows, range(len(table_data_rows))):
            try:
                logger.info('get_status_data: extracting table data | table_data=' + str(table_data))
                table_data_beautifulsoup_object = BeautifulSoup(str(table_data), 'html.parser')
                logger.info(
                    'get_status_data: convert table_data to beautifulsoup object | table_data_beautifulsoup_object=' + str(
                        table_data_beautifulsoup_object))
                try:
                    region = table_data_beautifulsoup_object.findAll('th')[0].text
                    logger.info('get_status_data: extracting region from table data | region=' + str(region))
                    data = table_data_beautifulsoup_object.findAll('td')
                    logger.info('get_status_data: extracting data from table data | data=' + str(data))
                    try:
                        status_data = {
                            'region': status_property.region_dictionary[re.sub('[  *]', '', region)],
                            'increased': int('0' + re.sub('[^0-9]', '', data[0].text)),
                            'increased_foreign': int('0' + re.sub('[^0-9]', '', data[1].text)),
                            'increased_local': int('0' + re.sub('[^0-9]', '', data[2].text)),
                            'certified': int('0' + re.sub('[^0-9]', '', data[3].text)),
                            'isolated': int('0' + re.sub('[^0-9]', '', data[4].text)),
                            'unisolated': int('0' + re.sub('[^0-9]', '', data[5].text)),
                            'dead': int('0' + re.sub('[^0-9]', '', data[6].text)),
                            'percentage': float('0' + re.sub('[^0-9.]', '', data[7].text))
                        }
                        logger.info('get_status_data: declare status data | status_data=' + str(status_data))

                        status_data_list.append(status_data)
                        logger.info(
                            'get_status_data: put status data into status data list | status_data_list=' + str(
                                status_data_list))
                    except Exception as ex:
                        if report_level < 1:
                            report_level = 1
                        dictionary_error_list[0] = 1
                        dictionary_error_list.append([ex, table_data])
                        logger.info('get_status_data: unregistered region name was found | ex=' + str(
                            ex) + ' | dictionary_error_list=' + str(dictionary_error_list))
                except Exception as ex:
                    if report_level < 2:
                        report_level = 2
                    database_error_list[0] = 1
                    database_error_list.append([ex, index_no])
                    logger.info('get_status_data: cannot extract region or data from table data | ex=' + str(
                        ex) + ' | index_no=' + str(index_no))
            except Exception as ex:
                if report_level < 2:
                    report_level = 2
                convert_error_list[0] = 1
                convert_error_list.append([ex, table_data])
                logger.info('get_status_data: cannot convert table_data to beautifulsoup object | ex=' + str(
                    ex) + ' | table_data=' + str(table_data))

    if convert_error_list[0] == 1:
        report_message += '- ERROR: cannot convert table_data to beautifulsoup object -\n\n\n'
        for error in convert_error_list[1:]:
            report_message += '---------------------------\n'
            report_message += f"{error[0]}\n\ntable_data:\n{error[1]}\n"
        report_message += '---------------------------\n'
        report_message += '\n\n\n\n\n'

    if database_error_list[0] == 1:
        report_message += '- ERROR: cannot extract region from table data -\n\n\n'
        for error in database_error_list[1:]:
            report_message += '---------------------------\n'
            report_message += f"{error[0]}\n\nindex_no:\n{error[1]}\n"
        report_message += '---------------------------\n'
        report_message += '\n\n\n\n\n'

    if dictionary_error_list[0] == 1:
        report_message += '- WARN: unregistered region name was found -\n\n\n'
        for error in dictionary_error_list[1:]:
            report_message += '---------------------------\n'
            report_message += f"{error[0]}\n\nregion_name:\n{error[1]}\n"
        report_message += '---------------------------\n'
        report_message += '\n\n\n\n\n'

    if report_level < 2:
        report_message += 'Crawling finished successfully\n'
        report_message += '\nThis report is based on (Unix Time)' + str(int(current_timestamp))
        if report_level == 0:
            mail_sender.send_mail(
                subject=f'[Dropper API](status_crawler) INFO: task report',
                message=report_message)
        elif report_level == 1:
            mail_sender.send_mail(
                subject=f'[Dropper API](status_crawler) WARN: task report',
                message=report_message)
    elif report_level == 2:
        report_message += 'Some error occurred while crawling\n'
        report_message += '\nThis report is based on (Unix Time)' + str(int(current_timestamp))
        mail_sender.send_mail(
            subject=f'[Dropper API](status_crawler) ERROR: task report',
            message=report_message)
    else:
        report_message += 'Fatal error occurred while crawling\n'
        report_message += '\nThis report is based on (Unix Time)' + str(int(current_timestamp))
        mail_sender.send_mail(
            subject=f'[Dropper API](status_crawler) FATAL: task report',
            message=report_message)

    logger.info('get_status_data: function ended | status_data_list=' + str(status_data_list))
    return status_data_list


if __name__ == '__main__':
    logger.info('start status_crawler.py')

    timestamp = int(time.time())
    logger.info('recorded a time stamp | timestamp=' + str(timestamp))

    result = get_status_data(target='http://ncov.mohw.go.kr/bdBoardList_Real.do?brdId=1&brdGubun=13',
                             current_timestamp=timestamp)
    logger.info('get result | result=' + str(result))

    dump_result(timestamp, result)
    logger.info('dump result | timestamp=' + str(timestamp) + ' | result=' + str(result))
    insert_result(timestamp, result)
    logger.info('insert result | timestamp=' + str(timestamp) + ' | result=' + str(result))

    logger.info('end status_crawler.py')
