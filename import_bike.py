from datetime import datetime

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


import requests

from datetime import datetime, timedelta

import config

conf = config.config()


with InfluxDBClient(url=conf['influxdb']['server'], token=conf['influxdb']['token'], org=conf['influxdb']['org']) as client:

    write_api = client.write_api(write_options=SYNCHRONOUS)
    # data = "mem,host=host1 used_percent=23.43234543"
    # write_api.write(bucket, org, data)

    #today = datetime.now()
    today = datetime(2022, 9, 21)
    yesterday = today - timedelta(days=1)

    dateformat = "%d/%m/%Y"
    today_string = today.strftime(dateformat)
    yesterday_string = yesterday.strftime(dateformat)

    url = f"https://www.eco-visio.net/api/aladdin/1.0.0/pbl/publicwebpageplus/data/100125116?debut={yesterday_string}&fin={today_string}&idOrganisme=4586&idPdc=100125116&interval=3&flowIds=101125116%3B102125116%3B353247560%3B353247561"

    resp = requests.get(url=url)
    day = resp.json() # 

    time = 0
    points = []
    for datapair in day:
        count = datapair[1]

        datatime = yesterday.replace(hour=time, minute=0, second=0, microsecond=0)

        point = Point("bike") \
        .tag("location", "koenigstorgraben") \
        .tag("timeframe", "hourly") \
        .field("bikes", int(count)) \
        .time(datatime, WritePrecision.NS)

        points.append(point)

        time += 1

        # -------------------
    write_api.write(conf['influxdb']['bucket'], points)



    # today = datetime.now()
    
    # yesterday = datetime.today() - timedelta(days=1)

    dateformat = "%d/%m/%Y"
    today_string = today.strftime(dateformat)
    yesterday_string = yesterday.strftime(dateformat)

    url = f"https://www.eco-visio.net/api/aladdin/1.0.0/pbl/publicwebpageplus/data/100125116?debut={yesterday_string}&fin={today_string}&idOrganisme=4586&idPdc=100125116&interval=2&flowIds=101125116%3B102125116%3B353247560%3B353247561"

    resp = requests.get(url=url)
    day = resp.json() # 

    #time = 0
    datatime = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    points = []
    for datapair in day:
        count = datapair[1]

        datatime = datatime + timedelta(minutes=15)


        point = Point("bike") \
        .tag("location", "koenigstorgraben") \
        .tag("timeframe", "fuzehn") \
        .field("bikes", int(count)) \
        .time(datatime, WritePrecision.NS)

    write_api.write(conf['influxdb']['bucket'], points)

        #time += 15


client.close()
