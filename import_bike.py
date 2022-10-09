from config import config
from typing import Iterable
from urllib.parse import urlencode
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import requests
from datetime import datetime, timedelta


def get_url(org: int, id: int,
            start: datetime, end: datetime, flows: Iterable['int'],
            interval: int):
    dateformat = "%d/%m/%Y"

    params = {
        'debut': start.strftime(dateformat),
        'fin': end.strftime(dateformat),
        'idOrganisme': org,
        'idPdc': id,
        'interval': interval,
        'flowIds': ';'.join(str(x) for x in flows)
    }
    query = urlencode(params)
    return f"https://www.eco-visio.net/api/aladdin/1.0.0/pbl/publicwebpageplus/data/{id}?{query}"


bikes = config.bikes


for bike in bikes:
    print(get_url(bike.org, bike.id, datetime.now(), datetime.now(), bike.flows, 3))


influxdb = config.influxdb


with InfluxDBClient(url=influxdb.server, token=influxdb.token, org=influxdb.org) as client:

    write_api = client.write_api(write_options=SYNCHRONOUS)

    today = datetime(2022, 10, 7)
    yesterday = today - timedelta(days=1)

    dateformat = "%d/%m/%Y"
    today_string = today.strftime(dateformat)
    yesterday_string = yesterday.strftime(dateformat)

    # url = f"https://www.eco-visio.net/api/aladdin/1.0.0/pbl/publicwebpageplus/data/100125116?debut={yesterday_string}&fin={today_string}&idOrganisme=4586&idPdc=100125116&interval=3&flowIds=101125116%3B102125116%3B353247560%3B353247561"
    url = get_url(bike.org, bike.id, yesterday, today, bike.flows, 3)
    print("first")
    print(url)

    resp = requests.get(url=url)
    day = resp.json()

    time = 0
    points = []
    for datapair in day:
        count = datapair[1]

        datatime = yesterday.replace(
            hour=time, minute=0, second=0, microsecond=0)

        point = Point("bike") \
            .tag("location", "koenigstorgraben") \
            .tag("timeframe", "hourly") \
            .field("bikes", int(count)) \
            .time(datatime, WritePrecision.NS)

        points.append(point)

        time += 1

        # -------------------
    print(points)
    write_api.write(influxdb.bucket, influxdb.org, points)

    # today = datetime.now()

    # yesterday = datetime.today() - timedelta(days=1)

    dateformat = "%d/%m/%Y"
    today_string = today.strftime(dateformat)
    yesterday_string = yesterday.strftime(dateformat)

    # url = f"https://www.eco-visio.net/api/aladdin/1.0.0/pbl/publicwebpageplus/data/100125116?debut={yesterday_string}&fin={today_string}&idOrganisme=4586&idPdc=100125116&interval=2&flowIds=101125116%3B102125116%3B353247560%3B353247561"
    url = get_url(bike.org, bike.id, yesterday, today, bike.flows, 2)
    print("second")
    print(url)

    resp = requests.get(url=url)
    day = resp.json()

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

        points.append(point)

    print(points)
    write_api.write(influxdb.bucket, influxdb.org, points)


client.close()
