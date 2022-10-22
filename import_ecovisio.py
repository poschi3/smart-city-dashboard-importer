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


def extract_points(bike, dayin: datetime, interval: int):
    end_time = dayin.replace(hour=0, minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(days=1)

    url = get_url(bike.org, bike.id, start_time, end_time, bike.flows, interval)
    print(url)

    resp = requests.get(url=url)
    day = resp.json()
    number_of_values = len(day)

    time_interval = timedelta(days=1) / number_of_values
    timeframe = int(time_interval.total_seconds())

    points = []
    current_time = start_time
    for datapair in day:
        print(current_time)
        count = datapair[1]

        point = Point("bike") \
            .tag("location", "Kunsthalle") \
            .tag("timeframe", timeframe) \
            .field("bikes", int(count)) \
            .time(current_time, WritePrecision.S)

        points.append(point)

        # After:
        current_time = current_time + time_interval

    return points


bikes = config.ecovisio
influxdb = config.influxdb


with InfluxDBClient(url=influxdb.server, token=influxdb.token, org=influxdb.org) as client:
    write_api = client.write_api(write_options=SYNCHRONOUS)
    for bike in bikes:
        # print(get_url(bike.org, bike.id, datetime.now(), datetime.now(), bike.flows, 3))
        points_3600 = extract_points(bike, datetime(2022, 10, 7), 3)
        write_api.write(influxdb.bucket, influxdb.org, points_3600)

        points_900 = extract_points(bike, datetime(2022, 10, 7), 2)
        write_api.write(influxdb.bucket, influxdb.org, points_900)
    client.close()
