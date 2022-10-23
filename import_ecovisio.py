#!/usr/bin/env python3

from config import config
from typing import Iterable
from urllib.parse import urlencode
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import requests
from datetime import datetime, timedelta
from enum import Enum


class Interval(Enum):
    MONTH = 6
    WEEK = 5
    DAY = 4
    HOUR = 3
    QUARTER_OF_AN_HOUR = 2


def get_url(org: int, id: int,
            start: datetime, end: datetime, flows: Iterable['int'],
            interval: Interval):
    dateformat = "%d/%m/%Y"

    params = {
        'debut': start.strftime(dateformat),
        'fin': end.strftime(dateformat),
        'idOrganisme': org,
        'idPdc': id,
        'interval': interval.value,
        'flowIds': ';'.join(str(x) for x in flows)
    }
    query = urlencode(params)
    return f"https://www.eco-visio.net/api/aladdin/1.0.0/pbl/publicwebpageplus/data/{id}?{query}"


def extract_points(bikecounter, from_day: datetime, to_day: datetime, interval: Interval):
    timeframes = {
        Interval.DAY: 86400,
        Interval.HOUR: 3600,
        Interval.QUARTER_OF_AN_HOUR: 900
    }

    try:
        timeframe = timeframes[interval]
    except KeyError:
        raise ValueError(f"Interval {interval} is not supported")

    url = get_url(bikecounter.org, bikecounter.id, from_day, to_day, bikecounter.flows, interval)
    print(f"Interval {interval}")
    print(url)

    response = requests.get(url=url).json()
    if len(response) <= 0:
        print("No values found")
        return

    print(f"Got {len(response)} values")
    first_date_in_data = response[0][0]

    current_time = datetime.strptime(first_date_in_data, "%m/%d/%Y")
    points = []
    for datapair in response:
        # print(current_time, datapair[0],  datapair[1])
        count = datapair[1]

        point = Point("bikecounter") \
            .tag("country", bikecounter.country) \
            .tag("city", bikecounter.city) \
            .tag("location", bikecounter.location) \
            .tag("timeframe", timeframe) \
            .tag("type", "ecovisio") \
            .field("bikes", int(count)) \
            .time(current_time, WritePrecision.S)

        points.append(point)

        # After:
        current_time = current_time + timedelta(seconds=timeframe)

    return points


influxdb = config.influxdb

with InfluxDBClient(url=influxdb.server, token=influxdb.token, org=influxdb.org) as client:
    write_api = client.write_api(write_options=SYNCHRONOUS)
    for bikecounter in config.ecovisio:
        print()
        print(f"Bikecounter {bikecounter.country}-{bikecounter.city}-{bikecounter.location}")
        to_day = datetime.now() + timedelta(days=1)
        from_day = to_day - timedelta(days=40)

        points_86400 = extract_points(bikecounter, from_day, to_day, Interval.DAY)
        write_api.write(influxdb.bucket, influxdb.org, points_86400)

        points_3600 = extract_points(bikecounter, from_day, to_day, Interval.HOUR)
        write_api.write(influxdb.bucket, influxdb.org, points_3600)

        points_900 = extract_points(bikecounter, from_day, to_day, Interval.QUARTER_OF_AN_HOUR)
        write_api.write(influxdb.bucket, influxdb.org, points_900)
    client.close()
