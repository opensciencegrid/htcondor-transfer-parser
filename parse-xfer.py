#!/usr/bin/env python

import elasticsearch
import elasticsearch.helpers
from elasticsearch_dsl import Search, Q
import certifi
import re
import datetime
import dateutil.parser
import sys
from multiprocessing import Pool
import hashlib
from geoip import geolite2
import argparse
import socket



# Retransmisisons & reordering
# Table with stats of login01 to UCSD (and etc)


def main():

    parser = argparse.ArgumentParser(description='Process HTCondor Transfer Logs')
    parser.add_argument('from_date', type=str, nargs='?',
                        help='From Date')
    parser.add_argument('to_date', type=str, nargs='?',
                        help='To Date')
    parser.add_argument('-1', '--one-hour', dest='onehour', action='store_true', default=False,
                        help='Summarze the last 1 hour')
    parser.add_argument('-p', '--period', dest='period', type=int, default=60,
                        help="Period (in minutes) in for which to summerize")

    args = parser.parse_args()

    dates = []

    if args.onehour:
        from_date = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
        to_date = datetime.datetime.utcnow()

        cur_date = from_date
        while cur_date < to_date:
            dates.append((cur_date, cur_date + datetime.timedelta(minutes=5)))
            cur_date += datetime.timedelta(minutes=5)

    else:

        period = datetime.timedelta(minutes=args.period)

        from_date = dateutil.parser.parse(args.from_date)
        to_date = dateutil.parser.parse(args.to_date)

        cur_date = from_date


        while cur_date < to_date:
            dates.append((cur_date, cur_date + period))
            cur_date += period

    processing_pool = Pool(5)
    processing_pool.map(process, dates, 1)

mapping = {
    "mappings": {
        "log": {
            "properties": {
                'location': { 'type': 'geo_point' },
                'dest': { 'type': 'ip' },
                'geoip.ip': { 'type': 'ip' }
            }
        }
    }

}

def fail_parse(record, list_of_operations, message):
    record['_op_type'] = 'index'
    record['_type'] = "log"

    # Parse the date from the @timestamp
    parsed_date = dateutil.parser.parse(record['@timestamp'])

    record['_index'] = "htcondor-xfer-stats-{0}".format(parsed_date.strftime("%Y.%m.%d"))
    record['tags'] = list(record['tags'])
    record['tags'].append(message)
    record['tags'].append("parserfail")
    print "Failed to parse: %s" % (record['message'])

    list_of_operations.append(record)


def process(dates):
    (from_date, to_date) = dates
    #es = elasticsearch.Elasticsearch( ["https://gracc.opensciencegrid.org/q"], use_ssl=True, ca_certs=certifi.where(), timeout=60 )
    es = elasticsearch.Elasticsearch( ["localhost:9200"], timeout=60 )

    # Create the index, if it doesn't exist, and set the mapping
    day_before_index = "htcondor-xfer-stats-{0}".format((from_date - datetime.timedelta(days=1)).strftime("%Y.%m.%d"))
    from_index = "htcondor-xfer-stats-{0}".format(from_date.strftime("%Y.%m.%d"))
    to_index = "htcondor-xfer-stats-{0}".format(to_date.strftime("%Y.%m.%d"))
    if not es.indices.exists(from_index):
        es.indices.create(index=from_index, body=mapping)
    if not es.indices.exists(to_index):
        es.indices.create(index=to_index, body=mapping)
    if not es.indices.exists(day_before_index):
        es.indices.create(index=day_before_index, body=mapping)

    split_re = re.compile("^([\d\s\.\/\:]+)\s*(?:\(D_STATS\))? (.*)$")
    peer_stats_re = re.compile("\(peer stats.*\):\s(.*)$")
    timestamp_re = re.compile("^([\d\:\/\s]+)")
    upload_re = re.compile("^(File Transfer Upload):\s(.*)")
    key_value_re = re.compile("^[\w\s]+: (.*)")
    split_key_values_re = re.compile("(\w+):\s([\d\.]+)")

    s = Search(using=es, index="transfer-logs-*")
    s = s.filter('range', **{'@timestamp': {'from': from_date, 'to': to_date }})
    print "Search from {0} to {1}".format(str(from_date), str(to_date))
    counter = 0
    list_of_operations = []

    s = s.params(scroll='3h')

    for result_hit in s.scan():

        result = result_hit['message']
        to_upload = result_hit.to_dict()
        m = hashlib.sha256()
        m.update(result_hit['message'])
        m.update(result_hit['host'])
        m.update(str(result_hit['offset']))
        to_upload['_id'] = m.hexdigest()

        #print result_hit
        #print result

        # Split the time stamp from the rest
        matches = split_re.search(result)
        if not matches:
            fail_parse(to_upload, list_of_operations, "time_stamp_split_fail")
            continue

        # Search for the peer stats
        peer_stats = peer_stats_re.search(matches.group(2))

        stats = matches.group(2)

        # Grab everything after the first ":", but account for the
        # peer stats as well
        if peer_stats:
            stats = peer_stats.group(1)
        else:
            stats = matches.group(2)[matches.group(2).find(":")+2:]


        timestamp_match = timestamp_re.search(matches.group(1))
        if not timestamp_match:
            #print "Timestamp didn't match"
            #print matches.group(1)
            fail_parse(to_upload, list_of_operations, "time_stamp_parse_fail")
            continue
        #else:
        #    print matches.group(1)
        timestamp = timestamp_match.group(1)

        parsed_date = dateutil.parser.parse(timestamp)


        upload_match = upload_re.search(stats)


        key_values = key_value_re.search(stats)

        if not key_values:
            #print "Key values didn't work: %s" % (stats)
            fail_parse(to_upload, list_of_operations, "key_value_split_fail")
            continue

        key_value = split_key_values_re.findall(key_values.group(1))
        key_value = dict(key_value)


        for key in key_value:
            try:
                key_value[key] = int(key_value[key])
            except ValueError as ve:
                try:
                    key_value[key] = float(key_value[key])
                except ValueError as ve2:
                    pass

        key_value['CreateDate'] = parsed_date


        # Now update the thing in ES
        #to_upload['_id'] = result_hit['_id']



        match = geolite2.lookup(key_value['dest'])
        if match:

            info_dict = match.get_info_dict()
            key_value['geoip'] = {
                'location': list(match.location),
                'ip': match.ip,
                'latitude':info_dict['location']['latitude'],
                'longitude':info_dict['location']['longitude'],
            }
            key_value['location'] = [info_dict['location']['longitude'], info_dict['location']['latitude']]

            try:
                key_value['geoip']['timezone'] = info_dict['location']['time_zone']
            except KeyError as ke:
                pass

            try:
                key_value['geoip']['continent_code'] = info_dict['continent']['code']
            except KeyError as ke:
                pass

            try:
                key_value['geoip']['city_name'] = info_dict['city']['names']['en']
            except KeyError as ke:
                pass

            try:
                key_value['geoip']['country_code2'] = info_dict['country']['iso_code']
            except KeyError as ke:
                pass

            try:
                key_value['geoip']['country_name'] = info_dict['country']['names']['en']
            except KeyError as ke:
                pass

            try:
                key_value['geoip']['region_name'] = info_dict['subdivisions'][0]['names']['en']
            except KeyError as ke:
                pass

            try:
                key_value['geoip']['postal_code'] = info_dict['postal']['code']
            except KeyError as ke:
                pass

            try:
                key_value['geoip']['region_code'] = info_dict['subdivisions'][0]['iso_code']
            except KeyError as ke:
                pass


        additional_tags = []
        try:
            hostname = socket.gethostbyaddr(key_value['dest'])
            key_value['dest_domain'] = ".".join(hostname[0].split('.')[-2:])
            key_value['dest_hostname'] = hostname[0]
        except socket.herror as hosterror:
            # If we can't get the hostname, we can't get the host
            additional_tags.append('host_lookup_error')

        key_value['tags'] = list(result_hit['tags'])
        key_value['tags'].extend(additional_tags)
        if peer_stats:
            key_value['tags'].append('peer_stats')

        if upload_match:
            key_value['tags'].append('upload')
        else:
            key_value['tags'].append('download')

        to_upload.update(key_value)
        to_upload['_op_type'] = 'index'
        to_upload['_type'] = "log"
        to_upload['_index'] = "htcondor-xfer-stats-{0}".format(parsed_date.strftime("%Y.%m.%d"))
        list_of_operations.append(to_upload)
        if (counter % 100) == 0:
            elasticsearch.helpers.bulk(es, list_of_operations)
            print "Search from {0} to {1}: {2}".format(str(from_date), str(to_date), counter)
            list_of_operations = []
        counter += 1

    elasticsearch.helpers.bulk(es, list_of_operations)


if __name__ == "__main__":
    main()
