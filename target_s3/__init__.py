#!/usr/bin/env python3

import boto3
import argparse
import datetime
import io
import os
import sys
import json
import shutil
import pkg_resources
import singer
from bson import objectid, timestamp, datetime as bson_datetime
import pytz
import time
import tzlocal
import dateutil.parser
from distutils.util import strtobool

logger = singer.get_logger()

def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()

def create_stream_to_record_map(stream_to_record_map, line, state, config):
    try:
        json_line = json.loads(line)
    except json.decoder.JSONDecodeError:
        logger.error("Unable to parse:\n {line}".format(line=line))
        raise

    if 'type' not in json_line:
        logger.error("Json line has no type:\n {line}".format(line=line))
        raise Exception(
            "Line is missing required key 'type': {}".format(line))

    t = json_line['type']

    if t == 'RECORD':
        if 'stream' not in json_line:
            raise Exception(
                "Line is missing required key 'stream': {}".format(line))

        time_created = datetime.datetime.now()
        replication_method = singer.get_bookmark(state['value'], json_line['stream'], 'replication_method')
        stream_name = f'{replication_method}::{json_line["stream"]}::{time_created.year}-{time_created.month}-{time_created.day}'

        if replication_method == "FULL_TABLE":
            for (k,v) in json_line['record'].items():
                try:
                    if k == "_id":
                        oid = objectid.ObjectId(v)
                        if oid.is_valid: time_created = oid.generation_time
                    elif k == "created_at":
                        time_created = dateutil.parser.parse(v)
                except:
                    pass

            stream_name = f'{replication_method}::{json_line["stream"]}::{time_created.year}-{time_created.month}-{time_created.day}'

        add_to_stream_records(stream_to_record_map, stream_name, line)

    if t == 'STATE' and "state_file_path" in config:
        state = json_line
        persist_state(json_line, config)

    return (stream_to_record_map, state)

def persist_stream_map(stream_map, tmp_path):
    for stream, lines in stream_map.items():
        save_and_upload_file(stream, lines, tmp_path)


def save_and_upload_file(stream, lines, tmp_path):
    path = tmp_path + stream
    with open(path, 'w') as f:
        for line in lines:
            f.write(line)
        logger.info("tmp file written " + path)


def add_to_stream_records(stream_map, stream_name, line):
    if stream_name not in stream_map:
        stream_map[stream_name] = []
    stream_map[stream_name].append(line)


def delete_tmp_dir(tmp_path):
    shutil.rmtree(tmp_path)
    logger.info("deleteing tmp dir " + tmp_path)


def upload_to_s3(tmp_path, config, s3):
    for f in os.listdir(tmp_path):
        replication_method, source, created = f.split("::", 3)
        dt = datetime.datetime.strptime(created, '%Y-%m-%d')
        dt_now = datetime.datetime.now()
        file_name = source

        if replication_method == 'LOG_BASED':
            file_name = source+"_"+str(dt_now.minute)+str(dt_now.second)+str(dt_now.microsecond)

        s3_file_name = os.path.join(
            "source="+config["source"],
            "collection="+source,
            "year="+str(dt.year),
            "month="+str(dt.month),
            "day="+str(dt.day),
            file_name+".json")

        print("S3 path")
        print(s3_file_name)
        logger.info('Uploading to s3: ' + s3_file_name)
        s3.upload_file(tmp_path + '/' + f, config['bucket'], s3_file_name)
        logger.info('Uploaded to s3: ' + s3_file_name)


def create_temp_dir():
    date = datetime.datetime.today().strftime('%Y-%m-%d-%H-%M-%s-%f')
    path = '/tmp/target-s3/' + date + '/'
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def persist_state(state, config):
    path = config["state_file_path"]
    state_dir = path.rsplit("/", 1)[0]
    if not os.path.exists(state_dir):
        os.makedirs(state_dir)

    with open(path, 'w') as f:
        f.write(json.dumps(state["value"]))

    logger.debug("state file written " + path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    s3 = boto3.client('s3')

    if not args.config:
        logger.error("config is required")
        exit(1)

    with open(args.config) as input:
        config = json.load(input)

    with io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8') as input:
        try:
            bytes_in_file = 0
            stream_map = {}
            state = {}

            tmp_path = create_temp_dir()

            for line in input:
                bytes_in_file += sys.getsizeof(line)
                try:
                    stream_map, state = create_stream_to_record_map(stream_map, line, state, config)
                except:
                    logger.info('Failed to process log: \n ' + line)

                # flush after 10mb
                if bytes_in_file > 10000000:
                    flush(stream_map, tmp_path, config, s3)
                    bytes_in_file = 0
                    stream_map = {}
                    tmp_path = create_temp_dir()

            flush(stream_map, tmp_path, config, s3)
        except:
            logger.info('Some par of buffer upload failed.')

def flush(stream_map, tmp_path, config, s3):
    persist_stream_map(stream_map, tmp_path)
    upload_to_s3(tmp_path, config, s3)
    delete_tmp_dir(tmp_path)

if __name__ == '__main__':
    main()
