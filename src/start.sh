#!/bin/bash

set -m

exec python http_server.py &

exec python xml_producer.py &

exec python xml_to_avro.py