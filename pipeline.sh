#!/bin/bash

parallel ::: "python3 stream_producer.py" "python3 stream_consumer.py"
