#!/usr/bin/env python3
import sys
import os
import re
import logging

logging.basicConfig(
    filename="mapper.log",
    filemode="a",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def tokenize(text):
    return re.findall(r'[a-zA-Z0-9]+', text.lower())

filename = os.environ.get('mapreduce_map_input_file') or os.environ.get('map_input_file', '')
if filename:
    current_doc_id = os.path.basename(filename).split('.')[0]
else:
    current_doc_id = 'unknown_doc'

logger.info(f"Using doc_id: {current_doc_id}")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    tokens = tokenize(line)
    for token in tokens:
        print(f"{token}\t{current_doc_id}\t1")
