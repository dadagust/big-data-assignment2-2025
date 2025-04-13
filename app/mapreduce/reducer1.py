#!/usr/bin/env python3
import sys
import traceback
import logging
from cassandra.cluster import Cluster

logging.basicConfig(
    filename="reducer.log",
    filemode="a",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

logger.info("Reducer started!")

try:
    # Подключение к Cassandra
    logger.info("Connecting to Cassandra...")
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect()

    KEYSPACE = "search_keyspace"
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """)
    session.set_keyspace(KEYSPACE)

    session.execute("""
        CREATE TABLE IF NOT EXISTS inverted_index (
            term text,
            doc_id text,
            freq int,
            PRIMARY KEY (term, doc_id)
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS doc_length (
            doc_id text,
            length int,
            PRIMARY KEY (doc_id)
        )
    """)

    current_term = None
    current_doc_id = None
    current_freq = 0
    doc_lengths = {}

    def flush(term, doc_id, freq):
        if not term or not doc_id:
            logger.warning(f"Skipping flush for empty term/doc_id: term={term}, doc_id={doc_id}")
            return
        try:
            session.execute("""
                INSERT INTO inverted_index (term, doc_id, freq) VALUES (%s, %s, %s)
            """, (term, doc_id, freq))
            doc_lengths[doc_id] = doc_lengths.get(doc_id, 0) + freq
        except Exception as e:
            logger.error(f"Error inserting into inverted_index: {e}")
            traceback.print_exc(file=sys.stderr)

    logger.info("Start reading lines from stdin...")

    for line in sys.stdin:
        try:
            line = line.strip()
            if not line:
                continue
            parts = line.split('\t')
            if len(parts) != 3:
                logger.warning(f"Skipping malformed line: {line}")
                continue
            term, doc_id, count = parts
            count = int(count)

            if (term == current_term) and (doc_id == current_doc_id):
                current_freq += count
            else:
                flush(current_term, current_doc_id, current_freq)
                current_term = term
                current_doc_id = doc_id
                current_freq = count
        except Exception as e:
            logger.error(f"Exception during processing line: {e}")
            traceback.print_exc(file=sys.stderr)

    flush(current_term, current_doc_id, current_freq)

    logger.info("Flushing document lengths...")

    for doc_id, total_length in doc_lengths.items():
        try:
            session.execute("""
                INSERT INTO doc_length (doc_id, length) VALUES (%s, %s)
            """, (doc_id, total_length))
        except Exception as e:
            logger.error(f"Error inserting into doc_length: {e}")
            traceback.print_exc(file=sys.stderr)

    logger.info("Reducer finished successfully!")

except Exception as e:
    logger.exception("Fatal exception in reducer!")
    sys.exit(1)
