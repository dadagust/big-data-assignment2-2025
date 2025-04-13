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

logger.info("Reducer2 started!")

try:
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
        CREATE TABLE IF NOT EXISTS statistics (
            term text PRIMARY KEY,
            df int,
            n int,
            avg_doc_length float
        )
    """)

    term_docs = {}
    total_docs = set()
    total_length = 0

    logger.info("Start reading lines from stdin...")

    for line in sys.stdin:
        try:
            line = line.strip()
            if not line:
                continue
            term, doc_id = line.split('\t')
            if term not in term_docs:
                term_docs[term] = set()
            term_docs[term].add(doc_id)
            total_docs.add(doc_id)
        except Exception as e:
            logger.error(f"Exception during processing line: {e}")
            traceback.print_exc(file=sys.stderr)

    logger.info("Fetching document lengths...")

    # Загружаем длину всех документов
    rows = session.execute("SELECT doc_id, length FROM doc_length")
    doc_lengths = {row.doc_id: row.length for row in rows}

    total_length = sum(doc_lengths.values())
    total_doc_count = len(doc_lengths)
    avg_doc_length = total_length / total_doc_count if total_doc_count else 0

    logger.info(f"Total documents: {total_doc_count}, Average document length: {avg_doc_length}")

    for term, docs in term_docs.items():
        try:
            df = len(docs)
            session.execute("""
                INSERT INTO statistics (term, df, n, avg_doc_length)
                VALUES (%s, %s, %s, %s)
            """, (term, df, total_doc_count, avg_doc_length))
        except Exception as e:
            logger.error(f"Error inserting into statistics: {e}")
            traceback.print_exc(file=sys.stderr)

    logger.info("Reducer2 finished successfully!")

except Exception as e:
    logger.exception("Fatal exception in reducer2!")
    sys.exit(1)
