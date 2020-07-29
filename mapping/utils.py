from time import asctime
import psycopg2
from psycopg2.errors import OperationalError
from psycopg2.extras import execute_values


FIND_UNMATCHED_MSIDS = """
    SELECT gid 
      INTO unmatched_msids
      FROM recording 
    EXCEPT 
           SELECT msb_recording_msid 
             FROM msd_mb_mapping
"""


def create_schema(conn):
    '''
        Create the relations schema if it doesn't already exist
    '''

    try:
        with conn.cursor() as curs:
            print(asctime(), "create schema")
            curs.execute("CREATE SCHEMA IF NOT EXISTS mapping")
            conn.commit()
    except OperationalError:
        print(asctime(), "failed to create schema 'mapping'")
        conn.rollback()


def insert_rows(curs, table, values):
    '''
        Use the bulk insert function to insert rows into the relations table.
    '''

    query = ("INSERT INTO %s VALUES " % table) + ",".join(values)
    try:
        curs.execute(query)
    except psycopg2.OperationalError:
        print(asctime(), "failed to insert rows")



def calculate_unmatched_msids():
    with psycopg2.connect('dbname=messybrainz user=msbpw host=musicbrainz-docker_db_1 password=messybrainz') as conn:
        with conn.cursor() as curs:
            conn.begin()
            curs.execute("DROP TABLE IF EXISTS unmatched_msids")
            curs.execute(FIND_UNMATCHED_IDS)
            conn.commit()


def insert_mapping_rows(curs, values):

    query = "INSERT INTO mapping.msd_mb_mapping VALUES %s"
    try:
        execute_values(curs, query, values, template=None)
    except psycopg2.OperationalError as err:
        print("failed to insert rows")
