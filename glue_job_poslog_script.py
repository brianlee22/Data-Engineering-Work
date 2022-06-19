import os
import sys
import json
import boto3
import datetime
import requests
import http.client
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sys.path.insert(0, '/opt')
import psycopg2

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'secret_name', 'region_name', 'bucket_name', 'threshold', 'infolder', 'infile', 'outfolder', 'outfile', 'api_url', 'okta_url', 'processedfolder'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

now = datetime.datetime.now()

JOB_NAME    = args['JOB_NAME']     # loyalty-glue-store-poslog-dev
SECRET_NAME = args['secret_name']  # loyalty/global/program-api/postgres/dev
REGION_NAME = args['region_name']  # us-west-2 
BUCKET_NAME = args['bucket_name']  # loyalty-db-data-dev
THRESHOLD   = args['threshold']    # 10000, see if there are too many missed transactions
INFOLDER    = args['infolder']     # store_poslog
INFILE      = args['infile']       # ETO_LSE_SALESDLY_STORE_LOAYLTYENGINE.csv
OUTFOLDER   = args['outfolder']    # store_poslog_missed_transactions
OUTFILE     = now.strftime("%Y%m%d_%H%M") + '_' + args['outfile']     # store_poslog_missed_transactions.json
API_URL     = args['api_url']      #
OKTA_URL    = args['okta_url']     #
PROCESSEDFOLDER = args['processedfolder'] #store_poslog_processed

# 0: Create AWS Secrets Manager client and Retrieve values from secrets
session     = boto3.session.Session()
client      = session.client(service_name='secretsmanager', region_name=REGION_NAME)
response    = client.get_secret_value(SecretId=SECRET_NAME)
secret      = response['SecretString']
json_secret = json.loads(secret)

pg_host     = json_secret['host']
pg_port     = json_secret['port']
pg_database = json_secret['database']
pg_user     = json_secret['program_user']  # program_api
pg_password = json_secret['program_password']
print("INFO: {} started".format(JOB_NAME))
print("INFO: Connection to Secrets Manager succeeded")

try:
    conn_string = "host={} user={} password={} dbname={}".format(pg_host, pg_user, pg_password, pg_database, pg_port)
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    print("INFO: Connection to RDS Postgres database, {}, succeeded".format(pg_database))
except psycopg2.DatabaseError as e:
    print("ERROR: Connection to RDS Postgres database, {}, failed".format(pg_database))
    print(e)
    sys.exit(0)

# program schema tables
schema  = 'program'
altid   = 'memberaltids'
member  = 'member'
tx      = 'transaction'
poslog  = 'job_store_poslog'
poshist = 'job_store_poslog_history'
posmiss = 'job_store_poslog_missed_transactions'

with conn.cursor() as cur:
    # schedule and/or trigger/crawler/s3_loyalty

    # 1: Truncate table
    query = "TRUNCATE TABLE {}.{}".format(schema, poslog)
    cur.execute(query)
    print("INFO: Truncate table, {}, succeeded".format(poslog))
    query = "TRUNCATE TABLE {}.{}".format(schema, posmiss)
    cur.execute(query)
    print("INFO: Truncate table, {}, succeeded".format(posmiss))

    s3prefix = INFOLDER + '/' + INFILE + '_' + now.strftime("%Y%m%d")
    s3prefix2 = INFOLDER + '/' + INFILE
    
    print("s3prefix: {}".format(s3prefix))
    s3 = session.resource('s3')
    s3bucket = s3.Bucket('{}'.format(BUCKET_NAME))
    imported = 0
    for s3obj in s3bucket.objects.filter(Prefix='{}'.format(s3prefix2)):
        print("s3file: {}".format(s3obj.key))
        s3file = s3obj.key
        basefile = os.path.basename(s3file)
        print("base file: {}".format(basefile))
        
        # 2: Import store poslog from s3 bucket
        query = "SELECT aws_s3.table_import_from_s3(" + \
                "    '{}.{}'".format(schema, poslog) + \
                "  , ' hd_file_extract_date_time, hd_order_or_tran_id,  hd_order_date_time, hd_loyalty_id" + \
                "    , hd_ean,                    hd_hybrisprimaryid,   hd_channel_type,    hd_brand" + \
                "    , hd_store_id,               hd_register_id,       hd_tendered_amt,    hd_shipping_cost" + \
                "    , hd_order_total_disc_amt,   hd_tax_amt,           hd_currency,        hd_tendered_giftcard_amt" + \
                "    , itm_sku,                   itm_quantity,         itm_amt,            itm_amt_after_disc" + \
                "    , discount_code1, discount_value1, discount_code2,  discount_value2" + \
                "    , discount_code3, discount_value3, discount_code4,  discount_value4" + \
                "    , discount_code5, discount_value5, discount_code6,  discount_value6" + \
                "    , discount_code7, discount_value7, discount_code8,  discount_value8" + \
                "    , discount_code9, discount_value9, discount_code10, discount_value10" + \
                "    , country, hd_orig_tran_id'" + \
                "  , ('CSV DELIMITER ''|'' NULL AS '''' HEADER')" + \
                "  , aws_commons.create_s3_uri(" + \
                "        '{}'".format(BUCKET_NAME) + \
                "      , '{}'".format(s3file) + \
                "    , '{}'))".format(REGION_NAME)
        try:
            cur.execute(query)
            print("INFO: Import S3 datafile, {}, succeeded".format(s3file))
            result = cur.fetchone()[0]
            print("{}".format(result))
        except:
            print("ERROR: Import S3 datafile, {}, failed".format(s3file))
            result = cur.fetchone()
            print("{}".format(result))
            sys.exit(0)
        imported += int(result.split()[0])
        try:
            copy_source = {
                'Bucket': BUCKET_NAME,
                'Key': s3file
            }
            s3.meta.client.copy(copy_source, BUCKET_NAME, PROCESSEDFOLDER + '/' + basefile)
            s3.Object(BUCKET_NAME, s3file).delete()
        except:
            print("ERROR: Moving S3 datafile, {}, from old bucket, {}, to new bucket, {}, failed".format(basefile, INFOLDER, PROCESSEDFOLDER))
            sys.exit(0)

    # 3-1. Select missed sale transactions from store poslog
    query = "INSERT INTO {}.{}".format(schema, posmiss) + \
            "  SELECT p.extract_ts" + \
            "       , p.tran_id" + \
            "       , p.tran_ts" + \
            "       , p.loyalty_id" + \
            "       , p.country" + \
            "       , p.store_id" + \
            "       , p.currency" + \
            "       , p.itm_sku" + \
            "       , p.itm_quantity" + \
            "       , p.itm_amount" + \
            "       , p.discount_code1,p.discount_code2,p.discount_code3,p.discount_code4,p.discount_code5" + \
            "       , p.discount_code6,p.discount_code7,p.discount_code8,p.discount_code9,p.discount_code10" + \
            "       , p.orig_tran_id" + \
            "  FROM (SELECT COALESCE(mc.id, ma.memberid) AS memberid" + \
            "             , TO_TIMESTAMP(x.hd_file_extract_date_time, 'YYYY-MM-DD HH24:MI:SS') AS extract_ts" + \
            "             , x.hd_order_or_tran_id AS tran_id" + \
            "             , TO_TIMESTAMP(x.hd_order_date_time, 'YYYY-MM-DD HH24:MI:SS') AS tran_ts" + \
            "             , x.hd_loyalty_id AS loyalty_id" + \
            "             , SUBSTRING(x.country, 1, 2) AS country" + \
            "             , x.hd_store_id AS store_id" + \
            "             , x.hd_currency AS currency" + \
            "             , x.itm_sku AS itm_sku" + \
            "             , x.itm_quantity AS itm_quantity" + \
            "             , x.itm_amt_after_disc AS itm_amount" + \
            "             , x.discount_code1,x.discount_code2,x.discount_code3,x.discount_code4,x.discount_code5" + \
            "             , x.discount_code6,x.discount_code7,x.discount_code8,x.discount_code9,x.discount_code10" + \
            "             , x.hd_orig_tran_id AS orig_tran_id" + \
            "        FROM (SELECT DISTINCT * FROM {}.{}".format(schema, poslog) + \
            "              EXCEPT" + \
            "              SELECT * FROM {}.{}) x".format(schema, poshist) + \
            "        LEFT JOIN (SELECT id, consumerid FROM {}.{}) mc".format(schema, member) + \
            "          ON x.hd_loyalty_id = mc.consumerid" + \
            "        LEFT JOIN (SELECT memberid, altid FROM {}.{}) ma".format(schema, altid) + \
            "          ON x.hd_loyalty_id = ma.altid" + \
            "        WHERE SPLIT_PART(x.hd_order_or_tran_id, '#', 1) = 'SALE'" + \
            "          AND (mc.id IS NOT NULL OR" + \
            "               ma.memberid IS NOT NULL)) p" + \
            "  LEFT JOIN {}.{} t".format(schema, tx) + \
            "    ON p.memberid = t.memberid AND" + \
            "       p.tran_ts  = t.date AND" + \
            "       p.country  = t.country AND" + \
            "       p.store_id = t.store AND" + \
            "       p.currency = t.currency AND" + \
            "       p.tran_id  = t.referencenumber AND" + \
            "       CAST(p.itm_amount AS REAL) = t.monetaryamount AND" + \
            "       t.type = 'PURCHASE'" + \
            "  WHERE t.memberid IS NULL"
    try:
        cur.execute(query)
        print("INFO: Select missed sale transactions succeeded")
        result = cur.statusmessage
        print("{}".format(result.split()[2]))
    except:
        print("ERROR: Select missed sale transactions failed")
        result = cur.fetchone()
        print("{}".format(result))
        sys.exit(0)

    # 3-2. Select missed return transactions from store poslog
    query = "INSERT INTO {}.{}".format(schema, posmiss) + \
            "  SELECT p.extract_ts" + \
            "       , p.tran_id" + \
            "       , p.tran_ts" + \
            "       , p.loyalty_id" + \
            "       , p.country" + \
            "       , p.store_id" + \
            "       , p.currency" + \
            "       , p.itm_sku" + \
            "       , p.itm_quantity" + \
            "       , p.itm_amount" + \
            "       , p.discount_code1,p.discount_code2,p.discount_code3,p.discount_code4,p.discount_code5" + \
            "       , p.discount_code6,p.discount_code7,p.discount_code8,p.discount_code9,p.discount_code10" + \
            "       , p.orig_tran_id" + \
            "  FROM (SELECT COALESCE(mc.id, ma.memberid) AS memberid" + \
            "             , TO_TIMESTAMP(x.hd_file_extract_date_time, 'YYYY-MM-DD HH24:MI:SS') AS extract_ts" + \
            "             , x.hd_order_or_tran_id AS tran_id" + \
            "             , TO_TIMESTAMP(x.hd_order_date_time, 'YYYY-MM-DD HH24:MI:SS') AS tran_ts" + \
            "             , x.hd_loyalty_id AS loyalty_id" + \
            "             , SUBSTRING(x.country, 1, 2) AS country" + \
            "             , x.hd_store_id AS store_id" + \
            "             , x.hd_currency AS currency" + \
            "             , x.itm_sku AS itm_sku" + \
            "             , x.itm_quantity AS itm_quantity" + \
            "             , x.itm_amt_after_disc AS itm_amount" + \
            "             , x.discount_code1,x.discount_code2,x.discount_code3,x.discount_code4,x.discount_code5" + \
            "             , x.discount_code6,x.discount_code7,x.discount_code8,x.discount_code9,x.discount_code10" + \
            "             , x.hd_orig_tran_id AS orig_tran_id" + \
            "        FROM (SELECT * FROM {}.{}".format(schema, poslog) + \
            "              EXCEPT" + \
            "              SELECT * FROM {}.{}) x".format(schema, poshist) + \
            "        LEFT JOIN (SELECT id, consumerid FROM {}.{}) mc".format(schema, member) + \
            "          ON x.hd_loyalty_id = mc.consumerid" + \
            "        LEFT JOIN (SELECT memberid, altid FROM {}.{}) ma".format(schema, altid) + \
            "          ON x.hd_loyalty_id = ma.altid" + \
            "        WHERE SPLIT_PART(x.hd_order_or_tran_id, '#', 1) = 'RETU'" + \
            "          AND (mc.id IS NOT NULL OR" + \
            "               ma.memberid IS NOT NULL)) p" + \
            "  LEFT JOIN {}.{} t".format(schema, tx) + \
            "    ON p.memberid = t.memberid AND" + \
            "       p.tran_ts = t.date AND" + \
            "       p.country  = t.country AND" + \
            "       p.store_id = t.store AND" + \
            "       p.currency = t.currency AND" + \
            "       p.tran_id = t.referencenumber AND" + \
            "       CAST(p.itm_amount AS REAL) = t.monetaryamount" + \
            "  LEFT JOIN (SELECT memberid, type, referencenumber FROM {}.{}) t2".format(schema, tx) + \
            "    ON p.memberid = t2.memberid AND" + \
            "       t2.type = 'RETURN' AND" + \
            "       p.orig_tran_id = t2.referencenumber" + \
            "  WHERE t.memberid  IS NULL" + \
            "    AND t2.memberid IS NULL"
    try:
        cur.execute(query)
        print("INFO: Select missed return transactions succeeded")
        result = cur.statusmessage
        print("{}".format(result.split()[2]))
    except:
        print("ERROR: Select missed return transactions failed")
        result = cur.fetchone()
        print("{}".format(result))
        sys.exit(0)

    # 3-3. Select missed void transactions from store poslog
    query = "INSERT INTO {}.{}".format(schema, posmiss) + \
            "  SELECT p.extract_ts" + \
            "       , p.tran_id" + \
            "       , p.tran_ts" + \
            "       , p.loyalty_id" + \
            "       , p.country" + \
            "       , p.store_id" + \
            "       , p.currency" + \
            "       , p.itm_sku" + \
            "       , p.itm_quantity" + \
            "       , p.itm_amount" + \
            "       , p.discount_code1,p.discount_code2,p.discount_code3,p.discount_code4,p.discount_code5" + \
            "       , p.discount_code6,p.discount_code7,p.discount_code8,p.discount_code9,p.discount_code10" + \
            "       , p.orig_tran_id" + \
            "  FROM (SELECT COALESCE(mc.id, ma.memberid) AS memberid" + \
            "             , TO_TIMESTAMP(x.hd_file_extract_date_time, 'YYYY-MM-DD HH24:MI:SS') AS extract_ts" + \
            "             , x.hd_order_or_tran_id AS tran_id" + \
            "             , TO_TIMESTAMP(x.hd_order_date_time, 'YYYY-MM-DD HH24:MI:SS') AS tran_ts" + \
            "             , x.hd_loyalty_id AS loyalty_id" + \
            "             , SUBSTRING(x.country, 1, 2) AS country" + \
            "             , x.hd_store_id AS store_id" + \
            "             , x.hd_currency AS currency" + \
            "             , x.itm_sku AS itm_sku" + \
            "             , x.itm_quantity AS itm_quantity" + \
            "             , x.itm_amt_after_disc AS itm_amount" + \
            "             , x.discount_code1,x.discount_code2,x.discount_code3,x.discount_code4,x.discount_code5" + \
            "             , x.discount_code6,x.discount_code7,x.discount_code8,x.discount_code9,x.discount_code10" + \
            "             , x.hd_orig_tran_id AS orig_tran_id" + \
            "        FROM (SELECT * FROM {}.{}".format(schema, poslog) + \
            "              EXCEPT" + \
            "              SELECT * FROM {}.{}) x".format(schema, poshist) + \
            "        LEFT JOIN (SELECT id, consumerid FROM {}.{}) mc".format(schema, member) + \
            "          ON x.hd_loyalty_id = mc.consumerid" + \
            "        LEFT JOIN (SELECT memberid, altid FROM {}.{}) ma".format(schema, altid) + \
            "          ON x.hd_loyalty_id = ma.altid" + \
            "        WHERE SPLIT_PART(x.hd_order_or_tran_id, '#', 1) = 'VOID'" + \
            "          AND (mc.id IS NOT NULL OR" + \
            "               ma.memberid IS NOT NULL)) p" + \
            "  LEFT JOIN {}.{} t".format(schema, tx) + \
            "    ON p.memberid = t.memberid AND" + \
            "       p.tran_ts = t.date AND" + \
            "       p.country  = t.country AND" + \
            "       p.store_id = t.store AND" + \
            "       p.currency = t.currency AND" + \
            "       p.tran_id = t.referencenumber AND" + \
            "       CAST(p.itm_amount AS REAL) = t.monetaryamount" + \
            "  LEFT JOIN (SELECT memberid, type, referencenumber FROM {}.{}) t2".format(schema, tx) + \
            "    ON p.memberid = t2.memberid AND" + \
            "       (t2.type = 'VOID_PURCHASE' OR t2.type = 'VOID_RETURN') AND" + \
            "       p.orig_tran_id = t2.referencenumber" + \
            "  WHERE t.memberid  IS NULL" + \
            "    AND t2.memberid IS NULL"
    try:
        cur.execute(query)
        print("INFO: Select missed void transactions succeeded")
        result = cur.statusmessage
        print("{}".format(result.split()[2]))
    except:
        print("ERROR: Select missed void transactions failed")
        result = cur.fetchone()
        print("{}".format(result))
        sys.exit(0)

    # 4. Count missed transaction
    query = "SELECT COUNT(*) FROM {}.{}".format(schema, posmiss)
    try:
        cur.execute(query)
        print("INFO: Count table, {}, succeeded".format(posmiss))
        result = cur.fetchone()
    except:
        print("ERROR: Count table, {}, failed".format(posmiss))
        result = cur.fetchone()
        print("{}".format(result))
        sys.exit(0)
    missed = int(result[0])
    if missed < int(THRESHOLD):
        print("{} rows missed transactions found and below threshold, {}".format(missed, THRESHOLD))
    else:
        print("ERROR: {} rows missed transactions found and reached threshold, {}. Exit job!".format(missed, THRESHOLD))
        sys.exit(0)

    exported = 0
    if missed > 0:
        # 5. Export missed transaction to s3 bucket
        query = "SELECT aws_s3.query_export_to_s3(" + \
                "    'SELECT row_to_json(t) FROM {}.{} AS t'".format(schema, posmiss) + \
                "  , aws_commons.create_s3_uri(" + \
                "        '{}'".format(BUCKET_NAME) + \
                "      , '{}/{}'".format(OUTFOLDER, OUTFILE) + \
                "      , '{}')".format(REGION_NAME) + \
                "  , options := '')"
        try:
            cur.execute(query)
            print("INFO: Export S3 datafile, {}, succeeded".format(OUTFILE))
            result = cur.fetchone()
            print("{} rows missed transactions exported".format(result[0][1:-1].split(',')[0]))
        except:
            print("ERROR: Export S3 datafile, {}, failed".format(OUTFILE))
            result = cur.fetchone()
            print("{}".format(result))
            sys.exit(0)
        exported = int(result[0][1:-1].split(',')[0])
   
        # 6. Call bulk purchase API
        #    api data query
        query = "SELECT row_to_json(t) FROM {}.{} AS t".format(schema, posmiss)
        try:
            cur.execute(query)
            print("INFO: Select table, {}.{}, succeeded".format(schema, posmiss))
            result = cur.fetchall()
            data = "["
            for row in result[:-1]:
                # print("json row: {}".format(json.dumps(row[0])))
                data += json.dumps(row[0]) + ","
            data += json.dumps(result[-1][0]) + "]"
            # print("fetched data: {}".format(data))
        except:
            print("ERROR: Select table, {}.{}, failed".format(schema, posmiss))
            result = cur.fetchall()
            print("{}".format(result))
            sys.exit(0)
        
        #    okta auth call
        try:
            headers = {'Content-Type': 'application/x-www-form-urlencoded',
                       'Accept': 'application/json',
                       'Authorization': 'Basic MG9hMTVnaTNzczFadUJ5azkwaDg6a3MwSG9nVDYyRENlS00wdzRRT2JVMjNxY1VHRG9rcVFiM3dIeHhuMQ==',
                       'Cookie': 'JSESSIONID=C88AF31FC4CB18AF44F9354B29BCA805'}
            params = {'grant_type': 'client_credentials',
                      'scope': 'READ'}
            response = requests.post(OKTA_URL, data=params, headers=headers)
            if response != None and response.status_code == requests.codes.ok:
                print("INFO: OKTA auth API call succeeded")
                print("status.code = {}".format(response.status_code))
                token = json.loads(response.text)['access_token']
            else:
                print("ERROR: OKTA auth API call failed")
                print("text = {}".format(response.text))
                print("status.code = {}".format(response.status_code))
                sys.exit(0)
        except:
            print("ERROR: OKTA auth API call failed")
            sys.exit(0)
   
        #    api call
        try:
            headers = {'Content-Type': 'application/json',
                       'Accept': 'application/json',
                       'OktaToken': '{}'.format(token)}
            response = requests.put(API_URL, data=data, headers=headers)
            if response != None and response.status_code == requests.codes.ok:
                print("INFO: Bulk Purchase API succeeded")
                print("status.code = {}".format(response.status_code))
            else:
                print("ERROR: Bulk Purchase API failed")
                print("text = {}".format(response.text))
                print("status.code = {}".format(response.status_code))
                sys.exit(0)
        except:
            print("ERROR: Bulk Purchase API call failed")
            sys.exit(0)
    
        # 7. Archive historical data
        query = "INSERT INTO {}.{}".format(schema, poshist) + \
                "  SELECT * FROM {}.{}".format(schema, poslog) + \
                "  EXCEPT" + \
                "  SELECT * FROM {}.{}".format(schema, poshist)
        try:
            cur.execute(query)
            print("INFO: Archive to table, {}, succeeded".format(poshist))
        except:
            print("ERROR: Archive to table, {}, failed".format(poshist))
            result = cur.fetchone()
            print("{}".format(result))
            sys.exit(0)

    # 8. Truncate table
    query = "TRUNCATE TABLE {}.{}".format(schema, poslog)
    cur.execute(query)
    print("INFO: Truncate table, {}, succeeded".format(poslog))
    query = "TRUNCATE TABLE {}.{}".format(schema, posmiss)
    cur.execute(query)
    print("INFO: Truncate table, {}, succeeded".format(posmiss))

    print("imported store rows = {}, exported missed rows = {}".format(imported, exported))
job.commit()
