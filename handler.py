# handler.py
# import datetime
import datetime
import json
import os
# from elasticsearch import Elasticsearch, RequestsHttpConnection
# from requests_aws4auth import AWS4Auth
from botocore.config import Config
import time
import mysql.connector
import boto3



def current_milli_time():
    return str(round(time.time() * 1000))


def write_mysql(current_value, algorithm, portfolio_id, portfolio, backtest_time):
    ENDPOINT = "quantegy.cluster-cheqyrklzyah.us-east-1.rds.amazonaws.com"
    PORT = "3306"
    USR = "quantegy"
    REGION = "us-east-1"
    os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

    # gets the credentials from .aws/credentials
    session = boto3.Session()
    client = session.client('rds')

    token = client.generate_db_auth_token(DBHostname=ENDPOINT, Port=PORT, DBUsername=USR, Region=REGION)

    try:
        conn = mysql.connector.connect(host=ENDPOINT, user=USR, passwd=token, port=PORT, database='quantegy')
        cur = conn.cursor()
        cur.execute("""SELECT now()""")
        query_results = cur.fetchall()
        print(query_results)
    except Exception as e:
        print("Database connection failed due to {}".format(e))


# def write_es(current_value, algorithm, portfolio_id, portfolio, backtest_time):
#
#     # client, current_value, algorithm, env, portfolio_id, exchange, data_type, portfolio, backtest_time=None):
#
#     host = 'search-quantegy-njo457ktl3upnncyeubz6p25v4.us-east-1.es.amazonaws.com'  # For example, my-test-domain.us-east-1.es.amazonaws.com
#     region = 'us-east-1'  # e.g. us-west-1
#
#     service = 'es'
#     credentials = boto3.Session().get_credentials()
#     awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
#
#     try:
#         t = time.mktime(
#             datetime.datetime.strptime(
#                 backtest_time,
#                 "%Y-%m-%d %H:%M:%S.%f"
#             ).timetuple()
#         )
#     except Exception:
#         t = time.mktime(
#             datetime.datetime.strptime(
#                 backtest_time,
#                 "%Y-%m-%d %H:%M:%S"
#             ).timetuple()
#         )
#
#     es = Elasticsearch(
#         hosts=[{'host': host, 'port': 443}],
#         http_auth=awsauth,
#         use_ssl=True,
#         verify_certs=True,
#         connection_class=RequestsHttpConnection,
#         region='us-east-1'
#     )
#
#     document = {
#         "current_value": float(current_value),
#         "algorithm": algorithm,
#         "portfolio-id": portfolio_id,
#         "portfolio": portfolio,
#         "time": int(t)
#     }
#     print(es.info())
#
#     # es.index(index="quantegy-backtest", doc_type="_doc", id="5", body=document)
#     es.index(index="quantegy-backtest", doc_type="_doc", body=document)
#
#     # print(es.get(index="quantegy-backtest", doc_type="_doc", id="5"))


def write_records(client, current_value, algorithm, env, portfolio_id, exchange, data_type, portfolio,
                      backtest_time):

    print("Writing records to " + env)
    current_time = current_milli_time()

    backtest_datetime = datetime.datetime.timestamp(backtest_time)
    backtest_datetime_str = backtest_datetime.strftime("%m/%d/%Y, %H:%M:%S")
    
    portfolioj = json.loads(portfolio)
    percent_value = (((float(current_value)/10000.0) * 100.0) - 100.0)

    dimensions = [
        {'Name': 'region', 'Value': 'us-east-1'},
        {'Name': 'az', 'Value': 'az1'},
        {'Name': 'hostname', 'Value': 'host1'},
        {'Name': 'algorithm', 'Value': algorithm},
        {'Name': 'environment', 'Value': env},
        {'Name': 'portfolio_id', 'Value': portfolio_id},
        {'Name': 'exchange', 'Value': exchange},
        {'Name': 'data_type', 'Value': data_type}

    ]
    portfolio_items = []
    for elem in portfolioj:
        item = {
            'Dimensions': dimensions,
            'MeasureName': 'portfolio_item',
            'MeasureValue': elem,
            'MeasureValueType': 'VARCHAR',
            'Time': current_milli_time()
        }
        portfolio_items.append(item)
        time.sleep(0.05)

    current_value = {
        'Dimensions': dimensions,
        'MeasureName': 'current_value',
        'MeasureValue': current_value,
        'MeasureValueType': 'DOUBLE',
        'Time': current_time
    }

    percent_value = {
        'Dimensions': dimensions,
        'MeasureName': 'percent_value',
        'MeasureValue': str(percent_value),
        'MeasureValueType': 'DOUBLE',
        'Time': current_time
    }

    portfolio_items.append(current_value)
    portfolio_items.append(percent_value)
    records = portfolio_items

    try:
        result = client.write_records(DatabaseName='quantegy-soak-db', TableName='portfolio-value-data',
                                           Records=records, CommonAttributes={})
        print("WriteRecords Status: [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
    except client.exceptions.RejectedRecordsException as err:
        print("RejectedRecords: ", err)
        for rr in err.response["RejectedRecords"]:
            print("Rejected Index " + str(rr["RecordIndex"]) + ": " + rr["Reason"])
        print("Other records were written successfully. ")
    except Exception as err:
        print("Error:", err)


def main(event, context):
    session = boto3.Session()
    message = json.loads(event['Records'][0]['Sns']['Message'])
    current_value = message['current_value']
    portfolio_id = message['portfolio_id']
    portfolio = message['portfolio']
    algorithm = message['algorithm']
    exchange = message['exchange']
    backtest_time = message['backtest-time']
    env = message['env']
    print(message)

    print("current_value = " + str(current_value))
    print("portfolio = " + portfolio)
    write_client = session.client('timestream-write', config=Config(read_timeout=20, max_pool_connections=5000, retries={'max_attempts': 10}))

    # if env == "backtest":
        # time.sleep(.05)
    write_records(write_client, str(current_value), algorithm, env, portfolio_id, exchange, env, portfolio, backtest_time)
        # write_mysql(str(current_value), algorithm, portfolio_id, portfolio, backtest_time)
        # write_es(str(current_value), algorithm, portfolio_id, portfolio, backtest_time)
        # write_records(write_client, str(current_value), algorithm, env, portfolio_id, exchange, env, portfolio, backtest_time)
    # else:
    #     write_records(write_client, str(current_value), algorithm, env, portfolio_id, exchange, env, portfolio)


if __name__ == "__main__":
    main('', '')