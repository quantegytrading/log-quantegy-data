# handler.py
import json
from datetime import datetime

import boto3
from botocore.config import Config
import time


def current_milli_time():
    return str(round(time.time() * 1000))


def write_records(client, current_value, algorithm, env, portfolio, exchange, data_type, backtest_time=None):
    print("Writing records")
    print("backtest-time:" + str(backtest_time))
    if backtest_time is None:
        current_time = current_milli_time()
    else:
        try:
            date = datetime.strptime(backtest_time, '%Y-%m-%d %H:%M:%S.%f')
            current_time = str(time.mktime(date.timetuple()))
        except Exception:
            date = datetime.strptime(backtest_time, '%Y-%m-%d %H:%M:%S')
            current_time = str(time.mktime(date.timetuple()) * 1000)


    dimensions = [
        {'Name': 'region', 'Value': 'us-east-1'},
        {'Name': 'az', 'Value': 'az1'},
        {'Name': 'hostname', 'Value': 'host1'},
        {'Name': 'algorithm', 'Value': algorithm},
        {'Name': 'environment', 'Value': env},
        {'Name': 'portfolio_id', 'Value': portfolio},
        {'Name': 'exchange', 'Value': exchange},
        {'Name': 'data_type', 'Value': data_type}

    ]

    current_value = {
        'Dimensions': dimensions,
        'MeasureName': 'current_value',
        'MeasureValue': current_value,
        'MeasureValueType': 'DOUBLE',
        'Time': current_time
    }

    records = [current_value]

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
    algorithm = message['algorithm']
    exchange = message['exchange']
    backtest_time = message['backtest-time']
    env = message['env']



    print("current_value = " + str(current_value))
    write_client = session.client('timestream-write', config=Config(read_timeout=20, max_pool_connections=5000, retries={'max_attempts': 10}))

    if env == "backtest":
        write_records(write_client, str(current_value), algorithm, env, portfolio_id, exchange, env, backtest_time)
    else:
        write_records(write_client, str(current_value), algorithm, env, portfolio_id, exchange, env)



if __name__ == "__main__":
    main('', '')