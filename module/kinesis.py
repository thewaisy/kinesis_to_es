import boto3
import json
import random
import traceback
import time
import uuid
import base64
from datetime import datetime, timedelta
from pytz import timezone
from config import Config

config = Config()

class Kinesis:
    shard_ids = []
    iterator = ''
    MAX_RETRY_COUNT = 3

    def __init__(self):
        if config.AWS_ACCESS_KEY_ID:
            self.client = boto3.client(
                'kinesis',
                config.AWS_REGION,
                aws_access_key_id=config.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY
            )
        else:
            self.client = boto3.client('kinesis', config.AWS_REGION)

        self.stream_name = config.KINESIS_STREAM

    def put_data(self, data_list):
        kds_data_list = []
        for data in data_list:
            partition_key = 'test-{:05}'.format(random.randint(1, 1024))
            kds_data = {'Data': json.dumps(data, sort_keys=False, ensure_ascii=False) + "\n",
                        'PartitionKey': partition_key}
            kds_data_list.append(kds_data)

        for _ in range(self.MAX_RETRY_COUNT):
            try:
                response = self.client.put_records(StreamName=config.KINESIS_STREAM, Records=kds_data_list)
                break
            except Exception as ex:
                traceback.print_exc()
                time.sleep(1)
        else:
            print("error")

    def read_data(self, iterator_type='TRIM_HORIZON'):
        self.iterator_type = iterator_type

        self._get_shard()

        response = self.client.get_records(
            ShardIterator=self.iterator,
            Limit=200  # 가져올 데이터 수
        )
        self.iterator = response['NextShardIterator']

        client_logs = []
        # datetime.strptime(data['created'], '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d %H:%M:%S')
        for record in response['Records']:
            if 'Data' in record and len(record['Data']) > 0:
                arrive_time = record['ApproximateArrivalTimestamp']
                data = json.loads(record['Data'])
                data_result = {
                    **data, **{'arrived_ts': self._convert_unixtime(arrive_time)}
                }
                client_logs.append(data_result)

        return client_logs

    def _get_shard(self):
        result = self.client.describe_stream(
            StreamName=self.stream_name,
            Limit=10
        )
        # print(json.dumps(result['StreamDescription']['Shards'], indent=2))
        for shard in result['StreamDescription']['Shards']:
            self.shard_ids.append(shard['ShardId'])

        self._get_iterator()

    def _get_iterator(self):
        response = self.client.get_shard_iterator(
            StreamName=self.stream_name,
            ShardId=self.shard_ids[0],
            ShardIteratorType=self.iterator_type
        )
        self.iterator = response['ShardIterator']

    def gen_random_data(self, num=1):
        event_list = [
            'page', 'click', 'login'
        ]
        page_list = [
            "main", "info", "test1", "test2"
        ]
        return [{
            "cgnt_id": f"ap-northeast-2:{uuid.uuid4()}",
            "created_ts": int(time.time() * 1000),
            "timezone": "Asia/Seoul",
            "id": random.randrange(1, 10000),
            "event": random.choice(event_list),
            "page": random.choice(page_list)
        } for _ in range(num)]

    def _convert_unixtime(self, ts):
        return int(ts.timestamp() * 1000)
