from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from elasticsearch.helpers.errors import BulkIndexError
from datetime import datetime, timedelta
from config import Config
import json

config = Config()

class ElasticSearch:

    def __init__(self):
        if config.ENV == 'dev':
            self.es = Elasticsearch(
                hosts=[{'host': config.ES_HOST_DEV, 'port': config.ES_HOST_PORT_DEV}],
                timeout=30,
                max_retries=10,
                retry_on_timeout=True
            )
        else:
            self.es = Elasticsearch(
                hosts=[{'host': config.ES_HOST, 'port': config.ES_HOST_PORT}],
                use_ssl=True,
                verify_certs=True,
                connection_class=RequestsHttpConnection,
                timeout=30,
                max_retries=10,
                retry_on_timeout=True
            )

    def _create_index(self, index):
        # 인덱스 생성
        self.es.indices.create(index=index)

    def _check_index(self, index):
        # 인덱스 확인
        return self.es.indices.exists(index)

    def put_data(self, client_logs):
        today = datetime.utcnow() + timedelta(hours=9)
        today = today.strftime('%Y-%m-%d')
        index = f"{config.ES_INDEX}{today}"

        try:
            if not self._check_index(index):
                self._create_index(index)

            docs = []
            for log in client_logs:
                docs.append({
                    '_index': index,
                    '_source': log
                })
            helpers.bulk(self.es, docs)

        except BulkIndexError as e:
            count = e.args[1]
            error_data = []
            for error in e.errors:
                data = error['index']
                error_data.append({'es_error': data['error'], 'data': data['data']})
            self._put_error_data(error_data)
        except Exception as e:
            print(f'client_log error {e}')

    def _put_error_data(self, error_logs):
        today = datetime.utcnow() + timedelta(hours=9)
        today = today.strftime('%Y-%m-%d')
        index = f"{config.ES_ERROR}{today}"
        try:
            if not self._check_index(index):
                self._create_index(index)

            docs = []
            for log in error_logs:
                docs.append({
                    '_index': index,
                    '_source': log
                })

            helpers.bulk(self.es, docs)
        except Exception as e:
            print(f'error_log bulk error {e}')
