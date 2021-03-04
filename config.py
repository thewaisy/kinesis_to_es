import os
class Config(object):

    ENV = os.environ.get('APP_ENV', 'dev')

    # AWS 셋팅
    AWS_REGION = "ap-northeast-2"
    AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', None)
    AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', None)

    # 키네시스 스트림
    KINESIS_STREAM = "<kinesis stream name>"

    # ES
    ES_HOST = "<elasticsearch host>"
    ES_HOST_PORT = 443
    ES_HOST_DEV = "localhost"
    ES_HOST_PORT_DEV = 9200
    ES_INDEX = "<es data prefix>"
    ES_ERROR = "<es error data prefix>"
