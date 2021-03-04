from apscheduler.schedulers.background import BackgroundScheduler
from config import Config
import time
import logging
from datetime import datetime
import timeit
import os, json
from module.kinesis import Kinesis
from module.elasticsearch import ElasticSearch

config = Config()
scheduler = BackgroundScheduler()
es = ElasticSearch()
kcl = Kinesis()

# 로그 폴더 생성
if not os.path.exists('./logs'):
    os.makedirs('./logs')
log = logging.basicConfig(filename=f"./logs/error.log", level=logging.ERROR)

@scheduler.scheduled_job('interval', seconds=1, id='read')
def read_kinesis():
    # code timer
    start_time = timeit.default_timer()

    # kinesis 데이터 읽기
    client_logs = kcl.read_data(iterator_type='LATEST')

    print(json.dumps(client_logs, indent=2))

    # 데이터 es 적재
    # if client_logs:
    #     es.put_data(client_logs)

    terminate_time = timeit.default_timer()  # 종료 시간 체크
    if terminate_time - start_time > 1:
        print("%s %d개 %f초 걸렸습니다." % (datetime.now(), len(checked_client_logs), terminate_time - start_time))

scheduler.start()
while True:
    time.sleep(0.5)
