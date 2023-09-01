# -*- coding: utf-8 -*-


import os
from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
scheduler = BlockingScheduler()

def run_flexible_rate_job():
    print(datetime.now(), "\tBigquery_Main")
    start_at = datetime.now()
    os.system("/home/elap/.pyenv/versions/3.11.0/envs/python311/bin/python3 bigquery_main.py")
    end_at = datetime.now()
    print(end_at - start_at)
    pass


if __name__ == '__main__':

    scheduler.add_job(run_flexible_rate_job, 'cron', hour='*', minute='0')

    scheduler.start()

    pass

