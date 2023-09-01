from datetime import datetime, timedelta
import pandas as pd
import integrated_boarding_history
import update_boarding_history
import requests
import traceback

### Slack message 함수
def send_slack_message(webhook_url, message):
    payload = {
        'text': message
    }
    response = requests.post(webhook_url, json=payload)
    if response.status_code != 200:
        print('슬랙으로의 메시지 전송이 실패했습니다.')

try:
    print('Setting Datatime Range and Basic Info')
    ## Live
    freq ='60min'
    start_day = datetime.now() - timedelta(seconds=3600)
    datetime_idx = pd.DatetimeIndex([start_day]).floor(freq=freq)
    start_datetime = str(datetime_idx[0])
    end_datetime_idx = datetime_idx + timedelta(seconds=3599)
    end_datetime = str(end_datetime_idx[0])

    ## datetime seeting manualy
    # start_datetime = '2023-08-31 04:00:00'
    # end_datetime = '2023-08-31 04:59:59'
    print(start_datetime, '~', end_datetime)

    # setting bigquery parameter
    table_id = '*****'
    KEY_PATH = "*****"
    sector_table_df = pd.read_csv('edamodule/sector_table_name.csv')

    _start = datetime.now()

    print('Insert Program Start')
    integrated_boarding_history.run(start_datetime, end_datetime, table_id, KEY_PATH, sector_table_df)

    print('Update Program Start')
    update_boarding_history.run(start_datetime, end_datetime, table_id, KEY_PATH, sector_table_df)

    _end = datetime.now()
    print('Total_Running_Time:', _end-_start)

except Exception as e:
    webhook_url = '**'
    error_message = f'sql_to_bigquery 파이썬 코드 실행 중 오류가 발생했습니다 프로그램 재실행합니다.\n{traceback.format_exc()}'
    send_slack_message(webhook_url, error_message)

    try:
        print('Program restart')
        print('Setting Datatime Range and Basic Info')
        ## Live
        freq ='60min'
        start_day = datetime.now() - timedelta(seconds=3600)
        datetime_idx = pd.DatetimeIndex([start_day]).floor(freq=freq)
        start_datetime = str(datetime_idx[0])
        end_datetime_idx = datetime_idx + timedelta(seconds=3599)
        end_datetime = str(end_datetime_idx[0])

        ## datetime seeting manualy
        # start_datetime = '2023-06-14 07:00:00'
        # end_datetime = '2023-06-14 07:59:59'
        print(start_datetime, '~', end_datetime)

        # setting bigquery parameter
        table_id = '*****'
        KEY_PATH = "******"
        sector_table_df = pd.read_csv('edamodule/sector_table_name.csv')

        _start = datetime.now()

        print('Insert Program Start')
        integrated_boarding_history.run(start_datetime, end_datetime, table_id, KEY_PATH, sector_table_df)

        print('Update Program Start')
        update_boarding_history.run(start_datetime, end_datetime, table_id, KEY_PATH, sector_table_df)

        _end = datetime.now()
        print('Total_Running_Time:', _end-_start)

    except Exception as e:
        webhook_url = '**'
        error_message_2 = f'sql_to_bigquery 재실행 중 오류가 발생했습니다.\n{traceback.format_exc()}'
        send_slack_message(webhook_url, error_message_2)


