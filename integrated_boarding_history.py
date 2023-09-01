import pandas as pd
from edamodule.DBConfig import DBConfig
from edamodule.QueryConfig import ServiceQuery
from edamodule import LoadConfigInfo
from edamodule.InsertLoadModule import DataLoad
from google.cloud import bigquery
from google.oauth2 import service_account
from edamodule.GroupSectorUtils import GetSector, GetCoordinate
from tqdm import tqdm
from datetime import datetime

def run(start_datetime, end_datetime, table_id, KEY_PATH, sector_table_df):

    program_start = datetime.now()

    ## 데이터 통합함수
    db_info_data = LoadConfigInfo.get_dbconfig_info()
    def load_data(db_info_data, query):

        _dbconfig = DBConfig(db_info_data)
        _conn, _curs = _dbconfig.ServiceRO()
        _loadconfig = DataLoad(_conn, _curs)
        data = _loadconfig.get_data(query)
        del _loadconfig
        return data

    config_query = ServiceQuery(start_datetime,end_datetime)
    ## 앱호출
    print('load_app_df')
    load_start = datetime.now()
    app_query = config_query.Get_app_call()
    _app_boarding = load_data(db_info_data, app_query)

    ## 일반주행
    print('load_general_df')
    general_query = config_query.Get_general()
    _general_boarding = load_data(db_info_data, general_query)

    ## 예약 취소 쿼리
    print('load_reservation_cancel_df')
    reservation_query = config_query.Get_reservation_cancel()
    _reservation_cancel = load_data(db_info_data, reservation_query)
    load_end = datetime.now()
    print('take_data_load_time:', load_end-load_start)

    ## 데이터 병합
    print('concat total_df')
    total_df = pd.concat([_app_boarding, _general_boarding])
    total_df = pd.concat([total_df, _reservation_cancel])
    total_df = total_df.sort_values(by='reg_datetime').reset_index(drop=True)
    total_df['estimated_datetime'] = pd.to_datetime(total_df['estimated_datetime'])
    del _app_boarding, _general_boarding, _reservation_cancel
    print(len(total_df))

    ## 섹터 판별
    print('wgs_to_xpos')
    _total_df_sector = total_df.copy()
    coordi_config = GetCoordinate()

    s_lng_lst = list(_total_df_sector['departure_lng'])
    s_lat_lst = list(_total_df_sector['departure_lat'])
    g_lng_lst = list(_total_df_sector['arrival_lng'])
    g_lat_lst = list(_total_df_sector['arrival_lat'])

    s_converted = coordi_config.wgstokatec(s_lng_lst, s_lat_lst)
    g_converted = coordi_config.wgstokatec(g_lng_lst, g_lat_lst)

    _total_df_sector['s_xpos'] = s_converted[0]
    _total_df_sector['s_ypos'] = s_converted[1]
    _total_df_sector['g_xpos'] = g_converted[0]
    _total_df_sector['g_ypos'] = g_converted[1]
    print('Get Sector Table')

    get_sector_config = GetSector(sector_table_df)

    out_xpos = sector_table_df['right_xpos'].max() + 10000
    out_ypos = sector_table_df['top_ypos'].max() + 10000

    xpos_col = ['s_xpos', 'g_xpos']
    ypos_col = ['s_ypos', 'g_ypos']
    _total_df_sector[xpos_col] = _total_df_sector[xpos_col].fillna(out_xpos)
    _total_df_sector[ypos_col] = _total_df_sector[ypos_col].fillna(out_ypos)

    s_sector_table_list = []
    g_sector_table_list = []
    for i in tqdm(range(len(_total_df_sector))):
        # print(f'{i} preprocessing')
        _s_xpos = _total_df_sector['s_xpos'][i]
        _s_ypos = _total_df_sector['s_ypos'][i]
        _g_xpos = _total_df_sector['g_xpos'][i]
        _g_ypos = _total_df_sector['g_ypos'][i]
        _s_sector_info = get_sector_config.get_sector_info_table(_s_xpos, _s_ypos)
        s_sector_table_list.append(_s_sector_info)
        _g_sector_info = get_sector_config.get_sector_info_table(_g_xpos, _g_ypos)
        g_sector_table_list.append(_g_sector_info)

    total_df['departure_sector_table'] = s_sector_table_list
    total_df['arrival_sector_table'] = g_sector_table_list
    _total_df_sector['departure_sector_table'] = s_sector_table_list
    _total_df_sector['arrival_sector_table'] = g_sector_table_list


    print('Get Sector')
    s_sector_table_info_list = list(total_df['departure_sector_table'].unique())
    g_sector_table_info_list = list(total_df['arrival_sector_table'].unique())

    ## 출발지 섹터 정보
    for _s_sector_info in tqdm(s_sector_table_info_list):
        _sector_df = _total_df_sector[_total_df_sector['departure_sector_table'] == _s_sector_info].reset_index(drop=True)

        if _s_sector_info == 'out_of_sector':
            _total_df_sector.loc[_total_df_sector['departure_sector_table'] == _s_sector_info, 's_sector'] = 9999

        else:
            xpos = 's_xpos'
            ypos = 's_ypos'
            _s_sector_lst = get_sector_config.get_sector(_s_sector_info, _sector_df, xpos, ypos)
            _total_df_sector.loc[_total_df_sector['departure_sector_table'] == _s_sector_info, 's_sector'] = _s_sector_lst

    ## 목적지 섹터 정보
    for _g_sector_info in tqdm(g_sector_table_info_list):
        _sector_df = _total_df_sector[_total_df_sector['arrival_sector_table'] == _g_sector_info].reset_index(drop=True)

        if _g_sector_info == 'out_of_sector':
            _total_df_sector.loc[_total_df_sector['arrival_sector_table'] == _g_sector_info, 'g_sector'] = 9999

        else:
            xpos = 'g_xpos'
            ypos = 'g_ypos'
            _g_sector_lst = get_sector_config.get_sector(_g_sector_info, _sector_df, xpos, ypos)
            _total_df_sector.loc[_total_df_sector['arrival_sector_table'] == _g_sector_info, 'g_sector'] = _g_sector_lst

    total_df['departure_sector'] = _total_df_sector['s_sector']
    total_df['arrival_sector'] = _total_df_sector['g_sector']

    total_df[['departure_sector', 'arrival_sector']] = total_df[['departure_sector', 'arrival_sector']].astype(int)
    # total_df.to_csv('dataset/test.csv', encoding='utf-8-sig')
    del _total_df_sector
    print('inset to bigquery')

    insert_start = datetime.now()
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    print(f'insert_into_{table_id}')
    table = client.get_table(table_id)
    client.load_table_from_dataframe(total_df, table)
    insert_end = datetime.now()
    print(insert_end-insert_start)

    program_end = datetime.now()
    print('**** END *****')
    print('program running time:', program_end - program_start)



