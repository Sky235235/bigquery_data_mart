import pandas as pd
from notion_client import Client

def get_dbconfig_info():
    # Notion API 키 입력
    notion = Client(auth="**")
    # 데이터베이스 ID 입력
    database_id = "**"

    # 데이터베이스 정보 가져오기
    results = notion.databases.query(
        database_id=database_id,
        sorts=[
            {
                "property": "DB_Name",
                "direction": "ascending"
            }
        ]
    )

    DB_NAME = []
    ID = []
    PW = []
    Local_IP = []
    Local_Port = []
    Server_IP = []
    Server_Port = []

    for i in range(len(results['results'])):
        _db_name = results['results'][i]['properties']['DB_Name']['title'][0]['text']['content']
        _id = results['results'][i]['properties']['ID']['rich_text'][0]['text']['content']
        _pw = results['results'][i]['properties']['PW']['rich_text'][0]['text']['content']
        _local_ip = results['results'][i]['properties']['Local IP']['rich_text'][0]['text']['content']
        _local_port = int(results['results'][i]['properties']['Local Port']['rich_text'][0]['text']['content'])
        _server_ip = results['results'][i]['properties']['Server IP']['rich_text'][0]['text']['content']
        _server_port = int(results['results'][i]['properties']['Server Port']['rich_text'][0]['text']['content'])

        DB_NAME.append(_db_name)
        ID.append(_id)
        PW.append(_pw)
        Local_IP.append(_local_ip)
        Local_Port.append(_local_port)
        Server_IP.append(_server_ip)
        Server_Port.append(_server_port)

    db_data = pd.DataFrame({'DB_NAME': DB_NAME,
                  'ID': ID,
                  'PW' : PW,
                  'Local_IP' : Local_IP,
                  'Local_Port': Local_Port,
                  'Server_IP': Server_IP,
                  'Server_Port': Server_Port})

    return db_data