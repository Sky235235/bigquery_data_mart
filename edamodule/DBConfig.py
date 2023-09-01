import pymysql

class DBConfig:
    def __init__(self, data):

        self.data = data

    def ServiceDev(self):

        access_data = self.data[self.data['DB_NAME'] == 'Service_dev'].reset_index(drop=True)

        ## 서버접근
        # host = list(access_data['Server_IP'])[0]
        # port = list(access_data['Server_Port'])[0]
        ## pc 접근
        host = list(access_data['Local_IP'])[0]
        port = list(access_data['Local_Port'])[0]

        database = 'im_mobility'
        username = list(access_data['ID'])[0]
        password = list(access_data['PW'])[0]

        conn = pymysql.connect(host=host, user=username, db=database, port=port, password=password)
        curs = conn.cursor(pymysql.cursors.DictCursor)

        return conn, curs

    def ServiceStage(self):
        access_data = self.data[self.data['DB_NAME'] == 'Service_stage'].reset_index(drop=True)

        ## 서버접근
        # host = list(access_data['Server_IP'])[0]
        # port = list(access_data['Server_Port'])[0]
        ## pc 접근
        host = list(access_data['Local_IP'])[0]
        port = list(access_data['Local_Port'])[0]

        database = 'im_mobility'
        username = list(access_data['ID'])[0]
        password = list(access_data['PW'])[0]

        conn = pymysql.connect(host=host, user=username, db=database, port=port, password=password)
        curs = conn.cursor(pymysql.cursors.DictCursor)

        return conn, curs

    def ServiceRO(self):
        access_data = self.data[self.data['DB_NAME'] == 'Service_live_ro'].reset_index(drop=True)

        ## 서버접근
        # host = list(access_data['Server_IP'])[0]
        # port = list(access_data['Server_Port'])[0]
        ## pc 접근
        host = list(access_data['Local_IP'])[0]
        port = list(access_data['Local_Port'])[0]

        database = 'im_mobility'
        username = list(access_data['ID'])[0]
        password = list(access_data['PW'])[0]

        conn = pymysql.connect(host=host, user=username, db=database, port=port, password=password)
        curs = conn.cursor(pymysql.cursors.DictCursor)

        return conn, curs

    def ServiceLive(self):
        access_data = self.data[self.data['DB_NAME'] == 'Service_live'].reset_index(drop=True)

        ## 서버접근
        host = list(access_data['Server_IP'])[0]
        port = list(access_data['Server_Port'])[0]
        ## pc 접근 없음

        database = 'im_mobility'
        username = list(access_data['ID'])[0]
        password = list(access_data['PW'])[0]

        conn = pymysql.connect(host=host, user=username, db=database, port=port, password=password)
        curs = conn.cursor(pymysql.cursors.DictCursor)

        return conn, curs

    def CarLogNew(self):
        access_data = self.data[self.data['DB_NAME'] == 'Car_log'].reset_index(drop=True)

        ## 서버접근
        # host = list(access_data['Server_IP'])[0]
        # port = list(access_data['Server_Port'])[0]
        ## pc 접근
        host = list(access_data['Local_IP'])[0]
        port = list(access_data['Local_Port'])[0]

        database = 'carlog'
        username = list(access_data['ID'])[0]
        password = list(access_data['PW'])[0]

        conn = pymysql.connect(host=host, user=username, db=database, port=port, password=password)
        curs = conn.cursor(pymysql.cursors.DictCursor)

        return conn, curs

