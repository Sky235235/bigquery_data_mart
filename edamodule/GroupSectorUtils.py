import pandas as pd
from tqdm import tqdm
from pyproj import Transformer

class GetCoordinate:
    def __init__(self):
        self.wgs84 = {'proj': 'latlong', 'datum': 'WGS84', 'ellps': 'WGS84'}
        self.tm128 = {'proj': 'tmerc', 'lat_0': '38N', 'lon_0': '128E', 'ellps': 'bessel',
                 'x_0': '400000', 'y_0': '600000', 'k': '0.9999', 'towgs84': '-115.80,474.99,674.11,1.16,-2.31,-1.63,6.43'}

    def wgstokatec(self, lng, lat):
        '''lng -> List, lat -> lst'''
        _WGS84 = self.wgs84
        _TM128 = self.tm128
        _transformer = Transformer.from_crs((_WGS84), (_TM128))
        converted = _transformer.transform(lng, lat)
        return converted

    def katectowgs(self, xpos, ypos):
        ''' xpos ->list , ypos->list'''
        _WGS84 = self.wgs84
        _TM128 = self.tm128
        _transformer = Transformer.from_crs((_TM128),(_WGS84))
        converted = _transformer.transform(xpos, ypos)
        return converted

class GetSector:

    def __init__(self, sector_table_df):
        self.sector_table_df = sector_table_df
        self.sector_info_tables = {}
        self.sector_table_list = list(sector_table_df['table_name'])

        for _sector_table in self.sector_table_list:
            _sector_info_df = self.sector_table_df[self.sector_table_df['table_name'] == _sector_table].reset_index(drop=True)
            _left_xpos = _sector_info_df['left_xpos'][0]
            _right_xpos = _sector_info_df['right_xpos'][0]
            _top_ypos = _sector_info_df['top_ypos'][0]
            _bottom_ypos = _sector_info_df['bottom_ypos'][0]
            self.sector_info_tables[_sector_table] = (_left_xpos, _right_xpos, _top_ypos, _bottom_ypos)

    def get_sector_info_table(self, xpos, ypos):
        for sector_table, sector_info in self.sector_info_tables.items():
            _left_xpos, _right_xpos, _top_ypos, _bottom_ypos = sector_info

            if (_left_xpos <= xpos <= _right_xpos) and (_bottom_ypos <= ypos <= _top_ypos):
                return sector_table

        return 'out_of_sector'

    def get_sector(self, sector_table, df, xpos, ypos):
        _sector_info_df = self.sector_table_df[self.sector_table_df['table_name'] == sector_table].reset_index(drop=True)
        _standard_xpos = 0
        _standard_ypos = 0

        if sector_table in ['sector_info_1', 'sector_info_4']:
           _standard_xpos = _sector_info_df['right_xpos'][0]
           _standard_ypos = _sector_info_df['bottom_ypos'][0]

        elif sector_table in ['sector_info_2', 'sector_info_3', 'sector_info_5', 'sector_info_6']:
            _standard_xpos = _sector_info_df['left_xpos'][0]
            _standard_ypos = _sector_info_df['bottom_ypos'][0]

        elif sector_table == 'sector_info_7':
            _standard_xpos = _sector_info_df['right_xpos'][0]
            _standard_ypos = _sector_info_df['top_ypos'][0]

        elif sector_table in ['sector_info_8', 'sector_info_9']:
            _standard_xpos = _sector_info_df['left_xpos'][0]
            _standard_ypos = _sector_info_df['top_ypos'][0]

        _xpos_list = (abs(df[xpos] - _standard_xpos) // 1000) * 100
        _ypos_list = abs(df[ypos] - _standard_ypos) // 1000

        _sector_list = []
        for x, y in zip(_xpos_list, _ypos_list):
            define_sector = x+y
            _sector_list.append(define_sector)

        sector_list = _sector_list
        return sector_list







