
import os
import pandas as pd
import psycopg2
import tempfile
from test_omega.config import Config

"""Модуль, который инициализирует класс AddingDataPsycopg,
предназначен для реализации процедуры выгрузки для нового набора данных"""


class AddingDataPsycopg:
    def __init__(self):
        attributes = Config(os.path.join('.', 'test_omega/config_to_bd.yml')).get_config('connection_from')
        self.conn = psycopg2.connect(f"""
        host={attributes['host_from']}
        port={int(attributes['port_from'])}
        sslmode={attributes['ssl_mode_from']}
        dbname={attributes['database_from']}
        user={attributes['user_from']}
        password={attributes['password_from']}
        target_session_attrs=read-write
        """)
        with self.conn.cursor() as cursor:
            cursor.execute('SELECT current_database ();')
            print(f'Вы подключены к базе: {cursor.fetchone()[0]}')

            
    def get_table_as_сsv_transactions(self, date_from: str, date_to: str) -> pd.DataFrame:
        """Функция, которая выгружает данные из таблицы CsvTransactions"""
        with tempfile.TemporaryFile() as tmpfile:
            copy_sql = f"""COPY (SELECT 
                                        DesmanOperationId AS 'Id',
                                        DATE(DocumentCreateDate) AS 'Datetime',
                                        RouteCode,
                                        VehicleNumber,
                                        CardType,
                                        TripNumber
                                 FROM CsvTransactions
                                 WHERE CardType NOT IN (355, 55) AND
                                 ('Datetime' BETWEEN {date_from} AND {date_to})) TO STDOUT WITH CSV HEADER"""
            with self.conn.cursor() as cursor:
                cursor.copy_expert(copy_sql, tmpfile)
            tmpfile.seek(0)
            df = pd.read_csv(tmpfile)
        return df
    
    
    def get_table_as_events(self, date_from: str, date_to: str) -> pd.DataFrame:
        """Функция, которая выгружает данные из таблицы Events"""
        with tempfile.TemporaryFile() as tmpfile:
            copy_sql = f"""COPY (SELECT 
                                       Id,
                                       Datetime,
                                       VehicleNumber
                                FROM Events
                                WHERE EventId NOT IN (555, 304) AND
                                (Datetime BETWEEN {date_from} AND {date_to})) TO STDOUT WITH CSV HEADER"""
            with self.conn.cursor() as cursor:
                cursor.copy_expert(copy_sql, tmpfile)
            tmpfile.seek(0)
            df = pd.read_csv(tmpfile)
        return df
    
   

#
