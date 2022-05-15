import pandas as pd

"""Модуль предназначен для реализации процедуры обработки
нового набора данных для формирования отчетов"""

def prepare_events(df):
    """Функция для данных в таблице Events"""
    df['payment_type'] = 'Наличная оплата'
    df['RouteCode'] = 'Нет данных'
    df['TripNumber'] = 'Нет данных'
    return df

def prepare_сsv_transactions(df):
    """Функция для данных в таблице CsvTransactions"""
    cashless_codes = [71, 350, 353, 354]
    df['payment_type'] = None
    df.loc[(df.CardType.isin(cashless_codes)), 'payment_type'] = 'Банковская карта'
    df.loc[(~df.CardType.isin(cashless_codes)), 'payment_type'] = 'Проездной билет'
    return df

def concat_df(df_1, df_2):
    """Функция для объединения таблиц Events и CsvTransactions"""
    new_df = pd.concat([prepare_сsv_transactions(df_1), prepare_events(df_2)], ignore_index=True)
    new_df = new_df[['Id', 'Datetime', 'VehicleNumber', 'payment_type', 'RouteCode', 'TripNumber']]
    new_df.rename(columns={'VehicleNumber': 'ТС', 'payment_type': 'Вид оплаты',
                           'TripNumber': 'Номер рейса', 'RouteCode': 'Маршрут',
                           'Id': 'Количество транзакций', 'Datetime': 'Дата'}, inplace=True)
    return new_df

def report_1(df):
    """Функция для отчета по общему количеству транзакций
    в разрезе дата, маршрут, транспортное средство и вид оплаты проезда"""
    df = df.groupby(['Дата','Маршрут', 'ТС', 'Вид оплаты'])['Количество транзакций'].count().reset_index()
    return df

def report_2(df):
    """Функция для отчета по общему количеству транзакций
    в разрезе дата, маршрут, транспортное средство, номер рейса"""
    df = df.groupby(['Дата','Маршрут', 'ТС', 'Номер рейса'])['Количество транзакций'].count().reset_index()
    return df

def report_3(df):
    """Функция для отчета по маршрутам, содержащий информацию
    по количеству транзакций за наличный расчет, оплаченные банковской картой и проездными билетами"""
    df = df.join(df['Вид оплаты'].str.get_dummies())
    df = df[['Наличная оплата', 'Банковская карта',
             'Проездной билет','Дата','Маршрут']].groupby(['Дата','Маршрут']).sum().reset_index()
    return df
#
