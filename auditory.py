import asyncio
import aiohttp
from pprint import pprint
import time
from datetime import datetime
import schedule
import platform
from gspread import Cell

from common import *
from acc_secret_info import accounts
if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# толока
token = accounts['td.pro']['token']
# token = accounts['Yandex']['token']
HEADERS = {"Authorization": "OAuth %s" % token, "Content-Type": "application/JSON"}

# данные для подключения к таблице
sheet_url = 'https://docs.google.com/spreadsheets/d/148B3MkcYPsDml1t4U94AitewazaIAnrC-yZK6mOtLhU/edit#gid=1414102686'
spreadsheet = gc.open_by_url(sheet_url)
hour_country = 'Hour country'
hour_lang = 'Hour language'
day_country = 'Day country'
day_lang = 'Day language'

# аргументы запросов
# countries = ['RU', 'KE', 'PK', 'NG', 'BR', 'IN', 'TR', 'UA', 'ET', 'VN', 'BY', 'KZ', 'PH', 'ID', 'MA', 'VE', 'UZ', 'CO', 'MX', 'US', 'FR', 'ZA', 'DZ', 'LK', 'EG', 'AR', 'MG', 'BD', 'PE', 'CI', 'GH', 'CN', 'TN', 'MZ', 'CM', 'AE', 'MD', 'ES', 'EC', 'PL', 'ZM', 'SA', 'DO', 'MM', 'MY', 'PT', 'BF', 'AM', 'SN', 'DK']
countries = ['RU', 'KE', 'PK', 'NG', 'BR', 'IN', 'TR', 'UA', 'ET', 'VN', 'BY', 'KZ', 'PH', 'ID', 'MA', 'VE', 'UZ', 'CO', 'MX', 'US', 'FR', 'ZA', 'DZ', 'LK', 'EG', 'AR', 'MG', 'BD', 'PE', 'CI', 'GH', 'CN', 'TN', 'MZ', 'CM', 'AE', 'MD', 'ES', 'EC', 'PL', 'ZM', 'SA', 'DO', 'MM', 'MY', 'PT', 'BF', 'AM', 'SN', 'DK', 'JO', 'AZ', 'CD', 'KG', 'TG', 'GB', 'DE', 'RO', 'RS', 'CA', 'CL', 'SV', 'IT', 'JM', 'TJ', 'UG', 'GE', 'GT', 'ZW', 'BJ', 'CR', 'BG', 'HT', 'BO', 'BW', 'MW', 'ML', 'NP', 'NL', 'PA', 'AU', 'JP', 'TZ', 'UY', 'AO', 'CG', 'FI', 'GA', 'GR', 'IQ', 'LV', 'LT', 'NI', 'SE', 'AT', 'MK', 'TH', 'BA', 'GN', 'HU', 'IL', 'RW', 'SL', 'TT', 'TM', 'YE', 'AL', 'BH', 'BE', 'TD', 'HN', 'IE', 'LB', 'PS', 'KR', 'TW', 'KH', 'HR', 'PF', 'IR', 'MU', 'NE', 'OM', 'PY', 'QA', 'BT', 'BN', 'BI', 'CZ', 'DJ', 'EE', 'KW', 'LS', 'LY', 'MQ', 'NA', 'PR', 'VC', 'SK', 'SZ', 'SY', 'AD', 'CU', 'CY', 'GF', 'MR', 'NZ', 'NO', 'SG', 'SR']
lang_skills = {
    "EN": "26366",
    "RU": "26296",
    "FR": "26711",
    "DE": "26377",
    "ES": "32346",
    "AR": "30724",
    "UK": "48836",
    "PT": "26714",
    "JA": "26513",
    "ID": "39821", }
languages = ['EN', 'RU', 'ES', 'DE', 'FR', 'PT', 'JA', 'AR', 'UK', 'TR', 'IT', 'NL', 'PL', 'ID', 'SW', 'SV', 'ZH', 'HI', 'BE', 'UR', 'KK', 'TL', 'TT', 'UZ', 'KI', 'BN', 'KO', 'VI', 'YO', 'AM', 'AZ', 'IG', 'EL', 'HE', 'BA', 'RO', 'TA', 'MS', 'ZU', 'LA', 'MR', 'SR', 'CS', 'XH', 'AF', 'KA', 'KY', 'ML', 'SI', 'BG', 'JV', 'HA', 'PA', 'CE', 'CV', 'TG', 'EO', 'ST', 'TE', 'TK', 'HY', 'HR', 'FI', 'GU', 'HU', 'PS', 'KN', 'WO', 'DA', 'OM', 'TH', 'AK', 'BS', 'CA', 'ET', 'GA', 'NO', 'SK', 'SU', 'AB', 'MY', 'NY', 'CU', 'FF', 'LG', 'LV', 'LN', 'OR', 'FA', 'TS', 'AS', 'AV', 'BM', 'HT', 'IS', 'IE', 'KU', 'LT', 'NE', 'OS']

# вставить пустую строку в начале таблицы
@api_decorator
def insert_empty_row(page: str):
    # отправить insertDimension запрос
    body = {"requests": [{"insertDimension": {"range": {
        "sheetId": spreadsheet.worksheet(page).id,
        "dimension": "ROWS",
        "startIndex": 1,
        "endIndex": 2}}}, ], }
    spreadsheet.batch_update(body)
    print('Вставлена пустая строка')


@api_decorator
def insert_data_row(data: list, page: str, row: int):
    # приготовить список с объектами ячеек
    cell_list = []
    for i, val in enumerate(data, start=1):
        cell_list.append(Cell(row=row, col=i, value=val))

    # обновить все разом
    spreadsheet.worksheet(page).update_cells(cell_list)
    print('обновлено ячеек:', len(data))


async def ping_auditory(by: str, value: str, session) -> int:
    filter_by = {
        # "region_by_phone": [{'category': "computed", 'key': "region_by_phone", 'operator': "IN", 'value': value}],
        "languages": [{"category": "profile", "key": "languages", "operator": "IN", "value": value}, ],
        "skill": [{"category": "skill", "key": lang_skills.get(value), "operator": "EQ", "value": 100}],
        "country": [{"category": "profile", "key": "country", "operator": "EQ", "value": value}],
    }
    # base_url = 'https://tasks.yandex.ru'
    base_url = 'https://platform.toloka.ai'
    project = 1482 if 'yandex' in base_url else 150010
    payload = {"projectId": project, "adultContent": False, "filter": {"or": filter_by[by]}}

    # запрос
    async with session.post(f'{base_url}/api/adjuster/adjustments/', headers=HEADERS, json=payload) as response:
        # print('response', value, payload)
        if response.status == 200:
            data = await response.json()
            return int(data['parameters']['value'])
        # если ответ плохой
        else:
            print(f'{response.status} error for {value}')
            return 0


async def auditory_update(by: str, field: list) -> list:
    # сгенерировать пустой словарь длиной в число колонок
    field = {i: 0 for i in field}

    # послать 3 одинаковые асинхронные серии запросов
    for req in range(1, 4):
        if req != 1:  # задержка между сериями
            await asyncio.sleep(10)

        print(req, 'check by', by)
        async with aiohttp.ClientSession() as session:
            tasks = [ping_auditory(by=by, value=i, session=session) for i in field]
            results = await asyncio.gather(*tasks)
            print('mid results:', results)

            # сохранить значение на каждую колонку
            for item, result in zip(field.keys(), results):
                # взять макс из двух - нового и старого значения (на случай ошибок 500, когда в ответе придет 0)
                field[item] = max(result, field[item])

    print('final results:', field)
    print()
    return list(field.values())


def hour_update():
    now = str(datetime.now(tz).strftime("%d/%m, %H:%M"))

    # страны
    data = asyncio.run(auditory_update(field=countries, by='country'))
    data = [now] + data
    insert_empty_row(page=hour_country)
    insert_data_row(data=data, row=2, page=hour_country)

    # языки
    data1 = asyncio.run(auditory_update(field=list(lang_skills.keys()), by="skill"))  # подтвержденные тестом
    data2 = asyncio.run(auditory_update(field=languages, by="languages"))  # все
    data = [now] + data1 + data2
    insert_empty_row(page=hour_lang)
    insert_data_row(data=data, row=2, page=hour_lang)


def daily_max(col_amount: int, from_page: str, to_page: str):
    print(f'daily_max from_page: {from_page}, to_page: {to_page}')
    today = str(datetime.now(tz).date())
    # сгенерировать пустой словарь длиной в число стран или языков
    field = {i: 0 for i in range(col_amount)}

    # весь лист за посл 24 часа
    while 1:
        try:
            last_24h_data = spreadsheet.worksheet(from_page).range(2, 2, 25, col_amount + 1)
            break
        except gspread.exceptions.APIError as e:
            print('ОШИБКА daily_max', e)
            time.sleep(5)
    print('прочитано ячеек', len(last_24h_data))

    for i, col in enumerate(field):
        # 24 верхние ячейки одной страны
        col_today = [last_24h_data[c] for c in range(i, len(last_24h_data), col_amount)]

        # макс из этих 24 шт
        max_today = max([int(cell.value) for cell in col_today])
        field[col] = max_today

    # вставить ряд
    data = [today] + list(field.values())
    insert_empty_row(page=to_page)
    insert_data_row(data=data, row=2, page=to_page)
    print(data)


def day_update():
    daily_max(col_amount=len(countries), from_page=hour_country, to_page=day_country)
    daily_max(col_amount=len(lang_skills) + len(languages), from_page=hour_lang, to_page=day_lang)


if __name__ == '__main__':
    hour_update()
    day_update()

    schedule.every().hour.at(':00').do(hour_update)
    schedule.every().day.at('23:30').do(daily_max)
    print(schedule.get_jobs())

    # поллинг каждые n секунд
    n = 20
    while 1:
        # проверить, не настало ли время
        schedule.run_pending()
        time.sleep(n)
