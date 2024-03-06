import asyncio
import aiohttp
from pprint import pprint
import time
from datetime import datetime
import schedule
import platform
from gspread import Cell
from common import api_decorator, tz, gc
from acc_secret_info import accounts
if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# данные для подключения к таблице
sheet_url = 'https://docs.google.com/spreadsheets/d/148B3MkcYPsDml1t4U94AitewazaIAnrC-yZK6mOtLhU/edit#gid=1414102686'
spreadsheet = gc.open_by_url(sheet_url)
# названия листов
hour_country = 'Hour country'
hour_lang = 'Hour language'
hour_yandex = 'Hour yandex'
day_country = 'Day country'
day_lang = 'Day language'
day_yandex = 'Day yandex'
main_page = 'Сводная'

# аргументы запросов
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


# вставить строку с числами в начале таблицы
@api_decorator
def insert_data_row(data: list, page: str, row: int):
    # приготовить список с объектами ячеек
    cell_list = []
    for i, val in enumerate(data, start=1):
        cell_list.append(Cell(row=row, col=i, value=val))

    # обновить все разом
    spreadsheet.worksheet(page).update_cells(cell_list)
    print('обновлено ячеек:', len(data))


# вставить в сводную две колонки, сортированные по числу либо имени
@api_decorator
def insert_two_cols(data: dict, by: str, start_row: int, start_col: int):
    data_tuples = [(k, data[k]) for k in data]
    if by == 'name':
        sorted_data = sorted(data_tuples, key=lambda x: x[0], reverse=False)
    elif by == 'amount':
        sorted_data = sorted(data_tuples, key=lambda x: x[1], reverse=True)

    # приготовить список с объектами ячеек
    cell_list = []
    for i, (key, val) in enumerate(sorted_data, start=1):
        cell_list.append(Cell(row=start_row+i, col=start_col, value=key))  # страна/язык
        cell_list.append(Cell(row=start_row+i, col=start_col+1, value=val))  # число аудитории

    # обновить все разом
    spreadsheet.worksheet(main_page).update_cells(cell_list)
    print('обновлено ячеек:', len(data)*2)


@api_decorator
def read_range(from_page: str, coord: tuple):
    data = spreadsheet.worksheet(from_page).range(*coord)
    print('прочитано ячеек', len(data))
    return data


async def ping_auditory(by: str, value: str, session, site: str) -> int:
    if 'yandex' == site:
        token = accounts['Yandex']['token']
        base_url = 'https://tasks.yandex.ru'
        project = 1482
    else:
        base_url = 'https://platform.toloka.ai'
        token = accounts['td.pro']['token']
        project = 150010

    filter_by = {
        # "region_by_phone": [{'category': "computed", 'key': "region_by_phone", 'operator': "IN", 'value': value}],
        "languages": [{"category": "profile", "key": "languages", "operator": "IN", "value": value}, ],
        "skill": [{"category": "skill", "key": lang_skills.get(value), "operator": "EQ", "value": 100}],
        "country": [{"category": "profile", "key": "country", "operator": "EQ", "value": value}],
    }
    headers = {"Authorization": "OAuth %s" % token, "Content-Type": "application/JSON"}
    payload = {"projectId": project, "adultContent": False, "filter": {"or": filter_by[by]}}

    # запрос
    try:
        async with session.post(f'{base_url}/api/adjuster/adjustments/', headers=headers, json=payload) as response:
            # print('response', value, payload)
            if response.status == 200:
                data = await response.json()
                return int(data['parameters']['value'])
            # если ответ плохой
            else:
                print(value, 'error:', response.status)
                return 0
    except Exception as e:
        print(value, 'error:', e)
        return -1


async def auditory_update(by: str, field: list, site: str) -> list:
    # сгенерировать пустой словарь длиной в число колонок
    field = {i: 0 for i in field}

    # послать несколько одинаковых асинхронных серий запросов
    for req in range(1, 5):
        if req != 1:  # задержка между сериями
            await asyncio.sleep(5)

        print(req, 'check by', by)
        async with aiohttp.ClientSession() as session:
            tasks = [ping_auditory(by=by, value=i, session=session, site=site) for i in field]
            results = await asyncio.gather(*tasks)
            print('mid results:', results)

            # сохранить значение на каждую колонку
            for item, result in zip(field.keys(), results):
                # взять макс из двух - нового и старого значения (на случай ошибок 500, когда в ответе придет 0)
                field[item] = max(result, field[item])

    print('final results:', field)
    print()
    return list(field.values())


# замер аудитории каждый час
def hour_update():
    now = str(datetime.now(tz).strftime("%d/%m, %H:%M"))

    # страны toloka
    data = asyncio.run(auditory_update(field=countries, by='country', site='toloka'))
    data = [now] + data
    insert_empty_row(page=hour_country)
    insert_data_row(data=data, row=2, page=hour_country)

    # языки toloka
    data1 = asyncio.run(auditory_update(field=list(lang_skills.keys()), by="skill", site='toloka'))  # подтвержденные тестом
    data2 = asyncio.run(auditory_update(field=languages, by="languages", site='toloka'))  # все
    data = [now] + data1 + data2
    insert_empty_row(page=hour_lang)
    insert_data_row(data=data, row=2, page=hour_lang)

    # языки yandex
    data = asyncio.run(auditory_update(field=languages, by="languages", site='yandex'))  # все
    data = [now] + data
    insert_empty_row(page=hour_yandex)
    insert_data_row(data=data, row=2, page=hour_yandex)


# прочитать последние 24 ряда в каждой колонке и вставить максимальное значение в дневной лист
def daily_max(col_amount: int, from_page: str, to_page: str) -> dict:
    print(f'daily_max from_page: {from_page}, to_page: {to_page}')
    today = str(datetime.now(tz).date())

    # весь лист за посл 24 часа
    last_24h_data = read_range(from_page=from_page, coord=(1, 2, 25, col_amount+1))

    # сгенерировать пустой словарь длиной в число стран или языков
    field = {item.value: 0 for item in last_24h_data[0:col_amount]}

    for i, col in enumerate(field):
        try:
            # 24 верхние ячейки одной колонки
            col_today = [last_24h_data[c] for c in range(i, len(last_24h_data), col_amount)]

            # макс из этих 24 шт
            max_today = max([int(cell.value) if cell.value.isnumeric() else 0 for cell in col_today])
            field[col] = max_today
        except Exception as e:
            print('error', e)
            # если в этой колонке нет 24 заполненных ячеек
            continue

    # вставить ряд
    insert_empty_row(page=to_page)
    insert_data_row(data=[today]+list(field.values()), row=2, page=to_page)

    return field


# средний максимум за последнюю неделю
def week_avg(col_amount: int, from_page: str):
    print(f'week_avg from_page: {from_page}')

    # весь лист за посл 7 суток
    last_7d_data = read_range(from_page=from_page, coord=(1, 2, 8, col_amount+1))

    # сгенерировать пустой словарь длиной в число стран или языков
    field = {item.value: 0 for item in last_7d_data[0:col_amount]}

    for i, col in enumerate(field):
        try:
            # 7 верхних ячеек одной колонки
            col_today = [last_7d_data[c] for c in range(i, len(last_7d_data), col_amount)]

            # среднее из этих 7 шт
            avg = int(sum([int(cell.value) if cell.value.isnumeric() else 0 for cell in col_today])/7)
            field[col] = avg
        except Exception as e:
            print('error', e)
            # если в этой колонке нет 7 заполненных ячеек
            continue
    return field


# прочитать макс на каждую страну за день, внести это новой строкой в Day и обновить этими данными сводную
def day_update():
    # толока страны
    daily_max(col_amount=len(countries), from_page=hour_country, to_page=day_country)
    week_avg_data = week_avg(col_amount=len(countries), from_page=day_country)
    insert_two_cols(data=week_avg_data, start_col=1, start_row=3, by='name')
    insert_two_cols(data=week_avg_data, start_col=3, start_row=3, by='amount')

    # толока языки
    daily_max(col_amount=len(lang_skills) + len(languages), from_page=hour_lang, to_page=day_lang)
    week_avg_data = week_avg(col_amount=len(lang_skills) + len(languages), from_page=day_lang)
    skilled, unskilled = {}, {}
    for lang in week_avg_data:
        if len(lang) == 2:
            skilled[lang] = week_avg_data[lang]
        else:
            unskilled[lang] = week_avg_data[lang]

    insert_two_cols(data=unskilled, start_col=6, start_row=3, by='name')    # не подтвержденные
    insert_two_cols(data=unskilled, start_col=8, start_row=3, by='amount')  # не подтвержденные
    insert_two_cols(data=skilled, start_col=11, start_row=3, by='amount')    # подтвержденные

    # яндекс языки
    daily_max(col_amount=len(languages), from_page=hour_yandex, to_page=day_yandex)
    week_avg_data = week_avg(col_amount=len(languages), from_page=day_yandex)
    insert_two_cols(data=week_avg_data, start_col=14, start_row=3, by='name')
    insert_two_cols(data=week_avg_data, start_col=16, start_row=3, by='amount')


if __name__ == '__main__':
    day_update()
    pass
    # hour_update()
