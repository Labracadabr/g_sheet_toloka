print('launching')
import asyncio
import aiohttp
import pytz
from pprint import pprint
import gspread
import time
from datetime import datetime
import schedule
import platform
from gspread import Cell
from acc_secret_info import accounts
if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# толока
token = accounts['td.pro']['token']
# token = accounts['Yandex']['token']
HEADERS = {"Authorization": "OAuth %s" % token, "Content-Type": "application/JSON"}

# текст обновления в gmt+3
tz = pytz.timezone("Etc/GMT-3")

# данные для подключения к таблице
service_file = 'token.json'
gc = gspread.service_account(filename=service_file)
sheet_url = 'https://docs.google.com/spreadsheets/d/148B3MkcYPsDml1t4U94AitewazaIAnrC-yZK6mOtLhU/edit#gid=1414102686'
spreadsheet = gc.open_by_url(sheet_url)
hour_country = 'Hour country'
hour_lang = 'Hour language'
day_country = 'Day country'
day_lang = 'Day language'

# аргументы запросов
countries = {'RU': 0, 'KE': 0, 'PK': 0, 'NG': 0, 'BR': 0, 'IN': 0, 'TR': 0, 'UA': 0, 'ET': 0, 'VN': 0, 'BY': 0,
             'KZ': 0, 'PH': 0, 'ID': 0, 'MA': 0, 'VE': 0, 'UZ': 0, 'CO': 0, 'MX': 0, 'US': 0, 'FR': 0, 'ZA': 0,
             'DZ': 0, 'LK': 0, 'EG': 0, 'AR': 0, 'MG': 0, 'BD': 0, 'PE': 0, 'CI': 0, 'GH': 0, 'CN': 0, 'TN': 0,
             'MZ': 0, 'CM': 0, 'AE': 0, 'MD': 0, 'ES': 0, 'EC': 0, 'PL': 0, 'ZM': 0, 'SA': 0, 'DO': 0, 'MM': 0,
             'MY': 0, 'PT': 0, 'BF': 0, 'AM': 0, 'SN': 0, 'DK': 0}
# countries = {'RU': 0, 'KE': 0, 'PK': 0, 'NG': 0, 'BR': 0, 'IN': 0, 'TR': 0, 'UA': 0, 'ET': 0, 'VN': 0, 'BY': 0, 'KZ': 0,
#              'PH': 0, 'ID': 0, 'MA': 0, 'VE': 0, 'UZ': 0, 'CO': 0, 'MX': 0, 'US': 0, 'FR': 0, 'ZA': 0, 'DZ': 0, 'LK': 0,
#              'EG': 0, 'AR': 0, 'MG': 0, 'BD': 0, 'PE': 0, 'CI': 0, 'GH': 0, 'CN': 0, 'TN': 0, 'MZ': 0, 'CM': 0, 'AE': 0,
#              'MD': 0, 'ES': 0, 'EC': 0, 'PL': 0, 'ZM': 0, 'SA': 0, 'DO': 0, 'MM': 0, 'MY': 0, 'PT': 0, 'BF': 0, 'AM': 0,
#              'SN': 0, 'DK': 0, 'JO': 0, 'AZ': 0, 'CD': 0, 'KG': 0, 'TG': 0, 'GB': 0, 'DE': 0, 'RO': 0, 'RS': 0, 'CA': 0,
#              'CL': 0, 'SV': 0, 'IT': 0, 'JM': 0, 'TJ': 0, 'UG': 0, 'GE': 0, 'GT': 0, 'ZW': 0, 'BJ': 0, 'CR': 0, 'BG': 0,
#              'HT': 0, 'BO': 0, 'BW': 0, 'MW': 0, 'ML': 0, 'NP': 0, 'NL': 0, 'PA': 0, 'AU': 0, 'JP': 0, 'TZ': 0, 'UY': 0,
#              'AO': 0, 'CG': 0, 'FI': 0, 'GA': 0, 'GR': 0, 'IQ': 0, 'LV': 0, 'LT': 0, 'NI': 0, 'SE': 0, 'AT': 0, 'MK': 0,
#              'TH': 0, 'BA': 0, 'GN': 0, 'HU': 0, 'IL': 0, 'RW': 0, 'SL': 0, 'TT': 0, 'TM': 0, 'YE': 0, 'AL': 0, 'BH': 0,
#              'BE': 0, 'TD': 0, 'HN': 0, 'IE': 0, 'LB': 0, 'PS': 0, 'KR': 0, 'TW': 0, 'KH': 0, 'HR': 0, 'PF': 0, 'IR': 0,
#              'MU': 0, 'NE': 0, 'OM': 0, 'PY': 0, 'QA': 0, 'BT': 0, 'BN': 0, 'BI': 0, 'CZ': 0, 'DJ': 0, 'EE': 0, 'KW': 0,
#              'LS': 0, 'LY': 0, 'MQ': 0, 'NA': 0, 'PR': 0, 'VC': 0, 'SK': 0, 'SZ': 0, 'SY': 0, 'AD': 0, 'CU': 0, 'CY': 0,
#              'GF': 0, 'MR': 0, 'NZ': 0, 'NO': 0, 'SG': 0, 'SR': 0
#              }
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
languages = {
    'EN': 0, 'RU': 0, 'ES': 0, 'DE': 0, 'FR': 0, 'PT': 0, 'JA': 0, 'AR': 0, 'UK': 0, 'TR': 0, 'IT': 0, 'NL': 0,
    'PL': 0, 'ID': 0, 'SW': 0, 'SV': 0, 'ZH': 0, 'HI': 0, 'BE': 0, 'UR': 0, 'KK': 0, 'TL': 0, 'TT': 0, 'UZ': 0,
    'KI': 0, 'BN': 0, 'KO': 0, 'VI': 0, 'YO': 0, 'AM': 0, 'AZ': 0, 'IG': 0, 'EL': 0, 'HE': 0, 'BA': 0, 'RO': 0,
    'TA': 0, 'MS': 0, 'ZU': 0, 'LA': 0, 'MR': 0, 'SR': 0, 'CS': 0, 'XH': 0, 'AF': 0, 'KA': 0, 'KY': 0, 'ML': 0,
    'SI': 0, 'BG': 0, 'JV': 0, 'HA': 0, 'PA': 0, 'CE': 0, 'CV': 0, 'TG': 0, 'EO': 0, 'ST': 0, 'TE': 0, 'TK': 0,
    'HY': 0, 'HR': 0, 'FI': 0, 'GU': 0, 'HU': 0, 'PS': 0, 'KN': 0, 'WO': 0, 'DA': 0, 'OM': 0, 'TH': 0, 'AK': 0,
    'BS': 0, 'CA': 0, 'ET': 0, 'GA': 0, 'NO': 0, 'SK': 0, 'SU': 0, 'AB': 0, 'MY': 0, 'NY': 0, 'CU': 0, 'FF': 0,
    'LG': 0, 'LV': 0, 'LN': 0, 'OR': 0, 'FA': 0, 'TS': 0, 'AS': 0, 'AV': 0, 'BM': 0, 'HT': 0, 'IS': 0, 'IE': 0,
    'KU': 0, 'LT': 0, 'NE': 0, 'OS': 0
}

# вставить пустую строку в начале таблицы
def insert_empty_row(page: str):
    # отправить insertDimension запрос
    body = {"requests": [{"insertDimension": {"range": {
        "sheetId": spreadsheet.worksheet(page).id,
        "dimension": "ROWS",
        "startIndex": 1,
        "endIndex": 2}}}, ], }
    spreadsheet.batch_update(body)
    print('Вставлена пустая строка')


def insert_data_row(data: list, page: str, row: int):
    # приготовить список с объектами ячеек
    cell_list = []
    for i, val in enumerate(data, start=1):
        cell_list.append(Cell(row=row, col=i, value=val))

    # обновить все разом
    while True:
        try:
            spreadsheet.worksheet(page).update_cells(cell_list)
            break
        except Exception as e:
            print('\ngoogle error', e)
            time.sleep(1)
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

    # несколько одинаковых запросов
    responses = [0 for _ in range(3)]
    for r in responses:
        async with session.post(f'{base_url}/api/adjuster/adjustments/', headers=HEADERS, json=payload) as response:
            if r != len(responses) - 1:  # пауза между запросами
                await asyncio.sleep(30)
            if response.status == 200:
                data = await response.json()
                responses[r] = data['parameters']['value']
            # если ответ плохой
            else:
                print(f'{response.status} error for {value}, r', r)
                responses[r] = 0

    # брать макс из всех ответов
    return max(responses)


async def auditory_update(by: str, field: dict) -> list:
    print('checking by', by)
    async with aiohttp.ClientSession() as session:
        # tasks = [detect_auditory(base_url=base_url, by='languages', value=i, session=session) for i in field]
        tasks = [ping_auditory(by=by, value=i, session=session) for i in field]
        results = await asyncio.gather(*tasks)
        for item, result in zip(field.keys(), results):
            field[item] = result
    # pprint(countries)
    return list(field.values())


def hour_update():
    now = str(datetime.now(tz).strftime("%d/%m, %H:%M"))

    # страны
    data = asyncio.run(auditory_update(field=countries, by='country'))
    insert_empty_row(page=hour_country)
    data = [now] + data
    insert_data_row(data=data, row=2, page=hour_country)

    # языки
    data1 = asyncio.run(auditory_update(field=lang_skills, by="skill"))  # подтвержденные тестом
    data2 = asyncio.run(auditory_update(field=languages, by="languages"))  # все
    insert_empty_row(page=hour_lang)
    data = [now] + data1 + data2
    insert_data_row(data=data, row=2, page=hour_lang)


def daily_max(col_amount: int, from_page: str, to_page: str):
    print(f'daily_max from_page: {from_page}, to_page: {to_page}')
    today = str(datetime.now(tz).date())
    # сгенерировать пустой словарь длиной в число стран или языков
    field = {i: 0 for i in range(col_amount)}

    # весь лист за посл 24 часа
    last_24h_data = spreadsheet.worksheet(from_page).range(2, 2, 25, col_amount + 1)
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
