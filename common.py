from functools import wraps
import time
import gspread
import pytz


# декоратор для обработки ошибок апи гугла
def api_decorator(func: callable) -> callable:
    @wraps(func)
    def wrapper(*args, **kwargs):
        tries = 0
        while True:
            try:
                result = func(*args, **kwargs)
                return result
            # при ошибке - подождать, затем исполнить функцию заново
            except gspread.exceptions.APIError as e:
                print(f'Ошибка {func}:', e)
                tries += 1
                wait = 2 ** tries  # если ошибка повторилась - каждый новый раз ожидать экспоненциально больше
                print(f'{tries} раз подряд, новая попытка через {wait} сек')
                time.sleep(wait)
    return wrapper


# данные для подключения к таблице
service_file = 'token.json'
gc = gspread.service_account(filename=service_file)

# текст обновления в gmt+3
tz = pytz.timezone("Etc/GMT-3")
