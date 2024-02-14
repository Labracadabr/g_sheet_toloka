import schedule
import time
from datetime import datetime

from new_pools import pools
from all_accounts import accounts_update
from auditory import hour_update, day_update
print('launching')


# корректировка по часовому поясу. например если на устройстве gmt+3, то превратить 22 в '01'
def gmt_shift(hour: int) -> str:
    gmt = int(-(time.timezone if (time.localtime().tm_isdst == 0) else time.altzone) / 60 / 60)  # часовой пояс компа
    hour = str((hour + gmt) % 24).zfill(2)
    return hour


# финансы аккаунтов - неск раз в день, часы указать в gmt 0
schedule.every().day.at(f"{gmt_shift(7)}:30").do(accounts_update)
schedule.every().day.at(f"{gmt_shift(15)}:30").do(accounts_update)

# # новые пулы - каждые 24 ч в полночь по gmt 0
schedule.every().day.at(f"{gmt_shift(23)}:55").do(pools)

# замер аудитории каждый час
schedule.every().hour.at(':00').do(hour_update)

# макс аудитории за день - раз в сутки
schedule.every().day.at(f"{gmt_shift(20)}:50").do(day_update)


print('Сейчас', datetime.now().strftime("%H:%M:%S"))
print(schedule.get_jobs())

# поллинг каждые n секунд
n = 10
while 1:
    # проверить, не настало ли время
    schedule.run_pending()

    nxt = schedule.next_run()
    now = datetime.now()
    dif = nxt-now
    print('\rслед действие через', int(dif.total_seconds()), 'сек', end='')

    time.sleep(n)
