import pygsheets
import time
import datetime

d = datetime.date(year=2023, month=8, day=14)
t = time.strftime("%d %b")
a = time.time()
# 1692133193

print('Сейчас', ':'.join(time.strftime("%H %M %S").split()))
print(a)