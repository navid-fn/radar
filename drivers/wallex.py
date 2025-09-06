import random
import requests
import datetime
from datetime import timedelta

def scrape(url: str) -> dict:
    url = "https://api.wallex.ir/v1/udf/history?symbol={}USDT&resolution=1&from={}&to={}"
    to_date = datetime.datetime.now().replace(second=0, microsecond=0)
    from_date = to_date - timedelta(minutes=1)

    r = requests.get(url.format(from_date, to_date))
    
    return r.json()
