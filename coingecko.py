import requests
import json
import pytz
from datetime import datetime
from time import sleep


def to_tehran_time(utc_timestamp):
    """Convert UTC ISO 8601 timestamp to Tehran (IRST) time."""
    try:
        utc_dt = datetime.fromisoformat(utc_timestamp.replace('Z', '+00:00'))
        tehran_tz = pytz.timezone('Asia/Tehran')
        tehran_dt = utc_dt.astimezone(tehran_tz)
        return tehran_dt.strftime('%Y-%m-%d %H:%M:%S %Z')
    except ValueError:
        return None  # Handle invalid timestamps

def fetch_binance_usdt_pairs():
    """Fetch all USDT pairs and their 24h USD volumes from Binance."""
    pairs_data = []
    page = 1
    while True:
        url = f"https://api.coingecko.com/api/v3/exchanges/binance/tickers?page={page}"
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Error fetching page {page}: {response.text}")
            break
        
        data = response.json()
        tickers = data.get('tickers', [])
        if not tickers:
            break  # No more pages
        
        for ticker in tickers:
            if ticker.get('target') == 'USDT':
                base = ticker.get('base', '')
                pair = f"{base}/USDT"
                usd_volume = ticker.get('volume')
                timestamp = ticker.get('last_fetch_at')
                pairs_data.append({
                    'Pair': pair,
                    '24h_Volume_USD': usd_volume,
                    'time': to_tehran_time(timestamp)
                })
        
        print(f"Fetched page {page}: {len(tickers)} tickers, {len([t for t in tickers if t.get('target') == 'USDT'])} USDT pairs")
        page += 1
        sleep(2)
        
    
    return pairs_data



if __name__ == '__main__':

    tickers = fetch_binance_usdt_pairs()
    # Filter for USDT pairs
    import json
    b = json.dumps(tickers, indent=1)
    print(b)

    

