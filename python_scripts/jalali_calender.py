# create a csv that contains a all dates from 1404 to 1450 for clickhouse lookup
# in quering in jalali datetime

import csv
import jdatetime
from datetime import timedelta, date

start_date = jdatetime.date(1404, 8, 1).togregorian()
end_date = jdatetime.date(1450, 12, 29).togregorian()

farsi_months = [
    "فروردین", "اردیبهشت", "خرداد", "تیر", "مرداد", "شهریور",
    "مهر", "آبان", "آذر", "دی", "بهمن", "اسفند"
]

with open('jalali_calendar.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    
    # Header Row
    writer.writerow(['gregorian_date', 'jalali_date', 'jalali_year', 'jalali_month', 'jalali_month_name'])

    current = start_date
    while current <= end_date:
        j_date = jdatetime.date.fromgregorian(date=current)
        
        writer.writerow([
            current.strftime('%Y-%m-%d'),       # 2025-01-01
            j_date.strftime('%Y-%m-%d'),        # 1403-10-12
            j_date.year,                        # 1403
            j_date.month,                       # 10
            farsi_months[j_date.month - 1]      # دی
        ])
        
        current += timedelta(days=1)

print("✅ 'jalali_calendar.csv' created successfully with Farsi names!")
