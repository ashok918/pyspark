from datetime import datetime
timestamp = "2024-11-14 08:45:30"
date_obj = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
day_of_week = date_obj.strftime("%A")
day_of_month  = date_obj.month #date_obj.day
print(day_of_week)
print(day_of_month)
