from datetime import datetime, timedelta
import calendar

import dateutil
import dateutil.parser

# TODO: we have to accoutn for holidays, like 21JUL is correct while 29 july 2021 is wrong it turns out
def is_last_thursday(thedate):
    if type(thedate) == str:
        thedate = dateutil.parser.parse(thedate)
    # weekdaynames = ['mon','tue','wed','thu','fri','sat','sun']
    thedate_monthrange = calendar.monthrange(thedate.year, thedate.month)
    days = [(thedate_monthrange[0]+idx)%7 for idx in range(0,thedate_monthrange[1])]
    lastthuidx = 0
    # we could pass the reversed list to find fast, but then again reversing takes whole iteration!
    # maybe we can play indexes in reverse but oh well, there are more impactful things to spend time in 
    for dayidx, day in enumerate(days):
        # 0 is monday, 3 is thursday
        if day==3:
            lastthuidx = dayidx
    lastthu_day = lastthuidx + 1
    if thedate.day == lastthu_day:
        return True
    return False

def get_last_thursdays_between_dates(start_datetime, end_datetime):
    if type(start_datetime) == str:
        start_datetime = dateutil.parser.parse(start_datetime)
    start_datetime = datetime(start_datetime.year, start_datetime.month, start_datetime.day)
    if type(end_datetime) == str:
        end_datetime = dateutil.parser.parse(end_datetime)
    end_datetime = datetime(end_datetime.year, end_datetime.month, end_datetime.day)
    
    if end_datetime < start_datetime:
        return []
    
    thursdays = []
    while start_datetime <= end_datetime:
        if is_last_thursday(start_datetime):
            thursdays.append(start_datetime)
        start_datetime += timedelta(days=1)
    
    return thursdays
    



if __name__ == "__main__":
    print(is_last_thursday(datetime(2021,7,29)), is_last_thursday(datetime(2021,7,30)))

    print("Thursdays between:", get_last_thursdays_between_dates("2021-07-17 14:44:08","2021-08-29"))