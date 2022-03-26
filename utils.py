import datetime


def parse_datetime(date):
    try:
        return datetime.datetime.strptime(date, "%d/%m/%Y %H:%M")
    except ValueError:
        return datetime.datetime.strptime(date, "%m/%d/%Y %H:%M")
