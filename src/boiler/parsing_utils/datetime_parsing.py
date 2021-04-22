import datetime
import re


def parse_datetime(datetime_as_str, datetime_patterns, timezone=None):
    for pattern in datetime_patterns:
        parsed = re.match(pattern, datetime_as_str)
        if parsed is not None:
            break
    else:
        raise ValueError("Date and time are not matched using existing patterns")

    year = int(parsed.group("year"))
    month = int(parsed.group("month"))
    day = int(parsed.group("day"))
    hour = int(parsed.group("hours"))
    minute = int(parsed.group("minutes"))
    second = 0
    millisecond = 0

    datetime_ = datetime.datetime(year, month, day, hour, minute, second, millisecond, tzinfo=timezone)
    return datetime_
