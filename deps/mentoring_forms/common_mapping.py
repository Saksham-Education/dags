from datetime import datetime


def validate_data(item):
    if item is not None:
        return item
    return 'NA'


def format_date(item):
    if item is None:
        return 'NA'
    if isinstance(item, datetime):
        return item.strftime('%d %B %Y')
    return datetime.strptime(item, '%Y-%m-%d').strftime('%d %B %Y')


def check_for_class(item):
    if item is None:
        return "NA"
    elif item == '1':
        return "Class 1"
    elif item == '2':
        return "Class 2"
    elif item == '3':
        return "Class 3"
    elif item == '4':
        return "Class 4"
    elif item == '5':
        return "Class 5"
    elif item == '6':
        return "Class 6"
    elif item == '7':
        return "Class 7"
    elif item == '8':
        return "Class 8"
    else:
        return item


def check_for_subject(item):
    if item is None:
        return "NA"
    elif item == '1':
        return "English"
    elif item == '2':
        return "Hindi"
    elif item == '3':
        return "Math"
    elif item == '4':
        return "EVS"
    elif item == '5':
        return "Science"
    elif item == '6':
        return "SST"
    else:
        return item


def parse_bool_or_other(item):
    if item is None:
        return 'NA'
    elif item == '0':
        return 'No'
    elif item == '1':
        return 'Yes'
    else:
        return item
