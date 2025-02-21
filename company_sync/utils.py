# File: company_sync/utils.py
import datetime

def get_fields(company: str) -> dict:
    company = company.lower()
    fields = {
        'aetna': {
            'memberID': 'Issuer Assigned ID',
            'paidThroughDate': 'Paid Through Date',
            'policyTermDate': 'Broker Term Date',
            'format': '%B %d, %Y'
        },
        'oscar': {
            'memberID': 'Member ID',
            'paidThroughDate': 'Paid Through Date',
            'policyTermDate': 'Coverage end date',
            'format': '%B %d, %Y'
        },
    }
    default = {
        'memberID': 'Member ID',
        'paidThroughDate': 'Paid Through Date',
        'format': '%m/%d/%Y'
    }
    return fields.get(company, default)

def conditional_update(company: str) -> dict:
    company = company.lower()
    if company == 'aetna':
        return {'Relationship': 'Self', 'Policy Status': 'Active'}
    elif company == 'ambetter':
        return {'Payable Agent': 'Health Family Insurance'}
    elif company == 'molina':
        return {'Status': 'Active'}
    elif company == 'oscar':
        return {'cond': '!=', 'Policy status': 'Inactive'}
    return {}

def calculate_paid_through_date(status: str, date_format: str = '%B %d, %Y') -> str:
    today = datetime.date.today()
    if status == 'Active':
        return last_day_of_month(today, date_format)
    elif status == 'Delinquent':
        two_months_ago = (today.replace(day=1) - datetime.timedelta(days=1))
        two_months_ago = (two_months_ago.replace(day=1) - datetime.timedelta(days=1))
        return last_day_of_month(two_months_ago, date_format)
    elif status == 'Grace period':
        last_month = today.replace(day=1) - datetime.timedelta(days=1)
        return last_day_of_month(last_month, date_format)
    else:
        return ''

def calculate_term_date(effective_date: str, input_format: str = '%B %d, %Y') -> str:
    effective_dt = datetime.datetime.strptime(effective_date, input_format)
    term_date = effective_dt.replace(year=effective_dt.year + 1, month=12)
    return last_day_of_month(term_date, input_format)

def last_day_of_month(any_day: datetime.date, date_format: str = '%B %d, %Y') -> str:
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    last_day = next_month - datetime.timedelta(days=next_month.day)
    return last_day.strftime(date_format)
