import argparse
import calendar
import datetime
import getpass
import json
import time
import socket
import urllib
import os

import urllib2


# TOGGLES:

# staging is hit iff DEBUG is True
# production is hit iff DEBUG is False
DEBUG = False

# ======

VERSION = 2.1
docstr = '''
[version: {}]
'''.format(VERSION)

# get runtime information
MODE_MYBRC = 'mybrc'
MODE_MYLRC = 'mylrc'
MODE = MODE_MYBRC if 'brc' in socket.gethostname() else MODE_MYLRC

# set contact information
SUPPORT_TEAM = 'BRC' if MODE == MODE_MYBRC else 'LRC'
SUPPORT_EMAIL = 'brc-hpc-help@berkeley.edu' if MODE == MODE_MYBRC else 'hpcshelp@lbl.gov'

# constants
timestamp_format_complete = '%Y-%m-%dT%H:%M:%S'
timestamp_format_minimal = '%Y-%m-%d'

BASE_URL = 'https://{}/api/'.format('mybrc.brc.berkeley.edu' if MODE == MODE_MYBRC else 'mylrc.lbl.gov')
ALLOCATION_ENDPOINT = BASE_URL + 'allocations/'
ALLOCATION_USERS_ENDPOINT = BASE_URL + 'allocation_users/'
JOB_ENDPOINT = BASE_URL + 'jobs/'

COMPUTE_RESOURCES_TABLE = {
    MODE_MYBRC: {
        'ac': 'Savio Compute',
        'co': 'Savio Compute',
        'fc': 'Savio Compute',
        'ic': 'Savio Compute',
        'pc': 'Savio Compute',
        'vector': 'Vector Compute',
        'abc': 'ABC Compute',
    },

    MODE_MYLRC: {
        'ac': 'LAWRENCIUM Compute',
        'lr': 'LAWRENCIUM Compute',
        'pc': 'LAWRENCIUM Compute',
    }
}


if DEBUG:
    BASE_URL = 'http://scgup-dev.lbl.gov/api/' if MODE == MODE_MYBRC else 'http://scgup-dev.lbl.gov:8443/api/'

# CONFIG_FILE = 'check_usage_{}.conf'.format(MODE)
CONFIG_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                           'check_usage_{}.conf'.format(MODE))

if not os.path.exists(CONFIG_FILE):
    print('config file {0} missing...'.format(CONFIG_FILE))
    exit()

with open(CONFIG_FILE, 'r') as f:
    AUTH_TOKEN = f.read().strip()

def red_str(vector):
    return "\033[91m{}\033[00m".format(vector)


def green_str(vector):
    return "\033[92m{}\033[00m".format(vector)


def yellow_str(vector):
    return "\033[93m{}\033[00m".format(vector)


def check_valid_date(s):
    '''check if date is in valid format(s)'''
    complete, minimal = None, None

    try:
        complete = datetime.datetime.strptime(s, timestamp_format_complete)
    except Exception:
        pass

    try:
        minimal = datetime.datetime.strptime(s, timestamp_format_minimal)
    except Exception:
        pass

    if not complete and not minimal:  # doesn't fit either format
        raise argparse.ArgumentTypeError('Invalid time specification {}'.format(s))
    else:
        return s


# date time string -> time stamp
def to_timestamp(date_time, to_utc=False):
    try:
        dt_obj = datetime.datetime.strptime(date_time, timestamp_format_complete)
    except ValueError:
        dt_obj = datetime.datetime.strptime(date_time, timestamp_format_minimal)

    if to_utc:
        return time.mktime(dt_obj.timetuple())

    else:
        return calendar.timegm(dt_obj.timetuple())


# utc time stamp -> utc date time string
def to_timestring(timestamp):
    date_time = datetime.datetime.utcfromtimestamp(timestamp)
    return date_time.strftime(timestamp_format_complete) + 'Z'


# utc time stamp -> local time stamp
def utc2local(utc):
    utc = datetime.datetime.utcfromtimestamp(utc)
    local = utc + datetime.timedelta(hours=-7)
    local = time.mktime(local.timetuple())

    if DEBUG:
        print('[utc2local] utc_timestamp: {} local_timestamp: {}'.format(utc, local))

    return local


def paginate_requests(url, params):
    request_url = url + '?' + urllib.urlencode(params)
    request = urllib2.Request(request_url)
    request.add_header('Authorization', AUTH_TOKEN)

    try:
        response = json.loads(urllib2.urlopen(request).read())
    except Exception as e:
        response = {'results': None}
        if DEBUG:
            print('[paginate_requests({}, {})] ERR: {}'.format(url, params, e))

        return []

    next_page = 2
    results = response['results']
    while response['next'] is not None:
        params['page'] = next_page
        request_url = url + '?' + urllib.urlencode(params)
        request = urllib2.Request(request_url)
        request.add_header('Authorization', AUTH_TOKEN)

        try:
            response = json.loads(urllib2.urlopen(request).read())
            results.extend(response['results'])
            next_page += 1
        except urllib2.URLError as e:
            response['next'] = None

            if DEBUG:
                print('[paginate_requests({}, {})] ERR: {}'.format(url, params, e))

    return results


def single_request(url, params=None):
    request_url = url
    if params:
        request_url += '?' + urllib.urlencode(params)
    request = urllib2.Request(request_url)
    request.add_header('Authorization', AUTH_TOKEN)

    try:
        response = json.loads(urllib2.urlopen(request).read())
    except Exception as e:
        response = {'results': None}

        if DEBUG:
            print('[single_request({}, {})] ERR: {}'.format(url, params, e))

    return response['results']


def get_project_start(project):
    allocation_id_url = ALLOCATION_ENDPOINT

    header = project.split('_')[0]
    compute_resources = COMPUTE_RESOURCES_TABLE[MODE].get(header, '{} Compute'.format(header.upper()))
    params = {'project': project, 'resources': compute_resources}

    response = single_request(allocation_id_url, params)
    if not response or len(response) == 0:
        if DEBUG:
            print('[get_project_start({}, {})] ERR'.format(project, user))

        return None

    try:
        creation = response[0]['start_date']
        return creation.split('.')[0] if '.' in creation else creation
    except Exception as e:
        if DEBUG:
            print('[get_project_start({}, {})] ERR: {}'.format(project, user, e))

        print('ERR: information missing in {} database, contact Support ({}) if problem persists.'
              .format(SUPPORT_TEAM, SUPPORT_EMAIL))
        exit(0)


current_month = datetime.datetime.now().month
current_year = datetime.datetime.now().year
break_month = '06' if MODE == MODE_MYBRC else '10'
year = current_year if current_month >= int(break_month) else (current_year - 1)
default_start = '{}-{}-01T00:00:00'.format(year, break_month)

parser = argparse.ArgumentParser(description=docstr)
parser.add_argument('-u', dest='user',
                    help='check usage of this user')
parser.add_argument('-a', dest='account',
                    help='check usage of this account')

parser.add_argument('-E', dest='expand', action='store_true',
                    help='expand user/account usage')
parser.add_argument('-s', dest='start', type=check_valid_date,
                    help='starttime for the query period (YYYY-MM-DD[THH:MM:SS])',
                    default=default_start)
parser.add_argument('-e', dest='end', type=check_valid_date,
                    help='endtime for the query period (YYYY-MM-DD[THH:MM:SS])',
                    default=datetime.datetime.now().strftime(timestamp_format_complete))
parsed = parser.parse_args()
user = parsed.user
account = parsed.account
expand = parsed.expand
_start = parsed.start
_end = parsed.end

default_start_used = _start == default_start
calculate_project_start = default_start_used and account

# convert all times to UTC
start = to_timestamp(_start, to_utc=True)  # utc start time stamp
end = to_timestamp(_end, to_utc=True)      # utc end time stamp
_start = to_timestring(start)              # utc start time string
_end = to_timestring(end)                  # utc end time string

if calculate_project_start:
    target_start_date = get_project_start(account)  # local time string

    if target_start_date is not None:
        start = to_timestamp(target_start_date, to_utc=True)
        _start = to_timestring(start)

    elif DEBUG:
        print('[get_account_start({})] ERR'.format(account))

# defaults
if not user and not account:
    user = getpass.getuser()

output_headers = {}
if user:
    output_header = 'Usage for USER {} [{}, {}]:'.format(user, _start, _end)
    output_headers['user'] = output_header

if account:
    output_header = 'Usage for ACCOUNT {} [{}, {}]:'.format(account, _start, _end)
    output_headers['account'] = output_header


def get_cpu_usage(user=None, account=None):
    params = {'start_time': start, 'end_time': end}
    if user:
        params['user'] = user

    if account:
        params['account'] = account

    request_url = JOB_ENDPOINT + '?' + urllib.urlencode(params)
    request = urllib2.Request(request_url)
    request.add_header('Authorization', AUTH_TOKEN)

    try:
        response = json.loads(urllib2.urlopen(request).read())
    except Exception as e:
        response = {'count': 0, 'total_cpu_time': 0, 'total_amount': 0,
                    'response': [], 'next': None}

        if DEBUG:
            print('[get_cpu_usage({}, {})] ERR: {}'.format(user, account, e))

        if user and not account:
            return -1, -1, -1

    job_count = response['count']
    total_cpu = response['total_cpu_time']
    total_amount = response['total_amount']

    return job_count, total_cpu, total_amount


def process_account_query():
    allocation_id_url = ALLOCATION_ENDPOINT

    header = account.split('_')[0]
    compute_resources = COMPUTE_RESOURCES_TABLE[MODE].get(header, '{} Compute'.format(header.upper()))
    response = single_request(allocation_id_url, {'project': account, 'resources': compute_resources})
    if not response or len(response) == 0:
        if DEBUG:
            print('[process_account_query()] ERR')

        print('ERR: Account not found: {}'.format(account))
        return

    allocation_id = response[0]['id']
    allocation_url = allocation_id_url + '{}/attributes/'.format(allocation_id)
    response = single_request(allocation_url, {'type': 'Service Units'})
    if not response or len(response) == 0:
        if DEBUG:
            print('[process_account_query()] ERR')

        raise urllib2.URLError('ERR: Backend Error, contact {} Support ({}).'
                               .format(SUPPORT_TEAM, SUPPORT_EMAIL))

    allocation = response[0]['value']
    allocation = int(float(allocation))

    if 'ac_' in account or 'co_' in account:
        # get usage from allocation attribute
        try:
            account_usage = response[0]['usage']['value']
        except KeyError:
            raise urllib2.URLError('ERR: Backend Error, contact {} Support ({}).'
                                   .format(SUPPORT_TEAM, SUPPORT_EMAIL))

        job_count, cpu_usage, _ = get_cpu_usage(account=account)
    else:
        # get usage from jobs
        job_count, cpu_usage, account_usage = get_cpu_usage(account=account)

    if not default_start_used:
        print('{} {} jobs, {:.2f} CPUHrs, {} SUs.'.format(output_headers['account'], job_count, cpu_usage, account_usage))
    else:
        print('{} {} jobs, {:.2f} CPUHrs, {} SUs used from an allocation of {} SUs.'.format(output_headers['account'], job_count, cpu_usage, account_usage, allocation))

    if expand:
        user_url = ALLOCATION_USERS_ENDPOINT
        user_list = paginate_requests(user_url, {'project': account})

        for user in user_list:
            if user['user'] is None:
                continue

            user_name = user['user']
            user_jobs, user_cpu, user_usage = get_cpu_usage(user_name, account)

            percentage = 0.0
            try:
                percentage = (float(user_usage) / float(account_usage)) * 100
            except Exception:
                percentage = 0.00

            if percentage < 75:
                color_fn = green_str
            elif percentage > 100:
                color_fn = red_str
            else:
                color_fn = yellow_str

            percentage = color_fn("{:.2f}".format(percentage))
            print('\tUsage for USER {} in ACCOUNT {} [{}, {}]: {} jobs, {:.2f} CPUHrs, {} ({}%) SUs.'
                  .format(user_name, account, _start, _end, user_jobs, user_cpu, user_usage, percentage))


def process_user_query():
    global start, _start

    total_jobs, total_cpu, total_usage = get_cpu_usage(user)
    if total_jobs == total_cpu == total_usage == -1:
        print('ERR: User not found: {}'.format(user))
        return

    print('{} {} jobs, {:.2f} CPUHrs, {} SUs used.'.format(output_headers['user'], total_jobs, total_cpu, total_usage))

    if expand:
        user_allocation_url = ALLOCATION_USERS_ENDPOINT
        response = paginate_requests(user_allocation_url, {'user': user})

        for allocation in response:
            allocation_account = allocation['project']
            allocation_jobs, allocation_cpu, allocation_usage = get_cpu_usage(user, allocation_account)
            prefix = '\t'
            if allocation['status'] == 'Removed':
                prefix += '(User removed from account) '
            message = prefix + (
                'Usage for USER {} in ACCOUNT {} [{}, {}]: {} jobs, {:.2f} CPUHrs, {} SUs.'.format(
                    user, allocation_account, _start, _end, allocation_jobs, allocation_cpu, allocation_usage))
            print(message)


for req_type in output_headers.keys():
    try:
        if start > end:
            print('ERR: Start time ({}) requested is after end time ({}).'.format(_start, _end))
            exit(0)

        if to_timestamp('2020-06-01', to_utc=True) > start:
            print('INFO: Information might be inaccurate, for accurate information contact {} support ({}).'
                  .format(SUPPORT_TEAM, SUPPORT_EMAIL))

        if req_type == 'user':
            process_user_query()

        if req_type == 'account':
            if account.startswith('ac_'):
                print('INFO: Start Date shown may be inaccurate.')

            process_account_query()

    except urllib2.URLError as e:
        print('ERR: Could not connect to backend, contact {} Support ({}) if problem persists.'
              .format(SUPPORT_TEAM, SUPPORT_EMAIL))
        if DEBUG:
            print('__main__ ERR: {}'.format(e))

    except Exception as e:
        if DEBUG:
            print('__main__ ERR: {}'.format(e))