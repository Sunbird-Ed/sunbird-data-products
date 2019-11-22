"""
Store all utility and reusable functions.
"""
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from time import sleep

import pandas as pd
import requests
from azure.common import AzureMissingResourceHttpError
from azure.storage.blob import BlockBlobService


def parse_tb(tb, returnable, row_):
    """
    Parse the tree/dictionary format of the textbook in to a list format to pass to a dataframe.
    :param tb: dictionary of collection (textbook or textbook unit)
    :param returnable: flattened list structure of the dictionary
    :param row_: textbook metadata
    :return: None
    """
    temp = {
        'tb_id': row_['identifier'],
        'board': row_['board'],
        'channel': row_['channel'],
        'medium': row_['medium'],
        'gradeLevel': row_['gradeLevel'],
        'subject': row_['subject'],
        'tb_name': row_['name'],
        'status': row_['status']
    }
    try:
        temp['identifier'] = tb['identifier']
    except KeyError:
        pass
    try:
        temp['name'] = tb['name']
    except KeyError:
        pass
    try:
        temp['contentType'] = tb['contentType']
    except KeyError:
        pass
    try:
        temp['dialcodes'] = tb['dialcodes'][0]
    except KeyError:
        pass
    try:
        temp['leafNodesCount'] = tb['leafNodesCount']
    except KeyError:
        pass
    returnable.append(temp)
    if ('children' in tb.keys()) and tb['children']:
        for child in tb['children']:
            parse_tb(child, returnable, row_)


def get_textbook_snapshot(result_loc_, content_search_, content_hierarchy_, date_):
    """
     get a list of textbook from LP API and iterate over the textbook hierarchy to create CSV
    :param result_loc_: pathlib.Path object to store resultant CSV at
    :param content_search_: ip and port of the server hosting LP content search API
    :param content_hierarchy_: ip and port of the server hosting LP content hierarchy API
    :param date_: datetime object
    :return:
    """
    result_loc_.joinpath(date_.strftime('%Y-%m-%d')).mkdir(exist_ok=True)
    tb_url = "{}v3/search".format(content_search_)
    payload = """{
                "request": {
                    "filters": {
                        "contentType": ["Textbook"],
                        "status": ["Live"]
                    },
                    "sort_by": {"createdOn":"desc"},
                    "limit": 10000
                }
            }"""
    tb_headers = {
        'content-type': "application/json; charset=utf-8",
        'cache-control': "no-cache"
    }
    retry_count = 0
    while retry_count < 5:
        retry_count += 1
        try:
            response = requests.request("POST", tb_url, data=payload, headers=tb_headers)
            textbooks = pd.DataFrame(response.json()['result']['content'])[
                ['identifier', 'channel', 'board', 'medium', 'gradeLevel', 'subject', 'name', 'status']]
            textbooks[textbooks.duplicated(subset=['identifier', 'status'])].to_csv(
                result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'duplicate_tb.csv'), index=False)
            textbooks.drop_duplicates(subset=['identifier', 'status'], inplace=True)
            textbooks.fillna({'gradeLevel': ' ', 'createdFor': ' '}, inplace=True)
            textbooks.fillna('', inplace=True)
            textbooks.to_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'tb_list.csv'), index=False)
            break
        except requests.exceptions.ConnectionError:
            print("Retry {} for textbook list".format(retry_count))
            sleep(10)
    else:
        print("Max retries reached...")
        with open(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'etb_error_log.log'), 'a') as f:
            f.write('ConnectionError: Could not get textbook list.\n')
        return
    counter = 0
    textbook_list = []
    for ind_, row_ in textbooks.iterrows():
        counter += 1
        print('Running for {} out of {}: {}%'.format(counter, textbooks.shape[0],
                                                     '%.2f' % (counter * 100 / textbooks.shape[0])))
        url = "{}learning-service/content/v3/hierarchy/{}".format(content_hierarchy_, row_['identifier'])
        retry_count = 0
        while retry_count < 5:
            retry_count += 1
            try:
                response = requests.request("GET", url)
                tb = response.json()['result']['content']
                parse_tb(tb=tb, returnable=textbook_list, row_=row_)
                break
            except requests.exceptions.ConnectionError:
                print("ConnectionError: Retry {} for textbook {}".format(retry_count, row_['identifier']))
                sleep(10)
            except KeyError:
                with open(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'etb_error_log.log'), 'a') as f:
                    f.write("KeyError: Resource not found for textbook {}\n".format(row_['identifier']))
                break
        else:
            with open(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'etb_error_log.log'), 'a') as f:
                f.write("ConnectionError: Max retries reached for textbook {}\n".format(row_['identifier']))
    textbook_df = pd.DataFrame(textbook_list)
    textbook_df.to_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'textbook_snapshot.csv'), index=False)
    post_data_to_blob(result_loc_=result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'textbook_snapshot.csv'),
                      backup=True)


def get_tenant_info(result_loc_, org_search_, date_):
    """
    get channel, slug, name of all orgs in current environment
    :param result_loc_: pathlib.Path object to store resultant CSV at
    :param org_search_: host ip and port of server hosting org search API
    :param date_: datetime object to pass to file path
    :return: None
    """
    url = "{}/org/v1/search".format(org_search_)
    payload = """{
    "request":{
        "filters": {
            "isRootOrg": true
        },
        "offset": 0,
        "limit": 1000,
        "fields": ["id","channel","slug","orgName"]
    }
}"""
    headers = {
        'Accept': "application/json",
        'Content-Type': "application/json",
        'cache-control': "no-cache"
    }
    retry_count = 0
    while retry_count < 5:
        retry_count += 1
        try:
            response = requests.request("POST", url, data=payload, headers=headers)
            data = pd.DataFrame(response.json()['result']['response']['content'],
                                columns=['id', 'channel', 'slug', 'orgName'])
            result_loc_.mkdir(exist_ok=True)
            result_loc_.joinpath(date_.strftime('%Y-%m-%d')).mkdir(exist_ok=True)
            data.to_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'tenant_info.csv'), index=False,
                        encoding='utf-8')
            post_data_to_blob(result_loc_=result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'tenant_info.csv'),
                              backup=True)
            break
        except requests.exceptions.ConnectionError:
            with open(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'etb_error_log.log'), 'a') as f:
                f.write("Retry {} for org list\n".format(retry_count))
            sleep(10)
        except KeyError as ke:
            print('Key not found in response: ', ke, response.text)
            break
    else:
        print("Max retries reached...")


def get_content_model(result_loc_, druid_, date_):
    """
    get current content model snapshot
    :param result_loc_: pathlib.Path object to store resultant CSV at
    :param druid_: host ip and port for druid broker
    :param date_: datetime object to pass in path
    :return: None
    """
    try:
        headers = {
            'Content-Type': "application/json"
        }
        url = "{}/druid/v2/".format(druid_)
        with open(Path(__file__).parent.parent.parent.parent.parent.joinpath('resources', 'queries',
                                                                                    'content_list.json')) as f:
            query = f.read()
        qr = json.loads(query)
        response = requests.request("POST", url, data=query, headers=headers)
        result = response.json()
        response_list = []
        while result[0]['result']['events']:
            data = [event['event'] for segment in result for event in segment['result']['events']]
            response_list.append(pd.DataFrame(data))
            qr['pagingSpec']['pagingIdentifiers'] = result[0]['result']['pagingIdentifiers']
            response = requests.request("POST", url, data=json.dumps(qr), headers=headers)
            result = response.json()
        content_model = pd.concat(response_list).drop(['', 'timestamp'], axis=1)
        content_model.to_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'content_model_snapshot.csv'),
                             index=False, encoding='utf-8-sig')
        post_data_to_blob(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'content_model_snapshot.csv'), backup=True)
    except Exception:
        raise Exception('Getting Content Snapshot Failed!')


def get_scan_counts(result_loc_, druid_, date_):
    """
    get dialcode level scan counts for given period
    :param result_loc_: pathlib.Path object to store resultant CSV at
    :param druid_: host ip and port for Druid broker
    :param date_: datetime object to pass to file path and for query period
    :return: None
    """
    try:
        headers = {
            'Content-Type': "application/json"
        }
        url = "{}/druid/v2/".format(druid_)
        with open(Path(__file__).parent.parent.parent.parent.parent.parent.joinpath('resources', 'queries',
                                                                                    'scan_counts.json')) as f:
            query = f.read()
        start_date = date_ - timedelta(days=7)
        query = query.replace('start_date', start_date.strftime('%Y-%m-%dT00:00:00+00:00'))
        query = query.replace('end_date', date_.strftime('%Y-%m-%dT00:00:00+00:00'))
        response = requests.post(url, data=query, headers=headers)
        result = response.json()
        scans_df = pd.DataFrame([x['event'] for x in result])
        scans_df['Date'] = date_.strftime('%Y-%m-%d')
        time_ = datetime.strftime(datetime.now(), '%Y-%m-%dT%H-%M-%S')
        scans_df.to_csv(
            result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'weekly_dialcode_counts_{}.csv'.format(time_)),
            encoding='utf-8-sig', index=False)
        post_data_to_blob(
            result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'weekly_dialcode_counts_{}.csv'.format(time_)),
            backup=True)
        get_data_from_blob(result_loc_.joinpath('dialcode_counts.csv'))
        scans_19 = pd.read_csv(result_loc_.joinpath('dialcode_counts.csv'), encoding='utf-8-sig')
        scans_19 = scans_19.append(scans_df).drop_duplicates(subset=['edata_filters_dialcodes', 'Date'], keep='last')
        scans_19.to_csv(result_loc_.joinpath('dialcode_counts.csv'), encoding='utf-8-sig', index=False)
        post_data_to_blob(result_loc_.joinpath('dialcode_counts.csv'))
        scans_19.to_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'dialcode_counts.csv'), encoding='utf-8-sig',
                        index=False)
        post_data_to_blob(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'dialcode_counts.csv'), backup=True)
    except Exception:
        raise Exception('Getting Scan Counts Failed!')


def create_json(read_loc_, last_update=False):
    """
    convert csv to json with last updated date (optional)
    :param read_loc_: pathilb.Path object to csv location
    :param last_update: Boolean on whether to append lastUpdate field to json. currently only possible if dataframe
    has a date column
    :return: None
    """
    try:
        df = pd.read_csv(read_loc_).fillna('')
        if last_update:
            try:
                if "Date" in df.columns.values.tolist():
                    _lastUpdateOn = pd.to_datetime(df['Date'], format='%d-%m-%Y').max().timestamp() * 1000
                else:
                    _lastUpdateOn = datetime.now().timestamp() * 1000
            except ValueError:
                return
            df = df.astype('str')
            json_file = {
                'keys': df.columns.values.tolist(),
                'data': json.loads(df.to_json(orient='records')),
                'tableData': df.values.tolist(),
                'metadata': {
                    'lastUpdatedOn': _lastUpdateOn
                }
            }
        else:
            df = df.astype('str')
            json_file = {
                'keys': df.columns.values.tolist(),
                'data': json.loads(df.to_json(orient='records')),
                'tableData': df.values.tolist()}
        with open(str(read_loc_).split('.csv')[0] + '.json', 'w') as f:
            json.dump(json_file, f)
    except Exception:
        raise Exception('Failed to create JSON!')


def get_data_from_blob(result_loc_):
    """
    read a blob storage file
    :param result_loc_: pathlib.Path object to store the file at. the last two names in path structure is used to locate
     file on blob storage container
    :return: None
    """
    try:
        result_loc_.parent.mkdir(exist_ok=True)
        account_name = os.environ['AZURE_STORAGE_ACCOUNT']
        account_key = os.environ['AZURE_STORAGE_ACCESS_KEY']
        block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)
        container_name = 'reports'
        block_blob_service.get_blob_to_path(
            container_name=container_name,
            blob_name=result_loc_.parent.name + '/' + result_loc_.name,
            file_path=str(result_loc_)
        )
    except AzureMissingResourceHttpError:
        raise AzureMissingResourceHttpError("Missing resource!", 404)
    except Exception:
        raise Exception('Could not read from blob!')


def post_data_to_blob(result_loc_, backup=False):
    """
    write a local file to blob storage.
    :param result_loc_: pathlib.Path object to read CSV from
    :param backup: boolean option used to store in a different container with different path structure
    :return: None
    """
    try:
        account_name = os.environ['AZURE_STORAGE_ACCOUNT']
        account_key = os.environ['AZURE_STORAGE_ACCESS_KEY']
        block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)
        if backup:
            container_name = 'portal-reports-backup'
            file_name = result_loc_.name
            date_name = result_loc_.parent.name
            report_name = result_loc_.parent.parent.name
            block_blob_service.create_blob_from_path(
                container_name=container_name,
                blob_name=report_name + '/' + date_name + '/' + file_name,
                file_path=str(result_loc_)
            )
        else:
            container_name = 'reports'
            block_blob_service.create_blob_from_path(
                container_name=container_name,
                blob_name=result_loc_.parent.name + '/' + result_loc_.name,
                file_path=str(result_loc_)
            )
            if result_loc_.parent.joinpath(result_loc_.name.replace('.csv', '.json')).exists():
                block_blob_service.create_blob_from_path(
                    container_name=container_name,
                    blob_name=result_loc_.parent.name + '/' + result_loc_.name.replace('.csv', '.json'),
                    file_path=str(result_loc_).replace('.csv', '.json')
                )
    except Exception:
        raise Exception('Failed to post to blob!')


def get_courses(result_loc_, druid_, query_file_, date_):
    """
    query content model snapshot on druid but filter for courses.
    :param result_loc_: pathlib.Path object to store resultant CSV at
    :param druid_: host and ip of druid broker
    :param query_file_: file name of druid json query
    :param date_: datetime object to pass to file path
    :return: Nones
    """
    with open(Path(__file__).parent.parent.parent.parent.parent.parent.joinpath('resources', 'queries',
                                                                                query_file_)) as f:
        query = f.read()
    response = requests.request("POST", url='{}/druid/v2'.format(druid_), data=query)
    result = response.json()
    courses = pd.DataFrame([eve['event'] for event in result for eve in event['result']['events']])
    courses = courses.drop(['', 'timestamp'], axis=1)
    courses.to_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'courses.csv'), index=False)
    post_data_to_blob(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'courses.csv'), backup=True)


def get_content_plays(result_loc_, date_, druid_):
    """
    Get content plays and timespent by content id.
    :param result_loc_: local path to store resultant csv
    :param date_: datetime object used for druid query
    :param druid_: druid broker ip and port in http://ip:port/ format
    :return: None
    """
    headers = {
        'Content-Type': "application/json"
    }
    url = "{}/druid/v2/".format(druid_)
    start_date = date_ - timedelta(days=1)
    with open(Path(__file__).parent.parent.parent.parent.parent.joinpath('resources', 'queries',
                                                                                'content_plays.json')) as f:
        druid_query = f.read()
    druid_query = druid_query.replace('start_date', start_date.strftime('%Y-%m-%dT00:00:00+00:00'))
    druid_query = druid_query.replace('end_date', date_.strftime('%Y-%m-%dT00:00:00+00:00'))
    response = requests.post(url, data=druid_query, headers=headers)
    result = response.json()
    data = pd.DataFrame([x['event'] for x in result])
    data['Date'] = date_.strftime('%Y%m%d')
    result_loc_.joinpath(date_.strftime('%Y-%m-%d')).mkdir(exist_ok=True)
    data.to_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'content_plays.csv'), index=False)
    post_data_to_blob(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'content_plays.csv'), backup=True)


def write_data_to_blob(read_loc, file_name):
    account_name = os.environ['AZURE_STORAGE_ACCOUNT']
    account_key = os.environ['AZURE_STORAGE_ACCESS_KEY']
    block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)

    container_name = 'reports'

    local_file = file_name

    full_path = os.path.join(read_loc, local_file)

    block_blob_service.create_blob_from_path(container_name, local_file, full_path)


def generate_metrics_summary(result_loc_, metrics):
    pass
