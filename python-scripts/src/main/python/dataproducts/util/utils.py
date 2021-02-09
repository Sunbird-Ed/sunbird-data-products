"""
Store all utility and reusable functions.
"""
import hashlib
import json
import os
import pandas as pd
import pdb
import requests
import time
from azure.common import AzureMissingResourceHttpError
from azure.storage.blob import BlockBlobService
from dataproducts.resources.common import common_config
from dataproducts.resources.queries import content_list_v1, content_list_v2, scan_counts, course_list, content_plays
from dataproducts.util import azure_utils
from dataproducts.util.kafka_utils import push_metrics
from datetime import datetime, timedelta
from pathlib import Path
from pytz import timezone
from time import sleep


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
    url = "{}v1/org/search".format(org_search_)
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


def get_location_info(result_loc_, location_search_, date_, iteration=0):
    """
    get districts and state mapping of current environment
    :param result_loc_: pathlib.Path object to store resultant CSV at
    :param location_search_: host ip and port of server hosting location search API
    :param date_: datetime object to pass to file path
    :return: None
    """
    url = "{}api/data/v1/location/search".format(location_search_)
    payload = """{
        "request": {
             "limit": 5000,
             "filters": {
                "type": ["district", "state"]
             }
        }
    }"""
    headers = {
        'Accept': "application/json",
        'Content-Type': "application/json",
        'cache-control': "no-cache",
        'Authorization': "Bearer {}".format(os.environ['API_KEY'])
    }

    try:
        response = requests.request("POST", url, data=payload, headers=headers)
        result = response.json()['result']['response']
        states = pd.DataFrame(
            list(filter(lambda x: x['type'] == "state", result))
        )
        states.rename(columns={"name": "state"}, inplace=True)
        districts = pd.DataFrame(
            list(filter(lambda x: x['type'] == "district", result))
        )
        districts.rename(columns={"name": "district"}, inplace=True)
        state_district_df = pd.merge(districts, states, left_on=['parentId'], right_on=['id'], how="inner")
        state_district_df = state_district_df[['state', 'district']]
        result_loc_.mkdir(exist_ok=True)
        result_loc_.joinpath(date_.strftime('%Y-%m-%d')).mkdir(exist_ok=True)
        state_district_df.to_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'state_district.csv'), index=False,
                                 encoding='utf-8')
        post_data_to_blob(result_loc_=result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'state_district.csv'),
                          backup=True)
    except requests.exceptions.ConnectionError:
        with open(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'etb_error_log.log'), 'a') as f:
            f.write("Retry {} for location list\n".format(iteration))
        if iteration < 5:
            iteration += 1
            get_location_info(result_loc_, location_search_, date_, iteration)
            sleep(10)
        else:
            print("Max retries reached...")
    except KeyError as ke:
        print('Key not found in response: ', ke, response.text)


def verify_state_district(loc_map_path_, state_, df_):
    loc_df = pd.read_csv(loc_map_path_.joinpath('state_district.csv'))
    state_districts = loc_df[loc_df['state'] == state_].district.to_list()
    df_['District'] = pd.np.where(
        df_['District'].isin(loc_df[loc_df['state'] == state_].district.to_list()),
        df_['District'],
        None)
    return df_


def get_content_model(result_loc_, druid_, date_, config_, version_, status_=["Live"]):
    """
    get current content model snapshot
    :param result_loc_: pathlib.Path object to store resultant CSV at
    :param druid_: host ip and port for druid broker
    :param date_: datetime object to pass in path
    :param config_: pass the mime type filters
    :param version_: differentiate between versions of query. values in ['v1', 'v2']
    :param status_: status of the content which has to retrieved
    :return: None
    """
    try:
        headers = {
            'Content-Type': "application/json"
        }
        url = "{}druid/v2/".format(druid_)
        if version_ == 'v1':
            qr = content_list_v1.init()
        elif version_ == 'v2':
            qr = content_list_v2.init()
        qr = json.loads(qr)
        if version_ == 'v2':
            qr['filter']['fields'][1]['values'] = config_['content']['mimeType']
        qr['filter']['fields'][2]['values'] = status_
        response = requests.request("POST", url, data=json.dumps(qr), headers=headers)
        result = response.json()
        response_list = []
        for segment in result:
            response_list.append(pd.DataFrame(segment['events']))
        content_model = pd.concat(response_list)
        if version_ == 'v2':
            content_model['mimeType'].replace(config_['rename_mimetype'], inplace=True)
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
        url = "{}druid/v2/".format(druid_)
        start_date = date_ - timedelta(days=7)
        query = scan_counts.init()
        query = query.replace('start_date', start_date.strftime('%Y-%m-%dT00:00:00+00:00'))
        query = query.replace('end_date', date_.strftime('%Y-%m-%dT00:00:00+00:00'))
        response = requests.post(url, data=query, headers=headers)
        result = response.json()
        scans_df = pd.DataFrame([x['event'] for x in result])
        scans_df['Date'] = date_.strftime('%Y-%m-%d')
        time_ = datetime.strftime(datetime.now(), '%Y-%m-%dT%H-%M-%S')
        scans_df.to_csv(
            result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'weekly_dialcode_counts.csv'),
            encoding='utf-8-sig', index=False)
        post_data_to_blob(
            result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'weekly_dialcode_counts.csv'),
            backup=True)
        get_data_from_blob(result_loc_.joinpath('dialcode_counts.csv'))
        scans_19 = pd.read_csv(result_loc_.joinpath('dialcode_counts.csv'), encoding='utf-8-sig')
        scans_19 = scans_19.append(scans_df).drop_duplicates(subset=['edata_filters_dialcodes', 'Date'], keep='last')
        scans_19.to_csv(result_loc_.joinpath('dialcode_counts.csv'), encoding='utf-8-sig', index=False)
        post_data_to_blob(result_loc_.joinpath('dialcode_counts.csv'))
        scans_19.to_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'dialcode_counts.csv'), encoding='utf-8-sig',
                        index=False)
        post_data_to_blob(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'dialcode_counts.csv'), backup=True)
    except Exception as e:
        raise Exception('Getting Scan Counts Failed! :: ' + str(e))


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


def file_missing_exception(raise_except=False):
    storage_provider = os.environ['STORAGE_PROVIDER']
    if storage_provider == "AZURE":
        return AzureMissingResourceHttpError("Missing resource!",
                                             404) if raise_except else AzureMissingResourceHttpError
    elif storage_provider == "S3":
        return Exception
    elif storage_provider == "GCP":
        return Exception


def download_file_from_store(blob_name, file_path, container_name, is_private=True):
    storage_provider = os.environ['STORAGE_PROVIDER']
    if storage_provider == "AZURE":
        azure_utils.get_data_from_store(container_name, blob_name, file_path, is_private)
    elif storage_provider == "S3":
        pass
    elif storage_provider == "GCP":
        pass


def upload_file_to_store(blob_name, file_path, container_name, is_private=True):
    storage_provider = os.environ['STORAGE_PROVIDER']
    if storage_provider == "AZURE":
        azure_utils.post_data_to_store(container_name, blob_name, file_path, is_private)
    elif storage_provider == "S3":
        pass
    elif storage_provider == "GCP":
        pass


def get_data_batch_from_blob(result_loc_, prefix=None, backup=False):
    try:
        account_name = os.environ['AZURE_STORAGE_ACCOUNT']
        account_key = os.environ['AZURE_STORAGE_ACCESS_KEY']
        block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)
        container_name = 'portal-reports-backup' if backup else 'reports'
        blob_list = block_blob_service.list_blobs(container_name, prefix=prefix)

        for blob in blob_list:
            blob_result_loc_ = result_loc_.joinpath(blob.name).parent
            os.makedirs(blob_result_loc_, exist_ok=True)
            block_blob_service.get_blob_to_path(
                container_name=container_name,
                blob_name=blob.name,
                file_path=str(result_loc_) + "/" + blob.name
            )

    except AzureMissingResourceHttpError:
        raise AzureMissingResourceHttpError("Missing resource!", 404)


def get_data_from_blob(result_loc_, backup=False, is_private=True):
    """
    read a blob storage file
    :param result_loc_: pathlib.Path object to store the file at. the last two names in path structure is used to locate
     file on blob storage container
    :return: None
    """
    try:
        result_loc_.parent.mkdir(exist_ok=True)
        if backup:
            container_name = os.environ['REPORT_BACKUP_CONTAINER']
            file_name = result_loc_.name
            date_name = result_loc_.parent.name
            report_name = result_loc_.parent.parent.name
            download_file_from_store(
                blob_name=report_name + '/' + date_name + '/' + file_name,
                file_path=str(result_loc_),
                container_name=container_name,
                is_private=is_private
            )
        else:
            container_name = os.environ['PRIVATE_REPORT_CONTAINER']
            download_file_from_store(
                blob_name=result_loc_.parent.name + '/' + result_loc_.name,
                file_path=str(result_loc_),
                container_name=container_name,
                is_private=is_private
            )
    except file_missing_exception():
        raise file_missing_exception(raise_except=True)
    except Exception as e:
        raise Exception('Could not read from blob!' + str(e))


def get_dqp_data_from_blob(file_path, result_loc_):
    """
    read a blob storage file
    :file_path: file path of the file which will be generated by DQP
    :param result_loc_: pathlib.Path object to store the file at.
    :return: None
    """
    try:
        local_file_path = result_loc_.joinpath(file_path)
        os.makedirs(local_file_path.parent, exist_ok=True)

        container_name = 'telemetry-data-store'

        download_file_from_store(
            container_name=container_name,
            blob_name=str(file_path),
            file_path=local_file_path
        )
    except file_missing_exception():
        raise file_missing_exception(raise_except=True)
    except Exception as e:
        raise Exception('Could not read from blob!' + str(e))


def post_data_to_blob(result_loc_, backup=False, is_private=True):
    """
    write a local file to blob storage.
    :param result_loc_: pathlib.Path object to read CSV from
    :param backup: boolean option used to store in a different container with different path structure
    :return: None
    """
    try:
        if backup:
            container_name = os.environ['REPORT_BACKUP_CONTAINER']
            file_name = result_loc_.name
            date_name = result_loc_.parent.name
            report_name = result_loc_.parent.parent.name
            upload_file_to_store(
                blob_name=report_name + '/' + date_name + '/' + file_name,
                file_path=str(result_loc_),
                container_name=container_name,
                is_private=is_private
            )
        else:
            container_name = os.environ['PRIVATE_REPORT_CONTAINER']
            upload_file_to_store(
                blob_name=result_loc_.parent.name + '/' + result_loc_.name,
                file_path=str(result_loc_),
                container_name=container_name,
                is_private=is_private
            )
            if result_loc_.parent.joinpath(result_loc_.name.replace('.csv', '.json')).exists():
                upload_file_to_store(
                    blob_name=result_loc_.parent.name + '/' + result_loc_.name.replace('.csv', '.json'),
                    file_path=str(result_loc_).replace('.csv', '.json'),
                    container_name=container_name,
                    is_private=is_private
                )
    except Exception:
        raise Exception('Failed to post to blob!')


def get_courses(result_loc_, druid_, date_):
    """
    query content model snapshot on druid but filter for courses.
    :param result_loc_: pathlib.Path object to store resultant CSV at
    :param druid_: host and ip of druid broker
    :param query_file_: file name of druid json query
    :param date_: datetime object to pass to file path
    :return: Nones
    """
    query = course_list.init()
    response = requests.request("POST", url='{}druid/v2'.format(druid_), data=query)
    result = response.json()
    courses = pd.DataFrame([eve['event'] for event in result for eve in event['result']['events']])
    courses = courses.drop(['', 'timestamp'], axis=1)
    courses.to_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'courses.csv'), index=False)
    post_data_to_blob(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'courses.csv'), backup=True)


def get_content_plays(result_loc_, start_date_, end_date_, druid_, config_, version_):
    """
    Get content plays and timespent by content id.
    :param result_loc_: local path to store resultant csv
    :param start_date_: datetime object used for druid query
    :param end_date_: datetime object used for druid query
    :param druid_: druid broker ip and port in http://ip:port/ format
    :param config_: Platform cofigs for druid query
    :param version_: to differentiate version of query. value in ['v1', 'v2']
    :return: None
    """
    headers = {
        'Content-Type': "application/json"
    }
    url = "{}druid/v2/".format(druid_)
    query = content_plays.init()
    query = query.replace('$start_date', start_date_.strftime('%Y-%m-%dT00:00:00+00:00'))
    query = query.replace('$end_date', end_date_.strftime('%Y-%m-%dT00:00:00+00:00'))
    query = query.replace('$app', config_['context']['pdata']['id']['app'])
    query = query.replace('$portal', config_['context']['pdata']['id']['portal'])
    query = json.loads(query)
    if version_ == 'v2':
        query['filter']['fields'].append(
            {
                "type": "in",
                "dimension": "content_mimetype",
                "values": config_['content']['mimeType']
            },
        )
    response = requests.post(url, data=json.dumps(query), headers=headers)
    result = response.json()
    data = pd.DataFrame([x['event'] for x in result])
    data.to_csv(result_loc_.joinpath(end_date_.strftime('content_plays_%Y-%m-%d.csv')), index=False)
    post_data_to_blob(result_loc_.joinpath(end_date_.strftime('content_plays_%Y-%m-%d.csv')), backup=True)


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


def push_metric_event(metrics_list, subsystem):
    env = os.environ['ENV']
    kafka_broker = os.environ['KAFKA_BROKER_HOST']
    config = common_config.init()
    kafka_topic = config['kafka_metrics_topic']
    eid = "METRIC"
    ets = int(round(time.time() * 1000))
    midStr = eid + str(ets) + subsystem
    actor = {
        "id": "analytics",
        "type": "System"
    }
    context = {
        "channel": "data-pipeline",
        "env": "",
        "pdata": {
            "id": "pipeline.monitoring",
            "ver": "1.0",
            "pid": "adhoc.job.metrics"
        }
    }
    metrics = {
        "system": "AdhocJob",
        "subsystem": subsystem,
        "metrics": metrics_list
    }
    metric = {
        "eid": eid,
        "ver": "3.0",
        "ets": ets,
        "mid": hashlib.md5(midStr.encode()).hexdigest(),
        "@timestamp": datetime.now(timezone("UTC")).strftime("%Y-%m-%dT%H:%M:%S%z"),
        "actor": actor,
        "context": context,
        "edata": metrics
    }
    topic = env + "." + kafka_topic
    push_metrics(kafka_broker, topic, metric)


def mime_type(series):
    """
    map the content format into preset buckets
    :param series: pandas series
    :return: pandas series
    """
    if series == 'video/x-youtube':
        return 'YouTube Content'
    elif series == 'application/vnd.ekstep.ecml-archive':
        return 'Created on Diksha'
    elif series == 'video/mp4' or series == 'video/webm':
        return 'Uploaded Videos'
    elif series == 'application/pdf' or series == 'application/epub':
        return 'Text Content'
    elif series == 'application/vnd.ekstep.html-archive' or series == 'application/vnd.ekstep.h5p-archive':
        return 'Uploaded Interactive Content'
    else:
        return None


def get_tb_content_mapping(result_loc_, date_, content_search_):
    """
     get a list of textbook from LP API and iterate over the textbook hierarchy to create CSV
    :param result_loc_: pathlib.Path object to store resultant CSV at
    :param date_: execution date for function
    :param content_search_: ip and port of the server hosting LP content search API
    """
    content_model = pd.read_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'content_model_snapshot.csv'))
    tb_url = "{}v3/search".format(content_search_)
    payload = """{
                "request": {
                    "filters": {
                        "contentType": ["Textbook"],
                        "status": ["Live"]
                    },
                    "fields": ["childNodes", "identifier", "name"],
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
                ["childNodes", "identifier", "name"]]
            textbooks.columns = ["identifier", "tb_id", "tb_name"]
            textbooks = textbooks.explode('identifier')
            textbooks = textbooks.groupby("identifier").agg(
                {"tb_id": ", ".join, "tb_name": ", ".join}).reset_index()
            content_model = content_model.join(textbooks.set_index('identifier'), how="left", on="identifier")
            content_model.to_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'content_model_snapshot.csv'),
                                 index=False, encoding='utf-8-sig')
            post_data_to_blob(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'content_model_snapshot.csv'),
                              backup=True)
            break
        except requests.exceptions.ConnectionError:
            print("Retry {} for textbook list".format(retry_count))
            time.sleep(10)
    else:
        print("Max retries reached...")


def get_batch_data_from_blob(result_loc_, prefix_, backup=False):
    try:
        account_name = os.environ['AZURE_STORAGE_ACCOUNT']
        account_key = os.environ['AZURE_STORAGE_ACCESS_KEY']
        block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)
        if backup:
            container_name = os.environ['REPORT_BACKUP_CONTAINER']
            blob_list = block_blob_service.list_blobs(container_name, prefix=prefix_)
            for blob in blob_list:
                report_name, date_name, file_name = blob.name.split('/')
                if file_name.startswith('content_consumption_lastweek'):
                    slug = file_name.split('_')[-1].split('.')[0]
                    result_loc_.joinpath(slug).mkdir(exist_ok=True)
                    file_path_ = result_loc_.joinpath(slug, 'content_consumption_lastweek_{}.csv'.format(date_name))
                    block_blob_service.get_blob_to_path(
                        container_name=container_name,
                        blob_name=blob.name,
                        file_path=str(file_path_)
                    )
    except AzureMissingResourceHttpError:
        raise AzureMissingResourceHttpError("Missing resource!", 404)
