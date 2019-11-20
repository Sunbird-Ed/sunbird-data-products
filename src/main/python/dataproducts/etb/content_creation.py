"""
Generate a report that specifies number of contents created in last week and overall across Live, Review and Draft.
"""
from datetime import date, timedelta, datetime
from pathlib import Path
from string import Template
from time import sleep

import argparse
import pandas as pd
import requests

from src.main.python.util.utils import get_tenant_info, create_json, post_data_to_blob


def weekly_creation(result_loc_, content_search_, date_):
    """
    Calculate number of contents created in the last week.
    :param result_loc_: pathlib.Path object to store the resultant csv at.
    :param content_search_: ip and port for service hosting content search API
    :param date_: datetime object to use in query as well as path
    :return: None
    """
    url = "{}v3/search".format(content_search_)
    payload_template = Template("""{
            "request": {
                "filters":{
                    "status": ["Live","Draft","Review"],
                    "contentType": ["Resource","Collection"],
                    "createdFor": "$tenant",
                    "createdOn": {">=":"$start_datetime", "<":"$end_datetime"}
                },
                "fields" :["name","createdBy","contentType","resourceType","mimeType","createdOn","createdFor"],
                "limit":10000,
                "sortBy": {"createdOn":"desc"},
                "exists": "createdFor",
                "facets":["status"]
            }
        }""")
    headers = {
        'content-type': "application/json; charset=utf-8",
        'cache-control': "no-cache"
    }
    final_dict = {'review': 0, 'draft': 0, 'live': 0}
    tenant_list = []
    tenant_name_mapping = pd.read_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'tenant_info.csv'))
    for tenant in tenant_name_mapping.id.unique():
        retry_count = 0
        while retry_count < 5:
            retry_count += 1
            try:
                payload = payload_template.substitute(tenant=tenant,
                                                      start_datetime=(date_ - timedelta(days=7)).strftime(
                                                          '%Y-%m-%dT00:00:00.000+0000'),
                                                      end_datetime=date_.strftime('%Y-%m-%dT00:00:00.000+0000'))
                response = requests.request("POST", url, data=payload, headers=headers)
                list_of_dicts = response.json()['result']['facets'][0]['values']
                tenant_wise = {'tenant': tenant}
                for single_val in list_of_dicts:
                    final_dict[single_val['name']] = final_dict[single_val['name']] + single_val['count']
                    tenant_wise[single_val['name']] = single_val['count']
                tenant_list.append(tenant_wise)
                break
            except requests.exceptions.ConnectionError:
                print(tenant, "Connection error.. retrying.")
                sleep(10)
            except KeyError:
                with open(result_loc_.joinpath('content_creation_week_errors.log'), 'a') as f:
                    f.write('KeyError: Response value erroneous for {}\n'.format(tenant))
                break
        else:
            with open(result_loc_.joinpath('content_creation_week_errors.log'), 'a') as f:
                f.write('ConnectionError: Max retries reached for {}\n'.format(tenant))
    result = pd.DataFrame(tenant_list).fillna(0)
    result_loc_.joinpath(date_.strftime('%Y-%m-%d')).mkdir(exist_ok=True)
    result.to_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'week.csv'), header=True, index=False)


def overall_creation(result_loc_, content_search_, date_):
    """
    Calculate number of contents created since beginning.
    :param result_loc_: pathlib.Path object to store the resultant csv at.
    :param content_search_: ip and port for service hosting content search API
    :param date_: datetime object to use in query as well as path
    :return: None
    """
    url = "{}v3/search".format(content_search_)
    payload_template = Template("""{
            "request": {
                "filters":{
                    "status": ["Live","Draft","Review"],
                    "contentType": ["Resource","Collection"],
                    "createdFor": "$tenant",
                    "createdOn": {"<":"$end_datetime"}
                },
                "fields" :["name","createdBy","contentType","resourceType","mimeType","createdOn","createdFor"],
                "limit":10000,
                "sortBy": {"createdOn":"desc"},
                "exists": "createdFor",
                "facets":["status"]
            }
        }""")
    headers = {
        'content-type': "application/json; charset=utf-8",
        'cache-control': "no-cache",
        'postman-token': "ea92fb87-996b-42d0-b86b-4ec2029bebb7"
    }
    final_dict = {'review': 0, 'draft': 0, 'live': 0}
    tenant_list = []
    tenant_name_mapping = pd.read_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'tenant_info.csv'))
    for tenant in tenant_name_mapping.id.unique():
        retry_count = 0
        while retry_count < 5:
            retry_count += 1
            try:
                payload = payload_template.substitute(tenant=tenant,
                                                      end_datetime=date_.strftime('%Y-%m-%dT00:00:00.000+0000'))
                response = requests.request("POST", url, data=payload, headers=headers)
                list_of_dicts = response.json()['result']['facets'][0]['values']
                tenant_wise = {'tenant': tenant}
                for single_val in list_of_dicts:
                    final_dict[single_val['name']] = final_dict[single_val['name']] + single_val['count']
                    tenant_wise[single_val['name']] = single_val['count']
                tenant_list.append(tenant_wise)
                break
            except requests.exceptions.ConnectionError:
                print(tenant, "Connection error.. retrying.")
                sleep(10)
            except KeyError:
                with open(result_loc_.joinpath('content_creation_overall_errors.log'), 'a') as f:
                    f.write('KeyError: Response erroneous for {}\n'.format(tenant))
                break
        else:
            with open(result_loc_.joinpath('content_creation_overall_errors.log'), 'a') as f:
                f.write('ConnectionError: Max retries reached for {}\n'.format(tenant))
    result = pd.DataFrame(tenant_list).fillna(0)
    result_loc_.joinpath(date_.strftime('%Y-%m-%d')).mkdir(exist_ok=True)
    result.to_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'overall.csv'), header=True, index=False)


def combine_creation_reports(result_loc_, date_):
    """
    Combine weekly and overall numbers.
    :param result_loc_: pathlib.Path object to store the resultant csv at.
    :param date_: datetime object to use in query as well as path
    :return: None
    """
    tenant_name_mapping = pd.read_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'tenant_info.csv'))
    _start_date = date_ - timedelta(days=7)
    week = pd.read_csv(result_loc_.joinpath(date_.strftime(format('%Y-%m-%d')), 'week.csv'))
    overall = pd.read_csv(result_loc_.joinpath(date_.strftime(format('%Y-%m-%d')), 'overall.csv'))
    while True:
        try:
            week['total'] = week['draft'] + week['live'] + week['review']
            break
        except KeyError as ke:
            week[ke.args[0]] = 0
    while True:
        try:
            overall['total'] = overall['draft'] + overall['live'] + overall['review']
            break
        except KeyError as ke:
            overall[ke.args[0]] = 0
    week = week.set_index('tenant')
    overall = overall.set_index('tenant')
    week_transpose = week.transpose()
    overall_transpose = overall.transpose()
    for ind_, row_ in tenant_name_mapping.iterrows():
        try:
            week_numbers = week_transpose[row_['id']]
            overall_numbers = overall_transpose[row_['id']]
            final_df = pd.concat([week_numbers, overall_numbers], axis=1)
            final_df.index.name = 'Content Status'
            final_df.columns = ['Week starting {}'.format(_start_date.strftime('%d %B')),
                                'As on {}'.format(date_.strftime('%d %B'))]
            final_df.to_csv(
                result_loc_.joinpath(date_.strftime('%Y-%m-%d'), '{}_Content_Status.csv'.format(row_['slug'])))
            final_df.index.name = 'Status'
            final_df.columns = ['Status over last week: starting {}'.format(_start_date.strftime('%d %B')),
                                'Status from the beginning']
            result_loc_.parent.joinpath('portal_dashboards', row_['slug']).mkdir(exist_ok=True)
            final_df.to_csv(result_loc_.parent.joinpath('portal_dashboards', row_['slug'], 'content_creation.csv'))
            create_json(result_loc_.parent.joinpath('portal_dashboards', row_['slug'], 'content_creation.csv'))
            post_data_to_blob(result_loc_.parent.joinpath('portal_dashboards', row_['slug'], 'content_creation.csv'))
        except KeyError as ke:
            print(row_['id'], ke)


parser = argparse.ArgumentParser()
parser.add_argument("data_store_location", type=str, help="data folder location")
parser.add_argument("org_search", type=str, help="host address for Org API")
parser.add_argument("content_search", type=str, help="host address for Content Search API")
parser.add_argument("-execution_date", type=str, default=date.today().strftime("%d/%m/%Y"),
                    help="DD/MM/YYYY, optional argument for backfill jobs")
args = parser.parse_args()
result_loc = Path(args.data_store_location).joinpath('content_creation')
org_search = args.org_search
content_search = args.content_search
execution_date = datetime.strptime(args.execution_date, "%d/%m/%Y")
get_tenant_info(result_loc_=result_loc, org_search_=org_search, date_=execution_date)
weekly_creation(result_loc_=result_loc, date_=execution_date, content_search_=content_search)
overall_creation(result_loc_=result_loc, date_=execution_date, content_search_=content_search)
combine_creation_reports(result_loc_=result_loc, date_=execution_date)
