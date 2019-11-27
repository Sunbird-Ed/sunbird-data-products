"""
Generate content wise reports for an aggregated, and status wise views
"""
import re
import sys, time
import os
from datetime import date, datetime
from pathlib import Path
from string import Template

import argparse
import numpy as np
import pandas as pd
import requests

util_path = os.path.abspath(os.path.join(__file__, '..', '..', '..', 'util'))
sys.path.append(util_path)

from utils import get_tenant_info, create_json, post_data_to_blob, push_metric_event


def grade_map(series):
    """
    convert the list of grades to string format
    :param series: pandas series
    :return: pandas series
    """
    dict_ = {}
    try:
        for item in series:
            try:
                key = list(map(int, re.findall('\d+', item)))[0]
                dict_[key] = item
            except IndexError:
                if item == 'KG':
                    dict_[0] = item
                elif item == 'Other':
                    dict_[13] = item
                else:
                    dict_[14] = item
        grade = ''
        for key in sorted(list(dict_.keys())):
            grade += dict_[key] + ', '
        grade = grade.rstrip(', ')
    except TypeError:
        grade = np.nan
    return grade


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


def grade_sort(series):
    """
    get an index to sort all the grades by, taking into account multiple grades
    :param series: panadas series
    :return: pandas series
    """
    try:
        result = None
        value = sorted(list(map(int, re.findall('\d+', series))))
        other = re.findall('Other', series)
        if value and other:
            result = value[0]
            if len(value) > 1:
                if value[-1] < 10:
                    result += value[-1] / 10.0
                else:
                    temp = 0.9
                    result += temp + value[-1] / 1000.0
            result += 0.003
        elif value and not other:
            result = value[0]
            if len(value) > 1:
                if value[-1] < 10:
                    result += value[-1] / 10.0
                else:
                    temp = 0.9
                    result += temp + value[-1] / 1000.0
        elif not value and other:
            result = 13
        elif not value and not other:
            result = 14
    except TypeError:
        result = 14
    return result


def date_format(date_):
    """
    date formatting from %y-%m-%dT00:00:00z000 to %y-%m-%d
    :param date_: pandas series
    :return: pandas series
    """
    return date_.split('T')[0]


def get_content_data(tenant_id_, result_loc_, content_search_):
    """
    Query content search API to get resource created for a channel.
    :param tenant_id_: channel id
    :param result_loc_: pathlib.Path object to store resultant CSV at.
    :param content_search_: ip and port for server hosting content search API
    :return: None
    """
    url = "{}v3/search".format(content_search_)
    headers = {
        'content-type': "application/json; charset=utf-8",
        'cache-control': "no-cache"
    }
    payload_template = Template("""{
            "request": {
                "filters":{
                    "status": ["Live","Draft","Review","Unlisted"],
                    "contentType": ["Resource"],
                    "createdFor": "$tenant"
                },
                "fields" :["channel", "identifier", "board", "gradeLevel", "medium", "subject", "status",
                "createdBy", "creator", "lastUpdatedBy", "lastUpdatedOn", "lastSubmittedOn", "lastPublishedBy",
                "lastPublishedOn", "createdFor", "createdOn", "pkgVersion", "versionKey", "contentType", "mimeType",
                "prevState", "resourceType", "attributions"],
                "limit":10000,
                "facets":["status"]
            }
        }""")
    payload = payload_template.substitute(tenant=tenant_id_)
    retry_count = 0
    while retry_count < 5:
        retry_count += 1
        try:
            response = requests.request("POST", url, data=payload, headers=headers).json()
            if response['result']['count'] > 0:
                response_df = pd.DataFrame(response['result']['content'])
                try:
                    response_df['grade'] = response_df['gradeLevel'].apply(grade_map)
                    response_df = response_df.drop(['gradeLevel'], axis=1)
                except KeyError:
                    pass
                try:
                    response_df['content format'] = response_df['mimeType'].apply(mime_type)
                    response_df = response_df.drop(['mimeType'], axis=1)
                except KeyError:
                    pass
                response_df.to_csv(result_loc_.joinpath('data.csv'), index=False, encoding='utf-8')
            break
        except requests.exceptions.ConnectionError:
            print("Retrying. Connection error for ", tenant_id_)


def gen_aggregated_report(result_loc_):
    """
    Generate report aggregated by BMGS, content type and content status.
    :param result_loc_: pathlib.Path object to store resultant CSV at.
    :return: None
    """
    dataframe = pd.read_csv(result_loc_.joinpath('data.csv'), encoding='utf-8')
    required_cols = ['board', 'medium', 'grade', 'subject', 'resourceType', 'status', 'content format']
    for col in required_cols:
        if col not in dataframe.columns:
            dataframe[col] = ''
    r1 = dataframe[['channel', 'identifier', 'board', 'medium', 'grade', 'subject', 'resourceType', 'status',
                    'content format']]
    df1 = r1.dropna(axis=0, how='any')
    report1_g = df1.groupby(['board', 'medium', 'grade', 'subject', 'resourceType'])
    contents = []
    for name, grp in report1_g:
        row = {
            'board': name[0],
            'medium': name[1],
            'grade': name[2],
            'subject': name[3],
            'resourceType': name[4],
            'Total Content': 0
        }
        statuses = grp.groupby(['status']).count()['identifier']
        for ind, item in statuses.iteritems():
            row[ind] = item
            row['Total Content'] += item
        mime_types = grp.groupby(['content format']).count()['identifier']
        for ind, item in mime_types.iteritems():
            row[ind] = item
        contents.append(row)
    df2 = r1[r1[['board', 'medium', 'grade', 'subject']].isnull().any(axis=1)]
    row = {
        'board': 'Metadata missing',
        'medium': 'Metadata missing',
        'grade': 'Metadata missing',
        'subject': 'Metadata missing',
        'resourceType': 'Metadata missing',
        'Total Content': 0
    }
    statuses = df2.groupby(['status']).count()['identifier']
    for ind, item in statuses.iteritems():
        row[ind] = item
        row['Total Content'] += item
    mime_types = df2.groupby(['content format']).count()['identifier']
    for ind, item in mime_types.iteritems():
        row[ind] = item
    contents.append(row)
    report1_df = pd.DataFrame(contents)
    required_cols = ['Draft', 'Live', 'Review', 'Unlisted', 'Created on Diksha', 'YouTube Content',
                     'Uploaded Videos', 'Text Content', 'Uploaded Interactive Content']
    for col in required_cols:
        if col not in report1_df.columns:
            report1_df[col] = 0
    report1_df = report1_df.fillna('0')
    report1_df['gradeSort'] = report1_df['grade'].apply(grade_sort)
    report1_df = report1_df.sort_values(by=['board', 'medium', 'gradeSort', 'subject', 'resourceType'],
                                        ascending=[False, True, True, True, True])
    report1_df = report1_df[
        ['board', 'medium', 'grade', 'subject', 'resourceType', 'Total Content', 'Live', 'Review',
         'Unlisted', 'Draft', 'Created on Diksha', 'YouTube Content', 'Uploaded Videos', 'Text Content',
         'Uploaded Interactive Content']]
    report1_df.columns = ['Board', 'Medium', 'Grade', 'Subject', 'Content Type', 'Total Content', 'Live',
                          'Review', 'Limited Sharing', 'Draft', 'Created on Diksha', 'YouTube Content',
                          'Uploaded Videos', 'Text Content', 'Uploaded Interactive Content']
    report1_df.to_csv(
        result_loc_.parent.parent.joinpath('portal_dashboards', result_loc_.name, 'Content_Progress_Report_1.csv'),
        index=False, encoding='utf-8')
    create_json(
        result_loc_.parent.parent.joinpath('portal_dashboards', result_loc_.name, 'Content_Progress_Report_1.csv'))
    post_data_to_blob(
        result_loc_.parent.parent.joinpath('portal_dashboards', result_loc_.name, 'Content_Progress_Report_1.csv'))


def gen_live_status_report(result_loc_):
    """
    List all live content and their metadata
    :param result_loc_: pathlib.Path object to store resultant CSV at.
    :return: None
    """
    df2 = pd.read_csv(result_loc_.joinpath('data.csv'), encoding='utf-8')
    required_cols = ['board', 'medium', 'grade', 'subject', 'creator', 'identifier', 'resourceType', 'status']
    for col in required_cols:
        if col not in df2.columns:
            df2[col] = ''
    if 'createdOn' not in df2.columns:
        df2['createdOn'] = 'T'
    if 'lastSubmittedOn' not in df2.columns:
        df2['lastSubmittedOn'] = 'T'
    if 'lastPublishedOn' not in df2.columns:
        df2['lastPublishedOn'] = 'T'
    review = df2[df2['status'] == 'Review'][
        ['board', 'medium', 'grade', 'subject', 'identifier', 'resourceType', 'status', 'lastSubmittedOn',
         'createdOn', 'creator']]
    review['createdOn'] = review['createdOn'].fillna('T').apply(date_format)
    review['lastSubmittedOn'] = review['lastSubmittedOn'].fillna('T').apply(date_format)
    review.columns = ['Board', 'Medium', 'Grade', 'Subject', 'Content ID', 'Content Type', 'Status',
                      'Pending in current status since', 'Creation Date', 'Created By']
    draft = df2[df2['status'] == 'Draft']
    draft1 = draft[draft['lastPublishedOn'].isna()]
    draft1['createdOn'] = draft1['createdOn'].fillna('T').apply(date_format)
    draft1.loc[:, 'Pending in current status since'] = draft1.loc[:, 'createdOn']
    draft1 = draft1[['board', 'medium', 'grade', 'subject', 'identifier', 'resourceType', 'status',
                     'Pending in current status since', 'createdOn', 'creator']]
    draft1.columns = ['Board', 'Medium', 'Grade', 'Subject', 'Content ID', 'Content Type', 'Status',
                      'Pending in current status since', 'Creation Date', 'Created By']
    draft2 = draft.dropna(subset=['lastPublishedOn'])
    draft2['createdOn'] = draft2['createdOn'].fillna('T').apply(date_format)
    draft2['lastPublishedOn'] = draft2['lastPublishedOn'].fillna('T').apply(date_format)
    draft2 = draft2[
        ['board', 'medium', 'grade', 'subject', 'identifier', 'resourceType', 'status', 'lastPublishedOn',
         'createdOn', 'creator']]
    draft2.columns = ['Board', 'Medium', 'Grade', 'Subject', 'Content ID', 'Content Type', 'Status',
                      'Pending in current status since', 'Creation Date', 'Created By']
    limited_sharing = df2[df2['status'] == 'Unlisted']
    limited_sharing['status'] = 'Limited Sharing'
    limited_sharing['createdOn'] = limited_sharing['createdOn'].fillna('T').apply(date_format)
    limited_sharing['lastPublishedOn'] = limited_sharing['lastPublishedOn'].fillna('T').apply(date_format)
    limited_sharing = limited_sharing[
        ['board', 'medium', 'grade', 'subject', 'identifier', 'resourceType', 'status', 'lastPublishedOn',
         'createdOn', 'creator']]
    limited_sharing.columns = ['Board', 'Medium', 'Grade', 'Subject', 'Content ID', 'Content Type', 'Status',
                               'Pending in current status since', 'Creation Date', 'Created By']
    report2_df = review.append(draft1, ignore_index=True)
    report2_df = report2_df.append(draft2, ignore_index=True)
    report2_df = report2_df.append(limited_sharing, ignore_index=True)
    report2_df['gradeSort'] = report2_df['Grade'].apply(grade_sort)
    report2_df = report2_df.sort_values(
        by=['Board', 'Medium', 'gradeSort', 'Subject', 'Content Type', 'Status'],
        ascending=[False, True, True, True, True, True])
    report2_df = report2_df[
        ['Board', 'Medium', 'Grade', 'Subject', 'Content ID', 'Content Type', 'Status',
         'Pending in current status since', 'Creation Date', 'Created By']]
    report2_df = report2_df.fillna('')
    report2_df.to_csv(
        result_loc_.parent.parent.joinpath('portal_dashboards', result_loc_.name, 'Content_Progress_Report_2.csv'),
        index=False, encoding='utf-8')
    create_json(
        result_loc_.parent.parent.joinpath('portal_dashboards', result_loc_.name, 'Content_Progress_Report_2.csv'))
    post_data_to_blob(
        result_loc_.parent.parent.joinpath('portal_dashboards', result_loc_.name, 'Content_Progress_Report_2.csv'))


def gen_non_live_status_report(result_loc_):
    """
    List all content that are in Review, Draft or Unlisted status and their metadata.
    :param result_loc_: pathlib.Path object to store resultant CSV at.
    :return: None
    """
    df3 = pd.read_csv(result_loc_.joinpath('data.csv'), encoding='utf-8')
    required_cols = ['board', 'medium', 'grade', 'subject', 'identifier', 'resourceType', 'creator']
    for col in required_cols:
        if col not in df3.columns:
            df3[col] = ''
    report3_df = df3[df3['status'] == 'Live']
    if 'pkgVersion' not in report3_df.columns:
        report3_df['pkgVersion'] = 0
    if 'lastPublishedOn' not in report3_df.columns:
        report3_df['lastPublishedOn'] = 'T'
    if 'createdOn' not in report3_df.columns:
        report3_df['createdOn'] = 'T'
    report3_df = report3_df[
        ['board', 'medium', 'grade', 'subject', 'identifier', 'resourceType', 'createdOn', 'pkgVersion',
         'creator', 'lastPublishedOn']]
    report3_df['createdOn'] = report3_df['createdOn'].fillna('T').apply(date_format)
    report3_df['lastPublishedOn'] = report3_df['lastPublishedOn'].fillna('T').apply(date_format)
    report3_df.columns = ['Board', 'Medium', 'Grade', 'Subject', 'Content ID', 'Content Type', 'Creation Date',
                          'Number of times Published', 'Created By', 'Latest Publish Date']
    report3_df['gradeSort'] = report3_df['Grade'].apply(grade_sort)
    report3_df = report3_df.sort_values(by=['Board', 'Medium', 'gradeSort', 'Subject', 'Content Type'],
                                        ascending=[False, True, True, True, True])
    report3_df = report3_df.fillna('')
    report3_df = report3_df[['Board', 'Medium', 'Grade', 'Subject', 'Content ID', 'Content Type',
                             'Creation Date', 'Number of times Published', 'Created By', 'Latest Publish Date']]
    report3_df.to_csv(
        result_loc_.parent.parent.joinpath('portal_dashboards', result_loc_.name, 'Content_Progress_Report_3.csv'),
        index=False, encoding='utf-8')
    create_json(
        result_loc_.parent.parent.joinpath('portal_dashboards', result_loc_.name, 'Content_Progress_Report_3.csv'))
    post_data_to_blob(
        result_loc_.parent.parent.joinpath('portal_dashboards', result_loc_.name, 'Content_Progress_Report_3.csv'))

start_time_sec = int(round(time.time()))
parser = argparse.ArgumentParser()
parser.add_argument("data_store_location", type=str, help="data folder location")
parser.add_argument("org_search", type=str, help="host address for Org API")
parser.add_argument("content_search", type=str, help="host address for API")
parser.add_argument("-execution_date", type=str, default=date.today().strftime("%d/%m/%Y"),
                    help="DD/MM/YYYY, optional argument for backfill jobs")
args = parser.parse_args()
org_search = args.org_search
content_search = args.content_search
result_loc = Path(args.data_store_location)
execution_date = datetime.strptime(args.execution_date, "%d/%m/%Y")
get_tenant_info(result_loc_=result_loc, org_search_=org_search, date_=execution_date)
board_slug = pd.read_csv(result_loc.joinpath(execution_date.strftime('%Y-%m-%d'), 'tenant_info.csv'))
result_loc.joinpath('content_progress').mkdir(exist_ok=True)
result_loc.joinpath('portal_dashboards').mkdir(exist_ok=True)
for ind_, row_ in board_slug.iterrows():
    result_loc.joinpath('content_progress', row_['slug']).mkdir(exist_ok=True)
    get_content_data(tenant_id_=row_['id'], result_loc_=result_loc.joinpath('content_progress', row_['slug']),
                     content_search_=content_search)
    if result_loc.joinpath('content_progress', row_['slug'], 'data.csv').exists():
        result_loc.joinpath('portal_dashboards', row_['slug']).mkdir(exist_ok=True)
        gen_aggregated_report(result_loc_=result_loc.joinpath('content_progress', row_['slug']))
        gen_live_status_report(result_loc_=result_loc.joinpath('content_progress', row_['slug']))
        gen_non_live_status_report(result_loc_=result_loc.joinpath('content_progress', row_['slug']))

end_time_sec = int(round(time.time()))
time_taken = end_time_sec - start_time_sec
metrics = [
    {
        "metric": "timeTakenSecs",
        "value": time_taken
    },
    {
        "metric": "date",
        "value": datetime.strptime(args.execution_date, "%Y-%m-%d")
    }
]
push_metric_event(metrics, "Content Progress")