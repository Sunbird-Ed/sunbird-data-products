"""
Generate ETB and Live QR Exception Report
"""
import sys
import time
from datetime import datetime
from pathlib import Path

import argparse
import pandas as pd
import requests
from anytree.importer import DictImporter
from anytree.search import findall

sys.path.append(Path(__file__).parent.parent.parent.parent.parent.parent)

from src.main.python.util.utils import create_json, post_data_to_blob, get_tenant_info, get_scan_counts, push_metric_event


def parse_etb(tb, row_):
    """
    Parse the textbook structure recursively for all TBUnits (chapters/ topics/ subtopics)
    :param tb: dictionary object representation of textbook
    :param row_: textbook metadata
    :return: list representation of that node of textbook
    """
    temp = {
        'tb_id': row_['identifier'],
        'board': row_['board'],
        'channel': row_['channel'],
        'medium': row_['medium'],
        'gradeLevel': row_['gradeLevel'],
        'subject': row_['subject'],
        'tb_name': row_['name'],
        'status': row_['status'],
        'identifier': tb['identifier'],
        'name': tb['name'],
        'contentType': tb['contentType']
    }
    try:
        temp['leafNodesCount'] = tb['leafNodesCount']
    except KeyError:
        temp['leafNodesCount'] = 0
    try:
        temp['dialcode'] = tb['dialcodes'][0]
    except KeyError:
        temp['dialcode'] = ''
    temp['children'] = []
    if ('children' in tb.keys()) and tb['children']:
        for child in tb['children']:
            if child['contentType'] == 'TextBookUnit':
                temp['children'].append(parse_etb(child, row_))
    if not temp['children']:
        del temp['children']
    return temp


def grade_sort(grades):
    """
    Grade in TB metadata is present as an array of string. In order to sort on grade, define the order
    :param grades: array of string
    :return: number used for sorting
    """
    global backend_grade
    minimum = 14
    for grade in grades:
        if grade != 'KG':
            grade = grade.title()
        if minimum > backend_grade.loc[grade]['number']:
            minimum = backend_grade.loc[grade]['number']
    return minimum


def grade_fix(df):
    """
    format the gradeLevel column. split multiple grades into separate rows.
    :param df: dataframe with gradeLevel column that needs formatting
    :return: dataframe with grade formatted
    """
    x = pd.DataFrame(df.Grade.str.split(', ').tolist(), index=df.index).stack()
    x.index = x.index.droplevel(-1)
    x.name = 'Class'
    df = df.join(x)
    df = df.drop('Grade', axis=1).groupby('Class').sum().join(backend_grade, how='left').sort_values(
        'number').reset_index().drop('number', axis=1)
    return df


def etb_dialcode(row, nodes, dialcode_level):
    """
    generate the ETB detailed report.
    :param row: textbook information
    :param nodes: nodes that are either leaf or qr linked or both
    :param dialcode_level: list of required details of nodes
    :return: dialcode_level
    """
    for node in nodes:
        result = {'Textbook ID': row['identifier'], 'Medium': row['medium'], 'Grade': row['grade'],
                  'Subject': row['subject'], 'Textbook Name': row['name'], 'Textbook Status': row['status'],
                  'Level 1 Name': '', 'Level 2 Name': '', 'Level 3 Name': '', 'Level 4 Name': '', 'Level 5 Name': '',
                  'channel': row['createdFor'][0], 'grade_sort': grade_sort(row['gradeLevel']),
                  'Type of Node': 'Leaf Node & QR Linked' if (
                          node.dialcode != '' and node.is_leaf) else 'Leaf Node' if node.is_leaf else 'QR Linked' if
                  node.dialcode != '' else ''}
        level = 0
        for path in node.path:
            result['Level {} Name'.format(level)] = path.name
            level += 1
        result['Number of contents'] = node.leafNodesCount
        result['QR Code'] = node.dialcode
        dialcode_level.append(result)


def etb_textbook(row, node, textbook_level):
    """
    ETB Textbook level report
    :param row: textbook information
    :param node: root node of textbook
    :param textbook_level: list of required details about textbooks
    :return: textbook_level
    """
    result = {'Textbook ID': row['identifier'], 'Medium': row['medium'], 'Grade': row['grade'],
              'Subject': row['subject'], 'Textbook Name': row['name'], 'Textbook Status': row['status'],
              'Created On': '/'.join(row['createdOn'].split('T')[0].split('-')[::-1]),
              'Last Updated On': '/'.join(row['lastUpdatedOn'].split('T')[0].split('-')[::-1]),
              'channel': row['createdFor'][0], 'grade_sort': grade_sort(row['gradeLevel']),
              'Total content linked': node.leafNodesCount, 'Total QR codes linked to content': len(
            findall(node, filter_=lambda node_: node_.leafNodesCount > 0 and node_.dialcode != '')),
              'Total number of QR codes with no linked content': len(
                  findall(node, filter_=lambda node_: node_.leafNodesCount == 0 and node_.dialcode != '')),
              'Total number of leaf nodes': len(findall(node, filter_=lambda node_: node_.is_leaf)),
              'Number of leaf nodes with no content': len(
                  findall(node, filter_=lambda node_: node_.is_leaf and node_.leafNodesCount == 0)),
              'With QR codes': 'With QR Code' if len(
                  findall(node, filter_=lambda node_: node_.dialcode != '')) > 0 else 'Without QR Code'}
    textbook_level.append(result)


def etb_aggregates(result_loc_, slug, df):
    """
    generate charts from ETB data
    :param result_loc_: pathlib.Path object for resultant CSVs.
    :param slug: slug for the channel
    :param df: ETB textbook dataframe for channel
    :return: None
    """
    textbook_status = pd.DataFrame(df['Textbook Status'].value_counts()).reindex(
        ['Live', 'Review', 'Draft']).reset_index().fillna(0)
    textbook_status.columns = ['Status', 'Count']
    textbook_status.to_csv(result_loc_.joinpath('portal_dashboards', slug, 'etb_textbook_status.csv'), index=False)
    create_json(result_loc_.joinpath('portal_dashboards', slug, 'etb_textbook_status.csv'))
    post_data_to_blob(result_loc_.joinpath('portal_dashboards', slug, 'etb_textbook_status.csv'))
    textbook_status_grade = pd.DataFrame(
        df.groupby(['Grade', 'Textbook Status'])['Textbook ID'].count()
    ).reset_index().pivot(index='Grade', columns='Textbook Status').fillna(0).reset_index()
    columns = ['Grade']
    for column in textbook_status_grade.columns[1:]:
        columns.append(column[1])
    textbook_status_grade.columns = columns
    textbook_status_grade = grade_fix(textbook_status_grade)
    statuses = ['Live', 'Review', 'Draft']
    column_order = ['Class']
    for status in statuses:
        if status not in textbook_status_grade.columns:
            textbook_status_grade[status] = 0
    textbook_status_grade = textbook_status_grade[column_order + statuses]
    textbook_status_grade.to_csv(result_loc_.joinpath('portal_dashboards', slug, 'etb_textbook_status_grade.csv'),
                                 index=False)
    create_json(result_loc_.joinpath('portal_dashboards', slug, 'etb_textbook_status_grade.csv'))
    post_data_to_blob(result_loc_.joinpath('portal_dashboards', slug, 'etb_textbook_status_grade.csv'))
    textbook_status_subject = pd.DataFrame(
        df.groupby(['Subject', 'Textbook Status'])['Textbook ID'].count()
    ).reset_index().pivot(index='Subject', columns='Textbook Status').fillna(0).reset_index()
    columns = ['Subject']
    for column in textbook_status_subject.columns[1:]:
        columns.append(column[1])
    textbook_status_subject.columns = columns
    column_order = ['Subject']
    for status in statuses:
        if status not in textbook_status_subject.columns:
            textbook_status_subject[status] = 0
    textbook_status_subject = textbook_status_subject[column_order + statuses]
    textbook_status_subject.to_csv(result_loc_.joinpath('portal_dashboards', slug, 'etb_textbook_status_subject.csv'),
                                   index=False)
    create_json(result_loc_.joinpath('portal_dashboards', slug, 'etb_textbook_status_subject.csv'))
    post_data_to_blob(result_loc_.joinpath('portal_dashboards', slug, 'etb_textbook_status_subject.csv'))
    qr_counts = pd.DataFrame(df.groupby(['channel', 'With QR codes'])['Textbook ID'].count()).reset_index().drop(
        'channel', axis=1)
    qr_counts.columns = ['Status', 'Count']
    qr_counts.to_csv(result_loc_.joinpath('portal_dashboards', slug, 'etb_qr_count.csv'), index=False)
    create_json(result_loc_.joinpath('portal_dashboards', slug, 'etb_qr_count.csv'))
    post_data_to_blob(result_loc_.joinpath('portal_dashboards', slug, 'etb_qr_count.csv'))
    qr_linkage = df[['Total QR codes linked to content', 'Total number of QR codes with no linked content']].sum()
    qr_linkage.index = ['QR Code With Content', 'QR Code Without Content']
    qr_linkage = pd.DataFrame(qr_linkage).reset_index()
    qr_linkage.columns = ['Status', 'Count']
    qr_linkage.to_csv(result_loc_.joinpath('portal_dashboards', slug, 'etb_qr_content_status.csv'), index=False)
    create_json(result_loc_.joinpath('portal_dashboards', slug, 'etb_qr_content_status.csv'))
    post_data_to_blob(result_loc_.joinpath('portal_dashboards', slug, 'etb_qr_content_status.csv'))
    qr_linkage_grade = df.groupby('Grade')[
        ['Total QR codes linked to content', 'Total number of QR codes with no linked content']].sum().reset_index()
    qr_linkage_grade.columns = ['Grade', 'QR Codes with content', 'QR Codes without content']
    qr_linkage_grade = grade_fix(qr_linkage_grade)
    qr_linkage_grade.to_csv(result_loc_.joinpath('portal_dashboards', slug, 'etb_qr_content_status_grade.csv'),
                            index=False)
    create_json(result_loc_.joinpath('portal_dashboards', slug, 'etb_qr_content_status_grade.csv'))
    post_data_to_blob(result_loc_.joinpath('portal_dashboards', slug, 'etb_qr_content_status_grade.csv'))
    qr_linkage_subject = df.groupby('Subject')[
        ['Total QR codes linked to content', 'Total number of QR codes with no linked content']].sum().reset_index()
    qr_linkage_subject.columns = ['Class', 'QR Codes with content', 'QR Codes without content']
    qr_linkage_subject.to_csv(result_loc_.joinpath('portal_dashboards', slug, 'etb_qr_content_status_subject.csv'),
                              index=False)
    create_json(result_loc_.joinpath('portal_dashboards', slug, 'etb_qr_content_status_subject.csv'))
    post_data_to_blob(result_loc_.joinpath('portal_dashboards', slug, 'etb_qr_content_status_subject.csv'))


def dce_dialcode(row, nodes, dialcode_level):
    """
    generate DCE detailed report
    :param row: textbook information
    :param nodes: dialcodes with leafNodesCount = 0
    :param dialcode_level: list of required details about detailed report
    :return: dialcode_level
    """
    for node in nodes:
        result = {'Textbook ID': row['identifier'], 'Medium': row['medium'], 'Grade': row['grade'],
                  'Subject': row['subject'], 'Textbook Name': row['name'], 'Level 1 Name': '', 'Level 2 Name': '',
                  'Level 3 Name': '', 'Level 4 Name': '', 'Level 5 Name': '', 'channel': row['createdFor'][0],
                  'grade_sort': grade_sort(row['gradeLevel'])}
        level = 0
        for path in node.path:
            result['Level {} Name'.format(level)] = path.name
            level += 1
        result['QR Code'] = node.dialcode
        result['Term'] = node.term
        dialcode_level.append(result)


def dce_textbook(row, node, textbook_level):
    """
    generate DCE Textbook level report
    :param row: textbook information
    :param node: textbook tree structure
    :param textbook_level: list of details about the textbook
    :return: textbook_level
    """
    result = {'Textbook ID': row['identifier'], 'Medium': row['medium'], 'Grade': row['grade'],
              'Subject': row['subject'], 'Textbook Name': row['name'],
              'Created On': '/'.join(row['createdOn'].split('T')[0].split('-')[::-1]),
              'Last Updated On': '/'.join(row['lastUpdatedOn'].split('T')[0].split('-')[::-1]),
              'channel': row['createdFor'][0], 'grade_sort': grade_sort(row['gradeLevel']),
              'Total number of QR codes': len(findall(node, filter_=lambda node_: node_.dialcode != '')),
              'Number of QR codes with atleast 1 linked content': len(
                  findall(node, filter_=lambda node_: node_.leafNodesCount > 0 and node_.dialcode != '')),
              'Number of QR codes with no linked content': len(
                  findall(node, filter_=lambda node_: node_.leafNodesCount == 0 and node_.dialcode != '')),
              'Term 1 QR Codes with no linked content': len(
                  findall(node, filter_=lambda
                      node_: node_.leafNodesCount == 0 and node_.dialcode != '' and node_.term == 'T1')),
              'Term 2 QR Codes with no linked content': len(
                  findall(node, filter_=lambda
                      node_: node_.leafNodesCount == 0 and node_.dialcode != '' and node_.term == 'T2'))}
    textbook_level.append(result)


def dce_aggregates(result_loc_, slug, df):
    """
    generate charts from DCE textbook data.
    :param result_loc_: pathlib.Path object with path to store resultant CSVs.
    :param slug: slug name for channel
    :param df: DCE textbook dataframe for the channel
    :return: None
    """
    qr_linked = df[
        ['Number of QR codes with atleast 1 linked content', 'Number of QR codes with no linked content']].sum()
    qr_linked.index = ['QR Code With Content', 'QR Code Without Content']
    qr_linked = pd.DataFrame(qr_linked).reset_index()
    qr_linked.columns = ['Status', 'Count']
    qr_linked.to_csv(result_loc_.joinpath('portal_dashboards', slug, 'dce_qr_content_status.csv'), index=False)
    create_json(result_loc_.joinpath('portal_dashboards', slug, 'dce_qr_content_status.csv'))
    post_data_to_blob(result_loc_.joinpath('portal_dashboards', slug, 'dce_qr_content_status.csv'))
    qr_linked_by_grade = df.groupby('Grade')[['Number of QR codes with atleast 1 linked content',
                                              'Number of QR codes with no linked content']].sum().reset_index()
    qr_linked_by_grade.columns = ['Grade', 'QR Codes with content', 'QR Codes without content']
    qr_linked_by_grade = grade_fix(qr_linked_by_grade)
    qr_linked_by_grade.to_csv(result_loc_.joinpath('portal_dashboards', slug, 'dce_qr_content_status_grade.csv'),
                              index=False)
    create_json(result_loc_.joinpath('portal_dashboards', slug, 'dce_qr_content_status_grade.csv'))
    post_data_to_blob(result_loc_.joinpath('portal_dashboards', slug, 'dce_qr_content_status_grade.csv'))
    qr_linked_by_subject = df.groupby('Subject')[['Number of QR codes with atleast 1 linked content',
                                                  'Number of QR codes with no linked content']].sum().reset_index()
    qr_linked_by_subject.columns = ['Subject', 'QR Codes with content', 'QR Codes without content']
    qr_linked_by_subject.to_csv(result_loc_.joinpath('portal_dashboards', slug, 'dce_qr_content_status_subject.csv'),
                                index=False)
    create_json(result_loc_.joinpath('portal_dashboards', slug, 'dce_qr_content_status_subject.csv'))
    post_data_to_blob(result_loc_.joinpath('portal_dashboards', slug, 'dce_qr_content_status_subject.csv'))


def generate_reports(result_loc_, content_search_, content_hierarchy_, date_):
    """
    generate the overall ETB and DCE reports at textbook and detailed levels
    :param hostname:IP and port to query the list of textbooks and hierarchy
    :param result_loc_: location to store data
    :return: None
    """
    importer = DictImporter()
    dialcode_etb = []
    textbook_etb = []
    dialcode_dce = []
    textbook_dce = []
    scans_df = pd.read_csv(result_loc_.joinpath('textbook_reports', 'dialcode_counts.csv'))
    scans_df = scans_df.groupby('edata_filters_dialcodes')['Total Scans'].sum()
    tb_url = "{}v3/search".format(content_search_)
    payload = """{
            "request": {
                "filters": {
                    "contentType": ["Textbook"],
                    "status": ["Live", "Review", "Draft"]
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
        try:
            response = requests.request("POST", tb_url, data=payload, headers=tb_headers)
            textbooks = pd.DataFrame(response.json()['result']['content'])[
                ['identifier', 'createdFor', 'createdOn', 'lastUpdatedOn', 'board', 'medium', 'gradeLevel', 'subject',
                 'name', 'status', 'channel']]
            textbooks[textbooks.duplicated(subset=['identifier', 'status'])].to_csv(
                result_loc_.joinpath('textbook_reports', date_.strftime('%Y-%m-%d'), 'duplicate_tb.csv'), index=False)
            textbooks.drop_duplicates(subset=['identifier', 'status'], inplace=True)
            textbooks['gradeLevel'] = textbooks['gradeLevel'].apply(lambda x: ['Unknown'] if type(x) == float else x)
            textbooks.fillna({'createdFor': ' '}, inplace=True)
            textbooks.fillna('Unknown', inplace=True)
            textbooks['grade'] = textbooks['gradeLevel'].apply(
                lambda grade: ', '.join([y if y == 'KG' else y.title() for y in grade]))
            textbooks.to_csv(result_loc_.joinpath('textbook_reports', date_.strftime('%Y-%m-%d'), 'tb_list.csv'),
                             index=False)
            break
        except requests.exceptions.ConnectionError:
            print("Retry {} for textbook list".format(retry_count + 1))
            retry_count += 1
            time.sleep(10)
    else:
        print("Max retries reached...")
        return
    counter = 0
    skipped_tbs = []
    for ind_, row_ in textbooks.iterrows():
        counter += 1
        print('Running for {} out of {}: {}% ({} sec/it)'.format(counter, textbooks.shape[0],
                                                                 '%.2f' % (counter * 100 / textbooks.shape[0]),
                                                                 '%.2f' % ((datetime.now() - start_time).total_seconds()
                                                                           / counter)))
        if isinstance(row_['gradeLevel'], list) and len(row_['gradeLevel']) == 0:
            row_['gradeLevel'].append(' ')
        if row_['status'] == 'Live':
            url = "{}learning-service/content/v3/hierarchy/{}".format(content_hierarchy_, row_['identifier'])
        else:
            url = "{}learning-service/content/v3/hierarchy/{}?mode=edit".format(content_hierarchy_, row_['identifier'])
        retry_count = 0
        while retry_count < 5:
            try:
                response = requests.get(url)
                tb = response.json()['result']['content']
                tree_obj = parse_etb(tb, row_)
                root = importer.import_(tree_obj)
                etb_dialcode(row_, (root,) + root.descendants, dialcode_etb)
                etb_textbook(row_, root, textbook_etb)
                if row_['status'] == 'Live':
                    chapters = findall(root, filter_=lambda node: node.depth == 1)
                    for i in range(len(chapters)):
                        term = 'T1' if i <= (len(chapters) / 2) else 'T2'
                        chapters[i].term = term
                        for descendant in chapters[i].descendants:
                            descendant.term = term
                    root.term = 'T1'
                    dialcode_wo_content = findall(root,
                                                  filter_=lambda node: node.dialcode != '' and node.leafNodesCount == 0)
                    dce_dialcode(row_, dialcode_wo_content, dialcode_dce)
                    dce_textbook(row_, root, textbook_dce)
                break
            except requests.exceptions.ConnectionError:
                retry_count += 1
                print("ConnectionError: Retry {} for textbook {}".format(retry_count, row_['identifier']))
                time.sleep(10)
            except KeyError:
                with open(result_loc_.joinpath('textbook_reports', date_.strftime('%Y-%m-%d'), 'etb_error_log.log'),
                          'a') as f:
                    f.write("KeyError: Resource not found for textbook {} in {}\n".format(row_['identifier'],
                                                                                          row_['status']))
                break
        else:
            print("Max retries reached...")
            continue
        if retry_count == 5:
            skipped_tbs.append(row_)
            continue
        if response.status_code != 200:
            continue

    etb_dc = pd.DataFrame(dialcode_etb)
    etb_dc.to_csv(result_loc_.joinpath('textbook_reports', date_.strftime('%Y-%m-%d'), 'ETB_dialcode_data_pre.csv'),
                  index=False, encoding='utf-8-sig')
    post_data_to_blob(result_loc_.joinpath('textbook_reports', date_.strftime('%Y-%m-%d'), 'ETB_dialcode_data_pre.csv'),
                      backup=True)
    etb_tb = pd.DataFrame(textbook_etb).fillna('')
    etb_tb.to_csv(result_loc_.joinpath('textbook_reports', date_.strftime('%Y-%m-%d'), 'ETB_textbook_data_pre.csv'),
                  index=False, encoding='utf-8-sig')
    post_data_to_blob(result_loc_.joinpath('textbook_reports', date_.strftime('%Y-%m-%d'), 'ETB_textbook_data_pre.csv'),
                      backup=True)
    dce_dc = pd.DataFrame(dialcode_dce)
    dce_dc.to_csv(result_loc_.joinpath('textbook_reports', date_.strftime('%Y-%m-%d'), 'DCE_dialcode_data_pre.csv'),
                  index=False, encoding='utf-8-sig')
    post_data_to_blob(result_loc_.joinpath('textbook_reports', date_.strftime('%Y-%m-%d'), 'DCE_dialcode_data_pre.csv'),
                      backup=True)
    dce_tb = pd.DataFrame(textbook_dce).fillna('')
    dce_tb.to_csv(result_loc_.joinpath('textbook_reports', date_.strftime('%Y-%m-%d'), 'DCE_textbook_data_pre.csv'),
                  index=False, encoding='utf-8-sig')
    post_data_to_blob(result_loc_.joinpath('textbook_reports', date_.strftime('%Y-%m-%d'), 'DCE_textbook_data_pre.csv'),
                      backup=True)
    channels = set()
    for c in etb_dc.channel.unique():
        if c in board_slug.index:
            channels.add(c)
    for c in etb_tb.channel.unique():
        if c in board_slug.index:
            channels.add(c)
    for c in dce_dc.channel.unique():
        if c in board_slug.index:
            channels.add(c)
    for c in dce_tb.channel.unique():
        if c in board_slug.index:
            channels.add(c)
    channels = list(channels)
    etb_dc = etb_dc.join(scans_df, on='QR Code', how='left').fillna('')
    etb_dc.sort_values(by=['channel', 'Medium', 'grade_sort', 'Subject', 'Textbook Name'], inplace=True)
    etb_dc = etb_dc[
        ['Textbook ID', 'channel', 'Medium', 'Grade', 'Subject', 'Textbook Name', 'Textbook Status', 'Type of Node',
         'Level 1 Name', 'Level 2 Name', 'Level 3 Name', 'Level 4 Name', 'Level 5 Name', 'QR Code', 'Total Scans',
         'Number of contents']]
    etb_dc.to_csv(result_loc_.joinpath('textbook_reports', date_.strftime('%Y-%m-%d'), 'ETB_dialcode_data.csv'),
                  index=False, encoding='utf-8-sig')
    etb_tb.sort_values(by=['channel', 'Medium', 'grade_sort', 'Subject', 'Textbook Name'], inplace=True)
    etb_tb = etb_tb[
        ['Textbook ID', 'channel', 'Medium', 'Grade', 'Subject', 'Textbook Name', 'Textbook Status', 'Created On',
         'Last Updated On', 'Total content linked', 'Total QR codes linked to content',
         'Total number of QR codes with no linked content', 'Total number of leaf nodes',
         'Number of leaf nodes with no content', 'With QR codes']]
    etb_tb.to_csv(result_loc_.joinpath('textbook_reports', date_.strftime('%Y-%m-%d'), 'ETB_textbook_data.csv'),
                  index=False, encoding='utf-8-sig')
    dce_dc = dce_dc.join(scans_df, on='QR Code', how='left').fillna('')
    dce_dc.sort_values(by=['channel', 'Medium', 'grade_sort', 'Subject', 'Textbook Name'], inplace=True)
    dce_dc = dce_dc[['Textbook ID', 'channel', 'Medium', 'Grade', 'Subject', 'Textbook Name', 'Level 1 Name',
                     'Level 2 Name', 'Level 3 Name', 'Level 4 Name', 'Level 5 Name', 'QR Code', 'Total Scans', 'Term']]
    dce_dc.to_csv(result_loc_.joinpath('textbook_reports', date_.strftime('%Y-%m-%d'), 'DCE_dialcode_data.csv'),
                  index=False, encoding='utf-8-sig')
    dce_tb.sort_values(by=['channel', 'Medium', 'grade_sort', 'Subject', 'Textbook Name'], inplace=True)
    dce_tb = dce_tb[
        ['Textbook ID', 'channel', 'Medium', 'Grade', 'Subject', 'Textbook Name', 'Created On', 'Last Updated On',
         'Total number of QR codes', 'Number of QR codes with atleast 1 linked content',
         'Number of QR codes with no linked content', 'Term 1 QR Codes with no linked content',
         'Term 2 QR Codes with no linked content']]
    dce_tb.to_csv(result_loc_.joinpath('textbook_reports', date_.strftime('%Y-%m-%d'), 'DCE_textbook_data.csv'),
                  index=False, encoding='utf-8-sig')
    for channel in channels:
        slug = board_slug.loc[channel]['slug']
        df_etb_dc = etb_dc[etb_dc['channel'] == channel]
        result_loc_.joinpath('portal_dashboards', slug).mkdir(exist_ok=True)
        etb_dc_path = result_loc_.joinpath('portal_dashboards', slug, 'ETB_dialcode_data.csv')
        df_etb_dc.drop('channel', axis=1).to_csv(etb_dc_path, index=False, encoding='utf-8-sig')
        create_json(etb_dc_path)
        post_data_to_blob(etb_dc_path)
        df_etb_tb = etb_tb[etb_tb['channel'] == channel]
        result_loc_.joinpath('portal_dashboards', slug).mkdir(exist_ok=True)
        etb_tb_path = result_loc_.joinpath('portal_dashboards', slug, 'ETB_textbook_data.csv')
        etb_aggregates(result_loc_, slug, df_etb_tb)
        df_etb_tb.drop(['channel', 'With QR codes'], axis=1).to_csv(etb_tb_path, index=False, encoding='utf-8-sig')
        create_json(etb_tb_path)
        post_data_to_blob(etb_tb_path)
        df_dce_dc = dce_dc[dce_dc['channel'] == channel]
        result_loc_.joinpath('portal_dashboards', slug).mkdir(exist_ok=True)
        dce_dc_path = result_loc_.joinpath('portal_dashboards', slug, 'DCE_dialcode_data.csv')
        df_dce_dc.drop('channel', axis=1).to_csv(dce_dc_path, index=False, encoding='utf-8-sig')
        create_json(dce_dc_path)
        post_data_to_blob(dce_dc_path)
        df_dce_tb = dce_tb[dce_tb['channel'] == channel]
        result_loc_.joinpath('portal_dashboards', slug).mkdir(exist_ok=True)
        dce_tb_path = result_loc_.joinpath('portal_dashboards', slug, 'DCE_textbook_data.csv')
        try:
            dce_aggregates(result_loc_, slug, df_dce_tb)
        except IndexError:
            pass
        df_dce_tb.drop('channel', axis=1).to_csv(dce_tb_path, index=False, encoding='utf-8-sig')
        create_json(dce_tb_path)
        post_data_to_blob(dce_tb_path)
    if skipped_tbs:
        with open(result_loc_.joinpath('textbook_reports', date_.strftime('%Y-%m-%d'), 'etb_error_log.log'), 'a') as f:
            for tb_id in skipped_tbs:
                f.write('ConnectionError: Failed to fetch Hierarchy for {} in {} state.\n'.format(tb_id['identifier'],
                                                                                                  tb_id['status']))

start_time_sec = int(round(time.time()))
start_time = datetime.now()
print("Started at: ", start_time.strftime('%Y-%m-%d %H:%M:%S'))
parser = argparse.ArgumentParser()
parser.add_argument("data_store_location", type=str, help="the path to local data folder")
parser.add_argument("org_search", type=str, help="host address for Org API")
parser.add_argument("content_search", type=str, help="host address for Content Search API")
parser.add_argument("content_hierarchy", type=str, help="host address for Content Hierarchy API")
parser.add_argument("Druid_hostname", type=str, help="host address for Druid API")
parser.add_argument("-execution_date", type=str, help="end date to consider for scan count",
                    default=datetime.today().strftime('%d/%m/%Y'))
args = parser.parse_args()
org_search = args.org_search
content_search = args.content_search
content_hierarchy = args.content_hierarchy
druid_ip = args.Druid_hostname
end_date_ = datetime.strptime(args.execution_date, "%d/%m/%Y")
data_store_location = Path(args.data_store_location)
data_store_location.joinpath('textbook_reports').mkdir(exist_ok=True)
data_store_location.joinpath('textbook_reports', end_date_.strftime('%Y-%m-%d')).mkdir(exist_ok=True)
data_store_location.joinpath('portal_dashboards').mkdir(exist_ok=True)
get_tenant_info(result_loc_=data_store_location.joinpath('textbook_reports'),
                org_search_=org_search, date_=end_date_)
# TODO: SB-15177 store scan counts in cassandra
get_scan_counts(result_loc_=data_store_location.joinpath('textbook_reports'), druid_=druid_ip, date_=end_date_)
backend_grade = pd.read_csv(
    Path(__file__).parent.parent.parent.parent.parent.parent.joinpath('resources', 'common', 'grade_sort.csv')
).set_index('grade')
board_slug = pd.read_csv(data_store_location.joinpath(
    'textbook_reports', end_date_.strftime('%Y-%m-%d'), 'tenant_info.csv'))[['id', 'slug']]
board_slug.set_index('id', inplace=True)
generate_reports(result_loc_=data_store_location, content_search_=content_search, content_hierarchy_=content_hierarchy,
                 date_=end_date_)
end_time = datetime.now()
print("Ended at: ", end_time.strftime('%Y-%m-%d %H:%M:%S'))
print("Time taken: ", str(end_time - start_time))

end_time_sec = int(round(time.time()))
time_taken = end_time_sec - start_time_sec
metrics = {
    "system": "AdhocJob",
    "subsystem": "ETB Creation Metrics",
    "metrics": [
        {
            "metric": "timeTakenSecs",
            "value": time_taken
        },
        {
            "metric": "date",
            "value": datetime.strptime(args.execution_date, "%Y-%m-%d")
        }
    ]
}
push_metric_event(metrics, "ETB Creation Metrics")