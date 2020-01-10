import os
import sys, time
import findspark
import requests
import pandas as pd
import natsort
import pdb

from anytree.importer import DictImporter
from anytree.search import findall
from natsort import natsorted
from datetime import datetime, timedelta
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

from dataproducts.util.utils import create_json, write_data_to_blob, post_data_to_blob, \
                get_data_from_blob, push_metric_event, get_scan_counts


class GPSLearning:
    def __init__(self, data_store_location, druid_hostname, content_search, content_hierarchy, execution_date, org_search):
        self.data_store_location = Path(data_store_location)
        self.druid_hostname = druid_hostname
        self.content_search = content_search
        self.content_hierarchy = content_hierarchy
        self.execution_date = execution_date
        self.org_search = org_search


    def traverse(self, data, index=""):
        final_dict = {}

        if 'dialcodes' not in data.keys():
            dial = ''
        else:
            dial = data['dialcodes'][0]

        if index == "":
            final_dict['index'] = ''
        else:
            final_dict['index'] = index[1:]

        final_dict['dialcode'] = dial
        final_dict['name'] = data['name']
        final_dict['contentType'] = data['contentType']

        final_dict['identifier'] = data['identifier']

        final_dict['children'] = []

        for item in data['children']:

            index1 = index + '.' + str(item['index'] if item.get('index') else 0)

            if type(item) == dict:
                if 'children' in item.keys():
                    child_dict = self.traverse(item, index1)

                    if child_dict != {}:
                        final_dict['children'].append(child_dict)
                else:
                    temp_dict = {}

                    if 'dialcodes' not in item.keys():
                        dial = ''
                    else:
                        dial = item['dialcodes'][0]

                    temp_dict['index'] = index1[1:]

                    temp_dict['dialcode'] = dial
                    temp_dict['name'] = item['name']
                    temp_dict['contentType'] = item['contentType']

                    temp_dict['identifier'] = item['identifier']

                    final_dict['children'].append(temp_dict)

        if final_dict['children'] == []:
            del final_dict['children']

        return final_dict


    def get_tbs(self):
        tb_url = "{}/api/content/v1/search".format(self.content_search)
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
        headers = {
            'content-type': "application/json; charset=utf-8",
            'cache-control': "no-cache"
        }
        retry_count = 0
        while 1:
            try:
                response = requests.request("POST", tb_url, data=payload, headers=headers)
                break
            except requests.exceptions.ConnectionError:
                print("Retry {} for textbook list".format(retry_count + 1))
                retry_count += 1
                sleep(10)
                if retry_count == 5:
                    print("Max retries reached...")
                    break

        list_of_textbooks = pd.DataFrame(response.json()['result']['content'])
        list_of_textbooks = list_of_textbooks[['identifier', 'channel', 'board', 'gradeLevel', 'medium', 'name', 'subject']]
        tb_list = list(list_of_textbooks.identifier.unique())
        list_of_textbooks.drop_duplicates(subset=['identifier'], keep='first', inplace=True)
        
        dialcode_df = pd.DataFrame()
        tb_count = 0

        for tb_id in tb_list:
            tb_count = tb_count + 1

            print("currently running for textbook number %d(%s)/%d" % (tb_count, tb_id, len(tb_list)))

            retry_count = 0
            url = "{}/api/course/v1/hierarchy/{}".format(self.content_hierarchy, tb_id)
            while 1:
                try:
                    response = requests.request("GET", url, headers=headers)
                    break
                except requests.exceptions.ConnectionError:
                    print("Retry {} for TOC {}".format(retry_count + 1, tb_id))
                    retry_count += 1
                    sleep(10)
                    if retry_count == 5:
                        print("Max retries reached...")
                        print("Skipping the run for TB ID %s" % (tb_id))
                        failed_tbs.append(tb_id)
                        break

            if response.json()['result'] != {}:
                
                tb = response.json()['result']['content']

                if 'children' in tb:
                    pass
                else:
                    continue

                if tb['children'] == None:
                    continue

                if 'index' not in tb['children'][0]:
                    continue

                tree_obj = self.traverse(tb)
                importer = DictImporter()
                root = importer.import_(tree_obj)
                resources = findall(root, filter_=lambda node: node.contentType in ("Resource"))
                dialcodes = findall(root, filter_=lambda node: node.dialcode not in (""))
                
                dialcodes_with_content = []
                for resource in resources:
                    for ancestor in resource.ancestors:
                        dialcodes_with_content.append((ancestor.dialcode,ancestor.index))
                        
                dialcodes_with_content = set([x for x in dialcodes_with_content if (x[0] != '')])

                dialcodes_all = []
                for dialcode in dialcodes:
                    dialcodes_all.append((dialcode.dialcode, dialcode.index))
                    
                dialcodes_all = set([x for x in dialcodes_all if (x != '')])

                no_content = pd.DataFrame(list(dialcodes_all - dialcodes_with_content), columns=['QR', 'Index'])
                no_content['TB_ID'] = tb_id
                no_content['status'] = 'no content'
                
                with_content = pd.DataFrame(list(dialcodes_with_content), columns=['QR', 'Index'])
                with_content['TB_ID'] = tb_id
                with_content['status'] = 'content linked'
                
                final_df = with_content.copy()
                final_df = final_df.append(no_content)
                
                final_df['Index'].fillna(int(0), inplace=True)
                final_df['Index'].loc[final_df['Index'] == ''] = 0
                final_df.Index = final_df.Index.astype('category')
                final_df.Index.cat.reorder_categories(natsorted(set(final_df.Index)), inplace=True, ordered=True)
                final_df_sorted_by_index = final_df.sort_values('Index')
                
                ranks_to_be_assigned_for_positions_of_QR = list(range(len(final_df_sorted_by_index.QR) + 1))[1:]
                
                final_df_ranked_for_QR = final_df_sorted_by_index
                
                final_df_ranked_for_QR['Position of QR in a TB'] = ranks_to_be_assigned_for_positions_of_QR
                final_df_ranked_for_QR['Position of QR in a TB'] = final_df_ranked_for_QR['Position of QR in a TB'].astype(int)
                
                dialcode_df = dialcode_df.append(final_df_sorted_by_index, ignore_index=True)

        dialcode_state = dialcode_df.merge(list_of_textbooks, how='left', left_on='TB_ID', right_on='identifier')
        dialcode_state_final = dialcode_state[['board', 'gradeLevel', 'QR', 'medium', 'subject', 'TB_ID', 'name', 'status', 'Index', 'Position of QR in a TB', 'channel']]

        execution_date_str = datetime.strptime(self.execution_date, "%d/%m/%Y").strftime('%Y-%m-%d')

        os.makedirs(self.data_store_location.joinpath('tb_metadata', execution_date_str), exist_ok=True)
        dialcode_state_final.to_csv(self.data_store_location.joinpath('tb_metadata', execution_date_str, 'qr_code_state.csv'), index=False, encoding='UTF-8')
        post_data_to_blob(self.data_store_location.joinpath('tb_metadata', execution_date_str, 'qr_code_state.csv'), backup=True)


    def generate_report(self):
        execution_date_str = datetime.strptime(self.execution_date, "%d/%m/%Y").strftime('%Y-%m-%d')
        week_last_date = (datetime.strptime(self.execution_date, "%d/%m/%Y") - timedelta(1)).strftime('%d/%m/%Y')

        board_slug = pd.read_csv(
                        self.data_store_location.joinpath('textbook_reports', execution_date_str, 'tenant_info.csv')
                    )[['id', 'slug']]
        board_slug.set_index('slug', inplace=True)

        scans_df = pd.read_csv(self.data_store_location.joinpath('textbook_reports', execution_date_str, 'weekly_dialcode_counts.csv'))
        scans_df["edata_filters_dialcodes"] = scans_df["edata_filters_dialcodes"].str.upper().str.strip()
        scans_df = scans_df.groupby("edata_filters_dialcodes").agg({"Total Scans": "sum"}).reset_index()

        tb_dial_df = pd.read_csv(self.data_store_location.joinpath('tb_metadata', execution_date_str, 'qr_code_state.csv'))
        tb_dial_df["QR"] = tb_dial_df["QR"].str.upper().str.strip()

        tb_dial_scans_df = pd.merge(scans_df, tb_dial_df, left_on="edata_filters_dialcodes", right_on="QR")
        tb_dial_scans_df['Index'] = tb_dial_scans_df['Index'].str.split('.').str[0].astype(int)

        tb_dial_scans_df.groupby(["channel", "TB_ID", "Index"]).agg({"Total Scans": "sum"}).reset_index()
        tb_dial_scans_df['weighted_scans'] = tb_dial_scans_df['Index'] * tb_dial_scans_df['Total Scans']

        weighted_avg_df = tb_dial_scans_df.groupby("channel").agg({"Total Scans": "sum", "weighted_scans": "sum"})

        weighted_avg_df['weighted_average'] = weighted_avg_df['weighted_scans'] / weighted_avg_df['Total Scans']
        weighted_avg_df['weighted_average'] = weighted_avg_df['weighted_average'].round(1)
        weighted_avg_df = weighted_avg_df.reset_index()[['channel', 'weighted_average']]
        weighted_avg_df.rename(columns={"weighted_average": "Index"}, inplace=True)
        weighted_avg_df['Date'] = week_last_date

        for slug, board_value in board_slug.iterrows():
            print(slug)
            try:
                get_data_from_blob(self.data_store_location.joinpath("portal_dashboards", slug, "gps_learning.csv"))
                blob_data = pd.read_csv(self.data_store_location.joinpath("portal_dashboards", slug, "gps_learning.csv"))
            except Exception:
                blob_data = pd.DataFrame(columns=["Date", "Index"])

            current_channel_df = weighted_avg_df[weighted_avg_df['channel']==board_value.id][["Date", "Index"]]

            blob_data = pd.concat([blob_data, current_channel_df])
            blob_data.drop_duplicates(subset=['Date'], keep='last', inplace=True)
            blob_data.to_csv(self.data_store_location.joinpath('portal_dashboards', slug, 'gps_learning.csv'), index=False)
            create_json(self.data_store_location.joinpath('portal_dashboards', slug, 'gps_learning.csv'))
            post_data_to_blob(self.data_store_location.joinpath('portal_dashboards', slug, 'gps_learning.csv'))


    def init(self):
        start_time_sec = int(round(time.time()))
        print("GPS::Start")
        execution_date_ = datetime.strptime(self.execution_date, "%d/%m/%Y")
        self.data_store_location.joinpath('textbook_reports', execution_date_.strftime('%Y-%m-%d')).mkdir(exist_ok=True)
        get_scan_counts(result_loc_=self.data_store_location.joinpath('textbook_reports'),
                        druid_=self.druid_hostname, date_=execution_date_)

        get_tenant_info(result_loc_=self.data_store_location.joinpath('textbook_reports'), org_search_=self.org_search,
                        date_=execution_date_)

        self.get_tbs()
        self.generate_report()
        print("GPS::End")
        end_time_sec = int(round(time.time()))
        time_taken = end_time_sec - start_time_sec
        metrics = [
            {
                "metric": "timeTakenSecs",
                "value": time_taken
            },
            {
                "metric": "date",
                "value": execution_date_.strftime("%Y-%m-%d")
            }
        ]
        push_metric_event(metrics, "ECG Learning")