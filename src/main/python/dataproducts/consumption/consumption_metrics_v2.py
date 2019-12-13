"""
Generate daily consumption metrics from blob storage
"""
import json, time
import os
import pdb
import sys
import argparse
import findspark
import pandas as pd

from datetime import date, timedelta, datetime
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as func

util_path = os.path.abspath(os.path.join(__file__, '..', '..', '..', 'util'))
sys.path.append(util_path)

from utils import create_json, post_data_to_blob, get_data_from_blob, \
    get_tenant_info, get_textbook_snapshot, push_metric_event

findspark.init()


# TODO: Compute Downloads using SHARE-In events
def downloads(result_loc_, date_):
    """
    Compute daily content downloads by channel
    :param result_loc_: pathlib.Path object to store resultant CSV at.
    :param date_: datetime object to pass in query and path
    :return: None
    """
    spark = SparkSession.builder.appName("content_downloads").master("local[*]").getOrCreate()
    account_name = os.environ['AZURE_STORAGE_ACCOUNT']
    account_key = os.environ['AZURE_STORAGE_ACCESS_KEY']
    container = 'telemetry-data-store'
    spark.conf.set('fs.azure.account.key.{}.blob.core.windows.net'.format(account_name), account_key)
    path = 'wasbs://{}@{}.blob.core.windows.net/telemetry-denormalized/raw/{}-*'.format(container, account_name,
                                                                                        date_.strftime('%Y-%m-%d'))
    data = spark.read.json(path).filter(
        (
            func.col("context.pdata.id").isin(config['context']['pdata']['id']['app']) &
            func.col("edata.subtype").isin("ContentDownload-Success") &
            func.col("eid").isin("INTERACT")
        ) | (
            func.col("context.pdata.id").isin(config['context']['pdata']['id']['desktop']) &
            func.col("edata.state").isin("COMPLETED") &
            func.col("context.env").isin("downloadManager")
        )
    ).select(
        func.col("context.pdata.id").alias("pdata_id"),
        func.col("object.id").alias("object_id"),
        "context.did"
    )
    content = spark.read.csv(
        str(result_loc_.parent.joinpath('tb_metadata', date_.strftime('%Y-%m-%d'), 'textbook_snapshot.csv')),
        header=True
    ).filter(
        func.col('contentType').isin('Resource')
    ).select(
        func.col('identifier'),
        func.col('channel')
    ).distinct()
    download_counts = data.join(
        content,
        data.object_id == content.identifier,
        how='left'
    ).groupBy(
        func.col('pdata_id'),
        func.col('channel')
    ).count().toPandas()
    x_download = download_counts.pivot(index='channel', columns='pdata_id')
    result_loc_.joinpath(date_.strftime('%Y-%m-%d')).mkdir(exist_ok=True)
    x_download.to_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'downloads.csv'))
    post_data_to_blob(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'downloads.csv'), backup=True)
    spark.stop()


# TODO: Have channel id for Object rollup L1
def app_and_plays(result_loc_, date_):
    """
    Compute App Sessions and content play sessions and time spent on content consumption.
    :param result_loc_: pathlib.Path object to store resultant CSV at.
    :param date_: datetime object to use in query and path
    :return: None
    """
    spark = SparkSession.builder.appName("content_plays").master("local[*]").getOrCreate()
    account_name = os.environ['AZURE_STORAGE_ACCOUNT']
    account_key = os.environ['AZURE_STORAGE_ACCESS_KEY']
    container = 'telemetry-data-store'
    spark.conf.set('fs.azure.account.key.{}.blob.core.windows.net'.format(account_name), account_key)
    path = 'wasbs://{}@{}.blob.core.windows.net/telemetry-denormalized/summary/{}-*'.format(container, account_name,
                                                                                            date_.strftime('%Y-%m-%d'))
    data = spark.read.json(path).filter(
        func.col("dimensions.pdata.id").isin(config['context']['pdata']['id']['app'],
                                             config['context']['pdata']['id']['portal'],
                                             config['context']['pdata']['id']['desktop']) &
        func.col("dimensions.type").isin("content", "app")
    ).select(
        func.col("dimensions.sid"),
        func.col("dimensions.pdata.id").alias("pdata_id"),
        func.col("dimensions.type"),
        func.col("dimensions.mode"),
        func.col("dimensions.did"),
        func.col("object.id").alias("object_id"),
        func.col("edata.eks.time_spent"),
        func.col("object.rollup.l1")
    )
    app = data.filter(
        func.col('type').isin('app') &
        func.col('pdata_id').isin(config['context']['pdata']['id']['app'], config['context']['pdata']['id']['desktop'])
    )
    app_df = app.groupBy(
        func.col('pdata_id')
    ).agg(
        func.count('sid').alias('Total App Sessions'),
        func.countDistinct('did').alias('Total Devices on App'),
        (func.sum('time_spent') / 3600).alias('Total Time on App (in hours)')
    ).toPandas()
    app_df['x_index'] = 0
    app_df.set_index("x_index", inplace=True)
    x_app = app_df.pivot(columns='pdata_id')
    result_loc_.joinpath(date_.strftime('%Y-%m-%d')).mkdir(exist_ok=True)
    x_app.to_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'app_sessions.csv'), index=False)
    post_data_to_blob(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'app_sessions.csv'), backup=True)
    play = data.filter(func.col('mode').isin('play'))
    content = spark.read.csv(
        str(result_loc_.parent.joinpath('tb_metadata', date_.strftime('%Y-%m-%d'), 'textbook_snapshot.csv')),
        header=True
    ).select(
        func.col('identifier'),
        func.col('channel')
    ).distinct()
    play_df = play.join(
        content,
        play.l1 == content.identifier,
        how='left'
    ).groupBy(
        func.col('channel'),
        func.col('pdata_id')
    ).agg(
        func.count('sid').alias('Total Content Plays'),
        func.countDistinct('did').alias('Total Devices that played content'),
        (func.sum('time_spent') / 3600).alias('Content Play Time (in hours)')
    ).toPandas()
    x_play = play_df.pivot(index='channel', columns='pdata_id')
    x_play.to_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'plays.csv'))
    post_data_to_blob(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'plays.csv'), backup=True)
    spark.stop()


def dialscans(result_loc_, date_):
    """
    compute failed/successful scans by channel
    :param result_loc_: pathlib.Path object to store resultant CSV at.
    :param date_: datetime object to use in query and path
    :return: None
    """
    spark = SparkSession.builder.appName("DialcodeScans").master("local[*]").getOrCreate()
    account_name = os.environ['AZURE_STORAGE_ACCOUNT']
    account_key = os.environ['AZURE_STORAGE_ACCESS_KEY']
    container = 'telemetry-data-store'
    spark.conf.set('fs.azure.account.key.{}.blob.core.windows.net'.format(account_name), account_key)
    path = 'wasbs://{}@{}.blob.core.windows.net/telemetry-denormalized/raw/{}-*'.format(container, account_name,
                                                                                        date_.strftime('%Y-%m-%d'))
    failed_flag = func.udf(lambda x: 'Successful QR Scans' if x > 0 else 'Failed QR Scans')
    data = spark.read.json(path).filter(
        func.col("eid").isin("SEARCH") &
        func.col('edata.filters.dialcodes').isNotNull()
    ).select(
        func.col('dialcodedata.channel').alias('dialcode_channel'),
        func.col('edata.filters.dialcodes'),
        failed_flag('edata.size').alias('failed_flag')
    )
    df = data.groupby(
        func.col('dialcode_channel'),
        func.col('failed_flag')
    ).count().toPandas()
    result_loc_.joinpath(date_.strftime('%Y-%m-%d')).mkdir(exist_ok=True)
    df.to_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'dial_scans.csv'), index=False)
    post_data_to_blob(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'dial_scans.csv'), backup=True)
    spark.stop()


def daily_metrics(read_loc_, date_):
    """
    merge the three metrics
    :param read_loc_: pathlib.Path object to read CSV from.
    :param date_: datetime object to use in path
    :return: None
    """
    try:
        board_slug = \
            pd.read_csv(
                data_store_location.joinpath('textbook_reports', date_.strftime('%Y-%m-%d'), 'tenant_info.csv'))[
                ['id', 'slug']]
        board_slug.set_index('id', inplace=True)
    except Exception:
        raise Exception('Board Slug Error!')
    try:
        scans_df = pd.read_csv(
            read_loc_.joinpath('dialcode_scans', date_.strftime('%Y-%m-%d'), 'dial_scans.csv')).fillna('')
        scans_df = scans_df.pivot(index='dialcode_channel', columns='failed_flag', values='count').reset_index().fillna(
            0)
        scans_df = scans_df.join(board_slug, on='dialcode_channel', how='left')[
            ['slug', 'Failed QR Scans', 'Successful QR Scans']]
        scans_df['Total QR scans'] = scans_df['Successful QR Scans'] + scans_df['Failed QR Scans']
        scans_df['Percentage (%) of Failed QR Scans'] = scans_df['Failed QR Scans'] * 100 / scans_df['Total QR scans']
        unmapped = scans_df[scans_df.slug.isna()]['Total QR scans'][0]
        scans_df.dropna(subset=['slug'], inplace=True)
    except Exception:
        raise Exception('Scans Error!')
    try:
        downloads_df = pd.read_csv(read_loc_.joinpath('downloads', date_.strftime('%Y-%m-%d'), 'downloads.csv'),
                                   header=[0, 1], index_col=0)
        # Setting the value as Nan if there is no data for desktop
        downloads_df[('count', config['context']['pdata']['id']['desktop'])] = downloads_df.reindex(
            columns=[('count', config['context']['pdata']['id']['desktop'])]).squeeze()
        downloads_df = downloads_df.join(board_slug, on='channel', how='left')
        downloads_df = downloads_df[[
            ('count', config['context']['pdata']['id']['app']),
            ('count', config['context']['pdata']['id']['desktop']),
            'slug'
        ]]
        downloads_df.columns = ['Total Content Downloads in Mobile App', 'Total Content Downloads in Desktop App', 'slug']

        downloads_df = downloads_df.dropna(subset=['slug'])

    except Exception:
        raise Exception('Downloads Error!')
    try:
        app_df = pd.read_csv(read_loc_.joinpath('play', date_.strftime('%Y-%m-%d'), 'app_sessions.csv'), header=[0, 1])
        # Setting the value as Nan if there is no data for desktop
        app_df[('Total App Sessions', config['context']['pdata']['id']['desktop'])] = app_df.reindex(
            columns=[('Total App Sessions', config['context']['pdata']['id']['desktop'])]).squeeze()
        app_df[('Total Devices on App', config['context']['pdata']['id']['desktop'])] = app_df.reindex(
            columns=[('Total Devices on App', config['context']['pdata']['id']['desktop'])]).squeeze()
        app_df[('Total Time on App (in hours)', config['context']['pdata']['id']['desktop'])] = app_df.reindex(
            columns=[('Total Time on App (in hours)', config['context']['pdata']['id']['desktop'])]).squeeze()

        app_df = app_df[[
            ('Total App Sessions', config['context']['pdata']['id']['app']),
            ('Total Devices on App', config['context']['pdata']['id']['app']),
            ('Total Time on App (in hours)', config['context']['pdata']['id']['app']),
            ('Total App Sessions', config['context']['pdata']['id']['desktop']),
            ('Total Devices on App', config['context']['pdata']['id']['desktop']),
            ('Total Time on App (in hours)', config['context']['pdata']['id']['desktop'])
        ]]
        app_df.columns = ['Total Mobile App Sessions', 'Total Devices on Mobile App',
                          'Total Time on Mobile App (in hours)',
                          'Total Desktop App Sessions', 'Total Devices on Desktop App',
                          'Total Time on Desktop App (in hours)']
        plays_df = pd.read_csv(read_loc_.joinpath('play', date_.strftime('%Y-%m-%d'), 'plays.csv'), header=[0, 1],
                               index_col=0)
        # Setting the value as Nan if there is no data for desktop
        plays_df[('Total Content Plays', config['context']['pdata']['id']['desktop'])] = plays_df.reindex(
            columns=[('Total Content Plays', config['context']['pdata']['id']['desktop'])]).squeeze()
        plays_df[('Total Devices that played content', config['context']['pdata']['id']['desktop'])] = plays_df.reindex(
            columns=[('Total Devices that played content', config['context']['pdata']['id']['desktop'])]).squeeze()
        plays_df[('Content Play Time (in hours)', config['context']['pdata']['id']['desktop'])] = plays_df.reindex(
            columns=[('Content Play Time (in hours)', config['context']['pdata']['id']['desktop'])]).squeeze()

        plays_df = plays_df.reset_index().join(board_slug, on='channel', how='left')[
            [('Total Content Plays', config['context']['pdata']['id']['app']),
             ('Total Content Plays', config['context']['pdata']['id']['portal']),
             ('Total Content Plays', config['context']['pdata']['id']['desktop']),
             ('Total Devices that played content', config['context']['pdata']['id']['app']),
             ('Total Devices that played content', config['context']['pdata']['id']['portal']),
             ('Total Devices that played content', config['context']['pdata']['id']['desktop']),
             ('Content Play Time (in hours)', config['context']['pdata']['id']['app']),
             ('Content Play Time (in hours)', config['context']['pdata']['id']['portal']),
             ('Content Play Time (in hours)', config['context']['pdata']['id']['desktop']), 'slug']].dropna(
            subset=['slug'])
        plays_df.columns = ['Total Content Plays on Mobile App', 'Total Content Plays on Portal',
                            'Total Content Plays on Desktop App',
                            'Total Devices that played content on Mobile App',
                            'Total Devices that played content on Portal',
                            'Total Devices that played content on Desktop App',
                            'Content Play Time on Mobile App (in hours)', 'Content Play Time on Portal (in hours)',
                            'Content Play Time on Desktop App (in hours)', 'slug']
    except Exception:
        raise Exception('App and Plays Error!')
    try:
        daily_metrics_df = scans_df.join(
            downloads_df.set_index('slug'), on='slug', how='outer'
        ).reset_index(drop=True).join(
            plays_df.set_index('slug'), on='slug', how='outer', rsuffix='_plays'
        ).fillna(0)
        daily_metrics_df['Date'] = '-'.join(date_.strftime('%Y-%m-%d').split('-')[::-1])
    except Exception:
        raise Exception('Daily Metrics Error!')
    try:
        overall = daily_metrics_df[
            ['Successful QR Scans', 'Failed QR Scans', 'Total Content Downloads in Mobile App',
             'Total Content Downloads in Desktop App', 'Total Content Plays on Mobile App',
             'Total Content Plays on Portal', 'Total Content Plays on Desktop App',
             'Total Devices that played content on Mobile App', 'Total Devices that played content on Portal',
             'Total Devices that played content on Desktop App',
             'Content Play Time on Mobile App (in hours)', 'Content Play Time on Portal (in hours)',
             'Content Play Time on Desktop App (in hours)']].sum().astype(int)
        overall['Total Mobile App Sessions'] = app_df['Total Mobile App Sessions'].loc[0]
        overall['Total Desktop App Sessions'] = app_df['Total Desktop App Sessions'].loc[0]
        overall['Total Devices on Mobile App'] = app_df['Total Devices on Mobile App'].loc[0]
        overall['Total Devices on Desktop App'] = app_df['Total Devices on Desktop App'].loc[0]
        overall['Total Time on Mobile App (in hours)'] = app_df['Total Time on Mobile App (in hours)'].loc[0]
        overall['Total Time on Desktop App (in hours)'] = app_df['Total Time on Desktop App (in hours)'].loc[0]
        overall['Date'] = '-'.join(date_.strftime('%Y-%m-%d').split('-')[::-1])
        overall['Unmapped QR Scans'] = unmapped
        overall['Total QR scans'] = overall['Successful QR Scans'] + overall['Failed QR Scans'] + overall[
            'Unmapped QR Scans']
        overall['Percentage (%) of Failed QR Scans'] = '%.2f' % (
                overall['Failed QR Scans'] * 100 / overall['Total QR scans'])
        overall['Percentage (%) of Unmapped QR Scans'] = '%.2f' % (
                overall['Unmapped QR Scans'] * 100 / overall['Total QR scans'])
        overall['Total Content Plays'] = overall['Total Content Plays on Mobile App'] + overall[
            'Total Content Plays on Portal'] + overall['Total Content Plays on Desktop App']
        overall['Total Devices that played content'] = overall['Total Devices that played content on Mobile App'] + \
                                                       overall['Total Devices that played content on Portal'] + \
                                                       overall['Total Devices that played content on Desktop App']
        overall['Total Content Play Time (in hours)'] = overall['Content Play Time on Mobile App (in hours)'] + \
                                                        overall['Content Play Time on Portal (in hours)'] + \
                                                        overall['Content Play Time on Desktop App (in hours)']
        overall = overall[['Date', 'Total QR scans', 'Successful QR Scans', 'Failed QR Scans', 'Unmapped QR Scans',
                           'Percentage (%) of Failed QR Scans', 'Percentage (%) of Unmapped QR Scans',
                           'Total Content Downloads in Mobile App', 'Total Content Downloads in Desktop App',
                           'Total Mobile App Sessions', 'Total Devices on Mobile App',
                           'Total Time on Mobile App (in hours)',
                           'Total Desktop App Sessions', 'Total Devices on Desktop App',
                           'Total Time on Desktop App (in hours)',
                           'Total Content Plays on Mobile App', 'Total Devices that played content on Mobile App',
                           'Content Play Time on Mobile App (in hours)',
                           'Total Content Plays on Portal', 'Total Devices that played content on Portal',
                           'Content Play Time on Portal (in hours)',
                           'Total Content Plays on Desktop App',
                           'Total Devices that played content on Desktop App',
                           'Content Play Time on Desktop App (in hours)',
                           'Total Content Plays', 'Total Devices that played content',
                           'Total Content Play Time (in hours)'
                           ]]
        read_loc_.joinpath('portal_dashboards', 'overall').mkdir(exist_ok=True)
        read_loc_.joinpath('portal_dashboards', 'mhrd').mkdir(exist_ok=True)
        try:
            get_data_from_blob(read_loc_.joinpath('portal_dashboards', 'overall', 'daily_metrics.csv'))
            blob_data = pd.read_csv(read_loc_.joinpath('portal_dashboards', 'overall', 'daily_metrics.csv'))
        except:
            blob_data = pd.DataFrame()

        # Changing old generic app fields Mobile app and adding Desktop app fields // first time use
        if 'Total Content Downloads in Mobile App' not in blob_data.columns:
            blob_data.rename(columns={'Total Content Downloads': 'Total Content Downloads in Mobile App',
                                      'Total App Sessions': 'Total Mobile App Sessions',
                                      'Total Devices on App': 'Total Devices on Mobile App',
                                      'Total Time on App (in hours)': 'Total Time on Mobile App (in hours)',
                                      'Total Content Plays on App': 'Total Content Plays on Mobile App',
                                      'Total Devices that played content on App':
                                          'Total Devices that played content on Mobile App',
                                      'Content Play Time on App (in hours)':
                                          'Content Play Time on Mobile App (in hours)'},
                             inplace=True)
            blob_data['Total Content Downloads in Desktop App'] = 0
            blob_data['Total Desktop App Sessions'] = 0
            blob_data['Total Devices on Desktop App'] = 0
            blob_data['Total Time on Desktop App (in hours)'] = 0
            blob_data['Total Content Plays on Desktop App'] = 0
            blob_data['Total Devices that played content on Desktop App'] = 0
            blob_data['Content Play Time on Desktop App (in hours)'] = 0
        blob_data = blob_data.append(pd.DataFrame(overall).transpose(), sort=False).fillna('')
        blob_data.index = pd.to_datetime(blob_data.Date, format='%d-%m-%Y')
        blob_data.sort_index(inplace=True)
        blob_data.drop_duplicates('Date', inplace=True, keep='last')

        # Excluding the desktop colums if no value
        sum_of_desktop_values = blob_data[['Total Content Downloads in Desktop App',
                                           'Total Desktop App Sessions', 'Total Devices on Desktop App',
                                           'Total Time on Desktop App (in hours)', 'Total Content Plays on Desktop App',
                                           'Total Devices that played content on Desktop App',
                                           'Content Play Time on Desktop App (in hours)']].values.sum()
        if sum_of_desktop_values == 0:
            exportable_cols = ['Date', 'Total QR scans', 'Successful QR Scans', 'Failed QR Scans',
                               'Unmapped QR Scans', 'Percentage (%) of Failed QR Scans',
                               'Percentage (%) of Unmapped QR Scans',
                               'Total Content Downloads in Mobile App',
                               'Total Mobile App Sessions', 'Total Devices on Mobile App',
                               'Total Time on Mobile App (in hours)',
                               'Total Content Plays on Mobile App', 'Total Devices that played content on Mobile App',
                               'Content Play Time on Mobile App (in hours)',
                               'Total Content Plays on Portal', 'Total Devices that played content on Portal',
                               'Content Play Time on Portal (in hours)',
                               'Total Content Plays', 'Total Devices that played content',
                               'Total Content Play Time (in hours)']
        else:
            exportable_cols = ['Date', 'Total QR scans', 'Successful QR Scans', 'Failed QR Scans',
                               'Unmapped QR Scans', 'Percentage (%) of Failed QR Scans',
                               'Percentage (%) of Unmapped QR Scans',
                               'Total Content Downloads in Mobile App', 'Total Content Downloads in Desktop App',
                               'Total Mobile App Sessions', 'Total Devices on Mobile App',
                               'Total Time on Mobile App (in hours)',
                               'Total Desktop App Sessions', 'Total Devices on Desktop App',
                               'Total Time on Desktop App (in hours)',
                               'Total Content Plays on Mobile App', 'Total Devices that played content on Mobile App',
                               'Content Play Time on Mobile App (in hours)',
                               'Total Content Plays on Portal', 'Total Devices that played content on Portal',
                               'Content Play Time on Portal (in hours)',
                               'Total Content Plays on Desktop App', 'Total Devices that played content on Desktop App',
                               'Content Play Time on Desktop App (in hours)',
                               'Total Content Plays', 'Total Devices that played content',
                               'Total Content Play Time (in hours)']
        # can remove after first run
        blob_data = blob_data[exportable_cols]
        blob_data.to_csv(read_loc_.joinpath('portal_dashboards', 'overall', 'daily_metrics.csv'), index=False)
        create_json(read_loc_.joinpath('portal_dashboards', 'overall', 'daily_metrics.csv'))
        post_data_to_blob(read_loc_.joinpath('portal_dashboards', 'overall', 'daily_metrics.csv'))
    except Exception:
        raise Exception('Overall Metrics Error!')
    try:
        daily_metrics_df['Total Content Plays'] = daily_metrics_df['Total Content Plays on Mobile App'] + \
                                                  daily_metrics_df['Total Content Plays on Portal'] + \
                                                  daily_metrics_df['Total Content Plays on Desktop App']
        daily_metrics_df['Total Devices that played content'] = daily_metrics_df[
                                                                    'Total Devices that played content on Mobile App'] \
                                                                + daily_metrics_df[
                                                                    'Total Devices that played content on Portal'] \
                                                                + daily_metrics_df[
                                                                    'Total Devices that played content on Desktop App']
        daily_metrics_df['Total Content Play Time (in hours)'] = daily_metrics_df[
                                                                     'Content Play Time on Mobile App (in hours)'] + \
                                                                 daily_metrics_df[
                                                                     'Content Play Time on Portal (in hours)'] + \
                                                                 daily_metrics_df[
                                                                     'Content Play Time on Desktop App (in hours)']
        daily_metrics_df.set_index(['slug'], inplace=True)
        daily_metrics_df = daily_metrics_df[['Date', 'Total QR scans', 'Successful QR Scans', 'Failed QR Scans',
                                             'Percentage (%) of Failed QR Scans',
                                             'Total Content Downloads in Mobile App',
                                             'Total Content Downloads in Desktop App',
                                             'Total Content Plays on Mobile App',
                                             'Total Devices that played content on Mobile App',
                                             'Content Play Time on Mobile App (in hours)',
                                             'Total Content Plays on Portal',
                                             'Total Devices that played content on Portal',
                                             'Content Play Time on Portal (in hours)',
                                             'Total Content Plays on Desktop App',
                                             'Total Devices that played content on Desktop App',
                                             'Content Play Time on Desktop App (in hours)',
                                             'Total Content Plays', 'Total Devices that played content',
                                             'Total Content Play Time (in hours)']]
        for slug, value in daily_metrics_df.iterrows():
            if slug != '':
                read_loc_.joinpath('portal_dashboards', slug).mkdir(exist_ok=True)
                for key, val in value.items():
                    if key not in ['Date', 'Percentage (%) of Failed QR Scans']:
                        value[key] = int(val)
                    elif key == 'Percentage (%) of Failed QR Scans':
                        value[key] = '%.2f' % val
                try:
                    get_data_from_blob(read_loc_.joinpath('portal_dashboards', slug, 'daily_metrics.csv'))
                    blob_data = pd.read_csv(read_loc_.joinpath('portal_dashboards', slug, 'daily_metrics.csv'))
                except:
                    blob_data = pd.DataFrame()

                # Changing old generic app fields Mobile app and adding Desktop app fields // first time use
                if 'Total Content Downloads in Mobile App' not in blob_data.columns:
                    blob_data.rename(columns={'Total Content Downloads': 'Total Content Downloads in Mobile App',
                                              'Total Content Plays on App': 'Total Content Plays on Mobile App',
                                              'Total Devices that played content on App':
                                                  'Total Devices that played content on Mobile App',
                                              'Content Play Time on App (in hours)':
                                                  'Content Play Time on Mobile App (in hours)'}, inplace=True)
                    blob_data['Total Content Downloads in Desktop App'] = 0
                    blob_data['Total Content Plays on Desktop App'] = 0
                    blob_data['Total Devices that played content on Desktop App'] = 0
                    blob_data['Content Play Time on Desktop App (in hours)'] = 0
                blob_data = blob_data.append(pd.DataFrame(value).transpose(), sort=False).fillna('')
                blob_data.index = pd.to_datetime(blob_data.Date, format='%d-%m-%Y')
                blob_data.sort_index(inplace=True)
                blob_data.drop_duplicates('Date', inplace=True, keep='last')

                # Excluding the desktop colums if no value
                sum_of_desktop_values = blob_data[['Total Content Downloads in Desktop App',
                                                   'Total Content Plays on Desktop App',
                                                   'Total Devices that played content on Desktop App',
                                                   'Content Play Time on Desktop App (in hours)']].values.sum()
                if sum_of_desktop_values == 0:
                    exportable_cols = ['Date', 'Total QR scans', 'Successful QR Scans', 'Failed QR Scans',
                                       'Percentage (%) of Failed QR Scans',
                                       'Total Content Downloads in Mobile App',
                                       'Total Content Plays on Mobile App',
                                       'Total Devices that played content on Mobile App',
                                       'Content Play Time on Mobile App (in hours)',
                                       'Total Content Plays on Portal', 'Total Devices that played content on Portal',
                                       'Content Play Time on Portal (in hours)',
                                       'Total Content Plays', 'Total Devices that played content',
                                       'Total Content Play Time (in hours)']
                else:
                    exportable_cols = ['Date', 'Total QR scans', 'Successful QR Scans', 'Failed QR Scans',
                                       'Percentage (%) of Failed QR Scans',
                                       'Total Content Downloads in Mobile App',
                                       'Total Content Downloads in Desktop App',
                                       'Total Content Plays on Mobile App',
                                       'Total Devices that played content on Mobile App',
                                       'Content Play Time on Mobile App (in hours)',
                                       'Total Content Plays on Portal', 'Total Devices that played content on Portal',
                                       'Content Play Time on Portal (in hours)',
                                       'Total Content Plays on Desktop App',
                                       'Total Devices that played content on Desktop App',
                                       'Content Play Time on Desktop App (in hours)',
                                       'Total Content Plays', 'Total Devices that played content',
                                       'Total Content Play Time (in hours)']
                blob_data = blob_data[exportable_cols]
                blob_data.to_csv(read_loc_.joinpath('portal_dashboards', slug, 'daily_metrics.csv'), index=False)
                create_json(read_loc_.joinpath('portal_dashboards', slug, 'daily_metrics.csv'))
                post_data_to_blob(read_loc_.joinpath('portal_dashboards', slug, 'daily_metrics.csv'))
    except Exception:
        raise Exception('State Metrics Error!')

start_time_sec = int(round(time.time()))
start_time = datetime.now()
print("Started at: ", start_time.strftime('%Y-%m-%d %H:%M:%S'))
parser = argparse.ArgumentParser()
parser.add_argument("--data_store_location", type=str, help="the path to local data folder")
parser.add_argument("--org_search", type=str, help="host address for Org API")
parser.add_argument("--content_search", type=str, help="host address for Content Search API")
parser.add_argument("--content_hierarchy", type=str, help="host address for Content Hierarchy API")
parser.add_argument("--execution_date", type=str, default=date.today().strftime("%d/%m/%Y"),
                    help="DD/MM/YYYY, optional argument for backfill jobs")
args = parser.parse_args()
data_store_location = Path(args.data_store_location)
org_search = args.org_search
content_search = args.content_search
content_hierarchy = args.content_hierarchy
execution_date = datetime.strptime(args.execution_date, "%d/%m/%Y")
analysis_date = execution_date - timedelta(1)

data_store_location.joinpath('tb_metadata').mkdir(exist_ok=True)
data_store_location.joinpath('play').mkdir(exist_ok=True)
data_store_location.joinpath('downloads').mkdir(exist_ok=True)
data_store_location.joinpath('dialcode_scans').mkdir(exist_ok=True)
data_store_location.joinpath('portal_dashboards').mkdir(exist_ok=True)
data_store_location.joinpath('config').mkdir(exist_ok=True)
get_data_from_blob(data_store_location.joinpath('config', 'diksha_config.json'))
with open(data_store_location.joinpath('config', 'diksha_config.json'), 'r') as f:
    config = json.loads(f.read())
get_textbook_snapshot(result_loc_=data_store_location.joinpath('tb_metadata'), content_search_=content_search,
                      content_hierarchy_=content_hierarchy, date_=analysis_date)
print('[Success] Textbook Snapshot')
get_tenant_info(result_loc_=data_store_location.joinpath('textbook_reports'), org_search_=org_search,
                date_=analysis_date)
print('[Success] Tenant Info')
app_and_plays(result_loc_=data_store_location.joinpath('play'), date_=analysis_date)
print('[Success] App and Plays')
dialscans(result_loc_=data_store_location.joinpath('dialcode_scans'), date_=analysis_date)
print('[Success] DIAL Scans')
downloads(result_loc_=data_store_location.joinpath('downloads'), date_=analysis_date)
print('[Success] Downloads')
daily_metrics(read_loc_=data_store_location, date_=analysis_date)
print('[Success] Daily metrics')
end_time = datetime.now()
print("Ended at: ", end_time.strftime('%Y-%m-%d %H:%M:%S'))
print("Time taken: ", str(end_time - start_time))

end_time_sec = int(round(time.time()))
time_taken = end_time_sec - start_time_sec
metrics = [
    {
        "metric": "timeTakenSecs",
        "value": time_taken
    },
    {
        "metric": "date",
        "value": execution_date
    }
]
push_metric_event(metrics, "Consumption Metrics")