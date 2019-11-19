"""
Generate daily consumption metrics from blob storage
"""
import argparse
import os
from datetime import date, timedelta, datetime
from pathlib import Path

import findspark
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from utils import create_json, post_data_to_blob, get_data_from_blob, get_tenant_info, get_textbook_snapshot

findspark.init()


# TODO: Compute Downloads using SHARE-In events
# TODO: Remove DIKSHA specific filters
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
        func.col("context.pdata.id").isin("prod.diksha.app") &
        func.col("edata.subtype").isin("ContentDownload-Success") &
        func.col("eid").isin("INTERACT")
    ).select(
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
        func.col('channel')
    ).count()
    result_loc_.joinpath(date_.strftime('%Y-%m-%d')).mkdir(exist_ok=True)
    download_counts.toPandas().to_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'downloads.csv'), index=False)
    post_data_to_blob(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'downloads.csv'), backup=True)
    spark.stop()


# TODO: Have channel id for Object rollup L1
# TODO: Remove DIKSHA specific filters
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
        func.col("dimensions.pdata.id").isin("prod.diksha.app", "prod.diksha.portal") &
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
        func.col('pdata_id').isin('prod.diksha.app')
    )
    app_df = app.select(
        func.count('sid').alias('Total App Sessions'),
        func.countDistinct('did').alias('Total Devices on App'),
        (func.sum('time_spent') / 3600).alias('Total Time on App (in hours)')
    ).toPandas()
    result_loc_.joinpath(date_.strftime('%Y-%m-%d')).mkdir(exist_ok=True)
    app_df.to_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'app_sessions.csv'), index=False)
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
    df = spark.read.json(path).filter(
        func.col("eid").isin("SEARCH") &
        func.col('edata.filters.dialcodes').isNotNull()
    ).select(
        func.col('dialcodedata.channel').alias('dialcode_channel'),
        func.col('edata.filters.dialcodes'),
        failed_flag('edata.size').alias('failed_flag')
    ).groupby(
        func.col('dialcode_channel'),
        func.col('failed_flag')
    ).count().toPandas()
    result_loc_.joinpath(date_.strftime('%Y-%m-%d')).mkdir(exist_ok=True)
    df.to_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'dial_scans.csv'), index=False)
    post_data_to_blob(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'dial_scans.csv'), backup=True)
    spark.stop()


# TODO: Remove DIKSHA specific filters
def daily_metrics(read_loc_, date_):
    """
    merge the three metrics
    :param read_loc_: pathlib.Path object to read CSV from.
    :param date_: datetime object to use in path
    :return: None
    """
    try:
        board_slug = \
        pd.read_csv(data_store_location.joinpath('textbook_reports', date_.strftime('%Y-%m-%d'), 'tenant_info.csv'))[
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
        downloads_df = pd.read_csv(read_loc_.joinpath('downloads', date_.strftime('%Y-%m-%d'), 'downloads.csv'))
        downloads_df = downloads_df.fillna('').join(board_slug, on='channel', how='left')[['count', 'slug']].dropna(
            subset=['slug'])
        downloads_df.columns = ['Total Content Downloads', 'slug']
    except Exception:
        raise Exception('Downloads Error!')
    try:
        app_df = pd.read_csv(read_loc_.joinpath('play', date_.strftime('%Y-%m-%d'), 'app_sessions.csv'))
        app_df = app_df[['Total App Sessions', 'Total Devices on App', 'Total Time on App (in hours)']]
        plays_df = pd.read_csv(read_loc_.joinpath('play', date_.strftime('%Y-%m-%d'), 'plays.csv'), header=[0, 1],
                               index_col=0)
        plays_df = plays_df.reset_index().join(board_slug, on='channel', how='left')[
            [('Total Content Plays', 'prod.diksha.app'),
             ('Total Content Plays', 'prod.diksha.portal'),
             ('Total Devices that played content', 'prod.diksha.app'),
             ('Total Devices that played content', 'prod.diksha.portal'),
             ('Content Play Time (in hours)', 'prod.diksha.app'),
             ('Content Play Time (in hours)', 'prod.diksha.portal'), 'slug']].dropna(subset=['slug'])
        plays_df.columns = ['Total Content Plays on App',
                            'Total Content Plays on Portal', 'Total Devices that played content on App',
                            'Total Devices that played content on Portal',
                            'Content Play Time on App (in hours)', 'Content Play Time on Portal (in hours)', 'slug']
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
            ['Successful QR Scans', 'Failed QR Scans', 'Total Content Downloads', 'Total Content Plays on App',
             'Total Content Plays on Portal', 'Total Devices that played content on App',
             'Total Devices that played content on Portal',
             'Content Play Time on App (in hours)', 'Content Play Time on Portal (in hours)']].sum().astype(int)
        overall['Total App Sessions'] = app_df['Total App Sessions'].loc[0]
        overall['Total Devices on App'] = app_df['Total Devices on App'].loc[0]
        overall['Total Time on App (in hours)'] = app_df['Total Time on App (in hours)'].loc[0]
        overall['Date'] = '-'.join(date_.strftime('%Y-%m-%d').split('-')[::-1])
        overall['Unmapped QR Scans'] = unmapped
        overall['Total QR scans'] = overall['Successful QR Scans'] + overall['Failed QR Scans'] + overall[
            'Unmapped QR Scans']
        overall['Percentage (%) of Failed QR Scans'] = '%.2f' % (
                overall['Failed QR Scans'] * 100 / overall['Total QR scans'])
        overall['Percentage (%) of Unmapped QR Scans'] = '%.2f' % (
                overall['Unmapped QR Scans'] * 100 / overall['Total QR scans'])
        overall['Total Content Plays'] = overall['Total Content Plays on App'] + overall[
            'Total Content Plays on Portal']
        overall['Total Devices that played content'] = overall['Total Devices that played content on App'] + overall[
            'Total Devices that played content on Portal']
        overall['Total Content Play Time (in hours)'] = overall['Content Play Time on App (in hours)'] + overall[
            'Content Play Time on Portal (in hours)']
        overall = overall[['Date', 'Total QR scans', 'Successful QR Scans', 'Failed QR Scans', 'Unmapped QR Scans',
                           'Percentage (%) of Failed QR Scans', 'Percentage (%) of Unmapped QR Scans',
                           'Total Content Downloads', 'Total App Sessions', 'Total Devices on App',
                           'Total Time on App (in hours)', 'Total Content Plays on App',
                           'Total Devices that played content on App',
                           'Content Play Time on App (in hours)',
                           'Total Content Plays on Portal',
                           'Total Devices that played content on Portal',
                           'Content Play Time on Portal (in hours)',
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
        blob_data = blob_data.append(pd.DataFrame(overall).transpose(), sort=False).fillna('')
        blob_data.index = pd.to_datetime(blob_data.Date, format='%d-%m-%Y')
        blob_data.sort_index(inplace=True)
        blob_data.drop_duplicates('Date', inplace=True, keep='last')
        # can remove after first run
        blob_data = blob_data[['Date', 'Total QR scans', 'Successful QR Scans', 'Failed QR Scans',
                               'Unmapped QR Scans', 'Percentage (%) of Failed QR Scans',
                               'Percentage (%) of Unmapped QR Scans', 'Total Content Downloads',
                               'Total App Sessions', 'Total Devices on App',
                               'Total Time on App (in hours)', 'Total Content Plays on App',
                               'Total Devices that played content on App',
                               'Content Play Time on App (in hours)', 'Total Content Plays on Portal',
                               'Total Devices that played content on Portal',
                               'Content Play Time on Portal (in hours)', 'Total Content Plays',
                               'Total Devices that played content', 'Total Content Play Time (in hours)']]
        blob_data.to_csv(read_loc_.joinpath('portal_dashboards', 'overall', 'daily_metrics.csv'), index=False)
        create_json(read_loc_.joinpath('portal_dashboards', 'overall', 'daily_metrics.csv'))
        post_data_to_blob(read_loc_.joinpath('portal_dashboards', 'overall', 'daily_metrics.csv'))
    except Exception:
        raise Exception('Overall Metrics Error!')
    try:
        daily_metrics_df['Total Content Plays'] = daily_metrics_df['Total Content Plays on App'] + daily_metrics_df[
            'Total Content Plays on Portal']
        daily_metrics_df['Total Devices that played content'] = daily_metrics_df[
                                                                    'Total Devices that played content on App'] + \
                                                                daily_metrics_df[
                                                                    'Total Devices that played content on Portal']
        daily_metrics_df['Total Content Play Time (in hours)'] = daily_metrics_df[
                                                                     'Content Play Time on App (in hours)'] + \
                                                                 daily_metrics_df[
                                                                     'Content Play Time on Portal (in hours)']
        daily_metrics_df.set_index(['slug'], inplace=True)
        daily_metrics_df = daily_metrics_df[['Date', 'Total QR scans', 'Successful QR Scans', 'Failed QR Scans',
                                             'Percentage (%) of Failed QR Scans', 'Total Content Downloads',
                                             'Total Content Plays on App',
                                             'Total Devices that played content on App',
                                             'Content Play Time on App (in hours)',
                                             'Total Content Plays on Portal',
                                             'Total Devices that played content on Portal',
                                             'Content Play Time on Portal (in hours)',
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
                blob_data = blob_data.append(pd.DataFrame(value).transpose(), sort=False).fillna('')
                blob_data.index = pd.to_datetime(blob_data.Date, format='%d-%m-%Y')
                blob_data.sort_index(inplace=True)
                blob_data.drop_duplicates('Date', inplace=True, keep='last')
                # can remove after first run
                blob_data = blob_data[['Date', 'Total QR scans', 'Successful QR Scans', 'Failed QR Scans',
                                       'Percentage (%) of Failed QR Scans', 'Total Content Downloads',
                                       'Total Content Plays on App',
                                       'Total Devices that played content on App',
                                       'Content Play Time on App (in hours)', 'Total Content Plays on Portal',
                                       'Total Devices that played content on Portal',
                                       'Content Play Time on Portal (in hours)', 'Total Content Plays',
                                       'Total Devices that played content', 'Total Content Play Time (in hours)']]
                blob_data.to_csv(read_loc_.joinpath('portal_dashboards', slug, 'daily_metrics.csv'), index=False)
                create_json(read_loc_.joinpath('portal_dashboards', slug, 'daily_metrics.csv'))
                post_data_to_blob(read_loc_.joinpath('portal_dashboards', slug, 'daily_metrics.csv'))
    except Exception:
        raise Exception('State Metrics Error!')


start_time = datetime.now()
print("Started at: ", start_time.strftime('%Y-%m-%d %H:%M:%S'))
parser = argparse.ArgumentParser()
parser.add_argument("data_store_location", type=str, help="the path to local data folder")
parser.add_argument("org_search", type=str, help="host address for Org API")
parser.add_argument("content_search", type=str, help="host address for Content Search API")
parser.add_argument("content_hierarchy", type=str, help="host address for Content Hierarchy API")
parser.add_argument("Druid_hostname", type=str, help="Host address for Druid")
parser.add_argument("-execution_date", type=str, default=date.today().strftime("%d/%m/%Y"),
                    help="DD/MM/YYYY, optional argument for backfill jobs")
args = parser.parse_args()
data_store_location = Path(args.data_store_location)
org_search = args.org_search
content_search = args.content_search
content_hierarchy = args.content_hierarchy
druid_ip = args.Druid_hostname
execution_date = datetime.strptime(args.execution_date, "%d/%m/%Y")
analysis_date = execution_date - timedelta(1)

data_store_location.joinpath('tb_metadata').mkdir(exist_ok=True)
data_store_location.joinpath('play').mkdir(exist_ok=True)
data_store_location.joinpath('downloads').mkdir(exist_ok=True)
data_store_location.joinpath('dialcode_scans').mkdir(exist_ok=True)
data_store_location.joinpath('portal_dashboards').mkdir(exist_ok=True)

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
