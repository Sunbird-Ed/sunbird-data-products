
CREATE KEYSPACE IF NOT EXISTS local_content_db WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

CREATE TABLE IF NOT EXISTS local_content_db.registered_tags(
	tag_id text,
	last_updated timestamp,
	active boolean,
	PRIMARY KEY (tag_id)
);

CREATE KEYSPACE IF NOT EXISTS local_device_db WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

CREATE TABLE IF NOT EXISTS local_device_db.device_usage_summary (
    device_id text,
    app_id text,
    channel text,
    start_time timestamp,
    end_time timestamp,
    num_days bigint,
    total_launches bigint,
    total_timespent double,
    avg_num_launches double,
    avg_time double,
    num_contents bigint,
    play_start_time timestamp,
    last_played_on timestamp,
    total_play_time double,
    num_sessions bigint,
    mean_play_time double,
    mean_play_time_interval double,
    last_played_content text,
    updated_date timestamp,
    PRIMARY KEY (device_id, app_id, channel)
);

CREATE TABLE IF NOT EXISTS local_device_db.device_profile (
    device_id text,
    first_access timestamp,
    last_access timestamp,
    total_ts double,
    total_launches bigint,
    avg_ts double,
    device_spec Map<text,text>,
    updated_date timestamp,
    state text,
    state_code text,
    country text,
    country_code text,
    state_custom text,
    state_code_custom text,
    district_custom text,
    city text,
    uaspec Map<text,text>,
    fcm_token text,
    producer_id text,
    api_last_updated_on timestamp,
    user_declared_state text,
    user_declared_district text,
    PRIMARY KEY (device_id)
);

CREATE KEYSPACE IF NOT EXISTS local_platform_db WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

CREATE TABLE IF NOT EXISTS local_platform_db.job_request (
    client_key text,
    request_id text,
    job_id text,
    status text,
    request_data text,
    location text,
    dt_file_created timestamp,
    dt_first_event timestamp,
    dt_last_event timestamp,
    dt_expiration timestamp,
    iteration int,
    dt_job_submitted timestamp,
    dt_job_processing timestamp,
    dt_job_completed timestamp,
    input_events int,
    output_events int,
    file_size bigint,
    latency int,
    execution_time bigint,
    err_message text,
    stage text,
    stage_status text,
    job_name text,
    PRIMARY KEY (client_key, request_id)
);

CREATE KEYSPACE IF NOT EXISTS local_content_store WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

CREATE TABLE IF NOT EXISTS local_content_store.content_data (
    content_id text PRIMARY KEY,
    body blob,
    last_updated_on timestamp,
    oldbody blob,
    stageicons blob);

CREATE TABLE IF NOT EXISTS local_platform_db.workflow_usage_summary_fact (
  d_period int,
  d_channel text,
  d_app_id text,
  d_tag text,
  d_type text,
  d_mode text,
  d_device_id text,
  d_content_id text,
  d_user_id text,
  m_publish_date timestamp,
  m_last_sync_date timestamp,
  m_last_gen_date timestamp,
  m_total_ts double,
  m_total_sessions bigint,
  m_avg_ts_session double,
  m_total_interactions bigint,
  m_avg_interactions_min double,
  m_total_pageviews_count bigint,
  m_avg_pageviews double,
  m_total_users_count bigint,
  m_total_content_count bigint,
  m_total_devices_count bigint,
  m_unique_users blob,
  m_device_ids blob,
  m_contents blob,
  m_updated_date timestamp,
  m_content_type text,
  PRIMARY KEY (d_period, d_channel, d_app_id, d_tag, d_type, d_mode, d_device_id, d_content_id, d_user_id)
);

CREATE TABLE IF NOT EXISTS local_platform_db.workflow_usage_summary (
    event_date date,
    total_content_play_sessions bigint,
    total_timespent double,
    total_interactions bigint,
    total_pageviews bigint,
    last_updated_at timestamp,
    PRIMARY KEY (event_date)
);

CREATE TABLE IF NOT EXISTS local_platform_db.experiment_definition (
    exp_id text,
    created_by text,
    created_on timestamp,
    criteria text,
    exp_data text,
    exp_description text,
    exp_name text,
    stats map<text, bigint>,
    status text,
    status_message text,
    updated_by text,
    updated_on timestamp,
    PRIMARY KEY(exp_id)
);

CREATE KEYSPACE IF NOT EXISTS dev_hierarchy_store WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

CREATE TABLE IF NOT EXISTS dev_hierarchy_store.content_hierarchy (
    identifier text PRIMARY KEY,
    hierarchy text
);

CREATE KEYSPACE IF NOT EXISTS sunbird WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

CREATE TABLE IF NOT EXISTS sunbird.organisation (
    id text PRIMARY KEY,
    channel text,
    contactdetail text,
    createdby text,
    createddate text,
    description text,
    email text,
    externalid text,
    hashtagid text,
    isrootorg boolean,
    istenant boolean,
    isssoenabled boolean,
    keys map<text, frozen<list<text>>>,
    locationids list<text>,
    orgname text,
    provider text,
    rootorgid text,
    slug text,
    status int,
    updatedby text,
    updateddate text,
    orglocation text
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
    
CREATE TABLE IF NOT EXISTS sunbird.location (
    id text PRIMARY KEY,
    code text,
    name text,
    parentid text,
    type text
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE KEYSPACE IF NOT EXISTS sunbird_courses WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': '1'
};

CREATE TABLE IF NOT EXISTS sunbird_courses.course_batch (
     courseid text,
     batchid text,
     cert_templates map<text, frozen<map<text, text>>>,
     createdby text,
     createddate text,
     createdfor list<text>,
     description text,
     enddate text,
     end_date timestamp,
     enrollmentenddate text,
     enrollment_enddate timestamp,
     enrollmenttype text,
     mentors list<text>,
     name text,
     startdate text,
     start_date timestamp,
     status int,
     updateddate text,
     PRIMARY KEY (courseid, batchid)
) WITH CLUSTERING ORDER BY (batchid ASC)
     AND bloom_filter_fp_chance = 0.01
     AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
     AND comment = ''
     AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
     AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
     AND crc_check_chance = 1.0
     AND dclocal_read_repair_chance = 0.1
     AND default_time_to_live = 0
     AND gc_grace_seconds = 864000
     AND max_index_interval = 2048
     AND memtable_flush_period_in_ms = 0
     AND min_index_interval = 128
     AND read_repair_chance = 0.0
     AND speculative_retry = '99PERCENTILE';

CREATE TABLE IF NOT EXISTS sunbird.user (
    id text PRIMARY KEY,
    channel text,
    countrycode text,
    createdby text,
    createddate text,
    dob text,
    email text,
    emailverified boolean,
    firstname text,
    flagsvalue int,
    framework map<text, frozen<list<text>>>,
    isdeleted boolean,
    lastname text,
    locationids list<text>,
    loginid text,
    managedby text,
    maskedemail text,
    maskedphone text,
    phone text,
    phoneverified boolean,
    prevusedemail text,
    prevusedphone text,
    recoveryemail text,
    recoveryphone text,
    roles list<text>,
    rootorgid text,
    status int,
    tncacceptedon timestamp,
    tncacceptedversion text,
    updatedby text,
    updateddate text,
    userid text,
    username text,
    usertype text,
    usersubtype text,
    profileusertype text,
    profileusertypes text,
    profilelocation text
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE IF NOT EXISTS sunbird.usr_external_identity (
    userid text,
    idtype text,
    provider text,
    createdby text,
    createdon timestamp,
    externalid text,
    lastupdatedby text,
    lastupdatedon timestamp,
    originalexternalid text,
    originalidtype text,
    originalprovider text,
    PRIMARY KEY (userid, idtype, provider)
) WITH CLUSTERING ORDER BY (idtype ASC, provider ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';


CREATE TABLE IF NOT EXISTS sunbird.user_declarations(
   userid text,
   orgid text,
   persona text,
   userinfo map<text,text>,
   status text,
   errortype text,
   createdon timestamp,
   createdby text,
   updatedon timestamp,
   updatedby text,
   primary key(userid,orgid,persona)
);

CREATE TABLE IF NOT EXISTS sunbird.system_settings (
id text PRIMARY KEY,
field text,
value text
);
CREATE INDEX IF NOT EXISTS inx_system_settings ON sunbird.system_settings (field);

CREATE TABLE IF NOT EXISTS sunbird.user_organisation (
  userid text,
  organisationid text,
  addedby text,
  addedbyname text,
  approvaldate text,
  approvedby text,
  hashtagid text,
  id text,
  isapproved boolean,
  isdeleted boolean,
  isrejected boolean,
  orgjoindate text,
  orgleftdate text,
  position text,
 roles list<text>,
 updatedby text,
 updateddate text,
PRIMARY KEY (userid, organisationid));

CREATE TABLE IF NOT EXISTS sunbird_courses.user_activity_agg (
    activity_type text,
    activity_id text,
    user_id text,
    context_id text,
    agg map<text, int>,
    aggregates map<text, double>,
    agg_last_updated map<text, timestamp>,
    agg_details list<text>,
    PRIMARY KEY ((activity_type, activity_id, user_id), context_id)
);

CREATE TABLE IF NOT EXISTS sunbird_courses.user_enrolments (
    userid text,
    courseid text,
    batchid text,
    active boolean,
    addedby text,
    certificates list<frozen<map<text, text>>>,
    completedon timestamp,
    completionpercentage int,
    contentstatus map<text, int>,
    datetime timestamp,
    enrolleddate text,
    enrolled_date timestamp,
    issued_certificates list<frozen<map<text, text>>>,
    lastreadcontentid text,
    lastreadcontentstatus int,
    progress int,
    status int,
    PRIMARY KEY (userid, courseid, batchid)
);

CREATE TYPE IF NOT EXISTS sunbird_courses.question (
    id text,
    assess_ts timestamp,
    max_score double,
    score double,
    type text,
    title text,
    resvalues list<frozen<map<text, text>>>,
    params list<frozen<map<text, text>>>,
    description text,
    duration decimal
);

CREATE TABLE IF NOT EXISTS sunbird.user_consent (
    user_id text,
    consumer_id text,
    object_id text,
    categories list<text>,
    consent_data text,
    consumer_type text,
    created_on timestamp,
    expiry timestamp,
    id text,
    last_updated_on timestamp,
    object_type text,
    status text,
    PRIMARY KEY (user_id, consumer_id, object_id)
);

CREATE TABLE IF NOT EXISTS sunbird_courses.assessment_aggregator (
    user_id text,
    course_id text,
    batch_id text,
    content_id text,
    attempt_id text,
    created_on timestamp,
    grand_total text,
    last_attempted_on timestamp,
    question list<frozen<question>>,
    total_max_score double,
    total_score double,
    updated_on timestamp,
    PRIMARY KEY ((user_id, course_id), batch_id, content_id, attempt_id)
);


CREATE KEYSPACE IF NOT EXISTS test_keyspace WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

CREATE TABLE IF NOT EXISTS test_keyspace.user_enrolment_test(
    batchid text,
    active boolean,
    addedby text,
    certificates list<frozen<map<text, text>>>,
    certstatus int,
    completedon timestamp,
    completionpercentage int,
    contentstatus map<text, int>,
    courseid text,
    datetime timestamp,
    enrolled_date timestamp,
    enrolleddate text,
    issued_certificates list<frozen<map<text, text>>>,
    lastreadcontentid text,
    lastreadcontentstatus int,
    progress int,
    status int,
    userid text,
    PRIMARY KEY(userid, courseid, batchid))
    WITH COMPACTION = { 'class': 'SizeTieredCompactionStrategy', 'enabled': 'false' };


INSERT INTO test_keyspace.user_enrolment_test(batchid, userid,courseid) VALUES ('b1','u1','c1');

CREATE TABLE IF NOT EXISTS test_keyspace.report_user_enrolments(
    batchid text,
    courseid text,
    userid text,
    active boolean,
    addedby text,
    certificates frozen<list<frozen<map<text, text>>>>,
    certstatus int,
    completedon timestamp,
    completionpercentage int,
    contentstatus frozen<map<text, int>>,
    datetime timestamp,
    enrolled_date timestamp,
    enrolleddate text,
    issued_certificates frozen<list<frozen<map<text, text>>>>,
    lastreadcontentid text,
    lastreadcontentstatus int,
    progress int,
    status int,
    PRIMARY KEY (batchid, courseid, userid))
    WITH COMPACTION = { 'class': 'SizeTieredCompactionStrategy', 'enabled': 'false' };