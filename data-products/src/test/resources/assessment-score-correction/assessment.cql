CREATE KEYSPACE IF NOT EXISTS sunbird_courses WITH replication = {
'class': 'SimpleStrategy',
'replication_factor': '1'
};


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
TRUNCATE sunbird_courses.assessment_aggregator;

INSERT INTO sunbird_courses.assessment_aggregator (course_id, batch_id, user_id, content_id, attempt_id, grand_total, total_max_score, total_score, question, updated_on) VALUES ('do_1130928636168192001667', 'batch-00001', 'user-001', 'do_11307972307046400011917', 'attempat-001', '20', 20, 20, [{id: 'do_213019475454476288155', assess_ts: '2021-06-18T20:15:56.490+0000', max_score: 1, score: 1, type: 'mcq', title: 'testQuestiontextandformula', resvalues: [{'1': '{"text":"A=\\\\pi r^2\n"}'}], params: [{'1': '{"text":"A=\\\\pi r^2\n"}'}, {'2': '{"text":"no\n"}'}, {'answer': '{"correct":["1"]}'}], description: 'testQuestiontextandformula', duration: 1.0}, {id: 'do_213019970118279168165', assess_ts: '2020-06-18T20:15:56.490+0000', max_score: 1, score: 1, type: 'mcq', title: 'test with formula', resvalues: [{'1': '{"text":"1\nA=\\\\pi r^2A=\\\\pi r^2\n"}'}], params: [{'1': '{"text":"1\nA=\\\\pi r^2A=\\\\pi r^2\n"}'}, {'2': '{"text":"2\n"}'}, {'answer': '{"correct":["1"]}'}], description: '', duration: 1.0}, {id: 'do_213019972814823424168', assess_ts: '2020-06-17T18:15:56.490+0000', max_score: 1, score: 0.33, type: 'mtf', title: 'Copy of - Match the following:\n\nx=\\frac{-b\\pm\\sqrt{b^2-4ac}}{2a}\nArrange the following equations in correct order.\n', resvalues: [{'lhs': '[{"1":"{\"text\":\"A=\\\\\\\\pi r^2\\n\"}"},{"2":"{\"text\":\"\\\\\\\\frac{4}{3}\\\\\\\\pi r^3\\n\"}"},{"3":"{\"text\":\"a^n\\\\\\\\times a^m=a^{n+m}\\n\"}"}]'}, {'rhs': '[{"1":"{\"text\":\"Volume of sphere\\n\"}"},{"2":"{\"text\":\"Area of Circle\\n\"}"},{"3":"{\"text\":\"Product Rule\\n\"}"}]'}], params: [{'lhs': '[{"1":"{\"text\":\"A=\\\\\\\\pi r^2\\n\"}"},{"2":"{\"text\":\"\\\\\\\\frac{4}{3}\\\\\\\\pi r^3\\n\"}"},{"3":"{\"text\":\"a^n\\\\\\\\times a^m=a^{n+m}\\n\"}"}]'}, {'rhs': '[{"1":"{\"text\":\"Volume of sphere\\n\"}"},{"2":"{\"text\":\"Product Rule\\n\"}"},{"3":"{\"text\":\"Area of Circle\\n\"}"}]'}, {'answer': '{"lhs":["1","2","3"],"rhs":["3","1","2"]}'}], description: '', duration: 2.0}, {id: 'do_2130256513760624641171', assess_ts: '2020-06-18T21:15:56.490+0000', max_score: 10, score: 10, type: 'mcq', title: '2 +2 is..? mark ia 10\n', resvalues: [{'1': '{"text":"4\n"}'}], params: [{'1': '{"text":"4\n"}'}, {'2': '{"text":"3\n"}'}, {'3': '{"text":"8\n"}'}, {'4': '{"text":"10\n"}'}, {'answer': '{"correct":["1"]}'}], description: '', duration: 12.0},{id: 'do_2130256513760624641171', assess_ts: '2020-06-11T21:15:56.490+0000', max_score: 10, score: 10, type: 'mcq', title: '2 +2 is..? mark ia 10\n', resvalues: [{'1': '{"text":"4\n"}'}], params: [{'1': '{"text":"4\n"}'}, {'2': '{"text":"3\n"}'}, {'3': '{"text":"8\n"}'}, {'4': '{"text":"10\n"}'}, {'answer': '{"correct":["1"]}'}], description: '', duration: 12.0},{id: 'do_2130256513760624641171', assess_ts: '2020-06-21T21:15:56.490+0000', max_score: 10, score: 10, type: 'mcq', title: '2 +2 is..? mark ia 10\n', resvalues: [{'1': '{"text":"4\n"}'}], params: [{'1': '{"text":"4\n"}'}, {'2': '{"text":"3\n"}'}, {'3': '{"text":"8\n"}'}, {'4': '{"text":"10\n"}'}, {'answer': '{"correct":["1"]}'}], description: '', duration: 12.0},{id: 'do_2130256513760624641171', assess_ts: '2020-06-22T21:15:56.490+0000', max_score: 10, score: 10, type: 'mcq', title: '2 +2 is..? mark ia 10\n', resvalues: [{'1': '{"text":"4\n"}'}], params: [{'1': '{"text":"4\n"}'}, {'2': '{"text":"3\n"}'}, {'3': '{"text":"8\n"}'}, {'4': '{"text":"10\n"}'}, {'answer': '{"correct":["1"]}'}], description: '', duration: 12.0}], toTimeStamp(toDate(now())));
INSERT INTO sunbird_courses.assessment_aggregator (course_id, batch_id, user_id, content_id, attempt_id, grand_total, total_max_score, total_score, question, updated_on) VALUES ('do_1130928636168192001667', 'batch-00001', 'user-001', 'do_11307972307046400011917', 'attempat-002', '20', 6, 20, [{id: 'do_2130194754544762881551', assess_ts: '2021-08-01T10:15:56.490+0000', max_score: 1, score: 1, type: 'mcq', title: 'testQuestiontextandformula', resvalues: [{'1': '{"text":"A=\\\\pi r^2\n"}'}], params: [{'1': '{"text":"A=\\\\pi r^2\n"}'}, {'2': '{"text":"no\n"}'}, {'answer': '{"correct":["1"]}'}], description: 'testQuestiontextandformula', duration: 1.0}, {id: 'do_213019970118279168165', assess_ts: '2021-08-01T11:15:56.490+0000', max_score: 1, score: 1, type: 'mcq', title: 'test with formula', resvalues: [{'1': '{"text":"1\nA=\\\\pi r^2A=\\\\pi r^2\n"}'}], params: [{'1': '{"text":"1\nA=\\\\pi r^2A=\\\\pi r^2\n"}'}, {'2': '{"text":"2\n"}'}, {'answer': '{"correct":["1"]}'}], description: '', duration: 1.0}, {id: 'do_213019972814823424168', assess_ts: '2021-08-01T11:00:56.490+0000', max_score: 1, score: 0.33, type: 'mtf', title: 'Copy of - Match the following:\n\nx=\\frac{-b\\pm\\sqrt{b^2-4ac}}{2a}\nArrange the following equations in correct order.\n', resvalues: [{'lhs': '[{"1":"{\"text\":\"A=\\\\\\\\pi r^2\\n\"}"},{"2":"{\"text\":\"\\\\\\\\frac{4}{3}\\\\\\\\pi r^3\\n\"}"},{"3":"{\"text\":\"a^n\\\\\\\\times a^m=a^{n+m}\\n\"}"}]'}, {'rhs': '[{"1":"{\"text\":\"Volume of sphere\\n\"}"},{"2":"{\"text\":\"Area of Circle\\n\"}"},{"3":"{\"text\":\"Product Rule\\n\"}"}]'}], params: [{'lhs': '[{"1":"{\"text\":\"A=\\\\\\\\pi r^2\\n\"}"},{"2":"{\"text\":\"\\\\\\\\frac{4}{3}\\\\\\\\pi r^3\\n\"}"},{"3":"{\"text\":\"a^n\\\\\\\\times a^m=a^{n+m}\\n\"}"}]'}, {'rhs': '[{"1":"{\"text\":\"Volume of sphere\\n\"}"},{"2":"{\"text\":\"Product Rule\\n\"}"},{"3":"{\"text\":\"Area of Circle\\n\"}"}]'}, {'answer': '{"lhs":["1","2","3"],"rhs":["3","1","2"]}'}], description: '', duration: 2.0}, {id: 'do_2130256513760624641171', assess_ts: '2021-08-01T11:15:56.490+0000', max_score: 10, score: 10, type: 'mcq', title: '2 +2 is..? mark ia 10\n', resvalues: [{'1': '{"text":"4\n"}'}], params: [{'1': '{"text":"4\n"}'}, {'2': '{"text":"3\n"}'}, {'3': '{"text":"8\n"}'}, {'4': '{"text":"10\n"}'}, {'answer': '{"correct":["1"]}'}], description: '', duration: 12.0},{id: 'do_213025651376062464117111', assess_ts: '2021-08-01T11:15:56.490+0000', max_score: 10, score: 10, type: 'mcq', title: '2 +2 is..? mark ia 10\n', resvalues: [{'1': '{"text":"4\n"}'}], params: [{'1': '{"text":"4\n"}'}, {'2': '{"text":"3\n"}'}, {'3': '{"text":"8\n"}'}, {'4': '{"text":"10\n"}'}, {'answer': '{"correct":["1"]}'}], description: '', duration: 12.0}], toTimeStamp(toDate(now())));
INSERT INTO sunbird_courses.assessment_aggregator (course_id, batch_id, user_id, content_id, attempt_id, grand_total, total_max_score, total_score, question, updated_on) VALUES ('do_1130928636168192001667', 'batch-00001', 'user-001', 'do_11307972307046400011917', 'attempat-003', '20', 4, 20, [{id: 'do_213019475454476288155', assess_ts: '2021-08-01T10:15:56.490+0000', max_score: 1, score: 1, type: 'mcq', title: 'testQuestiontextandformula', resvalues: [{'1': '{"text":"A=\\\\pi r^2\n"}'}], params: [{'1': '{"text":"A=\\\\pi r^2\n"}'}, {'2': '{"text":"no\n"}'}, {'answer': '{"correct":["1"]}'}], description: 'testQuestiontextandformula', duration: 1.0}, {id: 'do_213019970118279168165', assess_ts: '2021-08-01T11:15:56.490+0000', max_score: 1, score: 1, type: 'mcq', title: 'test with formula', resvalues: [{'1': '{"text":"1\nA=\\\\pi r^2A=\\\\pi r^2\n"}'}], params: [{'1': '{"text":"1\nA=\\\\pi r^2A=\\\\pi r^2\n"}'}, {'2': '{"text":"2\n"}'}, {'answer': '{"correct":["1"]}'}], description: '', duration: 1.0}, {id: 'do_213019972814823424168', assess_ts: '2021-08-01T11:00:56.490+0000', max_score: 1, score: 0.33, type: 'mtf', title: 'Copy of - Match the following:\n\nx=\\frac{-b\\pm\\sqrt{b^2-4ac}}{2a}\nArrange the following equations in correct order.\n', resvalues: [{'lhs': '[{"1":"{\"text\":\"A=\\\\\\\\pi r^2\\n\"}"},{"2":"{\"text\":\"\\\\\\\\frac{4}{3}\\\\\\\\pi r^3\\n\"}"},{"3":"{\"text\":\"a^n\\\\\\\\times a^m=a^{n+m}\\n\"}"}]'}, {'rhs': '[{"1":"{\"text\":\"Volume of sphere\\n\"}"},{"2":"{\"text\":\"Area of Circle\\n\"}"},{"3":"{\"text\":\"Product Rule\\n\"}"}]'}], params: [{'lhs': '[{"1":"{\"text\":\"A=\\\\\\\\pi r^2\\n\"}"},{"2":"{\"text\":\"\\\\\\\\frac{4}{3}\\\\\\\\pi r^3\\n\"}"},{"3":"{\"text\":\"a^n\\\\\\\\times a^m=a^{n+m}\\n\"}"}]'}, {'rhs': '[{"1":"{\"text\":\"Volume of sphere\\n\"}"},{"2":"{\"text\":\"Product Rule\\n\"}"},{"3":"{\"text\":\"Area of Circle\\n\"}"}]'}, {'answer': '{"lhs":["1","2","3"],"rhs":["3","1","2"]}'}], description: '', duration: 2.0}, {id: 'do_2130256513760624641171', assess_ts: '2021-08-01T11:15:56.490+0000', max_score: 10, score: 10, type: 'mcq', title: '2 +2 is..? mark ia 10\n', resvalues: [{'1': '{"text":"4\n"}'}], params: [{'1': '{"text":"4\n"}'}, {'2': '{"text":"3\n"}'}, {'3': '{"text":"8\n"}'}, {'4': '{"text":"10\n"}'}, {'answer': '{"correct":["1"]}'}], description: '', duration: 12.0}], toTimeStamp(toDate(now())));

INSERT INTO sunbird_courses.user_activity_agg (activity_type, activity_id, user_id, context_id, agg, agg_last_updated) VALUES('Course', 'do_1130928636168192001667', 'user-001', 'cb:batch-00001', {'completedCount': 1}, {'completedCount': '2020-08-17'});
INSERT INTO sunbird_courses.user_activity_agg (activity_type, activity_id, user_id, context_id, agg, agg_last_updated) VALUES('Course', 'do_1130928636168192001667', 'user-002', 'cb:batch-00001', {'completedCount': 0}, {'completedCount': '2020-09-17'});

DROP TABLE sunbird_courses.user_enrolments;
CREATE TABLE sunbird_courses.user_enrolments (
    batchid text,
    courseid text,
    userid text,
    "active" boolean,
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
    PRIMARY KEY (batchid, courseid, userid)
) WITH CLUSTERING ORDER BY (courseid ASC, userid ASC);

INSERT INTO sunbird_courses.user_enrolments (userid, courseid, batchid, issued_certificates) VALUES('user-001', 'do_1130928636168192001667', 'batch-00001', [{'identifier': 'e08017de-3cb7-47d2-a375-6dd3f8575806', 'lastIssuedOn': '2020-05-01T11:27:35.130+0000', 'name': 'Acknowledgement Certificate', 'token': 'U4M3F6', 'url': 'https://diksha.gov.in/certs/0126684405014528002_0130107203983114243/e08017de-3cb7-47d2-a375-6dd3f8575806.pdf'}]);
INSERT INTO sunbird_courses.user_enrolments (userid, courseid, batchid, issued_certificates) VALUES('user-002', 'do_1130928636168192001667', 'batch-00001',  [{'identifier': '0e045c27-5c9f-4d8d-9f2e-6c123eed63a7', 'lastIssuedOn': '2020-11-25T00:12:32.211+0000', 'name': 'Completion Certificate', 'token': 'N1C9D5'}]);

CREATE TABLE IF NOT EXISTS sunbird.cert_registry (
    id text PRIMARY KEY,
    accesscode text,
    createdat timestamp,
    createdby text,
    data text,
    isrevoked boolean,
    jsonurl text,
    pdfurl text,
    reason text,
    recipient text,
    related text,
    updatedat timestamp,
    updatedby text
);

INSERT INTO sunbird.cert_registry(id, accesscode, data) VALUES('e08017de-3cb7-47d2-a375-6dd3f8575806', 'Z3N7W6', '{"id":"https://loadtest.ntp.net.in/certs/0126796199493140480_0131495768541675525/7e8197e9-0a9c-4cb5-8abc-ea5eac38fa5a","type":["Assertion","Extension","extensions:CertificateExtension"],"issuedOn":"2020-11-19T00:00:00Z","recipient":{"identity":"7b5cc280-ec07-44e3-a5cc-e6fe54d9b79d","type":["id"],"hashed":false,"name":"1597748348-41199","@context":"https://loadtest.ntp.net.in/certs/v1/context.json"},"badge":{"id":"https://loadtest.ntp.net.in/certs/0126796199493140480_0131495768541675525/Badge.json","type":["BadgeClass"],"name":"Course 27012021 R21","criteria":{"type":["Criteria"],"narrative":"UPNISHTASVGTemplate"},"issuer":{"context":"https://loadtest.ntp.net.in/certs/v1/context.json","id":"https://loadtest.ntp.net.in/certs/Issuer.json","type":["Issuer"],"name":"up","url":"https://diksha.gov.in/up","publicKey":[]},"@context":"https://loadtest.ntp.net.in/certs/v1/context.json"},"evidence":{"id":"https://loadtest.ntp.net.in/certs/Evidence.json","type":["Evidence","Extension","extensions:TrainingEvidence"],"name":"Course 27012021 R21","@context":"https://loadtest.ntp.net.in/certs/v1/context.json"},"verification":{"type":["hosted"]},"revoked":false,"signatory":[{"identity":"up","type":["Extension","extensions:SignatoryExtension"],"hashed":false,"designation":"Uttar Pradesh","image":"https://diksha.gov.in/images/logo-sahaj.png","@context":"https://loadtest.ntp.net.in/certs/v1/extensions/SignatoryExtension/context.json"}],"@context":"https://loadtest.ntp.net.in/certs/v1/context.json"}');
INSERT INTO sunbird.cert_registry(id, accesscode, data) VALUES('0e045c27-5c9f-4d8d-9f2e-6c123eed63a7', 'C6B2J6', '{"id":"https://loadtest.ntp.net.in/certs/0126796199493140480_0131495768541675525/7e8197e9-0a9c-4cb5-8abc-ea5eac38fa5a","type":["Assertion","Extension","extensions:CertificateExtension"],"issuedOn":"2020-11-19T00:00:00Z","recipient":{"identity":"7b5cc280-ec07-44e3-a5cc-e6fe54d9b79d","type":["id"],"hashed":false,"name":"1597748348-41199","@context":"https://loadtest.ntp.net.in/certs/v1/context.json"},"badge":{"id":"https://loadtest.ntp.net.in/certs/0126796199493140480_0131495768541675525/Badge.json","type":["BadgeClass"],"name":"Course 27012021 R21","criteria":{"type":["Criteria"],"narrative":"UPNISHTASVGTemplate"},"issuer":{"context":"https://loadtest.ntp.net.in/certs/v1/context.json","id":"https://loadtest.ntp.net.in/certs/Issuer.json","type":["Issuer"],"name":"up","url":"https://diksha.gov.in/up","publicKey":[]},"@context":"https://loadtest.ntp.net.in/certs/v1/context.json"},"evidence":{"id":"https://loadtest.ntp.net.in/certs/Evidence.json","type":["Evidence","Extension","extensions:TrainingEvidence"],"name":"Course 27012021 R21","@context":"https://loadtest.ntp.net.in/certs/v1/context.json"},"verification":{"type":["hosted"]},"revoked":false,"signatory":[{"identity":"up","type":["Extension","extensions:SignatoryExtension"],"hashed":false,"designation":"Uttar Pradesh","image":"https://diksha.gov.in/images/logo-sahaj.png","@context":"https://loadtest.ntp.net.in/certs/v1/extensions/SignatoryExtension/context.json"}],"@context":"https://loadtest.ntp.net.in/certs/v1/context.json"}');
