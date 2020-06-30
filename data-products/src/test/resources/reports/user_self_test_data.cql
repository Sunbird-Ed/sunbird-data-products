INSERT INTO sunbird.location (id,code,name,parentid,type) VALUES ('0dd2b51d-baa2-4403-bf1e-012a970b323f','1916080007','Andhra',null,'state');
INSERT INTO sunbird.location (id,code,name,parentid,type) VALUES ('f8228633-7742-454b-8f61-519c6a875809','1922090001','Chittooor','0dd2b51d-baa2-4403-bf1e-012a970b323f','district');
INSERT INTO sunbird.location (id,code,name,parentid,type) VALUES ('439d9cf4-da87-47c6-8211-d24bbc5e3ca8','814090063','Karnataka',null,'state');
INSERT INTO sunbird.location (id,code,name,parentid,type) VALUES ('9c5fd76e-bf1b-4cee-9735-4d9235e819f3','2713050012','Gulbarga','439d9cf4-da87-47c6-8211-d24bbc5e3ca8','district');


INSERT INTO sunbird.user (id, userid, locationids, firstname, lastname) VALUES ('56c2d9a3-fae9-4341-9862-4eeeead2e9a1', '56c2d9a3-fae9-4341-9862-4eeeead2e9a1', null, 'localuser118f', 'localuser118l');
INSERT INTO sunbird.user (id, userid, locationids, firstname, lastname) VALUES ('8eaa1621-ac15-42a4-9e26-9c846963f331', '8eaa1621-ac15-42a4-9e26-9c846963f331', ['0dd2b51d-baa2-4403-bf1e-012a970b323f', 'f8228633-7742-454b-8f61-519c6a875809'],'localuser117f', 'localuser117l');
INSERT INTO sunbird.user (id, userid, locationids, firstname, lastname) VALUES ('7faa1621-bc14-32c4-9e26-4b846963g339', '7faa1621-bc14-32c4-9e26-4b846963g339', ['0dd2b51d-baa2-4403-bf1e-012a970b323f', 'f8228633-7742-454b-8f61-519c6a875809'],'localuser116f', 'localuser116l');
INSERT INTO sunbird.user (id, userid, locationids, firstname, lastname) VALUES ('3150aa06-333e-4d6d-8d81-3d8e14e7b245', '3150aa06-333e-4d6d-8d81-3d8e14e7b245', ['439d9cf4-da87-47c6-8211-d24bbc5e3ca8', '9c5fd76e-bf1b-4cee-9735-4d9235e819f3'],'localuser115f', 'localuser115l');
INSERT INTO sunbird.user (id, userid, locationids, firstname, lastname) VALUES ('6d8269de-c6ca-4106-9b63-305382d46175', '6d8269de-c6ca-4106-9b63-305382d46175', null, 'localuser114f', 'localuser114l');


INSERT INTO sunbird.usr_external_identity (userid, idtype, provider, originalexternalid ) VALUES('56c2d9a3-fae9-4341-9862-4eeeead2e9a1', 'declared-district', 'root_org', 'f8228633-7742-454b-8f61-519c6a875809');
INSERT INTO sunbird.usr_external_identity (userid, idtype, provider, originalexternalid ) VALUES('56c2d9a3-fae9-4341-9862-4eeeead2e9a1', 'declared-ext-id', 'root_org', 'sdkjdsf');
INSERT INTO sunbird.usr_external_identity (userid, idtype, provider, originalexternalid ) VALUES('56c2d9a3-fae9-4341-9862-4eeeead2e9a1', 'declared-phone', 'root_org', 'QlQ7FO2iK4LxklRsdxkPIZ93wgkfm4W28W0QMh2iiPsYP96xbB2LjUr2EILp+05tYFhZOyuDHb1i\nC7s/u3Fw8UpCe9dNGDObMN2TFBOhbuksEcfIPEfihuXho2E0dRBvTQtMGOn7lhkDdxs1iV8l8A==');
INSERT INTO sunbird.usr_external_identity (userid, idtype, provider, originalexternalid ) VALUES('56c2d9a3-fae9-4341-9862-4eeeead2e9a1', 'declared-school-name', 'root_org', 'test');
INSERT INTO sunbird.usr_external_identity (userid, idtype, provider, originalexternalid ) VALUES('56c2d9a3-fae9-4341-9862-4eeeead2e9a1', 'declared-school-udise-code', 'root_org', '22222222222');
INSERT INTO sunbird.usr_external_identity (userid, idtype, provider, originalexternalid ) VALUES('56c2d9a3-fae9-4341-9862-4eeeead2e9a1', 'declared-state', 'root_org', '0dd2b51d-baa2-4403-bf1e-012a970b323f');
INSERT INTO sunbird.usr_external_identity (userid, idtype, provider, originalexternalid ) VALUES('56c2d9a3-fae9-4341-9862-4eeeead2e9a1', 'declared-email', 'root_org', 'ZSRcFGKp5ap1oj2R3GzgL7kgIvHIXwUJVW9uG7jppEmN+CEnv/F6pn9DnJqby+5U0VlM2drSuVbg\nftsFxcQUuSlQM3Z2VJhNEM9uvd+cLzi907zPaD6k2OdZ6ERUgzykTQtMGOn7lhkDdxs1iV8l8A==');

INSERT INTO sunbird.usr_external_identity (userid, idtype, provider, originalexternalid ) VALUES('8eaa1621-ac15-42a4-9e26-9c846963f331', 'declared-ext-id', 'root_org', 'sdkjdsf');
INSERT INTO sunbird.usr_external_identity (userid, idtype, provider, originalexternalid ) VALUES('8eaa1621-ac15-42a4-9e26-9c846963f331', 'declared-phone', 'root_org', 'QlQ7FO2iK4LxklRsdxkPIZ93wgkfm4W28W0QMh2iiPsYP96xbB2LjUr2EILp+05tYFhZOyuDHb1i\nC7s/u3Fw8UpCe9dNGDObMN2TFBOhbuksEcfIPEfihuXho2E0dRBvTQtMGOn7lhkDdxs1iV8l8A==');
INSERT INTO sunbird.usr_external_identity (userid, idtype, provider, originalexternalid ) VALUES('8eaa1621-ac15-42a4-9e26-9c846963f331', 'declared-school-name', 'root_org', 'test');
INSERT INTO sunbird.usr_external_identity (userid, idtype, provider, originalexternalid ) VALUES('8eaa1621-ac15-42a4-9e26-9c846963f331', 'declared-school-udise-code', 'root_org', '22222222222');
INSERT INTO sunbird.usr_external_identity (userid, idtype, provider, originalexternalid ) VALUES('8eaa1621-ac15-42a4-9e26-9c846963f331', 'declared-email', 'root_org', 'ZSRcFGKp5ap1oj2R3GzgL7kgIvHIXwUJVW9uG7jppEmN+CEnv/F6pn9DnJqby+5U0VlM2drSuVbg\nftsFxcQUuSlQM3Z2VJhNEM9uvd+cLzi907zPaD6k2OdZ6ERUgzykTQtMGOn7lhkDdxs1iV8l8A==');

