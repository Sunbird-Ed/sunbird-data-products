package org.sunbird.analytics.util

import java.sql.{ResultSet, Statement}

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import java.sql.Connection

object EmbeddedPostgresSql {

    var pg: EmbeddedPostgres = null;
    var connection: Connection = null;
    var stmt: Statement = null;

    def start() {
        pg = EmbeddedPostgres.builder().setPort(65124).start()
        connection = pg.getPostgresDatabase().getConnection()
        stmt = connection.createStatement()
    }

    def createTables(): Boolean = {
        val query1 = "CREATE TABLE job_request(tag VARCHAR(50), request_id VARCHAR(50), job_id VARCHAR(50), status VARCHAR(50), request_data json, requested_by VARCHAR(50), requested_channel VARCHAR(50), dt_job_submitted TIMESTAMP, download_urls text[], dt_file_created TIMESTAMP, dt_job_completed TIMESTAMP, execution_time INTEGER, err_message VARCHAR(100), PRIMARY KEY (tag, request_id))"
        execute(query1)
    }

    def execute(sqlString: String): Boolean = {
        stmt.execute(sqlString)
    }

    def executeQuery(sqlString: String): ResultSet = {
        stmt.executeQuery(sqlString)
    }

    def close() {
        stmt.close()
        connection.close()
        pg.close()
    }
}