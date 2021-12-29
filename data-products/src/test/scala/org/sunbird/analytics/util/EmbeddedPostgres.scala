package org.sunbird.analytics.util

import java.sql.{ResultSet, Statement}

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import java.sql.Connection

object EmbeddedPostgresql {

  var pg: EmbeddedPostgres = null;
  var connection: Connection = null;
  var stmt: Statement = null;

  def start() {
    pg = EmbeddedPostgres.builder().setPort(65124).start()
    connection = pg.getPostgresDatabase().getConnection()
    stmt = connection.createStatement()
  }

  def createNominationTable(): Boolean = {
    val tableName: String = "nomination"
    val query = s"""
                   |CREATE TABLE IF NOT EXISTS $tableName (
                   |    program_id TEXT PRIMARY KEY,
                   |    status TEXT,
                   |    user_id TEXT)""".stripMargin

    execute(query)
  }

  def createProgramTable(): Boolean = {
    val tableName: String = "program"
    val query = s"""
                   |CREATE TABLE IF NOT EXISTS $tableName (
                   |    program_id TEXT PRIMARY KEY,
                   |    name TEXT,
                   |    enddate TEXT,
                   |    rootorg_id TEXT,
                   |    channel TEXT,
                   |    status TEXT,
                   |    startdate TEXT)""".stripMargin

    execute(query)
  }

  def createJobRequestTable(): Unit = {
    val tableName: String = "job_request"
    val query =
      s"""
         |CREATE TABLE IF NOT EXISTS $tableName (
         |   tag TEXT,
         |   request_id TEXT PRIMARY KEY,
         |   job_id TEXT,
         |   status TEXT,
         |   request_data json,
         |   requested_by TEXT,
         |   requested_channel TEXT,
         |   dt_job_submitted TIMESTAMP,
         |   download_urls TEXT[],
         |   dt_file_created TIMESTAMP,
         |   dt_job_completed TIMESTAMP,
         |   execution_time bigint,
         |   err_message TEXT,
         |   iteration int,
         |   encryption_key TEXT,
         |   processed_batches TEXT
         |)
      """.stripMargin
    execute(query)
  }


  def createConversationTable(): Unit = {
    val tableName: String = "bot"
    val query =
      s"""
         |CREATE TABLE IF NOT EXISTS $tableName (
         |   id UUID PRIMARY KEY,
         |   name TEXT,
         |   startingMessage TEXT,
         |   users TEXT[],
         |   logicIDs TEXT[],
         |   owners TEXT[],
         |   ownerorgid TEXT,
         |   created_at TIMESTAMP,
         |   updated_at TIMESTAMP,
         |   status TEXT,
         |   description TEXT,
         |   startDate DATE,
         |   endDate DATE,
         |   purpose TEXT
         |)
      """.stripMargin
    execute(query)
  }

  def createUserTable(): Unit = {
    val tableName: String = "users"
    val query =
      s"""
         |CREATE TABLE IF NOT EXISTS $tableName (
         |   id UUID PRIMARY KEY,
         |   active BOOLEAN,
         |   birth_date character(10),
         |   clean_speak_id UUID,
         |   data TEXT,
         |   expiry BIGINT,
         |   first_name character,
         |   full_name character,
         |   image_url TEXT,
         |   insert_instant bigint,
         |   last_name character,
         |   last_update_instant bigint,
         |   middle_name character,
         |   mobile_phone character,
         |   parent_email character,
         |   tenants_id uuid,
         |   timezone character
         |)
      """.stripMargin
    execute(query)
  }



  def createUserRegistrationTable(): Unit = {
    val tableName: String = "user_registrations"
    val query =
      s"""
         |CREATE TABLE IF NOT EXISTS $tableName (
         |   id UUID PRIMARY KEY,
         |   applications_id UUID,
         |   authentication_token character(255),
         |   clean_speak_id UUID,
         |   data TEXT,
         |   insert_instant BIGINT,
         |   last_login_instant BIGINT,
         |   last_update_instant BIGINT,
         |   timezone character,
         |   username character,
         |   username_status smallint,
         |   users_id uuid,
         |   verified boolean
         |)
      """.stripMargin
    execute(query)
  }

  def createIdentitiesTable(): Unit = {
    val tableName: String = "identities"
    val query =
      s"""
         |CREATE TABLE IF NOT EXISTS $tableName (
         |   id UUID PRIMARY KEY,
         |   applications_id UUID,
         |   authentication_token character(255),
         |   clean_speak_id UUID,
         |   data TEXT,
         |   insert_instant BIGINT,
         |   last_login_instant BIGINT,
         |   last_update_instant BIGINT,
         |   timezone character,
         |   username character(100),
         |   username_status smallint,
         |   users_id uuid,
         |   verified boolean
         |)
      """.stripMargin
    execute(query)
  }


  def execute(sqlString: String): Boolean = {
    stmt.execute(sqlString)
  }

  def executeQuery(sqlString: String): ResultSet = {
    stmt.executeQuery(sqlString)
  }

  def dropTable(tableName: String): Boolean = {
    stmt.execute(s"DROP TABLE $tableName")
  }

  def close() {
    stmt.close()
    connection.close()
    pg.close()
  }
}
