home=`echo $HOME`
jobJarPath="${home}/etl-jobs-1.0/etl-jobs-1.0.jar"
jobConfPath="${home}/etl-jobs-1.0/resources/.conf"

spark/bin/spark-submit \
--conf spark.driver.extraJavaOptions="-Dconfig.file=${jobConfPath}" \
--class org.sunbird.learner.jobs.UserLookupMigration \
${jobJarPath}