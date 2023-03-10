```sh
[INFO] Running org.apache.beam.examples.AppTest
[ERROR] Tests run: 1, Failures: 0, Errors: 1, Skipped: 0, Time elapsed: 254.64 s <<< FAILURE! - in org.apache.beam.examples.AppTest
[ERROR] org.apache.beam.examples.AppTest.testJdbcIO  Time elapsed: 254.64 s  <<< ERROR!
org.apache.beam.sdk.Pipeline$PipelineExecutionException: java.sql.SQLException: Cannot create PoolableConnectionFactory (The authentication type 10 is not supported. Check that you have configured the pg_hba.conf file to include the client's IP address or subnet, and that it is using an authentication scheme supported by the driver.)
	at org.apache.beam.examples.AppTest.testJdbcIO(AppTest.java:1709)
Caused by: java.sql.SQLException: Cannot create PoolableConnectionFactory (The authentication type 10 is not supported. Check that you have configured the pg_hba.conf file to include the client's IP address or subnet, and that it is using an authentication scheme supported by the driver.)
Caused by: org.postgresql.util.PSQLException: The authentication type 10 is not supported. Check that you have configured the pg_hba.conf file to include the client's IP address or subnet, and that it is using an authentication scheme supported by the driver.
```