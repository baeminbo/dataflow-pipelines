package baeminbo;

import javax.sql.DataSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcWritePipeline {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcWritePipeline.class);

  // Fill out the following constants

  private static final String DBNAME = "";
  private static final String INSTANCE = ""; // <PROJECT_ID>:<REGION>:<INSTANCE_NAME>
  private static final String USERNAME = "";
  private static final String PASSWORD = "";

  private static final String DRIVER_NAME = "com.mysql.jdbc.Driver";

  private static final String CONNECT_URL =
      String.format(
          "jdbc:mysql://google/%s?cloudSqlInstance=%s&socketFactory=com.google.cloud.sql.mysql.SocketFactory&user=%s&password=%s&useSSL=false",
          DBNAME,
          INSTANCE,
          USERNAME,
          PASSWORD);

  private static final DataSourceProvider dataSourceSingleton = new DataSourceProvider();


  // Using following query to create table
  // ```
  // CREATE TABLE `table1` (
  //   `id` bigint(20) NOT NULL,
  //   `value` varchar(1024) DEFAULT NULL,
  //   PRIMARY KEY (`id`)
  // ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  // ```
  private static final String INSERT_QUERY = "INSERT INTO table1 (id, value) VALUES (?, ?)";

  private static void  checkConstants() {
    if (DBNAME == null || DBNAME.isEmpty()) {
      throw new RuntimeException("DBNAME is null or empty");
    }

    if (INSTANCE == null || INSTANCE.isEmpty()) {
      throw new RuntimeException("INSTANCE is null or empty");
    }
    
    if (USERNAME == null || USERNAME.isEmpty()) {
      throw new RuntimeException("USERNAME is null or empty");
    }

    if (PASSWORD == null || PASSWORD.isEmpty()) {
      throw new RuntimeException("PASSWORD is nul or empty");
    }
  }

  public static void main(String[] args) {
    checkConstants();

    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    // The following pipeline will insert 10 rows to SQL
    // (0, "0"), (1, "1"), ..., (9, "9")
    pipeline
        .apply(GenerateSequence.from(0).to(10))
        .apply(JdbcIO.<Long>write()
            // .withDataSourceConfiguration(DataSourceConfiguration.create(DRIVER_NAME, CONNECT_URL))
            .withDataSourceProviderFn(dataSourceSingleton)
            .withStatement(INSERT_QUERY)
            .withPreparedStatementSetter((PreparedStatementSetter<Long>) (element, stmt) -> {
              LOG.info("Statement set. element:{}", element);
              stmt.setLong(1, element);
              stmt.setString(2, Long.toString(element));
            }));

    pipeline.run();
  }

  public static class DataSourceProvider implements SerializableFunction<Void, DataSource> {
    private BasicDataSource dataSource;

    @Override
    public synchronized BasicDataSource apply(Void input) {
      if (dataSource != null ) {
        LOG.info("Reusing DataSource");
        return dataSource;
      }

      LOG.info("Building new DataSource");
      BasicDataSource dataSource = new BasicDataSource();
      dataSource.setDriverClassName(DRIVER_NAME);
      dataSource.setUrl(CONNECT_URL);
      dataSource.setTestOnBorrow(true);
      dataSource.setValidationQuery("SELECT 1");

      return dataSource;
    }
  }
}
