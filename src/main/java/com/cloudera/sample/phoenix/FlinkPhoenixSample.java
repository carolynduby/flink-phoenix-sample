package com.cloudera.sample.phoenix;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
import java.time.Duration;

@Slf4j
public class FlinkPhoenixSample {

    private static final String PARAMS_CHECKPOINT_INTERVAL = "checkpoint.interval.ms";
    private static final int DEFAULT_CHECKPOINT_INTERVAL = 60000;
    public static final String PARAMS_PARALLELISM = "parallelism";
    private static final int DEFAULT_PARALLELISM = 1;

    private static final String DEFAULT_JOB_NAME = "Flink Phoenix Example";
    protected static final String DRIVER_NAME = "org.apache.phoenix.jdbc.PhoenixDriver";
    private static final String PARAMS_PHOENIX_DB_BATCH_SIZE = "phoenix.db.batchSize";

    private static final String TABLE_CREATE_SQL =
            "CREATE TABLE IF NOT EXISTS %s (\n" +
            "    SENSOR_ID INTEGER NOT NULL,\n" +
            "    MEASUREMENT_TIME TIMESTAMP NOT NULL,\n" +
            "    MEASUREMENT_VALUE DOUBLE NOT NULL,\n" +
            "     CONSTRAINT pk PRIMARY KEY(SENSOR_ID, MEASUREMENT_TIME,MEASUREMENT_VALUE)\n" +
            ")";

    private static final String TABLE_INSERT_SQL =
            "UPSERT INTO %s (SENSOR_ID, MEASUREMENT_TIME, MEASUREMENT_VALUE) VALUES(?, ?, ?) ON DUPLICATE KEY IGNORE";

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Path to the properties file is expected as the only argument.");
        }
        ParameterTool params = ParameterTool.fromPropertiesFile(args[0]);

        String phoenixMeasurementTableName =  params.getRequired("phoenix.measurement.table");
        String measurementTableCreate = String.format(TABLE_CREATE_SQL, params.getRequired("phoenix.measurement.table"));

        String phoenixDbUrl = params.getRequired("phoenix.db.url");
        String phoenixUser = params.getRequired("phoenix.db.user");
        String phoenixPw = params.getRequired("phoenix.db.pw");

        PhoenixThickClient   phoenixClient = new PhoenixThickClient(phoenixUser, phoenixPw, phoenixDbUrl);
        phoenixClient.executeSql(measurementTableCreate);

        log.info("Measurement table SQL {}", measurementTableCreate);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(params.getInt(PARAMS_CHECKPOINT_INTERVAL, DEFAULT_CHECKPOINT_INTERVAL), CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(params.getInt(PARAMS_PARALLELISM, DEFAULT_PARALLELISM));
        env.getConfig().setGlobalJobParameters(params);

        WatermarkStrategy<SensorData> watermarkStrategy = WatermarkStrategy
                .<SensorData>forBoundedOutOfOrderness(Duration.ofMillis(1000))
                .withTimestampAssigner((sensorData, timestamp) -> sensorData.getTimestamp());
        DataStream<SensorData> sensorMeasurements = env.addSource(new RandomSensorDataGenerator()).assignTimestampsAndWatermarks(watermarkStrategy);

        DataStream<SensorMaximum> sensorMaximumDataStream = sensorMeasurements.keyBy(SensorData::getId).window(TumblingEventTimeWindows.of(Time.minutes(2))).aggregate(new SensorAggregatorFunction());
        writeToPhoenix(params, String.format(TABLE_INSERT_SQL, phoenixMeasurementTableName), sensorMaximumDataStream, phoenixDbUrl, phoenixUser, phoenixPw);
        env.execute(params.get("flink.job.name", DEFAULT_JOB_NAME));

    }


    private static void writeToPhoenix(ParameterTool params, String measurementTableUpsertSql, DataStream<SensorMaximum> sensorMaximumDataStream, String phoenixDbUrl, String phoenixDbUser, String phoenixDbPw) {
        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName(DRIVER_NAME)
                .withUrl(phoenixDbUrl)
                .withUsername(phoenixDbUser)
                .withPassword(phoenixDbPw)
                .build();
        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(Integer.parseInt(params.getRequired(PARAMS_PHOENIX_DB_BATCH_SIZE)))
                .build();
        SinkFunction<SensorMaximum> jdbcSink = JdbcSink.sink(
                measurementTableUpsertSql,
                (preparedStatement, sensorMaximum) -> {
                        preparedStatement.setInt(1, sensorMaximum.getId());
                        preparedStatement.setTimestamp(2, new Timestamp(sensorMaximum.getTimestamp()));
                        preparedStatement.setDouble(3, sensorMaximum.getMaxMeasurement());
                },
                executionOptions,
                connectionOptions);
        sensorMaximumDataStream.addSink(jdbcSink).name("JDBC Sink").uid("jdbc.measurement.max.sink");
    }

}
