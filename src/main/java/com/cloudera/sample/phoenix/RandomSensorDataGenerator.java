package com.cloudera.sample.phoenix;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.time.Instant;
import java.util.Random;

public class RandomSensorDataGenerator extends RichParallelSourceFunction<SensorData> {

    private static final int MAX_SENSORS = 10;
    private volatile boolean cancelled = false;
    private Random random;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        random = new Random();
    }

    @Override
    public void run(SourceContext<SensorData> ctx) throws Exception {
        while (!cancelled) {
             for (int sensorId = 0; sensorId < MAX_SENSORS; sensorId++) {
                Double nextMeasure = random.nextDouble();
                ctx.collect(new SensorData(sensorId, Instant.now().toEpochMilli(), nextMeasure));
            }
            Thread.sleep(500);
        }
    }

    @Override
    public void cancel() {
        cancelled = true;
    }
}