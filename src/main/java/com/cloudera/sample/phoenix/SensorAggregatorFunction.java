package com.cloudera.sample.phoenix;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichAggregateFunction;

public class SensorAggregatorFunction implements AggregateFunction<SensorData, SensorAccumulator, SensorMaximum> {

    @Override
    public SensorAccumulator createAccumulator() {
        return new SensorAccumulator();
    }

    @Override
    public SensorAccumulator add(SensorData sensorData, SensorAccumulator sensorAccumulator) {
        sensorAccumulator.add(sensorData);
        return sensorAccumulator;
    }

    @Override
    public SensorMaximum getResult(SensorAccumulator sensorAccumulator) {
        return sensorAccumulator.getLocalValue();
    }

    @Override
    public SensorAccumulator merge(SensorAccumulator acc1, SensorAccumulator acc2) {
        acc1.merge(acc2);
        return acc1;
    }
}
