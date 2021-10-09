package com.cloudera.sample.phoenix;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.accumulators.Accumulator;

import java.time.Instant;

@AllArgsConstructor
@NoArgsConstructor
public class SensorAccumulator implements Accumulator<SensorData, SensorMaximum> {
    int sensorId;
    double maximumValue = Double.MIN_VALUE;

    @Override
    public void add(SensorData sensorData) {
        this.sensorId = sensorData.getId();
        this.maximumValue = Math.max(maximumValue, sensorData.getMeasure());
    }

    @Override
    public SensorMaximum getLocalValue() {
        return new SensorMaximum(sensorId, Instant.now().toEpochMilli(), maximumValue);
    }

    @Override
    public void resetLocal() {
        maximumValue = Double.MIN_VALUE;
    }

    @Override
    public void merge(Accumulator accumulator) {
        SensorAccumulator sensorAccumulator = ((SensorAccumulator)accumulator);
        sensorId = sensorAccumulator.sensorId;
        maximumValue = Math.max(((SensorAccumulator)accumulator).maximumValue, maximumValue);

    }

    @Override
    public Accumulator clone() {
        return new SensorAccumulator(sensorId, maximumValue);
    }
}
