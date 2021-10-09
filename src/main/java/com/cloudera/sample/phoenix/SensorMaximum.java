package com.cloudera.sample.phoenix;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class SensorMaximum implements Serializable {

    private static final long serialVersionUID = 3065800674328381716L;
    private Integer id;
    private Long timestamp;
    private Double maxMeasurement;
}