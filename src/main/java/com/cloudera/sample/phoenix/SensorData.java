package com.cloudera.sample.phoenix;

import java.io.Serializable;

import lombok.*;


@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class SensorData implements Serializable {

    private static final long serialVersionUID = 3065800674328381716L;
    private Integer id;
    private Long timestamp;
    private Double measure;

}