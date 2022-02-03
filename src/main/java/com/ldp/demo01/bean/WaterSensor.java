package com.ldp.demo01.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ID : 传感器编号
 * ts： 时间戳
 * vc: 水位
 */
/*@Data
@NoArgsConstructor
@AllArgsConstructor*/
public class WaterSensor {
    private String id;
    private Long ts;
    private Double vc;

    public WaterSensor() {
    }

    public WaterSensor(String id, Long ts, Double vc) {
        this.id = id;
        this.ts = ts;
        this.vc = vc;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Double getVc() {
        return vc;
    }

    public void setVc(Double vc) {
        this.vc = vc;
    }

    @Override
    public String toString() {
        return "WaterSensor{" +
                "id='" + id + '\'' +
                ", ts=" + ts +
                ", vc=" + vc +
                '}';
    }
}
