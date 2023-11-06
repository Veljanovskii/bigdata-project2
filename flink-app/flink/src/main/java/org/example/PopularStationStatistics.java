package org.example;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

import java.util.Date;

@Table(keyspace = "flink", name = "stats")
public class PopularStationStatistics {
    @Column(name = "time")
    public Date  time;
    @Column(name = "max_duration")
    public Float max_duration;
    @Column(name = "min_duration")
    public Float min_duration;
    @Column(name = "avg_duration")
    public Float avg_duration;
    @Column(name = "station1")
    public String station1;
    @Column(name = "num_rides1")
    public Integer num_rides1;
    @Column(name = "station2")
    public String station2;
    @Column(name = "num_rides2")
    public Integer num_rides2;
    @Column(name = "station3")
    public String station3;
    @Column(name = "num_rides3")
    public Integer num_rides3;

    public PopularStationStatistics() {
    }

    public PopularStationStatistics(Date time, Float max_duration, Float min_duration, Float avg_duration, String station1, Integer num_rides1, String station2, Integer num_rides2, String station3, Integer num_rides3) {
        this.time = time;
        this.max_duration = max_duration;
        this.min_duration = min_duration;
        this.avg_duration = avg_duration;
        this.station1 = station1;
        this.num_rides1 = num_rides1;
        this.station2 = station2;
        this.num_rides2 = num_rides2;
        this.station3 = station3;
        this.num_rides3 = num_rides3;
    }

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }

    public Float getMax_duration() {
        return max_duration;
    }

    public void setMax_duration(Float max_duration) {
        this.max_duration = max_duration;
    }

    public Float getMin_duration() {
        return min_duration;
    }

    public void setMin_duration(Float min_duration) {
        this.min_duration = min_duration;
    }

    public Float getAvg_duration() {
        return avg_duration;
    }

    public void setAvg_duration(Float avg_duration) {
        this.avg_duration = avg_duration;
    }

    public String getStation1() {
        return station1;
    }

    public void setStation1(String station1) {
        this.station1 = station1;
    }

    public Integer getNum_rides1() {
        return num_rides1;
    }

    public void setNum_rides1(Integer num_rides1) {
        this.num_rides1 = num_rides1;
    }

    public String getStation2() {
        return station2;
    }

    public void setStation2(String station2) {
        this.station2 = station2;
    }

    public Integer getNum_rides2() {
        return num_rides2;
    }

    public void setNum_rides2(Integer num_rides2) {
        this.num_rides2 = num_rides2;
    }

    public String getStation3() {
        return station3;
    }

    public void setStation3(String station3) {
        this.station3 = station3;
    }

    public Integer getNum_rides3() {
        return num_rides3;
    }

    public void setNum_rides3(Integer num_rides3) {
        this.num_rides3 = num_rides3;
    }
}
