package org.example;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class BikeRide {
    public String rideable_type;
    public String started_at;
    public String ended_at;
    public String start_station_name;
    public String start_station_id;
    public String end_station_name;
    public String end_station_id;
    public String start_lat;
    public String start_lng;
    public String end_lat;
    public String end_lng;
    public String member_casual;
    public int duration;

    public BikeRide(String started_at, String ended_at) {

    }

    public void calculateDuration() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime startTime = LocalDateTime.parse(started_at, formatter);
        LocalDateTime endTime = LocalDateTime.parse(ended_at, formatter);

        Duration rideDuration = Duration.between(startTime, endTime);

        long seconds = rideDuration.getSeconds();
        this.duration = (int) seconds;
    }
}


