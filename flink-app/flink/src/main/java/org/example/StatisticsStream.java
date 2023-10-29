package org.example;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.HashMap;

public class StatisticsStream extends ProcessAllWindowFunction<BikeRide, PopularStationStatistics, TimeWindow> {

    @Override
    public void process(ProcessAllWindowFunction<BikeRide, PopularStationStatistics, TimeWindow>.Context context,
                        Iterable<BikeRide> elements, Collector  <PopularStationStatistics> out) throws Exception {
        float sum = 0;
        float max = Float.MIN_VALUE;
        float min = Float.MAX_VALUE;
        float avg = 0;
        String station1 = "";
        int numRides1 = 0;
        String station2 = "";
        int numRides2 = 0;
        String station3 = "";
        int numRides3 = 0;
        float count = 0;

        HashMap<String, Integer> popular = new HashMap<>();

        for (BikeRide msg : elements) {
            count ++;
            msg.calculateDuration();
            sum += msg.duration;
            if (msg.duration > max)
                max = msg.duration;
            if (msg.duration < min)
                min = msg.duration;
            if(!popular.containsKey(msg.end_station_name)) {
                popular.put(msg.end_station_name, 1);
            } else {
                int newValue = popular.get(msg.end_station_name) + 1;
                popular.replace(msg.end_station_name, newValue);
            }
        }
        avg = sum / count;

        if (!popular.keySet().isEmpty()) {
            station1 = (String) popular.keySet().toArray()[0];
            numRides1 = popular.get(station1);
        }
        if (popular.keySet().size() > 1) {
            station2 = (String) popular.keySet().toArray()[1];
            numRides2 = popular.get(station2);
        }
        if (popular.keySet().size() > 2) {
            station3 = (String) popular.keySet().toArray()[2];
            numRides3 = popular.get(station3);
        }

        Date date = new Date();

        PopularStationStatistics res = new PopularStationStatistics(date, max, min, avg, station1, numRides1, station2, numRides2, station3, numRides3);
        out.collect(res);

    }

}

