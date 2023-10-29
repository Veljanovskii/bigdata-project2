package org.example;

import com.google.gson.Gson;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class DeserializationKafka implements DeserializationSchema<BikeRide> {
    private static final Gson gson = new Gson();

    @Override
    public TypeInformation<BikeRide> getProducedType() {
        return TypeInformation.of(BikeRide.class);
    }

    @Override
    public BikeRide deserialize(byte[] arg0) throws IOException {
        return gson.fromJson(new String(arg0), BikeRide.class);
    }

    @Override
    public boolean isEndOfStream(BikeRide arg0) {
        return false;
    }

}
