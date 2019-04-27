package org.apache.storm.starter;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class OurCustomMessageScheme implements Scheme {

    @Override
    public List<Object> deserialize(ByteBuffer bytes) {
        return new Values(new String(bytes.array(), StandardCharsets.UTF_8));
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(""); // "payload"
    }

}
