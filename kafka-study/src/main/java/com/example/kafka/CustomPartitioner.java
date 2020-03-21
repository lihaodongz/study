package com.example.kafka;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {
    @Override
    public int partition(String topic,
                         Object key,
                         byte[] keyBytes,
                         Object value,
                         byte[] valueBytes,
                         Cluster cluster) {

        List<PartitionInfo> partitons =
                cluster.partitionsForTopic(topic);
        int partitionNum = partitons.size();
        if(null == keyBytes || !(key instanceof String)){
            throw new InvalidRecordException("kafka message must have key");
        }
        if (1 == partitionNum){
            return 0;
        }
        if (key.equals("name")){
            return partitionNum-1;
        }
        return Math.abs(Utils.murmur2(keyBytes) % (partitionNum -1));
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
