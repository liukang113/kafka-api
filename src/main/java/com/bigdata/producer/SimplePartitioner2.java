package com.bigdata.producer;

/**
 * Created with IntelliJ IDEA.
 * Author: kang.liu
 * Date  : 2018/6/5 17:43
 * Description:
 */

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public class SimplePartitioner2 implements Partitioner {

    @Override
    public void configure(Map<String, ?> map) {
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int partition = 0;
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        String stringKey = (String) key;
        int offset = stringKey.lastIndexOf('.');
        if (offset > 0) {
            partition = Integer.parseInt(stringKey.substring(offset + 1)) % numPartitions;
        }

        return partition;
    }

    @Override
    public void close() {

    }
}