package com.atguigu.utils;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

public class MyKafkaUtil {
    private static String brokers="ubuntu:9092,ubuntu02:9092";
    private static String default_topic="DWD_DEFAULT_TOPIC";
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        return new FlinkKafkaProducer<String>(brokers,
                topic,
                new SimpleStringSchema());
    }

    public static <T> FlinkKafkaProducer<T> getKafkaProducer(KafkaSerializationSchema<T> KafkaSerializationSchema) {

        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers);
        return new FlinkKafkaProducer<T>(default_topic,
                KafkaSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.NONE);
    }
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic,String groupId){
        Properties properties=new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers);
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),properties);

    }
}
