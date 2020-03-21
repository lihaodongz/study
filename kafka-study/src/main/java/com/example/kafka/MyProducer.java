package com.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class MyProducer {


    private static KafkaProducer<String,String> producer;

    static {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","192.168.16.193:9092");
        //properties.put("zookeeper","192.168.16.193:2181");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("partitioner.class","com.example.kafka.CustomPartitioner");
        producer = new KafkaProducer<String, String>(properties);
    }

    /*异步发送消息*/
    private static void sendMessageForgetResult(){
        ProducerRecord record = new ProducerRecord<>(
                "topic123",
                "key1",
                "ayncvalue"
        );
        producer.send(record);
        record = new ProducerRecord<>(
                "topic123",
                "key2",
                "ayncvalue"
        );
        producer.send(record);
        record = new ProducerRecord<>(
                "topic123",
                "key3",
                "ayncvalue"
        );
        producer.send(record);
        record = new ProducerRecord<>(
                "topic123",
                "key4",
                "ayncvalue"
        );
        producer.send(record);
        producer.close();
    }


    /*同步发送消息*/
    public static void sendMessageSync() throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "test",
                "key",
                "syncvalue"
        );


        RecordMetadata result = producer.send(record).get();
        System.out.println(result.topic());
        System.out.println(result.partition());
        System.out.println(result.offset());
        producer.close();
    }

    public static class MyProducerCallBack implements Callback{

        @Override
        public void onCompletion(RecordMetadata result, Exception e) {
            if (e!=null){
                e.printStackTrace();
                return;
            }
            System.out.println(result.topic());
            System.out.println(result.partition());
            System.out.println(result.offset());
            System.out.println("comming in myproducercallback");
        }
    }

    public static void sendMessageCallBack(){
        ProducerRecord record = new ProducerRecord<>(
                "test",
                "消息",
                "消息内容"
        );
        producer.send(record,new MyProducerCallBack());
        producer.close();

    }
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        sendMessageForgetResult();
        /*sendMessageCallBack();
         sendMessageSync();*/
    }
}
