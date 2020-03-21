package com.example.kafka;

import com.sun.jmx.snmp.daemon.CommunicatorServer;
import kafka.admin.ConsumerGroupCommand;
import kafka.common.FailedToSendMessageException;
import kafka.common.Topic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import scala.util.Try;

import javax.lang.model.element.NestingKind;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/*消费方式
1.自动提交位移
2.手动提交当前位移
3.手动异步提交当前位移
4.手动异步提交当前位移带回调
5.混合同步与异步提交位移*/
public class MyConsumer {




    private static KafkaConsumer<String,String> consumer;
    private static Properties properties;
    static {
        properties = new Properties();
        properties.put("bootstrap.servers","192.168.16.193:9092");
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id","kafkaStydy");
    }


    private static void generalConusmerMesageAutoCommit(){

        properties.put("enable.auto.commit", true);  //取消自动提交
        KafkaConsumer consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton("topic123"));
        try {
            while (true){
                boolean flag = true;
                 ConsumerRecords<String,String> records
                         = consumer.poll(100);
                 for (ConsumerRecord<String,String> record :records){
                     System.out.println(String.format("topic = %s, " +
                             "partition = %s, key = %s,value = %s",record.topic(),
                             record.partition(), record.key(),record.value()));
                     if (record.value().equals("done")){
                         flag = false;
                     }
                 }
                if (!flag){
                    break;
                }
            }
        }finally {
            consumer.close();
        }
    }

    private static void generalConusmerMesageSyncCommit(){

        properties.put("enable.auto.commit", false);  //取消自动提交
        KafkaConsumer consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton("topic123"));
        try {
            while (true){
                boolean flag = true;
                ConsumerRecords<String,String> records
                        = consumer.poll(100);
                for (ConsumerRecord<String,String> record :records){
                    System.out.println(String.format("topic = %s, " +
                                    "partition = %s, key = %s,value = %s",record.topic(),
                            record.partition(), record.key(),record.value()));
                    if (record.value().equals("done")){
                        flag = false;
                    }
                }
                try {
                    /*同步提交 但是提交过程会阻塞线程 */
                    consumer.commitSync();
                }catch (CommitFailedException e){
                    e.printStackTrace();
                    System.out.println(e.getMessage());
                }
                if (!flag){
                    break;
                }
            }
        }finally {
            consumer.close();
        }
    }

    private static void generalConusmerMesageASyncCommit(){

        properties.put("enable.auto.commit", false);  //取消自动提交
        KafkaConsumer consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton("topic123"));
        try {
            while (true){
                boolean flag = true;
                ConsumerRecords<String,String> records
                        = consumer.poll(100);
                for (ConsumerRecord<String,String> record :records){
                    System.out.println(String.format("topic = %s, " +
                                    "partition = %s, key = %s,value = %s",record.topic(),
                            record.partition(), record.key(),record.value()));
                    if (record.value().equals("done")){
                        flag = false;
                    }
                }
                    /*异步提交 返回失败之后不会重试 不兼容失败
                    *  多个异步提交会发生位移覆盖
                    *  commit A  offset 2000
                    *  commit B  ofset  3000
                    *  */
                    consumer.commitAsync();
                if (!flag){
                    break;
                }
            }
        }finally {
            consumer.close();
        }
    }


    private  static  void generaConsumerMessageCommitWithCallback(){
        properties.put("enable.auto.commit", false);  //取消自动提交
        KafkaConsumer consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton("topic123"));
        try {
            while (true){
                boolean flag = true;
                ConsumerRecords<String,String> records
                        = consumer.poll(100);
                for (ConsumerRecord<String,String> record :records){
                    System.out.println(String.format("topic = %s, " +
                                    "partition = %s, key = %s,value = %s",record.topic(),
                            record.partition(), record.key(),record.value()));
                    if (record.value().equals("done")){
                        flag = false;
                    }
                }
               /* consumer.commitAsync((map,e) -> {
                        if (null != e) {
                            e.printStackTrace();
                            //打印信息
                        }
                        *//*设置失败次数，重试机制*//*
                });*/
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                        /////
                    }
                });
                if (!flag){
                    break;
                }
            }
        }finally {
            consumer.close();
        }
    }

    private static void mixSyncAndSyncCommit(){
        properties.put("enable.auto.commit", false);  //取消自动提交
        KafkaConsumer consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton("topic123"));
        try {
            
            while (true){
                ConsumerRecords<String, String> consumerRecords
                        = consumer.poll(100);
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    System.out.println(String.format("topic = %s, " +
                                    "partition = %s, key = %s,value = %s",record.topic(),
                            record.partition(), record.key(),record.value()));
                }
                consumer.commitAsync();
            }
        }catch (Exception e){
            System.out.println("commit async error:{}"+e.getMessage());
        }finally {
            try {
                consumer.commitSync();  //保证成功
            }finally {
                consumer.close();
            }
        }
    }
    public static void main(String[] args) {
        generalConusmerMesageAutoCommit();
    }

}
