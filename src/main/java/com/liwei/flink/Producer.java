package com.liwei.flink;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.util.Properties;

public class Producer implements Runnable {

    private final KafkaProducer<String, String> producer;
    private final String topic;

    public Producer(String topicName) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        this.producer = new KafkaProducer<String, String>(props);
        this.topic = topicName;
    }

    public void run() {
        FileInputStream fi = null;
        String data = null;
        byte[] b;
        try {
            fi = new FileInputStream(new File("D:\\result.txt"));
            b = new byte[1024];

            while (fi.read(b) != -1) {
                data = new String(b);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            for (; ; ) {
                producer.send(new ProducerRecord<String, String>(topic, "Message", data));
                Thread.sleep(1000 * 30);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    public static void main(String args[]) {
        Producer test = new Producer("k1");
        Thread thread = new Thread(test);
        thread.start();
    }

}
