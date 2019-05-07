/*
 * Poul Møller Hansen <ph@pbnet.dk> (2019)
 * Created on 27. apr. 2019
 */
package dk.pbnet.streams;

import java.text.DateFormat;
import java.util.Properties;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.streams.StreamsConfig;

/**
 *
 * @author pmh
 */
public class Producer {

    DateFormat df;
    Random random;
    TimeZone tz;
    Tuple tuple;
    UUID uuid;
    Long key = 0L;
    final String BOOTSTRAP_SERVERS = "testmq1:9092, testmq2:9092, "
            + "testmq3:9092, testmq4:9092, testmq5:9092, testmq6:9092";
    final String TOPIC = "test";

    public Producer() {
        random = new Random();

        Properties props = new Properties();
        props.put("max.block.ms", "1000");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, TOPIC);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Tuple.class.getName());

        try (KafkaProducer<Long, Tuple> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 3000; i++) {
                System.out.println("msg num: " + i);
                // Bogus data!
                tuple = new Tuple(
                        UUID.randomUUID(),
                        System.currentTimeMillis(),
                        random(-6480000, 6480000), // decimal longitude * 36000
                        random(-3240000, 3240000) // decimal latitude * 36000
                );

                ProducerRecord<Long, Tuple> record = new ProducerRecord<>(TOPIC, ++key, tuple);
                producer.send(record, (RecordMetadata recordMetadata, Exception e) -> {
                    if (e == null) {
                        System.out.println("Record sent successfully. \n"
                                + "Topic : " + recordMetadata.topic() + "\n"
                                + "Partition : " + recordMetadata.partition() + "\n"
                                + "Offset : " + recordMetadata.offset() + "\n"
                                + "Timestamp: " + recordMetadata.timestamp() + "\n");
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException ex) {
                            ex.printStackTrace();
                        }
                    } else {
                        System.out.println("Error sending producer: " + e);
                    }
                });
                producer.flush();
            }
        }
    }

    private Integer random(Integer min, Integer max) {
        return random.nextInt((max - min) + 1) + min;
    }

    public static void main(String[] args) throws Exception {
        new Producer();
    }
}
