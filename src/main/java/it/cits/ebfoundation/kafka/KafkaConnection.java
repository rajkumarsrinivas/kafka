package it.cits.ebfoundation.kafka;

import it.cits.ebfoundation.util.PropertyUtil;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.stream.IntStream;

public class KafkaConnection {
    private static Logger gLogger = Logger.getLogger(KafkaConnection.class);
    private static KafkaConsumer<String, String> gConsumer;
    private static KafkaConnection gKafkaConn;
    static {
        gKafkaConn = new KafkaConnection();
    }

    private KafkaConnection() {
        initialize();
    }

    public static KafkaConnection getInstance() throws NullPointerException {
        if (gKafkaConn == null) {
            throw new NullPointerException("Kafka Producer not initialized");
        }
        gLogger.info("Kafka consumer instance created, yet to be initialized...");
        return gKafkaConn;
    }

    private static void initialize() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertyUtil.getProperty("BOOTSTRAP_SERVERS_CONFIG"));
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, PropertyUtil.getProperty("SECURITY_PROTOCOL_CONFIG"));
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, PropertyUtil.getProperty("SSL_TRUSTSTORE_LOCATION_CONFIG"));
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, PropertyUtil.getProperty("SSL_TRUSTSTORE_PASSWORD_CONFIG"));
        props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, PropertyUtil.getProperty("SSL_TRUSTSTORE_TYPE_CONFIG"));
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, PropertyUtil.getProperty("SSL_KEYSTORE_LOCATION_CONFIG"));
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, PropertyUtil.getProperty("SSL_KEYSTORE_PASSWORD_CONFIG"));
        props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, PropertyUtil.getProperty("SSL_KEYSTORE_TYPE_CONFIG"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "attribute-contracts-otm");

        gConsumer = new KafkaConsumer<>(props);

        gLogger.info("kafka consumer initialized...");

    }



    public void seekToBeginning(String aTopicName) {

        //int thePartitions = getPartitions(aTopicName);

        //List<TopicPartition> theTopicCollection = generateTopicPartitions(aTopicName, thePartitions);


        gConsumer.subscribe(Arrays.asList(aTopicName));
        gConsumer.poll(0);
        gConsumer.seekToBeginning(gConsumer.assignment());

        gConsumer.assignment().forEach(System.out::println);
        //gConsumer.seekToBeginning(theTopicCollection);
    }


    public void subscribe(String aTopicName) {
        gConsumer.subscribe(Arrays.asList(aTopicName));
    }

    public void getMessages(String aTopicName) {
        gConsumer.subscribe(Arrays.asList(aTopicName));
        while(true) {
            ConsumerRecords<String, String> records = gConsumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                Map<String, Object> data = new HashMap<>();
                data.put("partition", record.partition());
                data.put("offset", record.offset());
                data.put("value", record.value());
                System.out.println(data);
            }
        }
    }

    public long getPosition(String aTopicName, int aPartition) {
        return gConsumer.position(new TopicPartition(aTopicName, aPartition));
    }

    private List<TopicPartition> generateTopicPartitions(String aTopicName, int aPartitions) {
        List<TopicPartition> theTopicCollection = new ArrayList<>(aPartitions);
        IntStream.range(0, aPartitions).forEach(count -> theTopicCollection.add(new TopicPartition(aTopicName, count)));
        return theTopicCollection;
    }

    public int getPartitions(final String topic) {
        return gConsumer.partitionsFor(topic).size();
    }


    public static void closeProducer() {
        gConsumer.close();
    }

    @SuppressWarnings("unused")
    public static void main(final String[] args) throws Exception {
        final KafkaConnection kafka = KafkaConnection.getInstance();
        kafka.initialize();
        System.out.println("inside main");
        final StringBuilder builder = new StringBuilder("\"message\":\"This is sample message\"");
        final String topic = "kafka11-test1";

        kafka.closeProducer();

    }
}
