package it.cits.ebfoundation.kafka;

import it.cits.ebfoundation.util.PropertyUtil;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class KafkaPublisher {
    private static Logger gLogger = Logger.getLogger(KafkaPublisher.class);
    private static KafkaProducer<String, String> gProducer;
    private static KafkaPublisher gKafkaProducer;
    private int total = 0;
    static {
        gKafkaProducer = new KafkaPublisher();
    }

    private KafkaPublisher() {
        initialize();
    }

    public static KafkaPublisher getInstance() throws NullPointerException {
        if (gKafkaProducer == null) {
            throw new NullPointerException("Kafka Producer not initialized");
        }
        return gKafkaProducer;
    }

    private static void initialize() {
        final Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertyUtil.getProperty("BOOTSTRAP_SERVERS_CONFIG"));
        prop.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, PropertyUtil.getProperty("SECURITY_PROTOCOL_CONFIG"));
        prop.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, PropertyUtil.getProperty("SSL_TRUSTSTORE_LOCATION_CONFIG"));
        prop.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, PropertyUtil.getProperty("SSL_TRUSTSTORE_PASSWORD_CONFIG"));
        prop.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, PropertyUtil.getProperty("SSL_TRUSTSTORE_TYPE_CONFIG"));
        prop.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, PropertyUtil.getProperty("SSL_KEYSTORE_LOCATION_CONFIG"));
        prop.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, PropertyUtil.getProperty("SSL_KEYSTORE_PASSWORD_CONFIG"));
        prop.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, PropertyUtil.getProperty("SSL_KEYSTORE_TYPE_CONFIG"));
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        prop.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        gProducer = new KafkaProducer<>(prop);
    }

    public void publishMessage(final List<ProducerRecord<String, String>> aKeyedMsg) {
        this.total = this.total + aKeyedMsg.size();
        gLogger.info("PUBLISHED :" + this.total);
        Iterator iter = aKeyedMsg.iterator();
        while (iter.hasNext()){
            ProducerRecord<String, String> record =(ProducerRecord<String, String>) iter.next();
            gLogger.info("PUBLISHED MESSAGE"+record.toString());
            gLogger.info("PUBLISHED MESSAGE"+record.key()+record.value());
        }
        gLogger.info("PUBLISHED :" + total);
        System.out.println("PUBLISHED :" + total);
        //aKeyedMsg.forEach(System.out::println);
        aKeyedMsg.forEach(gProducer::send);

    }



    public int getPartitions(final String topic) {
        return gProducer.partitionsFor(topic).size();
    }

    public void publishMessage(final ProducerRecord<String, String> aKeyedMsg) {
        gLogger.info("DELETE PUBLISHED :" + aKeyedMsg);
        System.out.println("DELETE : "+aKeyedMsg);
        gProducer.send(aKeyedMsg);
    }


    public static void closeProducer() {
        gProducer.flush();
        gProducer.close();
    }

    public void publishSamples(String aTopicName) {

        System.out.println("inside main");
        final StringBuilder builder = new StringBuilder("{ \"message\":\"This is sample message\" }");


        //final ProducerRecord<String, String> keyedMsg = new ProducerRecord<>(aTopicName, builder.toString());
        //gKafkaProducer.publishMessage(keyedMsg);

          final List<ProducerRecord<String, String>> theList = new ArrayList<>();
          for (int i = 0; i < 30; i++) {
              System.out.println(builder.toString());
              final ProducerRecord<String, String> keymsg1 = new ProducerRecord<>(aTopicName, String.valueOf(i), builder.toString());
              theList.add(keymsg1);
          }
        gKafkaProducer.publishMessage(theList);

    }
}
