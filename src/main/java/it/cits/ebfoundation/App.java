package it.cits.ebfoundation;

import it.cits.ebfoundation.kafka.KafkaConnection;
import it.cits.ebfoundation.kafka.KafkaPublisher;
import it.cits.ebfoundation.pojo.Arguments;

public class App {

    public static void main(String... args) {
        Arguments theArguments = new Arguments(args[0], args[1]);

        KafkaPublisher thePub = KafkaPublisher.getInstance();
       // thePub.publishSamples(theArguments.getTopicName());

        KafkaPublisher.closeProducer();

        KafkaConnection theSub = KafkaConnection.getInstance();

        //theSub.subscribe(theArguments.getTopicName());

          theSub.seekToBeginning(theArguments.getTopicName());

        System.out.println(theSub.getPosition(theArguments.getTopicName(), 0));
        System.out.println(theSub.getPosition(theArguments.getTopicName(), 1));
        System.out.println(theSub.getPosition(theArguments.getTopicName(), 2));
        System.out.println(theSub.getPosition(theArguments.getTopicName(), 3));
        System.out.println(theSub.getPosition(theArguments.getTopicName(), 4));

       //   theSub.getMessages(theArguments.getTopicName());

//        System.out.println(theSub.getPosition(theArguments.getTopicName(), 0));
//        System.out.println(theSub.getPosition(theArguments.getTopicName(), 1));
//        System.out.println(theSub.getPosition(theArguments.getTopicName(), 2));
//        System.out.println(theSub.getPosition(theArguments.getTopicName(), 3));
//        System.out.println(theSub.getPosition(theArguments.getTopicName(), 4));

    }
}
