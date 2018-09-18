package it.cits.ebfoundation.pojo;

import lombok.Data;

@Data
public class Arguments {

    //private String kafkaCluster;
    private String topicName;
    private String time;


    public Arguments(final String topicName, final String time) {
        this.topicName = topicName;
        this.time = time;
    }
}
