package wikiedits.io;


import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

import javax.jms.*;
import java.io.StringWriter;

/**
 */
public class FlinkMqttStreamSink extends RichSinkFunction<Tuple2<String, Integer>> {

    private static final long serialVersionUID = 1L;

//    private transient MessageProducer producer;
    private transient BlockingConnection connection;
//    private transient Queue destination;
//    private transient TextMessage textMessage;

    private final String outputQueue;
    private final ObjectMapper mapper = new ObjectMapper();

    public FlinkMqttStreamSink(String outputQueue) {
        this.outputQueue = outputQueue;
    }

    private void init() throws Exception {
        MQTT client = new MQTT();
        client.setHost("tcp://mqtt.netobjex.com:1883");
        client.setClientId("fusesourcesubscriber");
        client.setCleanSession(false);
        connection = client.blockingConnection();
        connection.connect();
//
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        init();
    }

    @Override
    public void close() throws Exception {
        connection.disconnect();
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {

        SimpleMessage simpleMessage = new SimpleMessage(value.f0, value.f1);
        StringWriter stringWriter = new StringWriter();
        mapper.writeValue(stringWriter, simpleMessage);
//        textMessage.setText(stringWriter.toString());

//        producer.send(destination, textMessage);
        connection.publish(outputQueue, stringWriter.toString().getBytes(), QoS.AT_LEAST_ONCE, false);



    }
}