package wikiedits.io;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.fusesource.mqtt.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.TextMessage;

/**
 *
 */
public class FlinkMqttStreamSource extends RichSourceFunction<String> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(FlinkMqttStreamSource.class);

    private transient volatile boolean running;

    //    private transient MessageConsumer consumer;
    private transient BlockingConnection connection;

    private void init() throws Exception {

        MQTT client = new MQTT();
        client.setHost("tcp://mqtt.netobjex.com:1883");
        client.setClientId("fusesourcesubscriber");
        client.setCleanSession(false);
        connection = client.blockingConnection();
        connection.connect();
        Topic[] topics = {new Topic("foo", QoS.AT_LEAST_ONCE)};
        byte[] qoses = connection.subscribe(topics);
        //System.out.println(new String(qoses));

    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        running = true;
        init();
    }

    @Override
    public void run(SourceContext<String> ctx) {
        // this source never completes

        while (running) {

            try {
                // Wait for a message
                Message message = connection.receive();
                System.out.println("Message recieved on topic: " + message.getTopic());
                System.out.println("Payload: " + new String(message.getPayload()));
                String text = new String(message.getPayload());

                message.ack();
                if (message instanceof TextMessage) {
                    System.out.println("TEXT MESSAGE");
                }
                ctx.collect(text);


//                if (message instanceof TextMessage) {
//                    TextMessage textMessage = (TextMessage) message;
//                    String text = textMessage.getText();
//                    ctx.collect(text);
//                } else {
//                    LOG.error("Don't know what to do .. or no message");
//                }
            } catch (Exception e) {
                LOG.error(e.getLocalizedMessage());
                running = false;
            }
        }
        try {
            close();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

    }

    @Override
    public void cancel() {

        running = false;
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing");
        try {
            connection.disconnect();
        } catch (Exception e) {
            throw new RuntimeException("Error while closing ActiveMQ connection ", e);
        }
    }

}