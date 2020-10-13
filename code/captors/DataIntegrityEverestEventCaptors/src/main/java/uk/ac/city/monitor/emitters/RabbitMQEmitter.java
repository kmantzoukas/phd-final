package uk.ac.city.monitor.emitters;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

public class RabbitMQEmitter extends Emitter {

    private Connection connection;
    private Channel channel;
    private String channelName;
    private String topic;

    final static Logger logger = Logger.getLogger(RabbitMQEmitter.class);

    RabbitMQEmitter(Properties props) {
        this.properties = props;
    }

    @Override
    public void connect() {

        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(properties.getProperty("host"));
            factory.setPort(Integer.valueOf(properties.getProperty("port")));
            factory.setUsername(properties.getProperty("username"));
            factory.setPassword(properties.getProperty("password"));
            this.connection = factory.newConnection();
            channel = this.connection.createChannel();
            channel.exchangeDeclare(properties.getProperty("channel"), "direct");
            channelName = properties.getProperty("channel");
            topic = properties.getProperty("topic");
        } catch (IOException ioe) {
            logger.error(ioe);
        } catch (TimeoutException te) {
            logger.error(te);
        }
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (IOException ioe) {
            logger.error(ioe);
        }
    }

    @Override
    public void send(String event) {
        try {
            if (!connection.isOpen())
                this.connect();
            channel.basicPublish(channelName, topic, null, event.getBytes());
        } catch (IOException ioe) {
            logger.error(ioe);
        }
    }
}
