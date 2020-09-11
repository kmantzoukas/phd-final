package uk.ac.city.monitor.emitters;


import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.Properties;

public class SocketEmitter extends Emitter {

    final static Logger logger = Logger.getLogger(SocketEmitter.class);

    private String host;
    private int port;
    private Socket socket;
    private BufferedWriter writer;

    public SocketEmitter(Properties props){
        this.properties = props;
        this.host = properties.getProperty("host");
        this.port = Integer.valueOf(properties.getProperty("port"));
    }

    @Override
    public void connect() {
        try {
            this.socket = new Socket(host,port);
            this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        } catch (IOException ioe) {
            logger.info(ioe);
        }

    }

    @Override
    public void close() {
        try{
            if (socket != null)
                socket.close();
            if (writer != null){
                writer.flush();
                writer.close();
            }
        }catch (IOException ioe){
            logger.error(ioe);
        }

    }

    @Override
    public void send(String event) {
        try {
            writer.write(event);
            writer.newLine();
            writer.flush();
        } catch (IOException ioe) {
            logger.error(ioe);
        }
    }
}
