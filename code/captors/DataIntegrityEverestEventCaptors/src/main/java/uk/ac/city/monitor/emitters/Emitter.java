package uk.ac.city.monitor.emitters;

import java.io.InputStream;
import java.util.Properties;

public abstract class Emitter {

    public Properties properties = new Properties();

    public abstract void connect();
    public abstract void close();
    public abstract void send(String event);

}
