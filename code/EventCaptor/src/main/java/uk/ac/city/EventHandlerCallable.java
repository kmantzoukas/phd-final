package uk.ac.city;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.Callable;

public class EventHandlerCallable implements Callable<Metric> {
    private Socket client;
    private Metric metric;
    private PrintWriter writer;

    public EventHandlerCallable(Socket socket, PrintWriter writer){
        this.client = socket;
        this.writer = writer;
        metric = new Metric();
    }

    public Metric call(){

        String line;
        Long counter = 0L;

        try{
            BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
            while ((line = in.readLine()) != null) {
                writer.println(line);
                writer.flush();
            }
            client.close();
            in.close();
        }catch (IOException ioe){
            System.out.println(ioe.getMessage());
        }
        return metric;
    }
}
