package uk.ac.city;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class EventCaptor {

    private static int port;
    private static String resultPath;
    private final static int NUM_OF_THREADS = 25;

    private static PrintWriter writer;

    public static void main(String[] args) throws Exception {

        ExecutorService pool = Executors.newFixedThreadPool(NUM_OF_THREADS);

        port = args.length != 0 ? Integer.valueOf(args[0]) : 9090;
        resultPath = args.length != 0 ? args[1] : null;

        ServerSocket listener = new ServerSocket(port);
        System.out.println("Event captor started listening on port " + port);
        List<Future<Metric>> futures = new ArrayList<Future<Metric>>();

        if(resultPath == null){
            writer = new PrintWriter(System.out);
        }else {
            File file = new File(args[1]);

            if(!file.createNewFile()){
                writer = new PrintWriter(file, "UTF-8");
                writer.print("");
                writer.flush();
            }
        }


        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                Long totalEvents = 0L;
                Long totalSizeOfEvents = 0L;
                for(Future<Metric> future : futures){
                    try {
                        totalEvents += future.get().getTotal();
                        totalSizeOfEvents += future.get().getSizeInBytes();
                    } catch (InterruptedException e) {
                        System.out.println(e.getMessage());
                    } catch (ExecutionException e) {
                        System.out.println(e.getMessage());
                    }
                }
                System.out.println();
                System.out.println("The total number of events captured is " + totalEvents);
                System.out.println("The total size of events captured is " + printTotalSize(totalSizeOfEvents));
                writer.close();
            }
        });

        try {
            while (true) {
                Socket client = listener.accept();
                futures.add(pool.submit(new EventHandlerCallable(client,writer)));
            }
        }catch(IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public static String printTotalSize(long bytes) {
        if (bytes < 1000) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(1000));
        return String.format("%.1f %sB", bytes / Math.pow(1000, exp), String .valueOf("KMGTPE".charAt(exp-1)));
    }
}
