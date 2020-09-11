package uk.ac.city.monitor.agent;

/*
Byte buddy imports
 */

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.implementation.MethodDelegation;

/*
Apache Log4j imports
 */

import net.bytebuddy.implementation.bind.annotation.Morph;
import org.apache.log4j.Logger;

/*
Standard Java SDK imports
 */

import java.io.IOException;
import java.io.StringReader;
import java.lang.instrument.Instrumentation;
import java.util.Properties;

/*
Agent's custom class imports
 */
import uk.ac.city.monitor.enums.EmitterType;
import uk.ac.city.monitor.interceptors.*;
import uk.ac.city.monitor.utils.Morpher;

import static net.bytebuddy.implementation.MethodDelegation.to;

/*
Main class that represents the java agent
 */
public class DataIntegrityEverestEventCaptor {

    final static Logger logger = Logger.getLogger(DataIntegrityEverestEventCaptor.class);
    private static EmitterType type;
    private static Properties properties = new Properties();

    public static void premain(String configuration, Instrumentation instrumentation) throws IOException {

        properties.load(new StringReader(configuration.replaceAll(",", "\n")));
        EmitterType emitterType = EmitterType.valueOf(properties.getProperty("emitter").toUpperCase());

        switch (emitterType){
            case RABBITMQ:
                type = EmitterType.RABBITMQ;
                break;
            case SOCKET:
                type = EmitterType.SOCKET;
                break;
        }

        new AgentBuilder.Default()
            /*
            Element matcher for class org.apache.spark.rdd.HadoopRDD
            */
            .type(type -> type.getName().equals("org.apache.spark.rdd.HadoopRDD"))
            /*
            Intercept all calls on HadoopRDD.compute() method
            */
            .transform((builder, typeDescription, classLoader, module) -> {
                return builder
                        .serialVersionUid(1L)
                        .method(method -> method.getName().equals("compute"))
                        .intercept(
                                MethodDelegation
                                        .withDefaultConfiguration()
                                        .withBinders(Morph.Binder.install(Morpher.class))
                                        .to(new HadoopRDDComputeInterceptor(type, properties)));
            })
            /*
            Element matcher for class org.apache.spark.rdd.MapPartitionsRDD
            */
            .type(type -> type.getName().equals("org.apache.spark.rdd.MapPartitionsRDD"))
            /*
            Intercept all calls on MapPartitionsRDD.compute() method
            */
            .transform((builder, typeDescription, classLoader, module) -> {
                return builder
                    .serialVersionUid(1L)
                    .method(method -> method.getName().equals("compute"))
                    .intercept(to(new MapPartitionsRDDComputeInterceptor(type, properties)));
            })
            /*
            Element matcher for class org.apache.spark.rdd.MapPartitionsRDD
            */
            .type(type -> type.getName().equals("org.apache.spark.api.python.PythonRDD"))
                /*
                Intercept all calls on MapPartitionsRDD.compute() method
                */
                .transform((builder, typeDescription, classLoader, module) -> {
                    return builder
                            .serialVersionUid(1L)
                            .method(method -> method.getName().equals("compute"))
                            .intercept(MethodDelegation
                                    .withDefaultConfiguration()
                                    .withBinders(Morph.Binder.install(Morpher.class))
                                    .to(new PythonRDDComputeInterceptor(type, properties)));
                })
            /*
            Element matcher for class org.apache.spark.rdd.ShuffledRDD
            */
            .type(type -> type.getName().equals("org.apache.spark.rdd.ShuffledRDD"))
            /*
            Intercept all calls on ShuffledRDD.compute() method
            */
            .transform((builder, typeDescription, classLoader, module) -> {
                return builder
                    .serialVersionUid(1L)
                    .method(method -> method.getName().equals("compute"))
                    .intercept(
                        MethodDelegation
                            .withDefaultConfiguration()
                            .withBinders(Morph.Binder.install(Morpher.class))
                            .to(new ShuffledRDDComputeInterceptor(type, properties)));
            })
            /*
            Element matcher for class org.apache.spark.util.collection.ExternalSorter
            */
            .type(type -> type.getName().equals("org.apache.spark.util.collection.ExternalSorter"))
            /*
            Intercept all calls on ExternalSorter.writePartitionedFile() method
            */
            .transform((builder, typeDescription, classLoader, module) -> {
                return builder
                    .serialVersionUid(1L)
                    .method(method -> method.getName().equals("writePartitionedFile"))
                    .intercept(MethodDelegation.to(new ExternalSorterWritePartitionedFileInterceptor(type, properties)));
            })
            .type(type -> type.getName().equals("org.apache.spark.shuffle.sort.BypassMergeSortShuffleWriter"))
            /*
            Intercept all calls on ExternalSorter.writePartitionedFile() method
            */
            .transform((builder, typeDescription, classLoader, module) -> {
                return builder
                    .serialVersionUid(1L)
                    .method(method -> method.getName().equals("writePartitionedFile"))
                    .intercept(MethodDelegation.to(new BypassMergeSortShuffleWriterWritePartitionedFileInterceptor(type, properties)));
            })
            /*
            Element matcher for class org.apache.spark.storage.ShuffleBlockFetcherIterator
            */
            .type(type -> type.getName().equals("org.apache.spark.storage.ShuffleBlockFetcherIterator"))
            /*
            Intercept all calls on ShuffleBlockFetcherIterator.flatMap() method
            */
            .transform((builder, typeDescription, classLoader, module) -> {
                return builder
                    .serialVersionUid(1L)
                    .method(method -> method.getName().equals("flatMap"))
                    .intercept(MethodDelegation
                        .withDefaultConfiguration()
                        .withBinders(Morph.Binder.install(Morpher.class))
                        .to(new ShuffleBlockFetcherIteratorFlatMapInterceptor(type, properties)));
            })
            /*
            Element matcher for class org.apache.spark.SparkContext
            */
            .type(type -> type.getName().equals("org.apache.spark.SparkContext"))
            /*
            Intercept all calls on SparkContext.runJob() method
            */
            .transform((builder, typeDescription, classLoader, module) -> {
                return builder
                    .serialVersionUid(1L)
                    .method(method -> (method.getName().equals("runJob") && method.getParameters().size() == 3))
                    .intercept(MethodDelegation
                        .withDefaultConfiguration()
                        .withBinders(Morph.Binder.install(Morpher.class))
                        .to(new SparkContextRunJobInterceptor(type, properties)));
             })
            /*
            Install the Java agent
             */
            .installOn(instrumentation);

            logger.info("Event captors has been successfully installed.");
    }
}