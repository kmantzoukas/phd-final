package uk.ac.city.monitor.agent;

import java.io.IOException;
import java.io.StringReader;
import java.lang.instrument.Instrumentation;
import java.util.Properties;

import org.apache.log4j.Logger;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.Morph;
import uk.ac.city.monitor.enums.EmitterType;
import uk.ac.city.monitor.interceptors.HadoopRDDComputeInterceptor;
import uk.ac.city.monitor.interceptors.MapPartitionsRDDComputeInterceptor;
import uk.ac.city.monitor.interceptors.ShuffledRDDInterceptor;
import uk.ac.city.monitor.utils.Morpher;

public class DataPrivacyAgent {

	final static Logger logger = Logger.getLogger(DataPrivacyAgent.class);
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
                                .intercept(
                                        MethodDelegation
                                                .withDefaultConfiguration()
                                                .withBinders(Morph.Binder.install(Morpher.class))
                                                .to(new MapPartitionsRDDComputeInterceptor(type, properties)));
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
                                                .to(new ShuffledRDDInterceptor(type, properties)));
                    })
                    /*
                    Install the Java agent
                     */
                    .installOn(instrumentation);

            logger.info("Event captors has been successfully installed.");
    }
}