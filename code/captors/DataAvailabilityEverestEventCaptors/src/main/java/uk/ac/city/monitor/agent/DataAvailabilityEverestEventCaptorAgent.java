package uk.ac.city.monitor.agent;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.*;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.log4j.Logger;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.Partition;
import org.apache.spark.SparkEnv$;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.slaatsoi.eventschema.*;
import scala.Function3;
import scala.Tuple2;
import scala.collection.Iterator;
import uk.ac.city.monitor.emitters.Emitter;
import uk.ac.city.monitor.emitters.EventEmitterFactory;
import uk.ac.city.monitor.enums.DirectionType;
import uk.ac.city.monitor.enums.EmitterType;
import uk.ac.city.monitor.enums.OperationType;
import uk.ac.city.monitor.utils.Morpher;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import java.io.*;
import java.lang.instrument.Instrumentation;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DataAvailabilityEverestEventCaptorAgent implements Serializable {

    final static Logger logger = Logger.getLogger(DataAvailabilityEverestEventCaptorAgent.class);
    private static EmitterType type;
    private static Properties properties = new Properties();

    public static void premain(String configuration, Instrumentation instrumentation) throws IOException {

        long start = new Date().getTime();
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
                                            .to(HadoopRDDComputeInterceptor.class));
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
                                            .to(MapPartitionsRDDComputeInterceptor.class));
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
                                            .to(ShuffledRDDInterceptor.class));
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
                            .intercept(
                                    MethodDelegation
                                            .withDefaultConfiguration()
                                            .withBinders(Morph.Binder.install(Morpher.class))
                                            .to(SparkContextRunJobInterceptor.class));
                })
                /*
                Install the Java agent
                 */
                .installOn(instrumentation);

        Emitter emitter = EventEmitterFactory.getInstance(emitterType, properties);
        emitter.connect();
        long end = new Date().getTime();
        String ip = InetAddress.getLocalHost().getHostAddress();
        String host = InetAddress.getLocalHost().getCanonicalHostName();
        emitter.send(String.format("%s, %s -> %s", host, ip, end - start));
        logger.info("Event captors has been successfully installed.");
    }

    /*
    Interceptor for method compute() for class HadoopRDD
     */
    public static class HadoopRDDComputeInterceptor {

        @RuntimeType
        public static <K, V> Iterator<Tuple2<K, V>> compute(
                @Argument(0) Partition theSplit,
                @Argument(1) TaskContext context,
                @This RDD<Tuple2<K, V>> rdd,
                @Morph Morpher<InterruptibleIterator<Tuple2<K, V>>> morpher) throws JAXBException, UnknownHostException, DatatypeConfigurationException {

            String applicationId = SparkEnv$.MODULE$.get().conf().get("spark.app.id");
            String applicationName = SparkEnv$.MODULE$.get().conf().get("spark.app.name");
            /*
            Operation ids for the start and end events should be same for EVEREST to be able to perform the unification of the rules
             */
            long operationId = generateRandomLong();

            Emitter emitter = EventEmitterFactory.getInstance(type, properties);
            emitter.connect();

            // emitter.send(createEventXML(operationId, OperationType.TRANSFORMATION, "start", applicationId, applicationName, rdd, theSplit));
            InterruptibleIterator<Tuple2<K, V>> output = morpher.invoke(theSplit, context);
            // emitter.send(createEventXML(operationId, OperationType.TRANSFORMATION,"end", applicationId, applicationName, rdd, theSplit));

            return output;
        }
    }

    /*
    Interceptor for method compute() for class MapPartitionsRDD
     */
    public static class MapPartitionsRDDComputeInterceptor {

        @RuntimeType
        public static <T, U> Iterator<U> compute(
                @Argument(0) Partition split,
                @Argument(1) TaskContext context,
                @This RDD<T> rdd, @FieldValue("f") Function3<TaskContext, Integer, Iterator<T>, Iterator<U>> f,
                @Morph Morpher<Iterator<U>> morpher) throws JAXBException, UnknownHostException, DatatypeConfigurationException {

            String applicationId = SparkEnv$.MODULE$.get().conf().get("spark.app.id");
            String applicationName = SparkEnv$.MODULE$.get().conf().get("spark.app.name");
             /*
            Operation ids for the start and end events should be same for EVEREST to be able to perform the unification of the rules
             */
            long operationId = generateRandomLong();

            Emitter emitter = EventEmitterFactory.getInstance(type, properties);
            emitter.connect();

            // emitter.send(createEventXML(operationId, OperationType.TRANSFORMATION,"start", applicationId, applicationName, rdd, split));
            Iterator<U> output = morpher.invoke(split, context);
            // emitter.send(createEventXML(operationId, OperationType.TRANSFORMATION, "end", applicationId, applicationName, rdd, split));

            return output;
        }
    }

    /*
    Interceptor for method compute() for class ShuffledRDD
     */
    public static class ShuffledRDDInterceptor {

        @RuntimeType
        public static <K, V, C> Iterator<Tuple2<K, C>> compute(
                @Argument(0) Partition partition,
                @Argument(1) TaskContext taskContext,
                @This RDD rdd,
                @Morph Morpher<Iterator<Tuple2<K, C>>> morpher) throws JAXBException, UnknownHostException, DatatypeConfigurationException {

            String applicationId = SparkEnv$.MODULE$.get().conf().get("spark.app.id");
            String applicationName = SparkEnv$.MODULE$.get().conf().get("spark.app.name");
             /*
            Operation ids for the start and end events should be same for EVEREST to be able to perform the unification of the rules
             */
            long operationId = generateRandomLong();

            Emitter emitter = EventEmitterFactory.getInstance(type, properties);
            emitter.connect();

            // emitter.send(createEventXML(operationId, OperationType.TRANSFORMATION,"start", applicationId, applicationName, rdd, partition));
            Iterator<Tuple2<K, C>> output = morpher.invoke(partition, taskContext);
            // emitter.send(createEventXML(operationId, OperationType.TRANSFORMATION,"end", applicationId, applicationName, rdd, partition));

            return output;
        }
    }

    /*
    Interceptor for method compute() for class SparkContext
     */
    public static class SparkContextRunJobInterceptor<T, U> {

        @RuntimeType
        public static Object[] runJob(
                @Argument(0) RDD rdd,
                @Argument(1) Object f,
                @Argument(2) Object classTag,
                @Morph Morpher<Object[]> m)
                throws JAXBException,
                UnknownHostException,
                DatatypeConfigurationException {

            String applicationId = SparkEnv$.MODULE$.get().conf().get("spark.app.id");
            String applicationName = SparkEnv$.MODULE$.get().conf().get("spark.app.name");
             /*
            Operation ids for the start and end events should be same for EVEREST to be able to perform the unification of the rules
             */
            long operationId = generateRandomLong();

            Emitter emitter = EventEmitterFactory.getInstance(type, properties);
            emitter.connect();
            Object[] result = m.invoke(new Object[]{rdd, f, classTag});
            if(!"NONE".equals(properties.getProperty("eventType"))) {
                emitter.send(createEventXML(operationId, OperationType.ACTION,"start", applicationId, applicationName, rdd, null));

                emitter.send(createEventXML(operationId, OperationType.ACTION,"end", applicationId, applicationName, rdd, null));
                emitter.close();
            }

            return result;
        }
    }

    /*
    Create an XML representation of the event to be emitted. A series of parameters are passed as arguments based in the security property
    that the agent collects events for. Every security property mandates the agent collects different types of meta-information
     */
    private static String createEventXML(long operationId,
                                         OperationType type,
                                         String operationName,
                                         String applicationId,
                                         String applicationName,
                                         RDD rdd,
                                         Partition split)
            throws JAXBException, UnknownHostException, DatatypeConfigurationException {

        EventInstance event = new EventInstance();

        EventIdType eventID = new EventIdType();
        eventID.setID(generateRandomLong());
        eventID.setEventTypeID("ServiceOperationCallEndEvent");

        EventContextType eventContext = new EventContextType();

        EventTime time = new EventTime();
        time.setTimestamp(System.currentTimeMillis());
        time.setCollectionTime(DatatypeFactory
                .newInstance()
                .newXMLGregorianCalendar(
                        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
                                .format(new Date())));
        time.setReportTime(DatatypeFactory
                .newInstance()
                .newXMLGregorianCalendar(
                        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
                                .format(new Date())));

        eventContext.setTime(time);

        EventNotifier notifier = new EventNotifier();
        InetAddress inetAddress = InetAddress.getLocalHost();
        notifier.setIP(inetAddress.getHostAddress());
        notifier.setName(inetAddress.getHostName());
        notifier.setPort(20302);

        eventContext.setNotifier(notifier);

        EventSource source = new EventSource();
        ServiceSourceType serviceSourceType = new ServiceSourceType();

        Entity sender = new Entity();
        sender.setName("toreadorSLA");
        sender.setIp("0.0.0.0");
        sender.setPort(10022);
        serviceSourceType.setSender(sender);

        Entity receiver = new Entity();
        receiver.setName("toreadorSLA");
        receiver.setIp("192.168.43.26");
        receiver.setPort(10022);
        serviceSourceType.setReceiver(receiver);

        source.setSwServiceLayerSource(serviceSourceType);

        eventContext.setSource(source);

        EventPayloadType eventPayload = new EventPayloadType();
        ArgumentList args = new ArgumentList();

        InteractionEventType interactionEvent = new InteractionEventType();
        interactionEvent.setOperationName(operationName);
        interactionEvent.setOperationID(operationId);

        /*
        If the operation name is "start" then the interaction event status should be a REQ-A whereas if the operation name is "end" the
        interaction event status should be RES-B. This is critical for the unification of rules from EVEREST
         */
        if("start".equals(operationName)){
            interactionEvent.setStatus("REQ-A");
        }else if("end".equals(operationName)){
            interactionEvent.setStatus("RES-B");
        }

        ArgumentType arg1 = new ArgumentType();
        SimpleArgument appIdArg = new SimpleArgument();
        appIdArg.setArgName("appId");
        appIdArg.setArgType("string");

        /*
        If the operation name is "start" then the direction of the parameters should be in (input) whereas if the operation name is "end" the
        direction of the parameters should be out (output). This is critical for the unification of rules from EVEREST
         */
        if("start".equals(operationName)){
            appIdArg.setDirection(DirectionType.IN.name());
        }else if("end".equals(operationName)){
            appIdArg.setDirection(DirectionType.OUT.name());
        }

        appIdArg.setValue(applicationId);
        arg1.setSimple(appIdArg);
        args.getArgument().add(arg1);

        ArgumentType arg2 = new ArgumentType();
        SimpleArgument appNameArg = new SimpleArgument();
        appNameArg.setArgName("appName");
        appNameArg.setArgType("string");

         /*
        If the operation name is "start" then the direction of the parameters should be in (input) whereas if the operation name is "end" the
        direction of the parameters should be out (output). This is critical for the unification of rules from EVEREST
         */
        if("start".equals(operationName)){
            appNameArg.setDirection(DirectionType.IN.name());
        }else if("end".equals(operationName)){
            appNameArg.setDirection(DirectionType.OUT.name());
        }
        appNameArg.setValue(applicationName);
        arg2.setSimple(appNameArg);
        args.getArgument().add(arg2);

        ArgumentType arg3 = new ArgumentType();
        SimpleArgument rddIdArg = new SimpleArgument();
        rddIdArg.setArgName("rddId");
        rddIdArg.setArgType("string");

        /*
        If the operation name is "start" then this in EC-Assertion terms refers to a REQ-A whereas if the operation name is "end" this refers to
        a RES-B. This is critical for the unification of rules from EVEREST
         */
        if("start".equals(operationName)){
            rddIdArg.setDirection(DirectionType.IN.name());
        }else if("end".equals(operationName)){
            rddIdArg.setDirection(DirectionType.OUT.name());
        }
        rddIdArg.setValue(String.valueOf(rdd.id()));
        arg3.setSimple(rddIdArg);
        args.getArgument().add(arg3);

        ArgumentType arg4 = new ArgumentType();
        SimpleArgument partitionIdArg = new SimpleArgument();
        partitionIdArg.setArgName("partId");
        partitionIdArg.setArgType("string");

        if("start".equals(operationName)){
            partitionIdArg.setDirection(DirectionType.IN.name());
        }else if("end".equals(operationName)){
                partitionIdArg.setDirection(DirectionType.OUT.name());
        }

        if(split != null){
            partitionIdArg.setValue(String.valueOf(split.index()));
        }else {
            partitionIdArg.setValue("NaN");
        }

        arg4.setSimple(partitionIdArg);
        args.getArgument().add(arg4);

        ArgumentType arg5 = new ArgumentType();
        SimpleArgument typeArg = new SimpleArgument();
        typeArg.setArgName("operationType");
        typeArg.setArgType("string");

         /*
        If the operation name is "start" then the direction of the parameters should be in (input) whereas if the operation name is "end" the
        direction of the parameters should be out (output). This is critical for the unification of rules from EVEREST
         */
        if("start".equals(operationName)){
            typeArg.setDirection(DirectionType.IN.name());
        }else if("end".equals(operationName)){
            typeArg.setDirection(DirectionType.OUT.name());
        }
        typeArg.setValue(type.name());
        arg5.setSimple(typeArg);
        args.getArgument().add(arg5);

        interactionEvent.setParameters(args);
        eventPayload.setInteractionEvent(interactionEvent);

        event.setEventID(eventID);
        event.setEventContext(eventContext);
        event.setEventPayload(eventPayload);

        JAXBContext context = JAXBContext.newInstance(EventInstance.class);

        Marshaller m = context.createMarshaller();
        m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        StringWriter sw = new StringWriter();
        m.marshal(event, sw);

        return sw.toString();

    }

    /*
    Generate random long ids for the eventId and operationId respectively
     */
    public static long generateRandomLong() {
        long leftLimit = 1L;
        long rightLimit = 100000L;

        return new RandomDataGenerator().nextLong(leftLimit, rightLimit);
    }
}