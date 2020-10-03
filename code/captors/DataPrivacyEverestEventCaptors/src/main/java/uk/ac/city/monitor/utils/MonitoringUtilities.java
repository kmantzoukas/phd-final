package uk.ac.city.monitor.utils;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.SparkEnv$;
import org.apache.spark.rdd.RDD;
import org.slaatsoi.eventschema.*;
import uk.ac.city.monitor.emitters.Emitter;
import uk.ac.city.monitor.emitters.EventEmitterFactory;
import uk.ac.city.monitor.enums.DirectionType;
import uk.ac.city.monitor.enums.EmitterType;
import uk.ac.city.monitor.enums.OperationType;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class MonitoringUtilities {

    final static protected Logger logger = Logger.getLogger(MonitoringUtilities.class.getName());

    public static String createEventXML(long operationId, OperationType type, Map<String, String> arguments)
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
        sender.setName("10.207.1.102");
        sender.setIp("10.207.1.102");
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
        interactionEvent.setOperationName(type.name().toLowerCase());
        interactionEvent.setOperationID(operationId);
        interactionEvent.setStatus("REQ-A");


        for(Map.Entry<String, String> entry : arguments.entrySet()){

            ArgumentType arg = new ArgumentType();
            SimpleArgument sarg = new SimpleArgument();
            sarg.setArgName(entry.getKey());
            sarg.setArgType("string");

            sarg.setDirection(DirectionType.IN.name());

            sarg.setValue(entry.getValue());

            arg.setSimple(sarg);
            args.getArgument().add(arg);
        }

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
    Generate random long numbers
    */
    public static long generateRandomLong(long... limits) {
        long lower;
        long upper;

        if (limits.length == 0) {
            lower = 1L;
            upper = 100000L;
        } else{
            lower = limits[0];
            upper = limits[1];
        }

        return new RandomDataGenerator().nextLong(lower, upper);
    }

    public static void emitIp(RDD rdd, Partition partition, EmitterType type, Properties properties) throws UnknownHostException, JAXBException, DatatypeConfigurationException {

        String applicationId = SparkEnv$.MODULE$.get().conf().get("spark.app.id");
        String applicationName = SparkEnv$.MODULE$.get().conf().get("spark.app.name");

        long operationId = generateRandomLong();

        Emitter emitter = EventEmitterFactory.getInstance(type, properties);
        emitter.connect();

        Map<String, String > parameters = new LinkedHashMap<>();
        parameters.put("appId", applicationId);
        parameters.put("appName", applicationName);
        parameters.put("rddId", String.valueOf(rdd.id()));
        parameters.put("partId",  String.valueOf(partition.index()));
        parameters.put("ip",  String.valueOf(InetAddress.getLocalHost().getHostAddress()));

        String event = MonitoringUtilities.createEvent(operationId, properties.getProperty("eventType"), OperationType.COMPUTE, parameters);

        if(event != null)
            emitter.send(event);

        //emitter.send(createEventXML(operationId, OperationType.COMPUTE, parameters));

    }


    static public String createEvent(long operationId, String style, OperationType type, Map<String, String> arguments) throws JAXBException, UnknownHostException, DatatypeConfigurationException {

        StringBuffer buffer = new StringBuffer();
        for (Map.Entry entry : arguments.entrySet()){
            buffer.append(entry.getValue()).append(',');
        }

        String parameters = buffer.toString().substring(0, buffer.length() - 1);

        String event = null;

        if("TEXT".equalsIgnoreCase(style)){
            event =  String.format("%s(%s)", type.name().toLowerCase(), parameters);
        } else if("XML".equalsIgnoreCase(style)){
            event = MonitoringUtilities.createEventXML(operationId, type, arguments);
        }

        return event;
    }

}