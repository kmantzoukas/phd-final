package uk.ac.city.monitor.utils;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.log4j.Logger;
import org.slaatsoi.eventschema.*;
import uk.ac.city.monitor.enums.DirectionType;
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
import java.util.Map;

public class MonitorUtilities {

    final static protected Logger logger = org.apache.log4j.Logger.getLogger(MonitorUtilities.class.getName());

    static public String createEventXML(long operationId,
                          OperationType type,
                          Map<String, String> arguments) {

        StringWriter sw = new StringWriter();

        try{

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
            interactionEvent.setOperationName(type.name().toLowerCase());
            interactionEvent.setOperationID(operationId);

            /*
            If it's a read operation - either READRDD or READSHUFFLE - the status needs to be REQ-A. Alternatively, if
            it's a write operation - either WRITERDD or WRITESHUFFLE - the status needs to be RES-B. This is done to
            satisfy EVEREST's idiosyncrasy with respect to event statuses
             */
            if(type == OperationType.READRDD || type == OperationType.READSHUFFLE){
                interactionEvent.setStatus("REQ-A");
            }else if(type == OperationType.WRITERDD || type == OperationType.WRITESHUFFLE){
                interactionEvent.setStatus("RES-B");
            }

            DirectionType direction = null;

            if(type == OperationType.READRDD || type == OperationType.READSHUFFLE){
                direction = DirectionType.IN;
            }else if(type == OperationType.WRITERDD || type == OperationType.WRITESHUFFLE){
                direction = DirectionType.OUT;
            }

            for(Map.Entry<String, String> entry : arguments.entrySet()){

                ArgumentType arg = new ArgumentType();
                SimpleArgument sarg = new SimpleArgument();
                sarg.setArgName(entry.getKey());
                sarg.setArgType("string");

                sarg.setDirection(direction.name());

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
            m.marshal(event, sw);

        }catch (JAXBException jxbe){
            logger.info(String.format("Unable to create the XML representation of an event"));
        }catch (UnknownHostException uhe){
            logger.info(String.format("Unable to find host"));
        }catch (DatatypeConfigurationException dce){
            logger.info(String.format("A configuration error has occured with message %s", dce.getMessage()));
        }

        return sw.toString();

    }

    /*
    Generate random long ids for the eventId and operationId respectively
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


    static public String createEvent(long operationId, String style, OperationType type, Map<String, String> arguments){

        StringBuffer buffer = new StringBuffer();
        for (Map.Entry entry : arguments.entrySet()){
            buffer.append(entry.getValue()).append(',');
        }

        String parameters = buffer.toString().substring(0, buffer.length() - 1);

        String event = null;

        if("TEXT".equalsIgnoreCase(style)){
            event =  String.format("%s(%s)", type.name().toLowerCase(), parameters);
        }else if("XML".equalsIgnoreCase(style)){
            event = createEventXML(operationId, type, arguments);
        }

        return event;
    }

}
