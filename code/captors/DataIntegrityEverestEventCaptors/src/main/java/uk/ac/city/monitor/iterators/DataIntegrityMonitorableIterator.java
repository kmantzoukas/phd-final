package uk.ac.city.monitor.iterators;

import org.apache.log4j.Logger;
import scala.collection.AbstractIterator;
import scala.collection.Iterator;
import uk.ac.city.monitor.emitters.Emitter;
import uk.ac.city.monitor.emitters.EventEmitterFactory;
import uk.ac.city.monitor.enums.EmitterType;
import uk.ac.city.monitor.enums.OperationType;
import uk.ac.city.monitor.utils.MonitorUtilities;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Properties;

public class DataIntegrityMonitorableIterator<A> extends AbstractIterator<A> {

    private Iterator<A> delegate;
    private MessageDigest md;
    private OperationType operation;
    private Map<String, String> parameters;
    private Properties properties;

    private Emitter emitter;

    final protected Logger logger = Logger.getLogger(DataIntegrityMonitorableIterator.class);

    public DataIntegrityMonitorableIterator(
            Iterator<A> delegate,
            EmitterType emitter,
            Properties properties,
            OperationType operation,
            Map<String, String> parameters) {

        this.delegate = delegate;
        this.operation = operation;
        this.parameters = parameters;
        this.properties = properties;

        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException nsae) {
            logger.error(nsae);
        }

        this.emitter = EventEmitterFactory.getInstance(emitter, properties);
        this.emitter.connect();
    }

    @Override
    public boolean hasNext() {
        if (!delegate.hasNext()) {
            long operationId = MonitorUtilities.generateRandomLong();
            parameters.put("checksum", DatatypeConverter.printHexBinary(md.digest()));
            String event = MonitorUtilities.createEvent(operationId, properties.getProperty("eventType"), operation, parameters);
            if (event != null)
                emitter.send(event);
            emitter.close();
            return false;
        } else {
            return true;
        }
    }

    @Override
    public A next() {
        A item = delegate.next();
        md.update(item.toString().getBytes());
        return item;
    }
}