package uk.ac.city.monitor.interceptors;

import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.Morph;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.This;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.collection.Iterator;
import uk.ac.city.monitor.enums.EmitterType;
import uk.ac.city.monitor.utils.MonitoringUtilities;
import uk.ac.city.monitor.utils.Morpher;

import javax.xml.bind.JAXBException;
import javax.xml.datatype.DatatypeConfigurationException;
import java.net.UnknownHostException;
import java.util.Properties;


public class ShuffledRDDInterceptor {

    private final EmitterType type;
    private final Properties properties;

    public ShuffledRDDInterceptor(EmitterType type, Properties properties){
        this.type = type;
        this.properties = properties;
    }

    @RuntimeType
    public Iterator compute(
            @Argument(0) Partition partition,
            @Argument(1) TaskContext context,
            @This RDD rdd,
            @Morph Morpher<Iterator> morpher) throws UnknownHostException, JAXBException, DatatypeConfigurationException {

        MonitoringUtilities.emitIp(rdd, partition, type, properties);
        return morpher.invoke(partition, context);
    }

}