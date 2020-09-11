package uk.ac.city.monitor.interceptors;

/*
Byte Buddy
 */
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.This;
import net.bytebuddy.implementation.bind.annotation.Morph;

/*
Apache Spark
 */
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;

/*
Scala
 */
import scala.collection.Iterator;

/*
Java
 */
import javax.xml.bind.JAXBException;
import javax.xml.datatype.DatatypeConfigurationException;
import java.net.UnknownHostException;
import java.util.Properties;

/*
City
 */
import uk.ac.city.monitor.enums.EmitterType;
import uk.ac.city.monitor.utils.MonitoringUtilities;
import uk.ac.city.monitor.utils.Morpher;

public class MapPartitionsRDDComputeInterceptor{

    private final EmitterType type;
    private final Properties properties;

    public MapPartitionsRDDComputeInterceptor(EmitterType type, Properties properties){
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