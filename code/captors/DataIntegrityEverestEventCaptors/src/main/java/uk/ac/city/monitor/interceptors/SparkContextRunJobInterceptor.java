package uk.ac.city.monitor.interceptors;

import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.Morph;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.This;
import org.apache.spark.SparkEnv$;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import scala.Function1;
import scala.Function2;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import uk.ac.city.monitor.enums.EmitterType;
import uk.ac.city.monitor.enums.OperationType;
import uk.ac.city.monitor.iterators.DataIntegrityMonitorableIterator;
import uk.ac.city.monitor.utils.Morpher;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

public class SparkContextRunJobInterceptor implements Serializable{

    private final EmitterType type;
    private final Properties properties;

    public SparkContextRunJobInterceptor(EmitterType type, Properties properties){
        this.type = type;
        this.properties = properties;
    }

    @RuntimeType
    public Object runJob(
            @Argument(0) RDD rdd,
            @Argument(1) Function2 f,
            @Argument(2) Object classTag,
            @Morph Morpher<Object> morpher,
            @This Object sc) throws IOException {

        String applicationId = SparkEnv$.MODULE$.get().conf().get("spark.app.id");
        String applicationName = SparkEnv$.MODULE$.get().conf().get("spark.app.name");

            /*
            Custom Scala function that creates a data integrity monitorable iterator and passes it
            along in the runJob() method to compute the hashes for the data that each iterator
            reads during the execution of the action
             */
        final class Func extends AbstractFunction2<TaskContext , Iterator, Object> implements Serializable {

            @Override
            public Object apply(TaskContext context, Iterator it) {

                Map<String,String> parameters = new LinkedHashMap<>();
                parameters.put("appId", applicationId);
                parameters.put("appName", applicationName);
                parameters.put("rddId", String.valueOf(rdd.id()));
                parameters.put("partitionId", String.valueOf(context.getPartitionId()));

                return  f.apply(context, new DataIntegrityMonitorableIterator(it, type, properties, OperationType.READRDD, parameters));
            }

        }
        return morpher.invoke(rdd, new Func(), classTag);
    }
}
