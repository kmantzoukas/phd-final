package uk.ac.city.monitor.interceptors;

import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.Morph;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.This;
import org.apache.spark.Partition;
import org.apache.spark.SparkEnv$;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.collection.Iterator;
import uk.ac.city.monitor.enums.EmitterType;
import uk.ac.city.monitor.enums.OperationType;
import uk.ac.city.monitor.iterators.DataIntegrityMonitorableIterator;
import uk.ac.city.monitor.utils.Morpher;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class ShuffledRDDComputeInterceptor {

    private final EmitterType type;
    private final Properties properties;

    public ShuffledRDDComputeInterceptor(EmitterType type, Properties properties){
        this.properties = properties;
        this.type = type;
    }

    @RuntimeType
    public <K, V, C> Iterator<Tuple2<K, C>> compute(
            @Argument(0) Partition partition,
            @Argument(1) TaskContext taskContext,
            @This RDD rdd,
            @Morph Morpher<Iterator<Tuple2<K, C>>> morpher) {

        String applicationId = SparkEnv$.MODULE$.get().conf().get("spark.app.id");
        String applicationName = SparkEnv$.MODULE$.get().conf().get("spark.app.name");

        Map<String,String> parameters = new LinkedHashMap<>();
        parameters.put("appId", applicationId);
        parameters.put("appName", applicationName);
        parameters.put("rddId", String.valueOf(rdd.id()));
        parameters.put("partitionId", String.valueOf(partition.index()));

        return new DataIntegrityMonitorableIterator(
                morpher.invoke(partition, taskContext),
                type,
                properties,
                OperationType.WRITERDD,
                parameters);
    }
}
