package uk.ac.city.monitor.interceptors;

import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.Morph;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.This;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.Partition;
import org.apache.spark.SparkEnv$;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.collection.Iterator;
import uk.ac.city.monitor.emitters.Emitter;
import uk.ac.city.monitor.enums.EmitterType;
import uk.ac.city.monitor.enums.OperationType;
import uk.ac.city.monitor.iterators.DataIntegrityMonitorableIterator;
import uk.ac.city.monitor.utils.Morpher;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class HadoopRDDComputeInterceptor {

    private final EmitterType type;
    private final Properties properties;

    public HadoopRDDComputeInterceptor(EmitterType type, Properties properties) {
        this.type = type;
        this.properties = properties;
    }

    @RuntimeType
    public <K, V> Iterator<Tuple2<K, V>> compute(
            @Argument(0) Partition theSplit,
            @Argument(1) TaskContext context,
            @This RDD<Tuple2<K, V>> rdd,
            @Morph Morpher<InterruptibleIterator<Tuple2<K, V>>> morpher) throws InterruptedException {


        String applicationId = SparkEnv$.MODULE$.get().conf().get("spark.app.id");
        String applicationName = SparkEnv$.MODULE$.get().conf().get("spark.app.name");

        Map<String, String> parameters = new LinkedHashMap<>();
        parameters.put("appId", applicationId);
        parameters.put("appName", applicationName);
        parameters.put("rddId", String.valueOf(rdd.id()));
        parameters.put("partitionId", String.valueOf(theSplit.index()));

        return new InterruptibleIterator(context, new DataIntegrityMonitorableIterator<Tuple2<K, V>>(
                morpher.invoke(theSplit, context),
                type,
                properties,
                OperationType.WRITERDD,
                parameters));
    }
}
