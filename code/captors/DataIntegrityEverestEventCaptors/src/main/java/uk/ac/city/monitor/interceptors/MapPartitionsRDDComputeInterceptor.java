package uk.ac.city.monitor.interceptors;

import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.FieldValue;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.This;
import org.apache.spark.Partition;
import org.apache.spark.SparkEnv$;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import scala.Function3;
import scala.collection.Iterator;
import uk.ac.city.monitor.enums.EmitterType;
import uk.ac.city.monitor.enums.OperationType;
import uk.ac.city.monitor.iterators.DataIntegrityMonitorableIterator;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class MapPartitionsRDDComputeInterceptor {

    private final EmitterType type;
    private final Properties properties;

    public MapPartitionsRDDComputeInterceptor(EmitterType type, Properties properties){
        this.type = type;
        this.properties = properties;
    }

    @RuntimeType
    public <T, U> Iterator<U> compute(
            @Argument(0) Partition split,
            @Argument(1) TaskContext context,
            @This RDD<T> rdd, @FieldValue("f") Function3<TaskContext, Integer, Iterator<T>, Iterator<U>> f) throws IOException {

        String applicationId = SparkEnv$.MODULE$.get().conf().get("spark.app.id");
        String applicationName = SparkEnv$.MODULE$.get().conf().get("spark.app.name");

        Map<String,String> readParams = new LinkedHashMap<>();
        readParams.put("appId", applicationId);
        readParams.put("appName", applicationName);
        readParams.put("rddId", String.valueOf(rdd.firstParent(rdd.elementClassTag()).id()));
        readParams.put("partId", String.valueOf(split.index()));

        Iterator<T> input =
                new DataIntegrityMonitorableIterator<T>(
                        rdd.firstParent(rdd.elementClassTag()).iterator(split, context),
                        type,
                        properties,
                        OperationType.READRDD,
                        readParams);

        Map<String,String> writeParams = new LinkedHashMap<>();
        writeParams.put("appId", applicationId);
        writeParams.put("appName", applicationName);
        writeParams.put("rddId", String.valueOf(rdd.id()));
        writeParams.put("partitionId", String.valueOf(split.index()));
        return
                new DataIntegrityMonitorableIterator<U>(
                        f.apply(context, split.index(), input),
                        type,
                        properties,
                        OperationType.WRITERDD,
                        writeParams);
    }
}
