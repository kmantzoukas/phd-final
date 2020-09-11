package uk.ac.city.monitor.interceptors;

import net.bytebuddy.implementation.bind.annotation.*;
import org.apache.spark.Partition;
import org.apache.spark.SparkEnv$;
import org.apache.spark.TaskContext;
import org.apache.spark.api.python.PythonFunction;
import org.apache.spark.api.python.PythonRunner;
import org.apache.spark.rdd.RDD;
import scala.Function3;
import scala.Tuple2;
import scala.collection.Iterator;
import uk.ac.city.monitor.enums.EmitterType;
import uk.ac.city.monitor.enums.OperationType;
import uk.ac.city.monitor.iterators.DataIntegrityMonitorableIterator;
import uk.ac.city.monitor.utils.Morpher;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

public class PythonRDDComputeInterceptor {

    private final EmitterType type;
    private final Properties properties;

    public PythonRDDComputeInterceptor(EmitterType type, Properties properties) {
        this.type = type;
        this.properties = properties;
    }

    @RuntimeType
    public  Iterator<byte[]> compute(
            @Argument(0) Partition split,
            @Argument(1) TaskContext context,
            @This RDD rdd,
            @FieldValue("bufferSize") int bufferSize,
            @FieldValue("reuseWorker") boolean reuseWorker,
            @FieldValue("func") Object func) {

        String applicationId = SparkEnv$.MODULE$.get().conf().get("spark.app.id");
        String applicationName = SparkEnv$.MODULE$.get().conf().get("spark.app.name");

        Map<String,String> readParams = new LinkedHashMap<>();
        readParams.put("appId", applicationId);
        readParams.put("appName", applicationName);
        readParams.put("rddId", String.valueOf(rdd.firstParent(rdd.elementClassTag()).id()));
        readParams.put("partId", String.valueOf(split.index()));

        Iterator input =
                new DataIntegrityMonitorableIterator(
                        rdd.firstParent(rdd.elementClassTag()).iterator(split, context),
                        type,
                        properties,
                        OperationType.READRDD,
                        readParams);

        PythonRunner runner = PythonRunner.apply(((PythonFunction) func), bufferSize, reuseWorker);

        Map<String,String> writeParams = new LinkedHashMap<>();
        writeParams.put("appId", applicationId);
        writeParams.put("appName", applicationName);
        writeParams.put("rddId", String.valueOf(rdd.id()));
        writeParams.put("partitionId", String.valueOf(split.index()));
        return
                new DataIntegrityMonitorableIterator<byte[]>(
                        runner.compute(input, split.index(), context),
                        type,
                        properties,
                        OperationType.WRITERDD,
                        writeParams);
    }
}
