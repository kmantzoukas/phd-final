package uk.ac.city.monitor.interceptors;

import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.FieldValue;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.This;
import org.apache.spark.Aggregator;
import org.apache.spark.SparkEnv$;
import org.apache.spark.TaskContext;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.storage.*;
import org.apache.spark.util.collection.ExternalSorter;
import org.apache.spark.util.collection.WritablePartitionedPairCollection;
import scala.Option;
import scala.Product2;
import scala.Some;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.math.Ordering;
import uk.ac.city.monitor.emitters.Emitter;
import uk.ac.city.monitor.emitters.EventEmitterFactory;
import uk.ac.city.monitor.enums.EmitterType;
import uk.ac.city.monitor.enums.OperationType;
import uk.ac.city.monitor.utils.MonitorUtilities;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class ExternalSorterWritePartitionedFileInterceptor {

    private final EmitterType type;
    private final Properties properties;

    public ExternalSorterWritePartitionedFileInterceptor(EmitterType type, Properties properties){
        this.type = type;
        this.properties = properties;
    }

    @RuntimeType
    public <K, V, C> long[] writePartitionedFile(
            @Argument(0) BlockId blockId,
            @Argument(1) File outputFile,
            @This Object sorter,
            @FieldValue("blockManager") BlockManager blockManager,
            @FieldValue("fileBufferSize") Integer fileBufferSize,
            @FieldValue("org$apache$spark$util$collection$ExternalSorter$$context") TaskContext context,
            @FieldValue("org$apache$spark$util$collection$ExternalSorter$$aggregator") Option<Aggregator<K, V, C>> aggregator,
            @FieldValue("org$apache$spark$util$collection$ExternalSorter$$ordering") Option<Ordering<K>> ordering,
            @FieldValue("org$apache$spark$util$collection$ExternalSorter$$keyComparator") Comparator<K> keyComparator,
            @FieldValue("org$apache$spark$util$collection$ExternalSorter$$numPartitions") Integer numPartitions,
            @FieldValue("org$apache$spark$util$collection$ExternalSorter$$serInstance") SerializerInstance serInstance,
            @FieldValue("map") WritablePartitionedPairCollection<K, C> map,
            @FieldValue("buffer") WritablePartitionedPairCollection<K, C> buffer) throws NoSuchAlgorithmException {

        String applicationId = SparkEnv$.MODULE$.get().conf().get("spark.app.id");
        String applicationName = SparkEnv$.MODULE$.get().conf().get("spark.app.name");

        long[] lengths = new long[numPartitions];
        DiskBlockObjectWriter writer =
                blockManager
                        .getDiskWriter(
                                blockId,
                                outputFile,
                                serInstance,
                                fileBufferSize * 1024,
                                context.taskMetrics().shuffleWriteMetrics());

        if(((ExternalSorter)sorter).numSpills() == 0){
            WritablePartitionedPairCollection collection = aggregator.isDefined() ? map : buffer;

            Iterator<Tuple2<Tuple2<Integer, K>, C>> sortedIterator =
                    collection.partitionedDestructiveSortedIterator(
                            (ordering.isDefined() || aggregator.isDefined()) ?
                                    Some.apply(keyComparator) :
                                    Option.empty());

            MessageDigest md = MessageDigest.getInstance("MD5");

            while(sortedIterator.hasNext()){

                Tuple2<Tuple2<Integer, K>, C> cur = sortedIterator.next();
                int partitionId = cur._1()._1();

                while (sortedIterator.hasNext() && cur._1()._1() == partitionId){
                    md.update(new Tuple2(cur._1()._2(), cur._2()).toString().getBytes());
                    writer.write(cur._1()._2(), cur._2());
                    cur = sortedIterator.next();
                }

                FileSegment segment = writer.commitAndGet();
                lengths[partitionId] = segment.length();

                Emitter emitter = EventEmitterFactory.getInstance(type, properties);
                emitter.connect();

                Map<String, String > parameters = new LinkedHashMap<>();
                parameters.put("appId", applicationId);
                parameters.put("appName", applicationName);
                parameters.put("shuffleId", String.valueOf(((ShuffleBlockId)blockId).shuffleId()));
                parameters.put("mapId",  String.valueOf(((ShuffleBlockId)blockId).mapId()));
                parameters.put("reduceId",  String.valueOf(partitionId));
                parameters.put("checksum", DatatypeConverter.printHexBinary(md.digest()));

                long operationId = MonitorUtilities.generateRandomLong();
                emitter.send(MonitorUtilities.createEvent(operationId, properties.getProperty("eventStype"), OperationType.WRITESHUFFLE, parameters));
                emitter.close();
            }
        }else {
            Iterator<Tuple2<Integer, Iterator<Product2>>> partitionedIterator = ((ExternalSorter)sorter).partitionedIterator();

            while(partitionedIterator.hasNext()){
                Tuple2 tuple = partitionedIterator.next();

                Integer id = (Integer) tuple._1();
                Iterator<Product2> elements = (Iterator<Product2>)tuple._2();

                if(elements.hasNext()){
                    while(elements.hasNext()){
                        Product2 elem = elements.next();
                        writer.write(elem._1(), elem._2());
                    }

                    FileSegment segment = writer.commitAndGet();
                    lengths[id] = segment.length();
                }
            }
        }

        writer.close();
        context.taskMetrics().incMemoryBytesSpilled(((ExternalSorter)sorter).memoryBytesSpilled());
        context.taskMetrics().incDiskBytesSpilled(((ExternalSorter)sorter).diskBytesSpilled());
        context.taskMetrics().incPeakExecutionMemory(((ExternalSorter)sorter).peakMemoryUsedBytes());

        return lengths;
    }

}
