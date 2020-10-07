package uk.ac.city.monitor.interceptors;

import com.google.common.io.Closeables;
import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.FieldValue;
import net.bytebuddy.implementation.bind.annotation.Morph;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkEnv$;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.storage.DiskBlockObjectWriter;
import org.apache.spark.storage.FileSegment;
import org.apache.spark.storage.ShuffleBlockId;
import org.apache.spark.util.Utils;
import org.apache.spark.util.collection.WritablePartitionedPairCollection;
import org.slf4j.Logger;
import uk.ac.city.monitor.emitters.Emitter;
import uk.ac.city.monitor.emitters.EventEmitterFactory;
import uk.ac.city.monitor.enums.EmitterType;
import uk.ac.city.monitor.enums.OperationType;
import uk.ac.city.monitor.utils.MonitorUtilities;
import uk.ac.city.monitor.utils.Morpher;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class BypassMergeSortShuffleWriterWritePartitionedFileInterceptor {

    private final EmitterType type;
    private final Properties properties;

    public BypassMergeSortShuffleWriterWritePartitionedFileInterceptor(EmitterType type, Properties properties){
        this.type = type;
        this.properties = properties;
    }

    @RuntimeType
    public long[] writePartitionedFile(
            @Argument(0) File output,
            @FieldValue("partitionWriters") DiskBlockObjectWriter[] partitionWriters,
            @FieldValue("partitionWriterSegments") FileSegment[] partitionWriterSegments,
            @FieldValue("numPartitions") int numPartitions,
            @FieldValue("writeMetrics") ShuffleWriteMetrics writeMetrics,
            @FieldValue("transferToEnabled") boolean transferToEnabled,
            @FieldValue("shuffleId") int shuffleId,
            @FieldValue("mapId") int mapId,
            @FieldValue("logger") Logger logger) throws IOException, NoSuchAlgorithmException {

        String applicationId = SparkEnv$.MODULE$.get().conf().get("spark.app.id");
        String applicationName = SparkEnv$.MODULE$.get().conf().get("spark.app.name");

        Emitter emitter = EventEmitterFactory.getInstance(type, properties);
        emitter.connect();

        MessageDigest md = MessageDigest.getInstance("MD5");

        Map<String, String > parameters = new LinkedHashMap<>();
        parameters.put("appId", applicationId);
        parameters.put("appName", applicationName);
        parameters.put("shuffleId", String.valueOf(shuffleId));
        parameters.put("mapId",  String.valueOf(mapId));

        final long[] lengths = new long[numPartitions];
        if (partitionWriters == null) {
            return lengths;
        }

        final FileOutputStream out = new FileOutputStream(output, true);
        final long writeStartTime = System.nanoTime();
        boolean threwException = true;
        try {
            for (int i = 0; i < numPartitions; i++) {
                final File file = partitionWriterSegments[i].file();
                if (file.exists()) {
                    final FileInputStream in = new FileInputStream(file);
                    boolean copyThrewException = true;
                    try {
                        byte[] bytes = IOUtils.toByteArray(in);
                        md.update(bytes);

                        long operationId = MonitorUtilities.generateRandomLong();
                        parameters.put("reduceId",  String.valueOf(i));
                        parameters.put("checksum", DatatypeConverter.printHexBinary(md.digest()));
                        parameters.put("length", String.valueOf(bytes.length));
                        String event = MonitorUtilities.createEvent(operationId, properties.getProperty("eventStype"), OperationType.WRITESHUFFLE, parameters);
                        emitter.send(event);

                        lengths[i] = Utils.copyStream(in, out, false, transferToEnabled);
                        copyThrewException = false;
                    } finally {
                        Closeables.close(in, copyThrewException);
                    }
                    if (!file.delete()) {
                        logger.error("Unable to delete file for partition {}", i);
                    }
                }
            }
            emitter.close();
            threwException = false;
        } finally {
            Closeables.close(out, threwException);
            writeMetrics.incWriteTime(System.nanoTime() - writeStartTime);
        }
        partitionWriters = null;
        return lengths;
    }

}
