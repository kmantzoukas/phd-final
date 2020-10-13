package uk.ac.city.monitor.interceptors;

import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.Morph;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import org.apache.spark.SparkEnv$;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.ShuffleBlockId;
import scala.Function1;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;
import uk.ac.city.monitor.emitters.Emitter;
import uk.ac.city.monitor.emitters.EventEmitterFactory;
import uk.ac.city.monitor.enums.EmitterType;
import uk.ac.city.monitor.enums.OperationType;
import uk.ac.city.monitor.iterators.DataIntegrityMonitorableIterator;
import uk.ac.city.monitor.utils.Morpher;

import java.io.InputStream;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class ShuffleBlockFetcherIteratorFlatMapInterceptor {

    private final EmitterType type;
    private final Properties properties;

    public ShuffleBlockFetcherIteratorFlatMapInterceptor(EmitterType type, Properties properties) {
        this.properties = properties;
        this.type = type;
    }

    @RuntimeType
    public Iterator<Tuple2<BlockId, InputStream>> flatMap(
            @Morph Morpher<Iterator<Tuple2<BlockId, InputStream>>> morpher,
            @Argument(0) Function1<Tuple2, Iterator> func) {


        String applicationId = SparkEnv$.MODULE$.get().conf().get("spark.app.id");
        String applicationName = SparkEnv$.MODULE$.get().conf().get("spark.app.name");

        final class Func extends AbstractFunction1<Tuple2<BlockId, InputStream>, Iterator<Tuple2<BlockId, InputStream>>> implements Serializable {
            @Override
            public Iterator<Tuple2<BlockId, InputStream>> apply(Tuple2<BlockId, InputStream> v1) {

                Emitter emitter = EventEmitterFactory.getInstance(type, properties);
                emitter.connect();
                emitter.send("HEYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY");

                ShuffleBlockId blockId = (ShuffleBlockId) v1._1;

                Map<String, String> parameters = new LinkedHashMap<>();
                parameters.put("appId", applicationId);
                parameters.put("appName", applicationName);
                parameters.put("shuffleId", String.valueOf(blockId.shuffleId()));
                parameters.put("mapId", String.valueOf(blockId.mapId()));
                parameters.put("reduceId", String.valueOf(blockId.reduceId()));

                return new DataIntegrityMonitorableIterator(
                        func.apply(v1), type, properties, OperationType.READSHUFFLE, parameters);
            }
        }
        return morpher.invoke(new Func());
    }

}
