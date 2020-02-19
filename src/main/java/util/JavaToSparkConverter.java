package util;

import org.apache.spark.api.java.JavaRDD;
import scala.collection.JavaConverters;

import java.util.Arrays;

public class JavaToSparkConverter {
    public static <T> JavaRDD<T> parallelize(T[] a, int partitions) {
        return SparkSessionHolder.getSparkSession().sparkContext()
                .parallelize(JavaConverters.asScalaIteratorConverter(Arrays.asList(a).iterator()).asScala().toSeq(),
                        partitions,
                        scala.reflect.ClassTag$.MODULE$.apply(a[0].getClass()))
                .toJavaRDD();
    }
}
