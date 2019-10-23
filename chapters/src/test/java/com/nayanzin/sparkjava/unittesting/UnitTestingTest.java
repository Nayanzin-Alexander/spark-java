package com.nayanzin.sparkjava.unittesting;

import com.holdenkarau.spark.testing.JavaRDDComparisons;
import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.io.Serializable;
import java.util.Arrays;

import static org.apache.spark.api.java.JavaPairRDD.toRDD;
import static org.apache.spark.api.java.JavaRDD.fromRDD;

public class UnitTestingTest extends SharedJavaSparkContext implements Serializable {
    private static final long serialVersionUID = -5681683598336701496L;
    private JavaRDD<String> inputRdd;
    private JavaPairRDD<String, Integer> expectedValidRDD;
    private ClassTag<Tuple2<String, Integer>> tag;

    @Before
    public void setup() {
        // Create test input data
        inputRdd = jsc().parallelize(Arrays.asList("A B C D D", "A B D D"), 4);

        // Create expected outputs
        tag = ClassTag$.MODULE$.apply(Tuple2.class);

        expectedValidRDD = jsc().parallelizePairs(Arrays.asList(
                new Tuple2<>("A", 2),
                new Tuple2<>("B", 2),
                new Tuple2<>("C", 1),
                new Tuple2<>("D", 4)));
    }

    @Test
    public void getWordsCount() {
        // Run under the test method
        JavaPairRDD<String, Integer> result = UnitTesting.getWordsCount(inputRdd);

        // Run the assertions on the result and expected
        JavaRDDComparisons.assertRDDEquals(
                fromRDD(toRDD(result), tag),
                fromRDD(toRDD(expectedValidRDD), tag)
        );
    }
}