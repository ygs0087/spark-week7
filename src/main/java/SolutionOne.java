import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SolutionOne {

    public static void main(String[] args) {
        String rootInputPath = SolutionOne.class.getClassLoader().getResource("invertedIndex/").getPath();

        SparkConf sparkConf = new SparkConf().setAppName("InvertedIndex").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaPairRDD<String, String> fileContentRDD = javaSparkContext.wholeTextFiles(rootInputPath, 1);
        // 构建每个单词 到出现的文件名
        JavaPairRDD<String, String> wordFileRDD = fileContentRDD.flatMapToPair((PairFlatMapFunction<Tuple2<String, String>, String, String>) fileNameContentPair -> {
            String rawFileName = fileNameContentPair._1();
            // 去除路径，只保留文件名
            String fileName = rawFileName.substring(rawFileName.lastIndexOf('/') + 1);

            String content = fileNameContentPair._2();
            String[] lines = content.split("[\r\n]");
            List<Tuple2<String, String>> fileNameWordPairs = new ArrayList<>(lines.length);
            for (String line : lines) {
                String[] wordsInCurrentLine = line.split(" ");
                fileNameWordPairs.addAll(Arrays.stream(wordsInCurrentLine).map(word -> new Tuple2<>(word, fileName)).collect(Collectors.toList()));
            }
            return fileNameWordPairs.iterator();
        });

        // 统计每个单词在文件名 的出现次数
        JavaPairRDD<Tuple2<String, String>, Integer> wordFileNameCountPerPairs = wordFileRDD.mapToPair(wordFileNamePair -> new Tuple2<>(wordFileNamePair, 1))
                .reduceByKey(Integer::sum);
        // 将key转换成 单词, value为出现的文件名 以及 对应文件的出现次数
        JavaPairRDD<String, Tuple2<String, Integer>> wordCountPerFileNamePairs = wordFileNameCountPerPairs.mapToPair(wordFileNameCountPerPair -> new Tuple2<>(wordFileNameCountPerPair._1._1, new Tuple2<>(wordFileNameCountPerPair._1._2, wordFileNameCountPerPair._2)));
        // 合并相同key, 并将相同key的value 以逗号分隔合并
        JavaPairRDD<String, String> result = wordCountPerFileNamePairs.groupByKey().mapToPair(wordCountPerFileNamePairIterator -> new Tuple2<>(wordCountPerFileNamePairIterator._1, StringUtils.join(wordCountPerFileNamePairIterator._2.iterator(), ','))).sortByKey();
        result.repartition(1).saveAsTextFile("invertedIndexResult");

    }

}
