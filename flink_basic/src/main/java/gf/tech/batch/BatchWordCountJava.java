package gf.tech.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCountJava {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String inputPath = "E:\\data\\file";
        String outPath="E:\\data\\result";
        //获取文件内容
        DataSource<String> text = env.readTextFile(inputPath);
        DataSet<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer()).groupBy(0).sum(1);
        counts.writeAsCsv(outPath,"\n"," ").setParallelism(1);
        env.execute("batch word count");


    }
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String,Integer>>{
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token:tokens) {
                if(token.length()>0){
                    out.collect(new Tuple2<String,Integer>(token,1));
                }
            }
        }
    }
}
