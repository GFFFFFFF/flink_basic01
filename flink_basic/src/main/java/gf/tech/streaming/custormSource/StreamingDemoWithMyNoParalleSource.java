package gf.tech.streaming.custormSource;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;

//使用并行度为1的source

public class StreamingDemoWithMyNoParalleSource {
    public static void main(String[] args)throws Exception {
        //获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据源
        DataStreamSource<Long> dataStreamSource = env.addSource(new MyNoParalleSource());//此source不支持多并行度

        DataStream<Long> num = dataStreamSource.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接收到数据："+value);
                return value;
            }
        });
        //每2秒处理一次数据
        DataStream<Long> sum = num.timeWindowAll(Time.seconds(2)).sum(0);

        //打印结果，线程为1
        sum.print().setParallelism(1);

        String jobName = StreamingDemoWithMyNoParalleSource.class.getSimpleName();
        env.execute(jobName);
    }
    }
