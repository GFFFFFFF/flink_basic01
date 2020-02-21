package gf.tech.streaming.custormSource;

//自定义实现一个支持并行的的source

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class MyParalleSource extends RichParallelSourceFunction<Long> {

    private long count = 0L;

    private boolean isRunning = true;

    //主要的方法
    //启动一个source
    //大部分情况下 在run方法中实现一个循环 这样就可以循环产生数据
    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning){
            ctx.collect(count);
            count++;
            //每秒产生一条数据
            Thread.sleep(1000);
        }
    }

    //取消一个cancel的时候调用的方法
    //停止任务时想做的一些操作：关闭资源、销毁连接等
    @Override
    public void cancel() {
        isRunning=false;
    }
}
