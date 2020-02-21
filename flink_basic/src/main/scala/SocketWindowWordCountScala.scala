import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

//滑动窗口计算
//通过socket模拟产生单词数据
//flink对数据进行统计计算
//需要实现每隔一秒对最近两秒的数据进行汇总计算

object SocketWindowWordCountScala {
  def main(args: Array[String]): Unit = {

    //隐式转换
    import org.apache.flink.api.scala._

    //获取socket端口号
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    }catch {
      case e:Exception => {
        System.err.println("我特娘的忘记设置端口号了，然后默认了一个1354")
      }
        1354
    }

    //获取运行环境
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //连接socket获取数据源
    val text = env.socketTextStream("hadoop101",port,'\n')

    //解析数据(把数据打平)、分组、窗口计算、并且聚合求sum
    val windowsCounts = text.flatMap(line => line.split("\\s"))
      .map(w => WordWithCount(w,1))
      .keyBy("word")
      .timeWindow(Time.seconds(2),Time.seconds(1))
      .sum("count")
    //打印到控制台
    windowsCounts.print().setParallelism(1)
    //执行任务
    env.execute("Socket windows count")

  }


  case class WordWithCount(word:String,count:Long)

}
