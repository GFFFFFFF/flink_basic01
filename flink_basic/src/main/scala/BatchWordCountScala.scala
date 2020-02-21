import org.apache.flink.api.scala.ExecutionEnvironment

object BatchWordCountScala {

  def main(args: Array[String]): Unit = {

    //隐式转换
    import org.apache.flink.api.scala._

    val inputPath = "E:\\data\\file"
    val outPut = "E:\\data\\result2"

    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile(inputPath)

    val counts = text.flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map(w=>(w,1))
      .groupBy(0)
      .sum(1)

    counts.writeAsCsv(outPut,"\n"," ")
    env.execute("batch word count")

  }

}
