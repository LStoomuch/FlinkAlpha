package xuwei.tech.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 滑动窗口计算
 *
 * 通过socket模拟产生单词数据
 * flink对数据进行统计计算
 *
 * 需要实现每隔1秒对最近2秒内的数据进行汇总计算
 *
 *
 * Created by xuwei.tech on 2018/10/8.
 */
public class SocketWindowWordCountJava {

    public static void main(String[] args) throws Exception{
        //获取需要的端口号

        //获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        //mac终端命令 nc -lk 9000
        int port = 9000;
        String delimiter = "\n";
        //连接socket获取输入的数据
        DataStreamSource<String> text = env.socketTextStream("localhost", port, delimiter);

        // a a c

        // a 1
        // a 1
        // c 1
        DataStream<WordWithCount> windowCounts = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                String[] splits = value.split("\\s");
                for (String word : splits) {
                    out.collect(new WordWithCount(word, 1L));
                }
            }
        }).keyBy("word")
//                .timeWindow(Time.seconds(5),Time.seconds(1))//指定时间窗口大小为2秒，指定时间间隔为1秒
                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) throws Exception {
                        System.out.println("ttttt");
                        return new WordWithCount(a.word,a.count+b.count);
                    }
                });
        //把数据打印到控制台并且设置并行度
        windowCounts.print().setParallelism(1);

        //这一行代码一定要实现，否则程序不执行
        env.execute("Socket window count");

    }

    public static class WordWithCount{
        public String word;
        public long count;
        public  WordWithCount(){}
        public WordWithCount(String word,long count){
            this.word = word;
            this.count = count;
        }
        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }



}
