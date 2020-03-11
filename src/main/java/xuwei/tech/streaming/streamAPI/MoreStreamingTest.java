package xuwei.tech.streaming.streamAPI;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import xuwei.tech.streaming.custormSource.Agg;
import xuwei.tech.streaming.custormSource.MyWordSource;

import java.util.ArrayList;

/**
 * split
 *
 * 根据规则把一个数据流切分为多个流
 *
 * 应用场景：
 * 可能在实际工作中，源数据流中混合了多种类似的数据，多种类型的数据处理规则不一样，所以就可以在根据一定的规则，
 * 把一个数据流切分成多个数据流，这样每个数据流就可以使用不用的处理逻辑了
 *
 * Created by xuwei.tech on 2018/10/23.
 */
public class MoreStreamingTest {

    public static void main(String[] args) throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据源
        DataStreamSource<Agg> text = env.addSource(new MyWordSource()).setParallelism(1);//注意：针对此source，并行度只能设置为1

        //对流进行切分，按照数据的奇偶性进行区分
        SplitStream<Agg> splitStream = text.split(new OutputSelector<Agg>() {
            @Override
            public Iterable<String> select(Agg agg) {
                ArrayList<String> outPut = new ArrayList<>();
                if (agg.getType().equals("c")) {
                    outPut.add("c");
                } else {
                    outPut.add("d");
                }
                return outPut;
            }
        });



        //选择一个或者多个切分后的流
        DataStream<WordWithCount> evenStream = text.filter(new FilterFunction<Agg>() {
            @Override
            public boolean filter(Agg agg) throws Exception {
                if (agg.getType().equals("c")) {
                    return true;
                }
                return false;
            }
        }).flatMap(new FlatMapFunction<Agg, WordWithCount>() {
            @Override
            public void flatMap(Agg agg, Collector<WordWithCount> collector) throws Exception {
                System.out.println("key:" + agg.getKey() + "  type:" + agg.getType());
                collector.collect(new WordWithCount(agg.getKey(), agg.getType(), 1));
            }
        }).keyBy("key")
                .sum("count");

        DataStream<WordWithCount> oddStream = text.filter(new FilterFunction<Agg>() {
            @Override
            public boolean filter(Agg agg) throws Exception {
                if (agg.getType().equals("d")) {
                    return true;
                }
                return false;
            }
        }).flatMap(new FlatMapFunction<Agg, WordWithCount>() {
            @Override
            public void flatMap(Agg agg, Collector<WordWithCount> collector) throws Exception {
                System.out.println("key:" + agg.getKey() + "  type:" + agg.getType());
                collector.collect(new WordWithCount(agg.getKey(), agg.getType(), 1));
            }
        }).keyBy("key")
                .sum("count");

//        DataStream<Agg> moreStream = splitStream.select("c","d");


        //打印结果
        evenStream.print().setParallelism(1);
        oddStream.print().setParallelism(1);

        String jobName = MoreStreamingTest.class.getSimpleName();
        env.execute(jobName);
    }

    public static class WordWithCount{
        public String key;
        public String type;
        public long count;
        public  WordWithCount(){}

        public WordWithCount(String key, String type, long count) {
            this.key = key;
            this.type = type;
            this.count = count;
        }
        @Override
        public String toString() {
            return "WordWithCount{" +
                    "key='" + key + '\'' +
                    "type='" + type + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
