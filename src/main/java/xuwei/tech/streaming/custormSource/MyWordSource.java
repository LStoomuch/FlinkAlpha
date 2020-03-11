package xuwei.tech.streaming.custormSource;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 自定义实现并行度为1的source
 *
 * 模拟产生从1开始的递增数字
 *
 *
 * 注意：
 * SourceFunction 和 SourceContext 都需要指定数据类型，如果不指定，代码运行的时候会报错
 * Caused by: org.apache.flink.api.common.functions.InvalidTypesException:
 * The types of the interface org.apache.flink.streaming.api.functions.source.SourceFunction could not be inferred.
 * Support for synthetic interfaces, lambdas, and generic or raw types is limited at this point
 *
 *
 * Created by xuwei.tech on 2018/10/23.
 */
public class MyWordSource implements SourceFunction<Agg>{



    private boolean isRunning = true;

    /**
     * 主要的方法
     * 启动一个source
     * 大部分情况下，都需要在这个run方法中实现一个循环，这样就可以循环产生数据了
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Agg> ctx) throws Exception {
        while(isRunning){
            Agg agg = new Agg();
            char c=(char)(int)(Math.random()*2+97);
            String value = String.valueOf(c);
            String type = String.valueOf((char) (int) (Math.random() * 2 + 99));
            agg.setKey(value);
            agg.setType(type);

            ctx.collect(agg);

            //每秒产生一条数据
            Thread.sleep(1000);
        }
    }

    /**
     * 取消一个cancel的时候会调用的方法
     *
     */
    @Override
    public void cancel() {
        isRunning = false;
    }

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            char c=(char)(int)(Math.random()*2+97);
            System.out.println(c);

            String type = String.valueOf((char) (int) (Math.random() * 2 + 99));
            System.out.println(type);
            //每秒产生一条数据
            Thread.sleep(5000);
        }

    }

}
