package com.atguigu;


import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
public class FlinkCDCwithCustomerDeserialization {

    public static void main(String[] args) throws Exception {
        //1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);



        //2通过FlinkCDC构建SourceFunction并读取数据
        DebeziumSourceFunction<String> sourceFuncation = MySQLSource.<String>builder()
                .hostname("ubuntu")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-210325-flink")
                .tableList("gmall-210325-flink.z_user_info")  //如果不添加该参数，则消费指定库中所有表的数据
                .deserializer(new CustomerDeserialization())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> streamSource=env.addSource(sourceFuncation);

        //3打印数据
        streamSource.print();


        //4 启动任务
        env.execute("FlinkCDCwithCustomerDeserialization");
    }
}
