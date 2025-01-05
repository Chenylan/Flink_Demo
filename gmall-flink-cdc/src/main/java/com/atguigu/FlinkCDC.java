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

public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        //1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        //1.1开启CK并指定状态后端为FS  memory  fs rocksdb
        env.setStateBackend(new FsStateBackend("hdfs://ubuntu:9000/gmall-flink_210325/ck"));
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(5000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart());
        //2通过FlinkCDC构建SourceFunction并读取数据
        DebeziumSourceFunction<String> sourceFuncation = MySQLSource.<String>builder()
                .hostname("ubuntu")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-210325-flink")
                .tableList("gmall-210325-flink.base_trademark")  //如果不添加该参数，则消费指定库中所有表的数据
                .deserializer(new StringDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> streamSource=env.addSource(sourceFuncation);

        //3打印数据
        streamSource.print();


        //4 启动任务
        env.execute("FlinkCDC");
    }
}
