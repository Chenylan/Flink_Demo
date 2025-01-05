package com.atguigu;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkCDCwithSQL {
    public static void main(String[] args) throws Exception {
        //1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2 DDL方式建表
        tableEnv.executeSql("CREATE TABLE mysql_binlog (" +
                "  id STRING NOT NULL," +
                "  tm_name STRING," +
                "  logo_url STRING" +
                ") WITH (" +
                "  'connector' = 'mysql-cdc'," +
                "  'hostname' = 'ubuntu'," +
                "  'port' = '3306'," +
                "  'username' = 'root'," +
                "  'password' = '123456'," +
                "  'database-name' = 'gmall-210325-flink'," +
                "  'table-name' = 'base_trademark'" +
                ")");

        //3查询数据
        Table table = tableEnv.sqlQuery("select * from mysql_binlog");

        //4将动态表转化为流
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
        retractStream.print();

        //5启动任务
        env.execute("FlinkCDCwithSQL");
    }
}
