package com.atguigu;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import net.minidev.json.JSONObject;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;


import java.util.List;

public class CustomerDeserialization  implements DebeziumDeserializationSchema<String> {

    //封装的数据格式
    /*
    {
    "database":""
    "tablename":""
    "type":"c r u d "
    "before":"{"":"","":"",....}"
    "after":"{"":"","":"",....}"
//    "ts":12345

    }
     */
    @Override
    public void deserialize(SourceRecord sourcerecord, Collector<String> collector) throws Exception {

        //创建json对象用于存储最终数据
        JSONObject result=new JSONObject();
        //获取数据库和表名
        String topic =sourcerecord.topic();
        String[] fields=topic.split("\\.");
        String database=fields[1];
        String tableName=fields[2];

        Struct value=(Struct)sourcerecord.value();


        //获取before数据
        Struct before=value.getStruct("before");
        JSONObject beforeJson=new JSONObject();
        if (before!=null) {
            Schema beforeSchema = before.schema();
            List<Field> beforeFields = beforeSchema.fields();
            for (Field field : beforeFields) {
                Object beforeValue = before.get(field);
                beforeJson.put(field.name(), beforeValue);
            }
        }

//        获取after数据

        Struct after=value.getStruct("after");
        JSONObject afterJson=new JSONObject();
        if (after!=null) {
            Schema afterSchema = after.schema();
            List<Field> afterFields = afterSchema.fields();
            for (Field field : afterFields) {
                Object afterValue = after.get(field);
                afterJson.put(field.name(), afterValue);
            }
        }

        //获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourcerecord);
        String type=operation.toString().toLowerCase();
        if("create".equals(type)){
            type="insert";
        }
//        System.out.println(operation);
        //将字段写入json对象
        result.put("database", database);
        result.put("tableName", tableName);
        result.put("before", beforeJson);
        result.put("after", afterJson);
        result.put("type", type);

        //输出数据
        collector.collect(result.toJSONString());

    }

    @Override
    public TypeInformation<String> getProducedType() {

        return BasicTypeInfo.STRING_TYPE_INFO;
    }

}
