package com.atguigu.chapter09StateProgramming;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;

public class TTLDemo {
    public static void main(String[] args) {
        StateTtlConfig ttlConfig = StateTtlConfig
                //状态TTL 配置的构造器方法，必须调用，返回一个Builder 之后再调用.build()方法就可以得到StateTtlConfig
                .newBuilder(Time.seconds(10))
                // 设置更新类型。更新类型指定了什么时候更新状态失效时间，这里的OnCreateAndWrite表示只有创建状态和更改状态（写操作）时更新失效时间
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                //设置状态的可见性，是指因为清除操作并不是实时，过期之后还有可能基于存在，这时如果对它进行访问，能否正常读取到的配置；NeverReturnExpired 是默认行为，表示从不返回过期值
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        ValueStateDescriptor<String> myState = new ValueStateDescriptor<>("myState", String.class);

        myState.enableTimeToLive(ttlConfig);
    }
}
