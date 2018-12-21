/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.cluster.loadbalance;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcStatus;

import java.util.List;
import java.util.Random;

/**
 * LeastActiveLoadBalance
 *
 */
public class LeastActiveLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "leastactive";

    private final Random random = new Random();

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        int length = invokers.size(); // Number of invokers 总个数
        int leastActive = -1; // The least active value of all invokers 最小的活跃数
        int leastCount = 0; // The number of invokers having the same least active value (leastActive) 相同最小活跃数的个数
        int[] leastIndexs = new int[length]; // The index of invokers having the same least active value (leastActive) 相同最小活跃数的下标
        int totalWeight = 0; // The sum of weights 总权重
        int firstWeight = 0; // Initial value, used for comparision 第一个权重，用于计算是否相同
        boolean sameWeight = true; // Every invoker has the same weight value? 是否所有权重相同
        for (int i = 0; i < length; i++) {
            Invoker<T> invoker = invokers.get(i);
            // 活跃数从 RpcStatus 中取得， dubbo 统计活跃数时以方法为维度
            int active = RpcStatus.getStatus(invoker.getUrl(), invocation.getMethodName()).getActive(); // Active number
            int weight = invoker.getUrl().getMethodParameter(invocation.getMethodName(), Constants.WEIGHT_KEY, Constants.DEFAULT_WEIGHT); // Weight
            // 发现最小的活跃数，重新开始
            if (leastActive == -1 || active < leastActive) { // Restart, when find a invoker having smaller least active value.
                leastActive = active; // Record the current least active value 记录最小活跃数
                leastCount = 1; // Reset leastCount, count again based on current leastCount 重新统计相同最小活跃数的个数
                leastIndexs[0] = i; // Reset 重新记录最小活跃数的小标
                totalWeight = weight; // Reset 重新累计总权重
                firstWeight = weight; // Record the weight the first invoker 记录第一个权中
                sameWeight = true; // Reset, every invoker has the same weight value? 还原权重标示
            } else if (active == leastActive) { // If current invoker's active value equals with leaseActive, then accumulating.
                leastIndexs[leastCount++] = i; // Record index number of this invoker 记录下标
                totalWeight += weight; // Add this invoker's weight to totalWeight. 累计总权重
                // If every invoker has the same weight?
                // 判断权重是否一致
                if (sameWeight && i > 0
                        && weight != firstWeight) {
                    sameWeight = false;
                }
            }
        }
        // assert(leastCount > 0)
        if (leastCount == 1) {
            // If we got exactly one invoker having the least active value, return this invoker directly.
            // 如果只有一个最小值直接返回
            return invokers.get(leastIndexs[0]);
        }
        // 如果权重不相同且权重大于 0 则按总权重数随机
        if (!sameWeight && totalWeight > 0) {
            // If (not every invoker has the same weight & at least one invoker's weight>0), select randomly based on totalWeight
            int offsetWeight = random.nextInt(totalWeight);
            // Return a invoker based on the random value.
            // 确定随机值落在哪个片段上
            for (int i = 0; i < leastCount; i++) {
                int leastIndex = leastIndexs[i];
                offsetWeight -= getWeight(invokers.get(leastIndex), invocation);
                if (offsetWeight <= 0)
                    return invokers.get(leastIndex);
            }
        }
        // If all invokers have the same weight value or totalWeight=0, return evenly.
        return invokers.get(leastIndexs[random.nextInt(leastCount)]);
    }
}
