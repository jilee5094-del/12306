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

package org.opengoofy.index12306.biz.orderservice.job;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opengoofy.index12306.biz.orderservice.common.enums.OrderStatusEnum;
import org.opengoofy.index12306.biz.orderservice.dao.entity.OrderDO;
import org.opengoofy.index12306.biz.orderservice.dao.entity.OrderItemDO;
import org.opengoofy.index12306.biz.orderservice.dao.mapper.OrderItemMapper;
import org.opengoofy.index12306.biz.orderservice.dao.mapper.OrderMapper;
import org.opengoofy.index12306.biz.orderservice.dto.req.TicketOrderItemCreateReqDTO;
import org.opengoofy.index12306.biz.orderservice.mq.event.DelayCloseOrderEvent;
import org.opengoofy.index12306.biz.orderservice.mq.produce.ReconcileCloseOrderSendProduce;
import org.opengoofy.index12306.framework.starter.common.toolkit.BeanUtil;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 超时订单对账补偿：延时关单消息丢失 / 进入死信 / 消费失败时，订单会一直停在待支付、座位一直锁定。
 * 定时扫描超时仍待支付的订单，重新立即投递关单消息，复用 ticket 服务的关单消费者释放座位与余票。
 * 补偿与原延时消息可能重复到达，由订单状态 CAS 保证座位只释放一次。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderTimeoutReconcileJob {

    private static final String LOCK_KEY = "index12306-order-service:lock:timeout-order-reconcile";

    private final OrderMapper orderMapper;
    private final OrderItemMapper orderItemMapper;
    private final ReconcileCloseOrderSendProduce reconcileCloseOrderSendProduce;
    private final RedissonClient redissonClient;

    @Value("${order.reconcile.enabled:true}")
    private boolean enabled;

    /**
     * 延时关单为 10 分钟，额外留 5 分钟宽限，避免与正常的延时消息频繁撞车
     */
    @Value("${order.reconcile.timeout-minutes:15}")
    private long timeoutMinutes;

    @Value("${order.reconcile.batch-size:100}")
    private int batchSize;

    @Scheduled(initialDelayString = "${order.reconcile.initial-delay-ms:30000}", fixedDelayString = "${order.reconcile.interval-ms:60000}")
    public void reconcile() {
        if (!enabled) {
            return;
        }
        // 多实例部署时只有一个实例执行本轮对账
        RLock lock = redissonClient.getLock(LOCK_KEY);
        if (!lock.tryLock()) {
            return;
        }
        try {
            Date deadline = new Date(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(timeoutMinutes));
            // 查询不带分片键，ShardingSphere 会广播到全部分表后归并；依赖各分表 (status, order_time) 联合索引
            List<OrderDO> timeoutOrders = orderMapper.selectList(Wrappers.lambdaQuery(OrderDO.class)
                    .eq(OrderDO::getStatus, OrderStatusEnum.PENDING_PAYMENT.getStatus())
                    .lt(OrderDO::getOrderTime, deadline)
                    .orderByAsc(OrderDO::getOrderTime)
                    .last("LIMIT " + batchSize));
            if (timeoutOrders.isEmpty()) {
                return;
            }
            int sent = 0;
            for (OrderDO each : timeoutOrders) {
                try {
                    reconcileCloseOrderSendProduce.sendMessage(buildCloseEvent(each));
                    sent++;
                } catch (Throwable ex) {
                    log.error("[超时订单对账] 订单号：{} 投递关单消息失败，下一轮重试", each.getOrderSn(), ex);
                }
            }
            log.warn("[超时订单对账] 发现超时未关闭订单 {} 笔，已投递补偿关单 {} 笔", timeoutOrders.size(), sent);
        } finally {
            lock.unlock();
        }
    }

    private DelayCloseOrderEvent buildCloseEvent(OrderDO orderDO) {
        List<OrderItemDO> orderItems = orderItemMapper.selectList(Wrappers.lambdaQuery(OrderItemDO.class)
                .eq(OrderItemDO::getOrderSn, orderDO.getOrderSn()));
        return DelayCloseOrderEvent.builder()
                .orderSn(orderDO.getOrderSn())
                .trainId(String.valueOf(orderDO.getTrainId()))
                .departure(orderDO.getDeparture())
                .arrival(orderDO.getArrival())
                .trainPurchaseTicketResults(BeanUtil.convert(orderItems, TicketOrderItemCreateReqDTO.class))
                .build();
    }
}
