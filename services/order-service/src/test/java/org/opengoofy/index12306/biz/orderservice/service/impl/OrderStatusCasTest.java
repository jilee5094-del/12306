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

package org.opengoofy.index12306.biz.orderservice.service.impl;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.TableInfoHelper;
import org.apache.ibatis.builder.MapperBuilderAssistant;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opengoofy.index12306.biz.orderservice.common.enums.OrderItemStatusEnum;
import org.opengoofy.index12306.biz.orderservice.common.enums.OrderStatusEnum;
import org.opengoofy.index12306.biz.orderservice.dao.entity.OrderDO;
import org.opengoofy.index12306.biz.orderservice.dao.entity.OrderItemDO;
import org.opengoofy.index12306.biz.orderservice.dao.mapper.OrderItemMapper;
import org.opengoofy.index12306.biz.orderservice.dao.mapper.OrderMapper;
import org.opengoofy.index12306.biz.orderservice.dto.domain.OrderStatusReversalDTO;
import org.opengoofy.index12306.biz.orderservice.dto.req.CancelTicketOrderReqDTO;
import org.opengoofy.index12306.framework.starter.convention.exception.ServiceException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 订单状态 CAS：所有从"待支付"出发的迁移都必须带 status = 待支付 条件，抢不到的一方不得做后续动作
 */
class OrderStatusCasTest {

    private static final String ORDER_SN = "749436893241472588352";

    private OrderMapper orderMapper;
    private OrderItemMapper orderItemMapper;
    private OrderServiceImpl orderService;

    @BeforeAll
    static void initTableInfo() {
        // Lambda 条件构造需要实体元数据，单元测试中手动初始化
        MapperBuilderAssistant assistant = new MapperBuilderAssistant(new MybatisConfiguration(), "");
        TableInfoHelper.initTableInfo(assistant, OrderDO.class);
        TableInfoHelper.initTableInfo(assistant, OrderItemDO.class);
    }

    @BeforeEach
    void setUp() {
        orderMapper = mock(OrderMapper.class);
        orderItemMapper = mock(OrderItemMapper.class);
        orderService = new OrderServiceImpl(orderMapper, orderItemMapper, null, null, null, null);
    }

    private static CancelTicketOrderReqDTO cancelReq() {
        CancelTicketOrderReqDTO requestParam = new CancelTicketOrderReqDTO();
        requestParam.setOrderSn(ORDER_SN);
        return requestParam;
    }

    private static OrderDO orderWithStatus(int status) {
        OrderDO orderDO = new OrderDO();
        orderDO.setStatus(status);
        return orderDO;
    }

    @SuppressWarnings("unchecked")
    @Test
    void closeOrder_updateCarriesPendingStatusCondition() {
        when(orderMapper.update(any(), any())).thenReturn(1);
        when(orderItemMapper.update(any(), any())).thenReturn(1);

        assertTrue(orderService.closeTickOrder(cancelReq()));

        ArgumentCaptor<Wrapper<OrderDO>> captor = ArgumentCaptor.forClass(Wrapper.class);
        verify(orderMapper).update(any(), captor.capture());
        LambdaUpdateWrapper<OrderDO> wrapper = (LambdaUpdateWrapper<OrderDO>) captor.getValue();
        String sql = wrapper.getSqlSegment();
        assertTrue(sql.contains("order_sn") && sql.contains("status"), "UPDATE 必须同时带订单号与状态条件: " + sql);
        assertTrue(wrapper.getParamNameValuePairs().containsValue(OrderStatusEnum.PENDING_PAYMENT.getStatus()));
    }

    @Test
    void closeOrder_whenAlreadyPaidOrClosed_returnsFalseAndSkipsItems() {
        when(orderMapper.update(any(), any())).thenReturn(0);

        assertFalse(orderService.closeTickOrder(cancelReq()));
        verify(orderItemMapper, never()).update(any(), any());
    }

    @Test
    void cancelOrder_loserOfConcurrentCancel_fails() {
        when(orderMapper.update(any(), any())).thenReturn(0);
        when(orderMapper.selectOne(any())).thenReturn(orderWithStatus(OrderStatusEnum.CLOSED.getStatus()));

        assertThrows(ServiceException.class, () -> orderService.cancelTickOrder(cancelReq()));
        verify(orderItemMapper, never()).update(any(), any());
    }

    @Test
    void cancelOrder_unknownOrder_fails() {
        when(orderMapper.update(any(), any())).thenReturn(0);
        when(orderMapper.selectOne(any())).thenReturn(null);

        assertThrows(ServiceException.class, () -> orderService.cancelTickOrder(cancelReq()));
    }

    @Test
    void cancelOrder_itemsNotUpdated_rollsBackByThrowing() {
        when(orderMapper.update(any(), any())).thenReturn(1);
        when(orderItemMapper.update(any(), any())).thenReturn(0);

        assertThrows(ServiceException.class, () -> orderService.cancelTickOrder(cancelReq()));
    }

    private static OrderStatusReversalDTO paid() {
        return OrderStatusReversalDTO.builder()
                .orderSn(ORDER_SN)
                .orderStatus(OrderStatusEnum.ALREADY_PAID.getStatus())
                .orderItemStatus(OrderItemStatusEnum.ALREADY_PAID.getStatus())
                .build();
    }

    @Test
    void payCallback_duplicateMessage_isIdempotent() {
        when(orderMapper.update(any(), any())).thenReturn(0);
        when(orderMapper.selectOne(any())).thenReturn(orderWithStatus(OrderStatusEnum.ALREADY_PAID.getStatus()));

        assertDoesNotThrow(() -> orderService.statusReversal(paid()));
        verify(orderItemMapper, never()).update(any(), any());
    }

    @Test
    void payCallback_afterOrderClosed_doesNotReopenOrder() {
        // 关单先赢：支付回调 CAS 失败，订单保持已关闭，不抛异常以免 MQ 无限重试
        when(orderMapper.update(any(), any())).thenReturn(0);
        when(orderMapper.selectOne(any())).thenReturn(orderWithStatus(OrderStatusEnum.CLOSED.getStatus()));

        assertDoesNotThrow(() -> orderService.statusReversal(paid()));
        verify(orderMapper, never()).updateById(any(OrderDO.class));
        verify(orderItemMapper, never()).update(any(), any());
    }
}
