/*
 * Copyright (c) 2019 LINE Corporation. All rights Reserved.
 * LINE Corporation PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package org.thywhite.springio.sharing;

import org.reactivestreams.Publisher;

import io.netty.buffer.ByteBuf;
import reactor.netty.ByteBufFlux;
import reactor.netty.NettyOutbound;
import reactor.netty.tcp.TcpServer;

/**
 * @author byeongguk.gim <byeongguk.gim@linecorp.com>
 */
public class ReactorNettyServer {

    public void start(int port) {
        TcpServer.create()
                .port(port)
                .handle((nettyInbound, nettyOutbound) -> {
                    ByteBufFlux receive = nettyInbound.receive();
                    NettyOutbound send = nettyOutbound.send((Publisher<ByteBuf>)receive.retain());
                    return (Publisher<Void>)send;
                })
                // 1 줄로도 가능
                //.handle((nettyInbound, nettyOutbound) -> nettyOutbound.send(nettyInbound.receive().retain()))
                .bindNow();
    }
}
