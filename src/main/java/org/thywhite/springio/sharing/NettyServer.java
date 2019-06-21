/*
 * Copyright (c) 2019 LINE Corporation. All rights Reserved.
 * LINE Corporation PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package org.thywhite.springio.sharing;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * @author byeongguk.gim <byeongguk.gim@linecorp.com>
 */
public class NettyServer {

    public void start(int port) throws InterruptedException {
        final NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
        final NioEventLoopGroup workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors());

        new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new EchoHandler());
                    }
                })
                .bind(port)
                .channel().closeFuture().sync();
    }

    static class EchoHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            ctx.channel().writeAndFlush(msg);
        }
    }

}
