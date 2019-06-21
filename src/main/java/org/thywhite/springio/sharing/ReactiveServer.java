/*
 * Copyright (c) 2019 LINE Corporation. All rights Reserved.
 * LINE Corporation PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package org.thywhite.springio.sharing;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * @author byeongguk.gim <byeongguk.gim@linecorp.com>
 */
public class ReactiveServer {

    public void start(int port) {
        Flux<Channel> channelFlux = Flux.<Channel>create(sink -> {
            NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
            NioEventLoopGroup workerGroup = new NioEventLoopGroup();

            new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            // Flux 에 채널을 계속 공급
                            sink.next(ch);
                        }

                        @Override
                        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                            sink.complete();
                        }
                    })
                    .bind(port);
        }).log();

        Flux<Tuple2<Channel, ByteBuf>> channelAndReceivedFlux = channelFlux.flatMap(channel -> Flux.create(sink -> {
            channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                @Override
                public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                    // Flux 에 채널이랑, 읽은 것을 계속 공급
                    sink.next(Tuples.of(ctx.channel(), (ByteBuf)msg));
                }

                @Override
                public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                    // 연결이 끊어지면, 더 이상 보낼 것이 없으므로 완료 처리
                    sink.complete();
                }
            });
        }));

        channelAndReceivedFlux.subscribe(new Subscriber<Tuple2<Channel, ByteBuf>>() {
            @Override
            public void onSubscribe(Subscription s) {
                // 계속 처리할 것이므로 최고값 요청
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(Tuple2<Channel, ByteBuf> tuple) {
                Channel channel = tuple.getT1();
                ByteBuf received = tuple.getT2();
                channel.writeAndFlush(received);
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onComplete() {
            }
        });
    }

    /*
     * 무쟈게 복잡해서 블로그에는 생략.. 그냥 관심 있으면 재미 삼아 보세요.
     */
    public void reactiveStreamsVersion(int port) throws IOException, InterruptedException {
        Publisher<Channel> socketChannelPublisher = createChannelPublisher(port);
        Publisher<Channel> subscribeOnChannelPublisher = subscribeOn(socketChannelPublisher, Executors.newSingleThreadExecutor());
        Publisher<Channel> publishOnChannelPublisher = publishOn(subscribeOnChannelPublisher, Executors.newCachedThreadPool());
        Publisher<Tuple2<Channel, ByteBuf>> channelAndDataPublisher = flatMap(publishOnChannelPublisher, this::readChannelData);
        channelAndDataPublisher.subscribe(createEchoSubscriber());
    }

    private Publisher<Channel> createChannelPublisher(int port) throws InterruptedException {
        final NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
        final NioEventLoopGroup workerGroup = new NioEventLoopGroup();

        BlockingQueue<SocketChannel> channelQueue = new LinkedBlockingQueue<>();

        new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        channelQueue.add(ch);
                    }
                })
                .bind(port).sync();

        return new Publisher<Channel>() {
            @Override
            public void subscribe(Subscriber<? super Channel> s) {
                s.onSubscribe(new Subscription() {
                    volatile boolean canceled = false;

                    @Override
                    public void request(long n) {
                        for (int i = 0; i < n; i++) {
                            if (canceled) {
                                return;
                            }
                            try {
                                final SocketChannel channel = channelQueue.take();
                                s.onNext(channel);
                            } catch (InterruptedException e) {
                                s.onError(e);
                                return;
                            }
                        }
                        s.onComplete();
                    }

                    @Override
                    public void cancel() {
                        canceled = true;
                    }
                });
            }
        };
    }

    private Publisher<Tuple2<Channel, ByteBuf>> readChannelData(Channel channel) {
        BlockingQueue<ByteBuf> byteBufQueue = new LinkedBlockingQueue<>();
        channel.pipeline()
                .addLast(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                        byteBufQueue.add((ByteBuf)msg);
                    }
                });

        return new Publisher<Tuple2<Channel, ByteBuf>>() {
            @Override
            public void subscribe(Subscriber<? super Tuple2<Channel, ByteBuf>> s) {
                s.onSubscribe(new Subscription() {
                    @Override
                    public void request(long n) {
                        for (int i = 0; i < n; i++) {
                            try {
                                final ByteBuf byteBuf = byteBufQueue.take();
                                s.onNext(Tuples.of(channel, byteBuf));
                            } catch (InterruptedException e) {
                                s.onError(e);
                            }
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                });
            }
        };
    }

    private Subscriber<? super Tuple2<Channel, ByteBuf>> createEchoSubscriber() {
        return new Subscriber<Tuple2<Channel, ByteBuf>>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(Tuple2<Channel, ByteBuf> objects) {
                ByteBuf received = objects.getT2();
                Channel channel = objects.getT1();

                channel.writeAndFlush(received);
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onComplete() {

            }
        };
    }

    private Subscriber<? super SocketChannel> createSocketChannelSubscriber() {
        return new Subscriber<SocketChannel>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(SocketChannel socketChannel) {
                socketChannel.pipeline().addLast(new NettyServer.EchoHandler());
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onComplete() {

            }
        };
    }

    /**
     * publish 를 지정한 threadPool 에서 수행한다.
     * 대상 메소드 : onNext, onError, onComplete
     */
    private <T> Publisher<T> publishOn(Publisher<T> publisher, ExecutorService threadPool) {
        return new Publisher<T>() {
            @Override
            public void subscribe(Subscriber<? super T> s) {
                publisher.subscribe(new Subscriber<T>() {
                    @Override
                    public void onSubscribe(Subscription subscription) {
                        s.onSubscribe(subscription);
                    }

                    @Override
                    public void onNext(T t) {
                        threadPool.submit(() -> s.onNext(t));
                    }

                    @Override
                    public void onError(Throwable t) {
                        threadPool.submit(() -> s.onError(t));
                    }

                    @Override
                    public void onComplete() {
                        threadPool.submit(s::onComplete);
                    }
                });
            }
        };
    }

    /**
     * subscribe 를 지정한 threadPool 을 사용해 실행한다.
     * 대상 메소드 : subscribe, onSubscribe, request
     */
    private <T> Publisher<T> subscribeOn(Publisher<T> publisher, ExecutorService threadPool) {
        return new Publisher<T>() {
            @Override
            public void subscribe(Subscriber<? super T> s) {
                threadPool.submit(() -> {
                    publisher.subscribe(new Subscriber<T>() {
                        @Override
                        public void onSubscribe(Subscription subscription) {
                            threadPool.submit(() ->
                                    s.onSubscribe(new Subscription() {
                                        @Override
                                        public void request(long n) {
                                            threadPool.submit(() -> subscription.request(n));
                                        }

                                        @Override
                                        public void cancel() {
                                            subscription.cancel();
                                        }
                                    }));
                        }

                        @Override
                        public void onNext(T t) {
                            s.onNext(t);
                        }

                        @Override
                        public void onError(Throwable t) {
                            s.onError(t);
                        }

                        @Override
                        public void onComplete() {
                            s.onComplete();
                        }
                    });
                });
            }
        };
    }

    private <T, R> Publisher<R> map(Publisher<T> publisher, Function<T, R> mapFn) {
        return new Publisher<R>() {
            @Override
            public void subscribe(Subscriber<? super R> s) {
                publisher.subscribe(new Subscriber<T>() {
                    @Override
                    public void onSubscribe(Subscription subscription) {
                        s.onSubscribe(subscription);
                    }

                    @Override
                    public void onNext(T t) {
                        s.onNext(mapFn.apply(t));
                    }

                    @Override
                    public void onError(Throwable t) {
                        s.onError(t);
                    }

                    @Override
                    public void onComplete() {
                        s.onComplete();
                    }
                });
            }
        };
    }

    /*
     * 간결함을 위해, 모든 데이터를 버퍼링 + 예외 처리를 생략하였음, reactor 에 있는 flatMap 은 괜찮음~
     * 참고 : 제대로 된 flatMap 구현 - https://github.com/reactor/reactive-streams-commons/blob/master/src/main/java/rsc/publisher/PublisherFlatMap.java
     */
    private <T, R> Publisher<R> flatMap(Publisher<T> publisher, Function<T, Publisher<R>> flatMapFn) {
        BlockingQueue<R> buffer = new LinkedBlockingQueue<>();

        publisher.subscribe(new Subscriber<T>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(T t) {
                final Publisher<R> rPublisher = flatMapFn.apply(t);
                rPublisher.subscribe(new Subscriber<R>() {
                    @Override
                    public void onSubscribe(Subscription s) {
                        s.request(Long.MAX_VALUE);
                    }

                    @Override
                    public void onNext(R r) {
                        buffer.add(r);
                    }

                    @Override
                    public void onError(Throwable t) {

                    }

                    @Override
                    public void onComplete() {

                    }
                });
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onComplete() {

            }
        });

        return new Publisher<R>() {
            @Override
            public void subscribe(Subscriber<? super R> s) {
                s.onSubscribe(new Subscription() {
                    private volatile boolean canceled;

                    @Override
                    public void request(long n) {
                        for (int i = 0; i < n; i++) {
                            if (canceled) {
                                return;
                            }
                            try {
                                final R r = buffer.take();
                                s.onNext(r);
                            } catch (InterruptedException e) {
                                s.onError(e);
                                return;
                            }
                        }
                        s.onComplete();
                    }

                    @Override
                    public void cancel() {
                        canceled = true;
                    }
                });
            }
        };
    }
}
