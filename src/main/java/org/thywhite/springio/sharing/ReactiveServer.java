/*
 * Copyright (c) 2019 LINE Corporation. All rights Reserved.
 * LINE Corporation PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package org.thywhite.springio.sharing;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import lombok.SneakyThrows;

/**
 * @author byeongguk.gim <byeongguk.gim@linecorp.com>
 */
public class ReactiveServer {

    public void start(int port) throws IOException {
        Publisher<SocketChannel> publisher = createSocketPublisher(port);

        publisher.subscribe(new Subscriber<SocketChannel>() {
            private final int nThreads = 2;
            private final ExecutorService publishOnExecutor = Executors.newFixedThreadPool(nThreads);
            private final ExecutorService subscribeOnExecutor = Executors.newSingleThreadExecutor();

            private final List<EchoProcessor> echoProcessors = new ArrayList<>();
            private Iterator<EchoProcessor> echoProcessorIterator;

            @SneakyThrows
            @Override
            public void onSubscribe(Subscription s) {
                for (int i = 0; i < nThreads; i++) {
                    final EchoProcessor echoProcessor = new EchoProcessor();
                    echoProcessors.add(echoProcessor);
                    publishOnExecutor.submit(echoProcessor);
                }

                echoProcessorIterator = echoProcessors.iterator();
                subscribeOnExecutor.submit(() -> s.request(Long.MAX_VALUE));
            }

            @Override
            public void onNext(SocketChannel channel) {
                if (!echoProcessorIterator.hasNext()) {
                    echoProcessorIterator = echoProcessors.iterator();
                }

                echoProcessorIterator.next().register(channel);
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onComplete() {

            }
        });
    }

    private Publisher<SocketChannel> createSocketPublisher(int port) throws IOException {
        Selector acceptSelector = Selector.open();
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();

        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.bind(new InetSocketAddress(port));

        serverSocketChannel.register(acceptSelector, SelectionKey.OP_ACCEPT);

        return new Publisher<SocketChannel>() {
            @Override
            public void subscribe(Subscriber<? super SocketChannel> s) {
                s.onSubscribe(new Subscription() {
                    @SneakyThrows
                    @Override
                    public void request(long n) {

                        while (acceptSelector.select() > 0) {
                            final Iterator<SelectionKey> keyIterator =
                                    acceptSelector.selectedKeys().iterator();

                            while (keyIterator.hasNext()) {
                                final SelectionKey key = keyIterator.next();
                                keyIterator.remove();

                                final ServerSocketChannel serverSocketChannel =
                                        (ServerSocketChannel)key.channel();

                                final SocketChannel socketChannel = serverSocketChannel.accept();

                                socketChannel.configureBlocking(false);
                                s.onNext(socketChannel);
                            }
                        }
                    }

                    @SneakyThrows
                    @Override
                    public void cancel() {
                        acceptSelector.close();
                    }
                });
            }
        };
    }

    private static class EchoProcessor implements Runnable {
        private final Selector selector;
        private final BlockingQueue<SocketChannel> channelBlockingQueue = new LinkedBlockingQueue<>();

        private EchoProcessor() throws IOException {
            selector = Selector.open();
        }

        @SuppressWarnings("Duplicates")
        @SneakyThrows
        @Override
        public void run() {
            for (; ; ) {
                SocketChannel newChannel;
                if ((newChannel = channelBlockingQueue.poll()) != null) {
                    newChannel.register(selector, SelectionKey.OP_READ);
                }

                if (selector.select(100) > 0) {
                    final Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();

                    while (keyIterator.hasNext()) {
                        final SelectionKey key = keyIterator.next();
                        keyIterator.remove();

                        SocketChannel channel = (SocketChannel)key.channel();
                        ByteBuffer buffer = ByteBuffer.allocate(2048);
                        final int bytesRead = channel.read(buffer);

                        if (bytesRead == -1) {
                            channel.close();
                            key.cancel();
                            continue;
                        } else {
                            buffer.flip();
                            channel.write(buffer);
                        }
                    }
                }
            }
        }

        @SneakyThrows
        public void register(SocketChannel channel) {
            channelBlockingQueue.add(channel);
        }
    }

}
