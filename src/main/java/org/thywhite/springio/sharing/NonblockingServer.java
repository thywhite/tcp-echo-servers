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
import java.util.Iterator;

/**
 * @author byeongguk.gim <byeongguk.gim@linecorp.com>
 */
public class NonblockingServer {

    @SuppressWarnings("Duplicates")
    public void start(int port) throws IOException {
        Selector selector = Selector.open();
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();

        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.bind(new InetSocketAddress(port));

        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

        while (!Thread.currentThread().isInterrupted() && selector.select() > 0) {
            final Iterator<SelectionKey> selectionKeyIterator = selector.selectedKeys().iterator();

            while (selectionKeyIterator.hasNext()) {
                final SelectionKey selectionKey = selectionKeyIterator.next();
                selectionKeyIterator.remove();

                if (!selectionKey.isValid()) {
                    continue;
                }
                if (selectionKey.isAcceptable()) {
                    ServerSocketChannel selectedChannel = (ServerSocketChannel)selectionKey.channel();

                    final SocketChannel socketChannel = selectedChannel.accept();
                    if (socketChannel == null) {
                        continue;
                    }
                    socketChannel.configureBlocking(false);
                    socketChannel.register(selector, SelectionKey.OP_READ);
                } else if (selectionKey.isReadable()) {
                    SocketChannel channel = (SocketChannel)selectionKey.channel();
                    final ByteBuffer buffer = ByteBuffer.allocate(2048);

                    final int bytesRead = channel.read(buffer);
                    if (bytesRead == -1) {
                        channel.close();
                        selectionKey.cancel();
                        continue;
                    } else {
                        buffer.flip();
                        channel.write(buffer);
                    }
                }
            }
        }

        selector.close();
    }
}
