/*
 * Copyright (c) 2019 LINE Corporation. All rights Reserved.
 * LINE Corporation PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package org.thywhite.springio.sharing;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author byeongguk.gim <byeongguk.gim@linecorp.com>
 */
public class ThreadServer {

    @SuppressWarnings("Duplicates")
    public void start(int port) throws IOException {
        ExecutorService threadPool = Executors.newFixedThreadPool(2);
        ServerSocket serverSocket = new ServerSocket(port);

        while (!Thread.currentThread().isInterrupted()) {
            final Socket socket = serverSocket.accept();
            threadPool.submit(() -> {
                final InputStream is = socket.getInputStream();

                byte[] buffer = new byte[2048];

                while (true) {
                    final int bytesRead = is.read(buffer);

                    if (bytesRead == -1) {
                        break;
                    }

                    final OutputStream os = socket.getOutputStream();
                    os.write(buffer, 0, bytesRead);
                    os.flush();
                }
                return null;
            });
        }

        threadPool.shutdownNow();
    }
}
