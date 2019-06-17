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

/**
 * @author byeongguk.gim <byeongguk.gim@linecorp.com>
 */
public class SingleServer {

    @SuppressWarnings("Duplicates")
    public void start(int port) throws IOException {
        final ServerSocket serverSocket = new ServerSocket(port);

        while (!Thread.currentThread().isInterrupted()) {
            final Socket socket = serverSocket.accept();

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
        }
    }

}
