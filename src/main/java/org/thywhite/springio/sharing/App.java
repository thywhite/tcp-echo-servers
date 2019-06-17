/*
 * Copyright (c) 2019 LINE Corporation. All rights Reserved.
 * LINE Corporation PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package org.thywhite.springio.sharing;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author byeongguk.gim <byeongguk.gim@linecorp.com>
 */
public class App {

    public static void main(String[] args) throws Exception {
        ExecutorService threadPool = Executors.newCachedThreadPool();

        threadPool.submit(() -> {
            new SingleServer().start(3030);
            return null;
        });
        threadPool.submit(() -> {
            new ThreadServer().start(3031);
            return null;
        });
        threadPool.submit(() -> {
            new NonblockingServer().start(3032);
            return null;
        });
        threadPool.submit(() -> {
            new ReactiveServer().start(3033);
            return null;
        });

        threadPool.shutdown();
    }

}
