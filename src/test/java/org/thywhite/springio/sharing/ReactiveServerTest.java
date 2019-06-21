/*
 * Copyright (c) 2019 LINE Corporation. All rights Reserved.
 * LINE Corporation PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package org.thywhite.springio.sharing;

import java.util.concurrent.CompletableFuture;

import org.junit.Test;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * @author byeongguk.gim <byeongguk.gim@linecorp.com>
 */
public class ReactiveServerTest {

    @Test
    public void testFlux() throws InterruptedException {

        final Flux<Integer> flux = Flux.<Integer>create(sink -> {
            System.out.println("Create function");
            sink.onRequest(n -> {
                for (int i = 0; i < 20; i++) {
                    try {
                        Thread.sleep(100L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    sink.next(i);
                }
            });
        }).log().subscribeOn(Schedulers.single()).publishOn(Schedulers.newParallel("exporter"));

        flux.map(i -> {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.out.println("Map : " + i + " Thread - " + Thread.currentThread().getName());
            return i * 2;
        }).subscribe(i -> {
            System.out.println(Thread.currentThread().getName() + " : " + i);
            try {
                Thread.sleep(2000L);
                System.out.println(Thread.currentThread().getName() + " <> " + i);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        Thread.sleep(10000L);

    }

    @Test
    public void testFluxMap() {

        Flux.just("String", "Integer")
                .flatMap(string -> Mono.fromCompletionStage(CompletableFuture.completedFuture(string + '/' + string)))
                .subscribe(System.out::println);
    }
}