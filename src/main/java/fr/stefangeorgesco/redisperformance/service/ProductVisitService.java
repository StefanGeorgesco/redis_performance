package fr.stefangeorgesco.redisperformance.service;

import jakarta.annotation.PostConstruct;
import org.redisson.api.BatchOptions;
import org.redisson.api.RBatchReactive;
import org.redisson.api.RScoredSortedSetReactive;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.client.codec.IntegerCodec;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class ProductVisitService {

    private final RedissonReactiveClient client;
    private final Sinks.Many<Integer> visitSink;

    public ProductVisitService(RedissonReactiveClient client) {
        this.client = client;
        this.visitSink = Sinks.many().unicast().onBackpressureBuffer();
    }

    public void addVisit(int productId) {
        visitSink.tryEmitNext(productId);
    }

    @PostConstruct
    private void init() {
        visitSink.asFlux()
                .buffer(Duration.ofSeconds(3))
                .map(integers -> integers.stream().collect(
                        Collectors.groupingBy(
                                Function.identity(),
                                Collectors.counting()
                        )
                ))
                .flatMap(this::updateBatch)
                .subscribe();
    }

    private Mono<Void> updateBatch(Map<Integer, Long> visitCounts) {
        RBatchReactive batch = client.createBatch(BatchOptions.defaults());
        String dateFormat = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDate.now(ZoneId.systemDefault()));
        RScoredSortedSetReactive<Integer> set = batch
                .getScoredSortedSet("product:visit:" + dateFormat, IntegerCodec.INSTANCE);
        return Flux.fromIterable(visitCounts.entrySet())
                .map(entry -> set.addScore(entry.getKey(), entry.getValue()))
                .then(batch.execute())
                .then();
    }
}
