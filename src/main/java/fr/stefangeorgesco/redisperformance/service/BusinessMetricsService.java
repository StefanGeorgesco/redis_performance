package fr.stefangeorgesco.redisperformance.service;

import org.redisson.api.RScoredSortedSetReactive;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.client.codec.IntegerCodec;
import org.redisson.client.protocol.ScoredEntry;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/*
    This should be in a separate microservice, but for the sake of simplicity, we will keep it here.
 */

@Service
public class BusinessMetricsService {

    private final RedissonReactiveClient client;

    public BusinessMetricsService(RedissonReactiveClient client) {
        this.client = client;
    }

    public Mono<Map<Integer, Double>> top3Products() {
        String dateFormat = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDate.now(ZoneId.systemDefault()));
        RScoredSortedSetReactive<Integer> set = client
                .getScoredSortedSet("product:visit:" + dateFormat, IntegerCodec.INSTANCE);
        return set.entryRangeReversed(0, 2)
                .map(scoredEntries -> scoredEntries.stream()
                        .collect(
                                Collectors.toMap(
                                        ScoredEntry::getValue,
                                        ScoredEntry::getScore,
                                        (score1, score2) -> score1,
                                        LinkedHashMap::new
                                )
                        ));
    }
}
