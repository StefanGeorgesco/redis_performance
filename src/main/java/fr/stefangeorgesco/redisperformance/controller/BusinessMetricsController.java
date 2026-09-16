package fr.stefangeorgesco.redisperformance.controller;

import fr.stefangeorgesco.redisperformance.service.BusinessMetricsService;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Map;

/*
    This should be in a separate microservice, but for the sake of simplicity, we will keep it here.
 */

@RestController
@RequestMapping("product/metrics")
public class BusinessMetricsController {

    private final BusinessMetricsService service;

    public BusinessMetricsController(BusinessMetricsService service) {
        this.service = service;
    }

    @GetMapping(value = "top3", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Map<Integer, Double>> top3Products() {
        return service.top3Products()
                .repeatWhen(l -> Flux.interval(Duration.ofSeconds(3)));
    }
}
