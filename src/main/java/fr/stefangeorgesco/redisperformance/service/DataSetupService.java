package fr.stefangeorgesco.redisperformance.service;

import fr.stefangeorgesco.redisperformance.entity.Product;
import fr.stefangeorgesco.redisperformance.repository.ProductRepository;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.io.Resource;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class DataSetupService implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(DataSetupService.class);

    private final ProductRepository repository;
    private final R2dbcEntityTemplate template;

    @Value("classpath:schema.sql")
    private Resource resource;

    public DataSetupService(ProductRepository repository, R2dbcEntityTemplate template) {
        this.repository = repository;
        this.template = template;
    }

    @Override
    public void run(String @NonNull ... args) throws Exception {
        String schemaCreationQuery = StreamUtils.copyToString(resource.getInputStream(), StandardCharsets.UTF_8);

        Mono<Void> insertMono = Flux.range(1, 1000)
                .map(i -> new Product(null, "Product-" + i,
                        Math.round(ThreadLocalRandom.current()
                                .nextDouble(1.0, 100.0) * 100.0) / 100.0))
                .collectList()
                .flatMapMany(repository::saveAll)
                .then();

        template.getDatabaseClient()
                .sql(schemaCreationQuery)
                .then()
                .then(insertMono)
                .doFinally(signalType -> log.info("Data setup completed with signal: {}", signalType))
                .subscribe();
    }
}
