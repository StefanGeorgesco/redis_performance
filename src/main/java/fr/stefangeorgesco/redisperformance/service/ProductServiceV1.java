package fr.stefangeorgesco.redisperformance.service;

import fr.stefangeorgesco.redisperformance.entity.Product;
import fr.stefangeorgesco.redisperformance.repository.ProductRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class ProductServiceV1 {

    private final ProductRepository repository;

    public ProductServiceV1(ProductRepository repository) {
        this.repository = repository;
    }

    public Mono<Product> getProduct(int id) {
        return repository.findById(id);
    }

    public Mono<Product> updateProduct(int id, Mono<Product> productMono) {
        return repository.findById(id)
                .flatMap(p -> productMono.doOnNext(pr -> pr.setId(id)))
                .flatMap(repository::save);
    }
}
