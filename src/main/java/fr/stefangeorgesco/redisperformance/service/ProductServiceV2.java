package fr.stefangeorgesco.redisperformance.service;

import fr.stefangeorgesco.redisperformance.entity.Product;
import fr.stefangeorgesco.redisperformance.service.util.CacheTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class ProductServiceV2 {

    private final CacheTemplate<Integer, Product> cacheTemplate;

    public ProductServiceV2(CacheTemplate<Integer, Product> cacheTemplate) {
        this.cacheTemplate = cacheTemplate;
    }

    public Mono<Product> getProduct(int id) {
        return cacheTemplate.get(id);
    }

    public Mono<Product> updateProduct(int id, Mono<Product> productMono) {
        return productMono
                .flatMap(product -> cacheTemplate.update(id, product));
    }

    public Mono<Void> deleteProduct(int id) {
        return cacheTemplate.delete(id);
    }
}
