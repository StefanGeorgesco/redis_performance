package fr.stefangeorgesco.redisperformance.service;

import fr.stefangeorgesco.redisperformance.entity.Product;
import fr.stefangeorgesco.redisperformance.service.util.CacheTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class ProductServiceV3 {

    private final CacheTemplate<Integer, Product> productLocalCacheTemplate;

    public ProductServiceV3(CacheTemplate<Integer, Product> productLocalCacheTemplate) {
        this.productLocalCacheTemplate = productLocalCacheTemplate;
    }

    public Mono<Product> getProduct(int id) {
        return productLocalCacheTemplate.get(id);
    }

    public Mono<Product> updateProduct(int id, Mono<Product> productMono) {
        return productMono
                .flatMap(product -> productLocalCacheTemplate.update(id, product));
    }

    public Mono<Void> deleteProduct(int id) {
        return productLocalCacheTemplate.delete(id);
    }
}
