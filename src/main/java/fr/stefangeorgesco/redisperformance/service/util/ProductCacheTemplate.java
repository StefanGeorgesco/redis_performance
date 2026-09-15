package fr.stefangeorgesco.redisperformance.service.util;

import fr.stefangeorgesco.redisperformance.entity.Product;
import fr.stefangeorgesco.redisperformance.repository.ProductRepository;
import org.redisson.api.RMapReactive;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.codec.TypedJsonJacksonCodec;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class ProductCacheTemplate extends CacheTemplate<Integer, Product> {

    private final ProductRepository repository;
    private final RMapReactive<Integer, Product> cache;

    public ProductCacheTemplate(ProductRepository repository, RedissonReactiveClient client) {
        this.repository = repository;
        this.cache = client.getMap("product", new TypedJsonJacksonCodec(Integer.class, Product.class));
    }

    @Override
    protected Mono<Product> getFromSource(Integer id) {
        return repository.findById(id);
    }

    @Override
    protected Mono<Product> getFromCache(Integer id) {
        return cache.get(id);
    }

    @Override
    protected Mono<Product> updateSource(Integer id, Product product) {
        return repository.findById(id)
                .doOnNext(p -> product.setId(id))
                .flatMap(p -> repository.save(product));
    }

    @Override
    protected Mono<Product> updateCache(Integer id, Product product) {
        return cache.fastPut(id, product).thenReturn(product);
    }

    @Override
    protected Mono<Void> deleteFromSource(Integer id) {
        return repository.deleteById(id);
    }

    @Override
    protected Mono<Void> deleteFromCache(Integer id) {
        return cache.remove(id).then();
    }
}
