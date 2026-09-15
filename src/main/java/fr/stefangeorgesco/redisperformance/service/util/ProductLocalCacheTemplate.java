package fr.stefangeorgesco.redisperformance.service.util;

import fr.stefangeorgesco.redisperformance.entity.Product;
import fr.stefangeorgesco.redisperformance.repository.ProductRepository;
import org.redisson.api.RLocalCachedMap;
import org.redisson.api.RedissonClient;
import org.redisson.api.options.LocalCachedMapOptions;
import org.redisson.codec.TypedJsonJacksonCodec;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class ProductLocalCacheTemplate extends CacheTemplate<Integer, Product> {

    private final ProductRepository repository;
    private final RLocalCachedMap<Integer, Product> cache;

    public ProductLocalCacheTemplate(ProductRepository repository, RedissonClient client) {
        this.repository = repository;
        LocalCachedMapOptions<Integer, Product> options = LocalCachedMapOptions.<Integer, Product>name("product-local")
                .codec(new TypedJsonJacksonCodec(Integer.class, Product.class))
                .syncStrategy(LocalCachedMapOptions.SyncStrategy.UPDATE)
                .reconnectionStrategy(LocalCachedMapOptions.ReconnectionStrategy.CLEAR);
        this.cache = client.getLocalCachedMap(options);
    }

    @Override
    protected Mono<Product> getFromSource(Integer id) {
        return repository.findById(id);
    }

    @Override
    protected Mono<Product> getFromCache(Integer id) {
        return Mono.justOrEmpty(cache.get(id));
    }

    @Override
    protected Mono<Product> updateSource(Integer id, Product product) {
        return repository.findById(id)
                .doOnNext(p -> product.setId(id))
                .flatMap(p -> repository.save(product));
    }

    @Override
    protected Mono<Product> updateCache(Integer id, Product product) {
        return Mono.create(sink ->
                cache.fastPutAsync(id, product)
                        .thenAccept(result -> sink.success(product))
                        .exceptionally(ex -> {
                            sink.error(ex);
                            return null;
                        })
        );
    }

    @Override
    protected Mono<Void> deleteFromSource(Integer id) {
        return repository.deleteById(id);
    }

    @Override
    protected Mono<Void> deleteFromCache(Integer id) {
        return Mono.create(sink ->
                cache.fastRemoveAsync(id)
                        .thenAccept(result -> sink.success())
                        .exceptionally(ex -> {
                            sink.error(ex);
                            return null;
                        })
        );
    }
}
