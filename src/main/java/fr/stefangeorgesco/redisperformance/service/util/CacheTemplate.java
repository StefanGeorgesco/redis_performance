package fr.stefangeorgesco.redisperformance.service.util;

import reactor.core.publisher.Mono;

public abstract class CacheTemplate<K, E> {

    public Mono<E> get(K key) {
        return getFromCache(key)
                .switchIfEmpty(getFromSource(key)
                        .flatMap(entity -> updateCache(key, entity)));
    }

    public Mono<E> update(K key, E entity) {
        return updateSource(key, entity)
                .flatMap(updatedEntity -> deleteFromCache(key).thenReturn(updatedEntity));
    }

    public Mono<Void> delete(K key) {
        return deleteFromSource(key)
                .then(deleteFromCache(key));
    }

    protected abstract Mono<E> getFromSource(K key);

    protected abstract Mono<E> getFromCache(K key);

    protected abstract Mono<E> updateSource(K key, E entity);

    protected abstract Mono<E> updateCache(K key, E entity);

    protected abstract Mono<Void> deleteFromSource(K key);

    protected abstract Mono<Void> deleteFromCache(K key);
}
