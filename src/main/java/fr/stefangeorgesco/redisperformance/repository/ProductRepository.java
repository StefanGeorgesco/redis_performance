package fr.stefangeorgesco.redisperformance.repository;

import fr.stefangeorgesco.redisperformance.entity.Product;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProductRepository extends ReactiveCrudRepository<Product, Integer> {
}
