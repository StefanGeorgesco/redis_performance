package fr.stefangeorgesco.redisperformance.controller;

import fr.stefangeorgesco.redisperformance.entity.Product;
import fr.stefangeorgesco.redisperformance.service.ProductServiceV1;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("product/v1")
public class ProductControllerV1 {

    private final ProductServiceV1 service;

    public ProductControllerV1(ProductServiceV1 service) {
        this.service = service;
    }

    @GetMapping("{id}")
    public Mono<Product> getProduct(@PathVariable int id) {
        return service.getProduct(id);
    }

    @PutMapping("{id}")
    public Mono<Product> updateProduct(@PathVariable int id, @RequestBody Mono<Product> productMono) {
        return service.updateProduct(id, productMono);
    }
}
