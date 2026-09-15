package fr.stefangeorgesco.redisperformance.controller;

import fr.stefangeorgesco.redisperformance.entity.Product;
import fr.stefangeorgesco.redisperformance.service.ProductServiceV2;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("product/v2")
public class ProductControllerV2 {

    private final ProductServiceV2 service;

    public ProductControllerV2(ProductServiceV2 service) {
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

    @DeleteMapping("{id}")
    public Mono<Void> deleteProduct(@PathVariable int id) {
        return service.deleteProduct(id);
    }
}
