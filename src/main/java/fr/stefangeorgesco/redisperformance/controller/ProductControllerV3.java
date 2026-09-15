package fr.stefangeorgesco.redisperformance.controller;

import fr.stefangeorgesco.redisperformance.entity.Product;
import fr.stefangeorgesco.redisperformance.service.ProductServiceV3;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("product/v3")
public class ProductControllerV3 {

    private final ProductServiceV3 service;

    public ProductControllerV3(ProductServiceV3 service) {
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
