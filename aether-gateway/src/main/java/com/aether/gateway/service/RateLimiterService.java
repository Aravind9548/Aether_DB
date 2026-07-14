package com.aether.gateway.service;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.Refill;
import io.github.bucket4j.distributed.ExpirationAfterWriteStrategy;
import io.github.bucket4j.distributed.proxy.ProxyManager;
import io.github.bucket4j.redis.redisson.cas.RedissonBasedProxyManager;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.command.CommandExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class RateLimiterService {

    private final ProxyManager<String> proxyManager;
    private final BucketConfiguration bucketConfiguration;

    @Autowired
    public RateLimiterService(RedissonClient redissonClient) {
        CommandExecutor commandExecutor = (CommandExecutor) ((Redisson) redissonClient).getCommandExecutor();

        this.proxyManager = RedissonBasedProxyManager.builderFor(commandExecutor)
                .withExpirationStrategy(ExpirationAfterWriteStrategy.basedOnTimeForRefillingBucketUpToMax(Duration.ofMinutes(1)))
                .build();

        Refill refill = Refill.intervally(5, Duration.ofSeconds(60));
        Bandwidth limit = Bandwidth.classic(5, refill);
        this.bucketConfiguration = BucketConfiguration.builder()
                .addLimit(limit)
                .build();
    }

    public boolean tryConsume(String key) {
        Bucket bucket = proxyManager.builder().build(key, () -> bucketConfiguration);
        return bucket.tryConsume(1);
    }
}
