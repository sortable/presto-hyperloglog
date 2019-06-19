# presto-hyperloglog

A 10+ times faster HyperLogLog merge aggregation UDF support for Facebook Presto (prestodb.io).

### Aggregate Function

`merge_hll(HyperLogLog) -> P4HyperLogLog`

It takes `HyperLogLog` as input and result `P4HyperLogLog` (HyperLogLog dense format) as output.

### Test

run `mvn test`

### Build

run `mvn clean install`

### Deployment

After above build step, it will generate `presto-hyperloglog-0.XXX-jar-with-dependencies.jar`
in `target` directory.

Put above jar in presto cluster (for both coordinator and workers) at
`/mnt/presto-server-0.XXX/plugin/presto-hyperloglog/presto-hyperloglog-0.XXX-jar-with-dependencies.jar`.

Finally, restart presto service.

### Upgrade

It's currently targeting on presto `v0.206`.

You can change presto version in `pom.xml` to upgrade to later versions.

### Reference

- https://prestodb.github.io/docs/current/functions/hyperloglog.html
- https://prestodb.github.io/docs/current/develop/spi-overview.html
- https://prestodb.github.io/docs/current/develop/functions.html

### Q&A

- Is this `merge_hll` faster than built-in `merge` ?
    - Yes. `merge_hll` is more than 10 times faster than built-in `merge` when we compare them in production env.

- Why facebook does not make built-in `merge` function as fast as this one ?
    - This `merge_hll` has a side-effect, which is that the returned result is `P4HyperLogLog` instead of `HyperLogLog`.

- What's the impact of above "side-effect" ?
    - The merged HyperLogLog sketch will remain in dense format even it may should stay in sparse format. This will
    result slightly lower precision for low cardinality estimation. It does not affect high cardinality estimation.
    You can find a more detailed technical explanation in 5.3.1 from [this paper][3].

- Why built-in `merge` is slow ?
    - built-in `merge` uses airlift's [`HyperLogLog.mergeWith`][1] underneath, which is not
    very optimised for `sparse + dense` case, because it just simply [cast sparse to dense][2] and merge
    them, which is not very efficient. Also, `sparse + sparse` is fairly expensive due to it requires
    `HyperLogLog` as result.

- Why this `merge_hll` is faster ?
    - `merge_hll` starts with dense format, which is effectively a bitmap. Then it reads info from
    serialised HyperLogLog binary directly and update the bitmap accordingly.

- When should we use `merge_hll` instead of built-in `merge` ?
    - When we need to make merge fast and we do not care about result size (because size of
    `P4HyperLogLog` >= size of `HyperLogLog`). For example, when we run
    `cardinality(merge_hll(hyperloglog_sketch))`, we only care about cardinality and we do not
    care about size of merged result.

- When should we use built-in `merge` instead of this `merge_hll` ?
    - When we need to make merged result space-efficient. For example, when we want to save
    merged hyperloglog sketch to AWS S3, we should use built-in `merge` to make it space-efficiency.

[1]: https://github.com/airlift/airlift/blob/c5ebbd57fa32c76bf0e9754bd80620191cbce849/stats/src/main/java/io/airlift/stats/cardinality/HyperLogLog.java#L81
[2]: https://github.com/airlift/airlift/blob/c5ebbd57fa32c76bf0e9754bd80620191cbce849/stats/src/main/java/io/airlift/stats/cardinality/HyperLogLog.java#L89
[3]: http://static.googleusercontent.com/media/research.google.com/en//pubs/archive/40671.pdf
