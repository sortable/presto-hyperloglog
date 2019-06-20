# presto-hyperloglog

A 10+ times faster HyperLogLog merge aggregation UDF support for Facebook Presto (prestodb.io).

### Aggregate Function

`merge_p4(HyperLogLog) -> P4HyperLogLog`

It takes `HyperLogLog` as input and result `P4HyperLogLog` (HyperLogLog dense format) as output.

`merge_p4(HyperLogLog)` is equivalent to `cast(merge(HyperLogLog) as P4HyperLogLog)`.

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

### Presto Reference

- [Presto's HyperLogLog doc][1]
- [Presto's Plugin doc][2]
- [Presto's UDF doc][3]

### Implementation Reference

- [Airlift's HyperLogLog format doc][4] (how HyperLogLog instance is serialised)
- [Airlift's Dense HLL code][5] (how HyperLogLog in dense format works)
- [Airlift's Sparse HLL code][6] (how HyperLogLog in sparse format works)

### Q&A

- Is this `merge_p4` faster than built-in `merge` ?
    - Yes. `merge_p4` is more than 10 times faster than built-in `merge` when we compare them in production env.

- Why Presto does not make built-in `merge` function as fast as this one ?
    - This `merge_p4` has a side-effect, which is that the returned result is `P4HyperLogLog` instead of `HyperLogLog`.

- What's the impact of above "side-effect" ?
    - The merged HyperLogLog sketch will remain in dense format even it may should stay in sparse format. This will
    result slightly lower precision for low cardinality estimation. It does not affect high cardinality estimation.
    You can find a more detailed technical explanation in 5.3.1 from [this paper][7].

- Why built-in `merge` is slow ?
    - built-in `merge` uses airlift's [`HyperLogLog.mergeWith`][8] underneath, which is not
    very optimised for `sparse + dense` case, because it just simply [cast sparse to dense][9] and merge
    them, which is not very efficient. Also, `sparse + sparse` is fairly expensive due to it requires
    `HyperLogLog` as result.

- Why this `merge_p4` is faster ?
    - `merge_p4` starts with dense format, which is effectively a bitmap. Then it reads info from
    serialised HyperLogLog binary directly and update the bitmap accordingly.

- When should we use `merge_p4` instead of built-in `merge` ?
    - When we need to make merge fast and we do not care about result size (because size of
    `P4HyperLogLog` >= size of `HyperLogLog`). For example, when we run
    `cardinality(merge_p4(hyperloglog_sketch))`, we only care about cardinality and we do not
    care about size of merged result.

- When should we use built-in `merge` instead of this `merge_p4` ?
    - When we need to make merged result space-efficient. For example, when we want to save
    merged hyperloglog sketch to AWS S3, we should use built-in `merge` to make it space-efficiency.

[1]: https://prestodb.github.io/docs/current/functions/hyperloglog.html
[2]: https://prestodb.github.io/docs/current/develop/spi-overview.html
[3]: https://prestodb.github.io/docs/current/develop/functions.html
[4]: https://github.com/airlift/airlift/blob/c5ebbd57fa32c76bf0e9754bd80620191cbce849/stats/docs/hll.md
[5]: https://github.com/airlift/airlift/blob/c5ebbd57fa32c76bf0e9754bd80620191cbce849/stats/src/main/java/io/airlift/stats/cardinality/DenseHll.java
[6]: https://github.com/airlift/airlift/blob/c5ebbd57fa32c76bf0e9754bd80620191cbce849/stats/src/main/java/io/airlift/stats/cardinality/SparseHll.java
[7]: http://static.googleusercontent.com/media/research.google.com/en//pubs/archive/40671.pdf
[8]: https://github.com/airlift/airlift/blob/c5ebbd57fa32c76bf0e9754bd80620191cbce849/stats/src/main/java/io/airlift/stats/cardinality/HyperLogLog.java#L81
[9]: https://github.com/airlift/airlift/blob/c5ebbd57fa32c76bf0e9754bd80620191cbce849/stats/src/main/java/io/airlift/stats/cardinality/HyperLogLog.java#L89
