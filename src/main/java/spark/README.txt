BDH Assessed Exercise 2 - Pagerank w/ Spark
Domantas Jurkus 2141380
Joseph O'Hagan 213612

- Article names are assumed to be unique.

- `JavaRDD<String>` stores relevant revisions after filtering out future ones and irrelevant past ones.
- `JavaPairRDD<String, Iterable<String>>` stores outlinks per revision, with duplicates and self-loops removed from the outlink hash-set.
- Initial ranks are given to all revisions and outlinks, covering cases when an article does not have inlinks and when an outlink points to an non-existing article.
- `JavaPairRDD<String, Tuple2<Optional<Double>, Optional<Iterable<String>>>>` is used to store the article name, rank and outlinks. Computed by a full outer join between ranks and outlinks.
- `JavaPairRDD<String, Double>` stores contributions to outlinks. Each contributing article contributes 0.0 to itself in order to retain trace.
- Results are sorted by value by swapping values and keys between sorting.