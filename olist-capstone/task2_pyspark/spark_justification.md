# Why Spark suits this workload

Spark suits this workload for four reasons.

**Scale.** Three months of trips is 4.67 million rows, and the raw CSV expands to roughly 290 MB once decompressed and shuffled. Pandas would load all of it into one process's memory; Spark streams it through in partitions, so the same code would handle a full year (around 30 million rows) by adding cores or machines rather than rewriting anything.

**Distributed processing.** Every transformation here is lazy: Spark built one optimised plan, fused filters and casts with whole-stage code generation, broadcast the 265-row zone table instead of shuffling 4.5 million trips, and aggregated locally before any shuffle. On my 2-core laptop that runs in local mode, but the identical DataFrame code runs on a cluster by changing only the master URL.

**Partitioning and performance.** The pruning experiment showed that layout matters as much as compute: storing the data by date cut a one-day query from 84 MiB to under 1 MiB read and made it about 2.5 times faster. Caching the cleaned data also let four KPIs reuse it without re-reading the CSV.

**Limitation.** Spark has real overhead. JVM start-up, task scheduling and shuffles meant some simple steps took seconds where Pandas would take milliseconds, and gzip input limited the first stage to three tasks. Small-file problems also appeared with daily partitions. For data that fits comfortably in memory, Pandas or DuckDB would be simpler; Spark earns its place as the data grows.
