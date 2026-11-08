# Evidence

| File | Shows | Status |
|---|---|---|
| `spark_ui_clean_job_dag.png` | Spark UI job DAG: CSV scan, whole-stage codegen, shuffle exchange | included |
| `spark_ui_partition_pruning_sql.png` | Spark SQL plan: 1 partition, 1 file, 919.7 KiB read | included |
| `spark_ui_jobs_list.png` | Spark jobs list with job descriptions | included |
| `airflow_dag_success.png` | `olist_delivery_etl`, all four tasks green | included |
| `docker_compose_ps.png` | output of `docker compose ps` with every service up | **capture on your machine** |
| `docker_desktop.png` | Docker Desktop containers view (optional) | **capture on your machine** |
| `kafka_producer_consumer.png` | the two terminals side by side, with offsets and LATE / HIGH VALUE alerts | **capture on your machine** |
| `kafdrop_topic.png` | Kafdrop topic page: `olist-order-events`, 3 partitions, message counts | **capture on your machine** |
| `kafdrop_messages.png` | Kafdrop *View Messages* with the JSON payloads | **capture on your machine** |
| `kafdrop_consumer_group.png` | `delivery-monitor` group with lag at or near 0 | **capture on your machine** |

The SQL notebook keeps its outputs (tables, charts and both `EXPLAIN ANALYSE` plans) inside the `.ipynb`, so it needs no separate screenshots.
