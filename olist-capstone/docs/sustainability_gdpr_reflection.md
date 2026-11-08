# Cross-cutting reflection: cost, sustainability and GDPR

Everything in this portfolio runs locally in Docker on one laptop, and that choice shaped most of what follows.

**Cloud vs local cost.** Locally the only cost is hardware I already own, and the whole stack (Postgres, Airflow, Kafka, Kafdrop) fits in about 4 GB of RAM. The same stack on managed cloud services bills by the hour whether a DAG runs or not, and for a pipeline that runs once a day most of that money pays for idle time. The cloud wins once data outgrows one machine: 4.7M taxi rows ran comfortably here, but a full year would justify a short-lived Spark cluster that shuts down when the job ends.

**Sustainability.** My biggest saving came from data layout. Partitioning the taxi data by date meant a one-day query read one folder out of 90 instead of every file, and Parquet is far smaller than the CSV it replaced. Less data read means less CPU and less energy on every query. The DAG runs daily rather than every few minutes because the source changes slowly, and in the cloud I would schedule batch jobs in a low-carbon region or hour.

**Data residency and protection.** Olist customers are Brazilian, so LGPD applies, and data about EU residents would fall under GDPR. Data minimisation is built into the code: the Airflow mart and the Kafka events drop `customer_id`, and analytical outputs are aggregated by state and month. In the cloud I would pick a region matching where customers live (`sa-east-1` for Brazil, an EU region for EU data) and sign the provider's data processing agreement.

**Encryption and logging.** Credentials live in a git-ignored `.env` file and Airflow reads the warehouse connection from an environment variable, so no password is in the DAG. The Fernet key encrypts secrets in Airflow's metadata database. What is missing is encryption in transit: Kafka uses `PLAINTEXT` listeners and Postgres accepts unencrypted connections, which is fine on a laptop but not in production. Task logs and `late_deliveries.log` record order IDs only, so they can be kept for debugging without becoming a privacy risk.

**Limitation and lock-in.** Because the stack uses open-source images, it can move between a laptop, AWS, Azure or GCP unchanged. The risk would appear if I swapped in proprietary managed services, since DAGs and producers would then depend on one provider's APIs and pricing.
