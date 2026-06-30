# Spark Analytics

Spark analytics is not planned in detail yet.

There is an existing prototype under `spark_service/`, but the new architecture
cleanup has not reached the Spark stage. Do not treat the current Spark code as
the final model-training or historical-export design.

Future plan should define:

1. Which Kafka topics or exported files become training input.
2. What historical event schema Spark should read.
3. What model artifacts the RFC scoring service should load.
4. How training outputs are versioned and validated.
