# overload

This repo has a collection of workloads to put a heavy load on postgres database.

Currently it only has a simple COPY ingest workload, that tries to insert data into a postgres database as fast as possible. The data is generated on the client side and inserted using a COPY protocol.