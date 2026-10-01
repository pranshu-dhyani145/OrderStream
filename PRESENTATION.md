# OrderStream: 2-3 minute presentation

**Before you start (do this 5 minutes earlier):**
- `docker compose up -d --build` and wait until `docker compose ps` shows `kafka`, `api` and `consumer` running.
- Browser tabs ready: `http://localhost:8000`, the README architecture diagram, and the GitHub **Actions** page showing a green run.
- A terminal ready with `docker compose logs -f consumer` running.

---

## Script (about 2.5 minutes, roughly 380 words)

**1. What it is (0:00 - 0:20)**
"My project is OrderStream, an event-driven order processing pipeline built on Apache Kafka. When a customer places an order, the system accepts it, passes it through Kafka as an event, and a separate service processes and stores it."

**2. Why Kafka (0:20 - 0:45)**
"If the API called the processing code directly, a slow or crashed processor would break order placement. With Kafka, the API just publishes an event and moves on. Kafka holds the event safely until the consumer reads it. So the two sides are decoupled: they only share a topic name, and they can be restarted or scaled independently. This is message passing between distributed components."

**3. Architecture (0:45 - 1:10)** *(show the diagram)*
"The flow is: Client, then a FastAPI service, then the Kafka `orders` topic, then a Consumer, then SQLite. The `orders` topic has 2 partitions. The consumer belongs to a consumer group called `order-processors`. I key every message by `user_id`, so all orders from one user go to the same partition and stay in order. Kafka guarantees order within a partition, not across partitions. If I run two consumers in the group, Kafka splits the two partitions between them."

**4. Docker Compose (1:10 - 1:25)**
"Everything runs with one command, `docker compose up`. It starts Kafka in KRaft mode, a one-time job that creates the topics, the API container and the consumer container. The API and consumer share a Docker volume for the SQLite database."

**5. Live demo (1:25 - 2:05)**
1. Open `http://localhost:8000`. "This is the dashboard served by the API."
2. Click **Send order**. "The API replied with the partition and offset Kafka assigned, so Kafka has accepted the event."
3. Point at the table. "A moment later the order appears here with status PROCESSED. It was stored by the consumer, not the API."
4. Switch to the terminal. "Here is the consumer log: `processed ORD-... (partition N, offset M)`."
5. Send 2-3 more orders with different user IDs. "Different users land on different partitions."

**6. CI and tests (2:05 - 2:30)** *(show GitHub Actions)*
"On every push and pull request, GitHub Actions runs two jobs. First `test` runs Ruff for linting and 18 pytest tests. Only if that passes does `docker-build` run, which validates the Compose file and builds the Docker image. Here you can see it is green. The tests mock Kafka, so they run in under a second without a broker."

**Close (2:30 - 2:40)**
"So OrderStream demonstrates event-driven design, a message broker, partitions and consumer groups, containers with Docker Compose, and automated testing with CI. Thank you."

---

## Optional extra (only if you have time)
Show consumer groups: `docker compose up -d --scale consumer=2`, send a few orders from different users, and show that the two consumer containers' logs handle different partitions.

---

## Likely viva questions and simple answers

**What is Kafka?**
A distributed message broker. Producers write events to topics, consumers read them. Events are stored on disk, so a consumer can be offline and catch up later.

**Why not just call the processor from the API?**
Decoupling. If the processor is slow or down, orders are still accepted and wait in Kafka. It also lets me add more consumers without changing the API.

**What is a topic? A partition?**
A topic is a named stream of events (`orders`). It is split into partitions, which are ordered logs. Partitions let several consumers read in parallel.

**What is a consumer group?**
Consumers with the same group ID share the work. Each partition is read by only one consumer in the group. My group is `order-processors`.

**Is ordering guaranteed?**
Only within a partition. I use `user_id` as the message key, so one user's orders always go to the same partition and are processed in order.

**What if the consumer crashes while processing?**
The consumer commits its offset only after the order is saved. After a restart, Kafka redelivers the unconfirmed message. The database insert ignores duplicates by order ID, so nothing is stored twice.

**What happens to a bad message?**
It is sent to a separate dead-letter topic, `orders.dlq`, so it does not block the queue.

**What if Kafka is down when someone posts an order?**
The API returns HTTP 503 instead of pretending it worked.

**Why does POST /orders return 202?**
202 means accepted. The event is in Kafka, but the consumer stores it asynchronously, so it may appear in the list a moment later.

**What do your 18 tests cover?**
Input validation, the producer (topic, key, broker failure), the consumer (valid event, invalid event, duplicate delivery, dead-letter), the database, and the API (202, 422, 503, 404). Kafka is replaced with a fake, so they run fast.

**What does your CI do?**
`test` runs Ruff and pytest. `docker-build` runs only after `test` passes and validates the Compose file and builds the image.

**Are you deploying to the cloud?**
Not currently. The project runs locally with Docker Compose. The repo includes an optional EC2 setup script and a manual-only deploy workflow, so it can be hosted on EC2 later, but I have not deployed it.

**What would you change for production?**
Three Kafka brokers with replication, PostgreSQL instead of SQLite, authentication on the API, HTTPS, and monitoring.

---

## Say / do not say

- Say: "runs locally with Docker Compose", "CI runs tests and builds the Docker image".
- Do not say: "deployed on AWS", "auto-deploys on merge", or "GitHub blocks pushes". If you enabled branch protection on `main` with `test` and `docker-build` as required checks, you can say "failing checks block merging into main". If you did not, leave that out.
