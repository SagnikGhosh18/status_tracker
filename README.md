# Async Status Page Tracker

A lightweight Python implementation of an async status page tracker with Redis Streams pub/sub architecture. No frameworks, no heavy dependencies — just standard Python async primitives and minimal libraries.

## Features

- **Async HTTP polling** with conditional requests (ETag support)
- **Redis Streams** for event publishing and consumption
- **Diff detection** using in-memory cache to track incident changes
- **Exponential backoff** with jitter for resilient network handling
- **Silent seeding** on startup to avoid noise from pre-existing incidents
- **Concurrent producer/consumer** running in a single event loop

## Architecture

### Producer (Status Page Fetcher)
- Polls status pages concurrently using `asyncio` and `aiohttp`
- Monitors `https://status.openai.com/api/v2/incidents.json` every 60 seconds
- Uses conditional HTTP requests with `If-None-Match` ETag headers
- Maintains in-memory cache `{incident_id: last_updated_at}` for change detection
- Only publishes events when incidents are new or updated
- Seeds cache silently on startup without publishing existing incidents
- Adds ±5 second jitter to poll intervals to prevent thundering herd
- Implements exponential backoff (2s → 4s → 8s, capped at 60s) on errors

### Consumer (Console Output)
- Reads from Redis Streams using consumer groups
- Processes events with `XREADGROUP` (COUNT 10, BLOCK 2000ms)
- Prints formatted incident updates to console
- Acknowledges messages with `XACK` after processing

### Event Schema

Every published event is a flat dictionary with these fields:

```python
{
    "event_id": "uuid4-string",
    "provider": "OpenAI",
    "incident_id": "incident-id-from-api",
    "incident_name": "Incident name from API",
    "impact": "none|minor|major|critical",
    "affected_components": "Component A, Component B",
    "status": "investigating|identified|monitoring|resolved",
    "update_body": "Latest incident update message",
    "detected_at": "2025-11-03T14:32:00+00:00",
    "source_url": "https://status.openai.com/incidents/..."
}
```

## Project Structure

```
py_status_tracker/
├── status_tracker.py    # All application logic
├── docker-compose.yml   # Redis service definition
├── requirements.txt     # Python dependencies
├── setup.sh            # Setup script
└── README.md           # This file
```

## Dependencies

Only two external packages required:

- `aiohttp==3.9.1` - Async HTTP client
- `redis==5.0.1` - Redis async client

All other functionality uses Python standard library (`asyncio`, `uuid`, `datetime`, etc.)

## Quick Start

### 1. Setup

Run the setup script to create a virtual environment and install dependencies:

```bash
bash setup.sh
```

The script will:
- Check for Python 3.11+ (required)
- Create a virtual environment at `./venv`
- Install dependencies from `requirements.txt`

### 2. Start Redis

Launch Redis using Docker Compose:

```bash
docker compose up -d
```

This starts Redis 7 (Alpine) on `localhost:6379` with:
- Health checks using `redis-cli ping`
- Persistent storage with named volume `status-tracker-redis-data`
- Automatic restart policy

### 3. Run the Tracker

Activate the virtual environment and start the tracker:

```bash
source venv/bin/activate
python status_tracker.py
```

## How It Works

### Startup Sequence

1. **Connect to Redis** - Connects to `localhost:6379` and verifies connection
2. **Create Consumer Group** - Sets up `status-consumers` group on `status-events` stream
3. **Seed Cache** - Fetches initial incidents silently to populate the cache
4. **Start Monitoring** - Launches producer and consumer coroutines concurrently

### Runtime Behavior

**Producer Loop:**
1. Wait for poll interval (60s) with ±5s jitter
2. Send HTTP GET with `If-None-Match` header (if ETag exists)
3. Handle response:
   - `304 Not Modified` → Skip processing
   - `200 OK` → Process incidents and update cache
   - Other status → Retry with exponential backoff
4. For each incident, check if `updated_at` changed
5. Publish event to Redis Streams if changed (after initial seeding)

**Consumer Loop:**
1. Read messages from `status-events` stream using consumer group
2. Format and print incident updates to console
3. Acknowledge each message with `XACK`

### Example Output

```
[INFO] Starting Status Page Tracker...
[INFO] Connected to Redis at localhost:6379
[INFO] Created consumer group 'status-consumers'
[INFO] OpenAI: Cache seeded with 3 incidents
[INFO] Monitor is now live. Watching for status page updates...
[INFO] Press Ctrl+C to stop.

[2025-11-03 14:32:00] Product: OpenAI - API, ChatGPT
Status: [investigating] We are currently investigating elevated error rates affecting API requests and ChatGPT responses.

[2025-11-03 14:45:00] Product: OpenAI - API, ChatGPT
Status: [identified] The issue has been identified and a fix is being deployed.
```

## Configuration

Edit [status_tracker.py](status_tracker.py) to customize:

### Add More Status Pages

```python
STATUS_PAGES = [
    {
        "provider": "OpenAI",
        "url": "https://status.openai.com/api/v2/incidents.json",
        "poll_interval": 60,
    },
    {
        "provider": "GitHub",
        "url": "https://www.githubstatus.com/api/v2/incidents.json",
        "poll_interval": 60,
    },
    # Add more here...
]
```

### Redis Connection

```python
REDIS_HOST = "localhost"
REDIS_PORT = 6379
```

### Stream Names

```python
STREAM_NAME = "status-events"
CONSUMER_GROUP = "status-consumers"
```

## Redis Streams Details

### Stream Structure

- **Stream name:** `status-events`
- **Consumer group:** `status-consumers`
- **Consumer name:** `console-consumer-1`

### Commands Used

- `XADD` - Publish events to the stream
- `XGROUP CREATE` - Create consumer group (with `MKSTREAM` and `$` start ID)
- `XREADGROUP` - Read new messages from the stream
- `XACK` - Acknowledge processed messages

### Data Persistence

The Docker Compose configuration uses a named volume `status-tracker-redis-data` to persist stream data across container restarts.

## Error Handling

### Network Errors
- Exponential backoff: 2s → 4s → 8s → 60s (capped)
- Automatic retry on `aiohttp.ClientError`
- HTTP status codes other than 200/304 trigger backoff

### Redis Errors
- Consumer group creation handles `BUSYGROUP` error gracefully
- Connection failures are reported with clear error messages
- Failed event publishing is logged but doesn't crash the producer

### Graceful Shutdown

Press `Ctrl+C` to stop the tracker. It will:
1. Catch `KeyboardInterrupt`
2. Close Redis connection cleanly
3. Print shutdown confirmation

## Technical Details

### Why Redis Streams?

- **Pub/Sub with persistence** - Messages aren't lost if consumer is down
- **Consumer groups** - Multiple consumers can process messages in parallel
- **Acknowledgments** - Track which messages have been processed
- **Lightweight** - No need for heavyweight message brokers

### Why Conditional HTTP?

- **Bandwidth efficiency** - Server returns 304 if content unchanged
- **Server load reduction** - Less processing on the status page server
- **Rate limit friendly** - Counts as a lighter request

### Why Silent Seeding?

Prevents spam on startup. Without seeding:
- First poll would detect all existing incidents as "new"
- Consumer would print dozens of old incidents
- Defeats the purpose of real-time monitoring

With silent seeding:
- Cache populated on first poll
- Only subsequent changes trigger events
- Clean startup with no noise

## Troubleshooting

### "Failed to connect to Redis"

Make sure Redis is running:
```bash
docker compose up -d
docker compose ps
```

### Python Version Error

The tracker requires Python 3.11 or newer:
```bash
python3 --version
```

### Import Errors

Make sure you've activated the virtual environment:
```bash
source venv/bin/activate
pip list  # Should show aiohttp and redis
```

### No Events Being Published

Check that incidents actually changed on the status page. The tracker only publishes on updates, not every poll.

## Extending the Project

### Add More Consumers

Create additional consumer scripts that read from the same stream:

```python
# email_notifier.py
consumer = ConsoleConsumer(redis_client)
consumer.CONSUMER_NAME = "email-consumer-1"
# Add email sending logic in _process_message()
```

### Add Webhooks

Modify `_process_message()` to POST events to a webhook:

```python
async with aiohttp.ClientSession() as session:
    await session.post(WEBHOOK_URL, json=event)
```

### Store in Database

Add a database consumer that persists events:

```python
# Reads from Redis Stream
# Inserts into PostgreSQL/SQLite/etc.
# Tracks history of all incidents
```

## License

This is a reference implementation for educational purposes.

## Requirements

- **Python:** 3.11 or newer
- **Docker:** For Redis container
- **OS:** Linux, macOS, or Windows (with WSL)
