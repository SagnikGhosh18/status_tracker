#!/usr/bin/env python3
"""
Lightweight async status page tracker with Redis Streams.
Monitors status pages and publishes incident updates to Redis.
"""

import asyncio
import random
import sys
import uuid
from datetime import datetime, timezone
from typing import Dict, Optional

import aiohttp
import redis.asyncio as redis


# Configuration
REDIS_HOST = "localhost"
REDIS_PORT = 6379
STREAM_NAME = "status-events"
CONSUMER_GROUP = "status-consumers"
CONSUMER_NAME = "console-consumer-1"

STATUS_PAGES = [
    {
        "provider": "OpenAI",
        "url": "https://status.openai.com/api/v2/incidents.json",
        "poll_interval": 60,
    }
]


class StatusPageFetcher:
    """Producer: Fetches status page data and publishes events to Redis Streams."""

    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.incident_cache: Dict[str, str] = {}  # {incident_id: last_updated_at}
        self.etag: Optional[str] = None
        self.seeded = False

    async def fetch_and_publish(
        self, provider: str, url: str, poll_interval: int
    ) -> None:
        """Continuously poll a status page and publish changes."""
        backoff_delay = 2  # Start with 2 seconds
        max_backoff = 60  # Cap at 60 seconds

        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    # Add jitter to avoid thundering herd
                    jitter = random.uniform(-5, 5)
                    await asyncio.sleep(max(0, poll_interval + jitter))

                    # Prepare headers for conditional HTTP
                    headers = {}
                    if self.etag:
                        headers["If-None-Match"] = self.etag

                    async with session.get(url, headers=headers) as response:
                        # Handle 304 Not Modified
                        if response.status == 304:
                            print(f"[INFO] {provider}: No changes (304 Not Modified)")
                            backoff_delay = 2  # Reset backoff on success
                            continue

                        # Handle non-200 responses with exponential backoff
                        if response.status != 200:
                            print(
                                f"[WARN] {provider}: HTTP {response.status}, retrying in {backoff_delay}s"
                            )
                            await asyncio.sleep(backoff_delay)
                            backoff_delay = min(backoff_delay * 2, max_backoff)
                            continue

                        # Update ETag
                        if "ETag" in response.headers:
                            self.etag = response.headers["ETag"]

                        data = await response.json()
                        backoff_delay = 2  # Reset backoff on success

                        # Process incidents
                        incidents = data.get("incidents", [])
                        await self._process_incidents(provider, url, incidents)

                        if not self.seeded:
                            print(f"[INFO] {provider}: Cache seeded with {len(self.incident_cache)} incidents")
                            self.seeded = True

                except aiohttp.ClientError as e:
                    print(f"[ERROR] {provider}: Network error - {e}, retrying in {backoff_delay}s")
                    await asyncio.sleep(backoff_delay)
                    backoff_delay = min(backoff_delay * 2, max_backoff)
                except Exception as e:
                    print(f"[ERROR] {provider}: Unexpected error - {e}, retrying in {backoff_delay}s")
                    await asyncio.sleep(backoff_delay)
                    backoff_delay = min(backoff_delay * 2, max_backoff)

    async def _process_incidents(
        self, provider: str, source_url: str, incidents: list
    ) -> None:
        """Process incidents and publish events for new or updated ones."""
        for incident in incidents:
            incident_id = incident.get("id")
            updated_at = incident.get("updated_at")

            if not incident_id or not updated_at:
                continue

            # Check if this is a new or updated incident
            cached_updated_at = self.incident_cache.get(incident_id)

            if cached_updated_at != updated_at:
                # Update cache
                self.incident_cache[incident_id] = updated_at

                # Only publish if we've already seeded (skip initial incidents)
                if self.seeded:
                    await self._publish_event(provider, source_url, incident)

    async def _publish_event(
        self, provider: str, source_url: str, incident: dict
    ) -> None:
        """Publish an incident event to Redis Streams."""
        # Get the latest incident update
        incident_updates = incident.get("incident_updates", [])
        latest_update = incident_updates[0] if incident_updates else {}

        # Extract affected components
        affected_components = incident.get("components", [])
        component_names = ", ".join([comp.get("name", "") for comp in affected_components])

        # Build the event
        event = {
            "event_id": str(uuid.uuid4()),
            "provider": provider,
            "incident_id": incident.get("id", ""),
            "incident_name": incident.get("name", ""),
            "impact": incident.get("impact", "none"),
            "affected_components": component_names,
            "status": latest_update.get("status", ""),
            "update_body": latest_update.get("body", ""),
            "detected_at": datetime.now(timezone.utc).isoformat(),
            "source_url": incident.get("shortlink", source_url),
        }

        # Publish to Redis Streams
        try:
            await self.redis.xadd(STREAM_NAME, event)
            print(f"[INFO] Published event: {provider} - {incident.get('name', 'Unknown')}")
        except Exception as e:
            print(f"[ERROR] Failed to publish event: {e}")


class ConsoleConsumer:
    """Consumer: Reads events from Redis Streams and prints to console."""

    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

    async def setup_consumer_group(self) -> None:
        """Create the consumer group if it doesn't exist."""
        try:
            await self.redis.xgroup_create(
                STREAM_NAME, CONSUMER_GROUP, id="$", mkstream=True
            )
            print(f"[INFO] Created consumer group '{CONSUMER_GROUP}'")
        except redis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                print(f"[INFO] Consumer group '{CONSUMER_GROUP}' already exists")
            else:
                raise

    async def consume(self) -> None:
        """Continuously read and process events from the stream."""
        while True:
            try:
                # Read messages from the stream
                messages = await self.redis.xreadgroup(
                    CONSUMER_GROUP,
                    CONSUMER_NAME,
                    {STREAM_NAME: ">"},
                    count=10,
                    block=2000,
                )

                if not messages:
                    continue

                for stream_name, stream_messages in messages:
                    for message_id, stream_messages in stream_messages:
                        await self._process_message(message_id, stream_messages)

            except Exception as e:
                print(f"[ERROR] Consumer error: {e}")
                await asyncio.sleep(1)

    async def _process_message(self, message_id: bytes, message_data: dict) -> None:
        """Process a single message from the stream."""
        try:
            # Decode bytes to strings
            event = {
                k.decode() if isinstance(k, bytes) else k:
                v.decode() if isinstance(v, bytes) else v
                for k, v in message_data.items()
            }

            # Parse detected_at timestamp
            detected_at = event.get("detected_at", "")
            try:
                dt = datetime.fromisoformat(detected_at.replace("Z", "+00:00"))
                timestamp_str = dt.strftime("%Y-%m-%d %H:%M:%S")
            except:
                timestamp_str = detected_at

            # Format and print the event
            provider = event.get("provider", "Unknown")
            affected_components = event.get("affected_components", "")
            status = event.get("status", "unknown")
            update_body = event.get("update_body", "No details available")

            product_line = f"{provider}"
            if affected_components:
                product_line += f" - {affected_components}"

            print(f"\n[{timestamp_str}] Product: {product_line}")
            print(f"Status: [{status}] {update_body}")

            # Acknowledge the message
            await self.redis.xack(STREAM_NAME, CONSUMER_GROUP, message_id)

        except Exception as e:
            print(f"[ERROR] Failed to process message: {e}")


async def main():
    """Main entry point: runs producer and consumer concurrently."""
    print("[INFO] Starting Status Page Tracker...")

    # Connect to Redis
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)

    try:
        # Test Redis connection
        await redis_client.ping()
        print(f"[INFO] Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
    except Exception as e:
        print(f"[ERROR] Failed to connect to Redis: {e}")
        print("[ERROR] Make sure Redis is running: docker compose up -d")
        sys.exit(1)

    # Setup consumer group
    consumer = ConsoleConsumer(redis_client)
    await consumer.setup_consumer_group()

    # Create producer
    fetcher = StatusPageFetcher(redis_client)

    # Start producer coroutines for all status pages
    producer_tasks = [
        fetcher.fetch_and_publish(
            page["provider"], page["url"], page["poll_interval"]
        )
        for page in STATUS_PAGES
    ]

    # Start consumer coroutine
    consumer_task = consumer.consume()

    print("[INFO] Monitor is now live. Watching for status page updates...")
    print("[INFO] Press Ctrl+C to stop.\n")

    try:
        # Run producer and consumer concurrently
        await asyncio.gather(*producer_tasks, consumer_task)
    except KeyboardInterrupt:
        print("\n[INFO] Shutting down gracefully...")
    finally:
        await redis_client.close()
        print("[INFO] Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())
