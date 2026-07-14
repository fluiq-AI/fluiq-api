"""Pull connectors — import traces from external observability platforms.

Each connector fetches from a platform's read API, maps to Fluiq events, and
POSTs to ``/api/v1/ingest/otel``. Run as scheduled jobs (cron / ECS scheduled
task):

    python -m connectors.langsmith --hours 24
    python -m connectors.braintrust --limit 500

Phoenix and Langfuse don't need a connector — point their OTel exporter at
``/api/v1/ingest/otel`` (OTLP push) instead.
"""
