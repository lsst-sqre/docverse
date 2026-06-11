# Docverse

Docverse is a hosting platform for versioned documentation websites, built by [Rubin Observatory](https://www.lsst.io).

The design is documented in [SQR-112: Docverse documentation hosting platform design](https://sqr-112.lsst.io).

## Development setup

Docverse uses [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
make init    # Install dependencies and set up pre-commit hooks
make test    # Run tests
make lint    # Run linters
make typing  # Run type checking
make run     # Start the development server on port 8080
```

## Application metrics

Docverse publishes Sasquatch application metrics (SQR-112) through a Safir
`EventManager`. The metrics configuration is selected from the environment
by `metrics_configuration_factory`:

- `METRICS_APPLICATION` — application name reported on every event
  (`docverse`).
- `METRICS_ENABLED` / `METRICS_MOCK` — select the manager variant. In
  production set `METRICS_ENABLED=true`. In tests set `METRICS_MOCK=true`
  with `METRICS_ENABLED=false` to use an in-memory mock manager (this is
  what the nox `test` session sets). With neither mock nor disabled
  selected, the Kafka-backed manager is used.
- `KAFKA_*` — Kafka connection settings (bootstrap servers, security
  protocol, and any TLS material) used when metrics are enabled.
- `SCHEMA_MANAGER_*` — the Confluent-compatible schema-registry URL used to
  register and evolve the event Avro schemas.

Events publish to the `lsst.square.metrics.events.docverse` topic. The
`phalanx-docverse` deployment is responsible for supplying the `KAFKA_*` /
`SCHEMA_MANAGER_*` values and for registering that topic in Sasquatch;
that work is tracked separately from this repository.
