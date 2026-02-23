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
