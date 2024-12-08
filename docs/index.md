![tidy_tools_logo](_assets/logo.png)

Tidy Tools is a declarative programming library promoting functional PySpark DataFrame workflows.
The package is an extension of the PySpark API and can be easily integrated into existing code.

## Key features

- **Fast**: Written from scratch in Rust, designed close to the machine and without external
  dependencies.
- **I/O**: First class support for all common data storage layers: local, cloud storage & databases.
- **Intuitive API**: Write your queries the way they were intended. Polars, internally, will
  determine the most efficient way to execute using its query optimizer.
- **Out of Core**: The streaming API allows you to process your results without requiring all your
  data to be in memory at the same time.
- **Parallel**: Utilises the power of your machine by dividing the workload among the available CPU
  cores without any additional configuration.
- **Vectorized Query Engine**: Using [Apache Arrow](https://arrow.apache.org/), a columnar data
  format, to process your queries in a vectorized manner and SIMD to optimize CPU usage.
- **GPU Support**: Optionally run queries on NVIDIA GPUs for maximum performance for in-memory
  workloads.

## Philosophy

The goal of Tidy Tools is to provide an extension of the PySpark DataFrame API that:

- Utilizes all available cores on your machine.
- Optimizes queries to reduce unneeded work/memory allocations.
- Handles datasets much larger than your available RAM.
- A consistent and predictable API.
- Adheres to a strict schema (data-types should be known before running the query).

On top of the existing API, Tidy Tools provides recipes for converting PySpark expressions
into tidy expressions.

## Contributing

All contributions are welcome, from reporting bugs to implementing new features. Read our
[contributing guide](development/contribution.md) to learn more.

## License

This project is licensed under the terms of the
[MIT license](https://github.com/pola-rs/polars/blob/main/LICENSE).
