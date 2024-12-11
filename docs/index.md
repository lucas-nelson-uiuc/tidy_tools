<h1 style="text-align:center">Functional PySpark Workflows</h1>
<div align="center">
  <!-- PyPI Latest Release -->
  <a href="https://pypi.org/project/tidy-tools/">
    <img src="https://img.shields.io/pypi/v/tidy-tools.svg" alt="PyPI Latest Release"/>
  </a>
  <!-- GitHub Actions Build Status -->
  <a href="https://github.com/lucas-nelson-uiuc/tidy_tools/actions">
    <img src="https://img.shields.io/github/actions/workflow/status/lucas-nelson-uiuc/tidy_tools/ci.yml?branch=main" alt="Build Status"/>
  </a>
</div>


Tidy Tools is a declarative programming library promoting functional PySpark DataFrame workflows.
The package is an extension of the PySpark API and can be easily integrated into existing code.

---

## Quick Links

- **[Installation](user-guide/installation.md)**: Set up `tidy_tools` in your environment.
- **[Getting Started](user-guide/getting-started.md)**: Learn how to build workflows step by step.
- **[API Reference](api/api-reference.md)**: Explore all available functions and classes.

---

## Philosophy

The goal of Tidy Tools is to provide an extension of the PySpark DataFrame API that:

- Utilizes all available cores on your machine.
- Optimizes queries to reduce unneeded work/memory allocations.
- Handles datasets much larger than your available RAM.
- A consistent and predictable API.
- Adheres to a strict schema (data-types should be known before running the query).

On top of the existing API, Tidy Tools provides recipes for converting PySpark expressions
into tidy expressions.

---

## Contributing

All contributions are welcome, from reporting bugs to implementing new features. Read our
[contributing guide](development/contribution.md) to learn more.

---

## License

This project is licensed under the terms of the
[MIT license](https://github.com/pola-rs/polars/blob/main/LICENSE).
