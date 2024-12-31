# Tidy Tools

Declarative programming for PySpark workflows.

## What is it?

Tidy Tools is a package of packages that make working in PySpark *simpler*. Each module
extends the DataFrame API to reduce otherwise tedious procedures, greatly reducing the
time to write code.

Each module offers its own solutions that create a *tidy ecosystem*:

- **Core**: column selectors (like Polars) and functional filters
- **DataFrame**: extension of PySpark DataFrame to support in-process logging and configurations
- **Functions**: utility functions for iterative tasks (e.g. concatenating multiple DataFrames)
- **Model**: class-driven data conversions and validations (using *attrs*)
- **Workflow**: functions for easily composing pipelines

This package aims to be an equal balance of effectiveness and experiementation.

## How do I use Tidy Tools?

See the steps below to get started. Read the user guide to learn more advanced features.

### Installation

```bash
# using uv tools
uv pip install tidy-tools

# using uv run
uvx --with tidy-tools script.py

# using pip
pip install tidy-tools
```

### Importing

```python
# import top-level package (includes welcome message)
import tidy_tools

# import specific modules (more common)
from tidy_tools.core import selectors as cs
from tidy_tools.dataframe import TidyDataFrame
from tidy_tools.model import TidyDataModel
```

## Getting Help

If you encounter a clear bug, please file an issue with a minimal reproducible example on GitHub.

## Contribution

See contribution guidelines.

## License

This project is licensed under the MIT License.

## About

This project is developed and maintained by myself, Lucas Nelson. Feel free to connect if you want
to get in touch!
