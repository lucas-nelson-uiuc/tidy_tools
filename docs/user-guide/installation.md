# Installation

Tidy Tools can be installed using a simple one-line command.

``` bash
pip install tidy-tools
```

## Importing

To use the package, import it into your project:

``` python
# import top-level package (includes welcome message)
import tidy_tools

# import specific modules as needed
from tidy_tools.core import selectors as cs, filter as ttf
from tidy_tools.frame import TidyDataFrame
from tidy_tools.models import TidyDataModel
from tidy_tools.workflow import pipe, compose
```
