# OTLP-PSQLPy

This library allows tracing PostgreSQL queries made by the psqlpy library.

## Usage

```python
import psqlpy
from otlp_psqlpy import PSQLPyPGInstrumentor

# You can optionally pass a custom TracerProvider to PSQLPyPGInstrumentor.instrument()
PSQLPyPGInstrumentor().instrument()

async def main():
    pool = psqlpy.ConnectionPool()
    conn = await pool.connection()

    await conn.execute("SELECT * FROM psqlpy")
```
