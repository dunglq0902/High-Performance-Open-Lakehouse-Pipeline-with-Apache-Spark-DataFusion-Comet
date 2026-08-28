# Deterministic E-commerce fixture generator

The generator writes the five source tables from the project data contract as
sorted Snappy Parquet files. Values are derived from counter-based SHA-256
inputs, so generation does not depend on row iteration, process-global random
state, or Parquet file boundaries. Content identity uses canonical rows and
fixed first-primary-key ranges; Parquet file SHA-256 values separately capture
the locked writer version and physical layout.

```bash
python -m data.generator generate \
  --profile data/generator/configs/fixture.yaml \
  --output data/generated/ecommerce-fixture

python -m data.generator validate \
  --dataset data/generated/ecommerce-fixture \
  --profile data/generator/configs/fixture.yaml
```

Output paths are immutable: generation fails if the target already exists.
The `fixture` and normative `tiny` profiles are both non-research datasets and
must not be included in primary benchmark results.
