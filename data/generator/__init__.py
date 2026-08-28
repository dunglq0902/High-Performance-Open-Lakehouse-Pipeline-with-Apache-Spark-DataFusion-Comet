"""Deterministic synthetic E-commerce dataset generator.

The public API is deliberately small so callers do not need to depend on the
command-line interface.  Importing this module requires PyArrow because the
dataset contract is expressed as explicit Arrow schemas.
"""

from data.generator.generate import generate_dataset
from data.generator.profiles import GeneratorProfile, load_profile
from data.generator.validation import DatasetValidationError, ValidationReport, validate_dataset

__all__ = [
    "DatasetValidationError",
    "GeneratorProfile",
    "ValidationReport",
    "generate_dataset",
    "load_profile",
    "validate_dataset",
]
