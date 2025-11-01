# Upgrade to Python 3.14 when dependencies support it

## Summary

Currently, the project is constrained to Python 3.13.x (`requires-python = "~=3.13.0"`) due to compatibility limitations with core dependencies. This issue tracks the upgrade to Python 3.14 once the required dependencies add support.

## Blocked By

The following dependencies need to release Python 3.14 compatible versions:

1. **DuckDB** (`duckdb>=0.9.0`)
   - Current version does not support Python 3.14
   - Need to monitor: https://github.com/duckdb/duckdb/releases

2. **asyncpg** (`asyncpg>=0.29.0`)
   - Current version does not support Python 3.14
   - Need to monitor: https://github.com/MagicStack/asyncpg/releases

## Current State

### Python Version Constraint
```toml
requires-python = "~=3.13.0"
```

This constraint ensures we only use Python 3.13.x until dependencies are ready.

## Evidence

### DuckDB Compatibility Issue
<!-- INSERT SCREENSHOT/ERROR IMAGE HERE -->


### asyncpg Compatibility Issue
<!-- INSERT SCREENSHOT/ERROR IMAGE HERE -->


## Task Checklist

When both dependencies support Python 3.14:

- [ ] Verify DuckDB has released a Python 3.14 compatible version
- [ ] Verify asyncpg has released a Python 3.14 compatible version
- [ ] Test installation with Python 3.14 in a clean virtual environment
- [ ] Update `pyproject.toml`:
  ```toml
  requires-python = ">=3.14,<3.15"
  ```
- [ ] Update `.python-version` file to `3.14.0`
- [ ] Update tool configurations:
  - `[tool.ruff]` target-version (py313 → py314)
  - `[tool.mypy]` python_version (3.13 → 3.14)
- [ ] Run full test suite with Python 3.14
- [ ] Update `uv.lock` with Python 3.14 resolution
- [ ] Update CI/CD configuration (if applicable)

## Testing Plan

1. Create a test environment with Python 3.14
2. Run `uv sync` and verify no conflicts
3. Run full test suite: `pytest tests/ -v`
4. Verify all 353 tests pass
5. Test in development mode
6. Test production build

## Related

- PR #XXX - Python 3.13 setup and dependency cleanup
- Python 3.14 Release Notes: https://docs.python.org/3.14/whatsnew/3.14.html

## Additional Context

This upgrade should be straightforward once dependencies catch up, as we've already:
- ✅ Cleaned up unused dependencies
- ✅ Established proper Python version constraints
- ✅ Set up modern tooling (Ruff, UV)
- ✅ All tests passing on Python 3.13

The main blocker is waiting for DuckDB and asyncpg maintainers to add Python 3.14 support.

---

**Monitoring**: Check dependency releases monthly or subscribe to release notifications for both repositories.
