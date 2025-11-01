# Fix failing tests and optimize project configuration

## Summary

This PR fixes all 10 failing unit tests and significantly improves the project's dependency and tooling configuration by removing unused packages and modernizing the development workflow.

## Changes

### 🧪 Test Fixes (10 tests fixed)
- **API dataset tests** (6 tests): Added `authenticated_client` fixture that uses FastAPI's `dependency_overrides` to bypass database authentication in unit tests
- **CLI tenant tests** (4 tests):
  - Added `test_tenant` fixture to automatically create/cleanup test tenants
  - Fixed command name: `list-key` → `list-keys`

**Result**: All 353 tests now passing ✅

### 📦 Dependency Cleanup
Removed unused/redundant dependencies:

**Runtime dependencies removed:**
- `uvloop` - Not needed, Python 3.13 has excellent async performance
- `python-jose[cryptography]` - JWT library, never used in codebase
- `passlib[bcrypt]` - Redundant, `bcrypt` used directly instead

**Dev dependencies removed:**
- `black` - Replaced by ruff format (10-100x faster)
- `isort` - Replaced by ruff import sorting

**Impact**:
- Reduced dependency tree by ~25-30 transitive packages
- Smaller attack surface (fewer dependencies = more secure)
- Simpler tooling (ruff replaces black + isort)
- Faster CI/CD with ruff's performance

### ⚙️ Configuration Improvements
**Consistency fixes:**
- Aligned line-length to 100 across all tools (ruff, mypy)
- Python version constraint: `~=3.13.0` (exact 3.13.x compatibility)

**Modern tooling:**
- Added `[tool.uv]` section for UV-specific configuration
- Configured `[tool.ruff]` for formatting + linting + import sorting
- Removed black and isort (consolidated into ruff)
- Single tool for all code quality checks

## Testing

All 353 tests passing:
```bash
uv run python -m pytest tests/ -q
# 353 passed, 6 warnings in 26.03s
```

## Backwards Compatibility

- ✅ No breaking changes to production code
- ✅ All existing tests pass
- ✅ Python version constraint set to 3.13.x (waiting for Python 3.14 dependency support)
- ✅ Ruff respects existing black/isort configuration for compatibility

## Files Changed

**Tests** (3 files):
- `tests/unit/api/test_api_datasets.py` - Added authenticated client fixture
- `tests/unit/cli/conftest.py` - Added test tenant fixture
- `tests/unit/cli/test_tenant.py` - Fixed command name, added fixture usage

**Configuration** (2 files):
- `pyproject.toml` - Dependency cleanup, removed black/isort, ruff configuration
- `uv.lock` - Updated lockfile (significant reduction in dependencies)

## Checklist

- [x] All tests pass
- [x] No production code changes (tests and config only)
- [x] Dependencies verified as unused (grep analysis)
- [x] Configuration tested with `uv sync`
- [x] Commit messages are clear and descriptive

---

## Additional Context

**Why these specific test fixtures?**
- The API tests were hitting real database during authentication, causing "no such table: api_keys" errors
- FastAPI's `dependency_overrides` is the standard pattern for mocking dependencies in tests
- CLI tests needed actual tenant creation since they test the full command flow

**Why remove these dependencies?**
- Ran `grep -r "import <package>"` across entire codebase to verify zero usage
- `greenlet` was kept because SQLAlchemy requires it for async operations (discovered through test failures)
- `black` and `isort` are redundant since ruff provides the same functionality

**Why switch to Ruff?**
- 10-100x faster than black/isort (matters for CI/CD and large codebases)
- Single tool for formatting, linting, and import sorting
- Drop-in replacement - respects black/isort config
- Better maintained and actively developed
- Already had it installed, just consolidated usage
