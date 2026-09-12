# Contributing to KTU Data Engineer Assessment

Thank you for your interest in this project! This document provides guidelines for contributing.

## Code of Conduct

Be respectful, inclusive, and professional in all interactions.

## Getting Started

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Serverless notebook compute
- Azure account (for Databricks)
- Python 3.8+
- Git

### Setting Up Your Development Environment

1. Clone the repository:
```bash
git clone https://github.com/your-username/ktu-data-engineer-assessment.git
cd ktu-data-engineer-assessment
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies (if any are added):
```bash
pip install -r requirements.txt
```

## Workflow

1. Create a feature branch:
```bash
git checkout -b feature/your-feature-name
```

2. Make your changes, following the code style guidelines below.

3. Test your changes thoroughly.

4. Commit with clear, descriptive messages:
```bash
git commit -m "Add feature description"
```

5. Push to your branch:
```bash
git push origin feature/your-feature-name
```

6. Create a Pull Request with a clear description of changes.

## Code Style Guidelines

### Databricks Notebooks

- Use clear, descriptive variable names
- Add comments explaining business logic and non-obvious decisions
- Include cell headers with `# ====` decorators to separate logical sections
- Keep cells focused on a single responsibility
- Document assumptions and limitations

### Python Code

- Follow PEP 8 style guide
- Use type hints where helpful
- Write docstrings for functions and classes
- Limit line length to 100 characters
- Use meaningful variable names

### SQL

- Use uppercase for keywords (SELECT, FROM, WHERE, etc.)
- Align column names vertically for readability
- Use table aliases consistently
- Comment complex logic

## Documentation Standards

### README Updates

If you modify functionality, update the README.md with:
- What changed
- Why it changed
- How to use the new feature

### CHANGELOG Updates

Add an entry to CHANGELOG.md following the Keep a Changelog format:
- Use present tense ("Add feature" not "Added feature")
- Group changes by category (Added, Fixed, Changed, Removed, etc.)
- Link to related issues or PRs

### Comment Standards

- Explain **why**, not **what** (the code shows what it does)
- Use TODO comments sparingly and descriptively
- Remove commented-out code before committing

## Data Quality and Validation

Before submitting changes:

1. Ensure all validation checks pass (17 checks in gold.validation_results)
2. Verify all reconciliation checks pass (5 checks in gold.reconciliation_results)
3. Test with a small subset of data first, then full dataset
4. Document any new assumptions or limitations
5. Update the CHANGELOG with the defect or feature

## Reporting Issues

Include:
- A clear title
- Reproduction steps (if applicable)
- Expected behavior
- Actual behavior
- Validation results or error messages
- Environment details (Databricks workspace, Python version, etc.)

## Performance Considerations

- Always profile before and after changes
- Document any changes to file fingerprinting or hashing logic
- Test idempotency: re-running on unchanged input should produce identical output
- Keep unmapped value processing efficient (sql GROUP BY, not Python loops)

## Testing

### Manual Testing Checklist

- [ ] Pipeline runs without errors
- [ ] All 17 validation checks pass
- [ ] All 5 reconciliation checks pass
- [ ] Row counts reconcile: Silver fact = dedup + excluded
- [ ] No event_key overlaps between dedup and excluded
- [ ] audit.pipeline_run_log shows SUCCESS for all notebooks
- [ ] Output metrics match expected business rules
- [ ] Audit tables are populated correctly

### Edge Cases to Test

- Re-running on unchanged input (idempotency)
- Handling of NULL values in key columns
- Processing of rows outside Q4 2025 reporting period
- Unmapped value capture and de-duplication
- Handling of synthetic/anonymised identifiers

## Security Considerations

- **Never commit credentials or secrets** (.env, credentials/, *.pem, *.key)
- Use .gitignore to exclude sensitive files
- If you accidentally commit secrets, contact maintainers immediately
- For Databricks, use Unity Catalog credentials, not personal tokens in code

## Commit Message Format

Use clear, descriptive commit messages:

```
Type: Brief description (50 chars or less)

More detailed explanation if needed (wrap at 72 chars).
- Use bullet points for multiple changes
- Explain the "why" behind the change
- Reference related issues: "Fixes #123"
```

**Type** can be:
- `feat:` A new feature or significant improvement
- `fix:` A bug fix or defect correction
- `docs:` Documentation updates
- `refactor:` Code restructuring without changing behavior
- `test:` Test additions or modifications
- `perf:` Performance improvements
- `chore:` Maintenance or dependency updates

### Example

```
fix: Correct event_key collision by including event_date_raw

The initial formula excluded event_date_raw, causing two rows for
the same participant on the same course but different dates to
produce identical hashes. This resulted in 43 event_key values
appearing in both dedup fact and excluded records.

Added event_date_raw to the hash formula.

Fixes #42
```

## Questions?

Refer to the Technical Design Document for detailed architecture and design decisions, or open an issue for discussion.

Thank you for contributing!
