---
description: Trader: generate pytest unit tests for selected module
---

Create isolated unit tests using pytest for the selected code module following these guidelines:

1. Essential Guidelines:

- Follow general copilot instructions in .github/instructions/copilot-instructions.md
- follow folder structure mirroring src code being tested
- if a directory is given, create test for all in that directory
- Use descriptive names: `test_<function>_<scenario>_<expected>`
- Follow the Arrange-Act-Assert pattern
- Keep tests focused and independent
- if existing tests, improve them or add new ones as needed
- Focus on main functional paths not the coverage of every line
- Aim to 90% coverage or more

2. Implementation Details:

- Use `@pytest.fixture` for common setup/teardown
- Never use user modifiable config or data files, use test fixtures instead
- Leverage `pytest.mark.parametrize` for multiple test cases
- Implement `pytest.raises` for error scenarios
- Mock external dependencies using `pytest.mock`

3. Test Structure Example:

```python
def test_function_scenario_expected():
    # Arrange: Set up test data and dependencies
    # Act: Call the function being tested
    # Assert: Verify the expected outcome
```

4. Code Coverage Requirements:

- Happy path scenarios
- Edge cases and boundary values
- Error conditions and invalid inputs
- Integration points with dependencies

5. Best Practices:

- Use descriptive assertion messages
- Keep test code simple and maintainable
- Document complex test scenarios
- Follow project naming conventions
- run make test to ensure all tests pass
- Iterate on tests until they meet high standards and all pass

Reference:

- pytest documentation: https://docs.pytest.org/
