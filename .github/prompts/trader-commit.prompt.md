---
name: trader-commit
description: 'Trader: run make qa, then create scoped conventional commits'
argument-hint: '[optional scope, e.g. engine+tests]'
---

Review the codebase changes and create logically separated commits based on the scope of work:

1. Run make qa to ensure code quality checks pass, solving any issues if necessary.

1. Analyze changes using version control status/diff

1. Group modifications by their functional purpose:

   - API changes
   - Naming convention updates
   - Feature implementations
   - Bug fixes
   - Configuration changes
   - etc.

1. For each logical group:

   - Stage related files/changes using `git add`
   - Create a commit with a clear, descriptive message following conventional commit format
   - Include relevant ticket/issue references

1. Before committing:

   - Run pre-commit hooks to validate changes
   - Address any linting/formatting issues raised by hooks
   - Re-stage files modified by automated tooling
   - Verify changes meet project standards

1. Proceed with commits only after all pre-commit validations pass

Example commit structure:

```
feat(api): implement new endpoint for user authentication
chore(style): update variable names to follow camelCase convention
```

Note: Handle any automated changes from pre-commit hooks by including them in the relevant commits.
