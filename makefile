# Project Configuration
# ---------------------
PROJECT_NAME := QS-Trader
PYTHON_VERSION := 3.13
VENV := .venv
BIN := $(VENV)/bin
SRC_DIR := src
UV := env -u VIRTUAL_ENV uv

# Terminal Colors
# ---------------
CYAN := \033[0;36m
GREEN := \033[0;32m
RED := \033[0;31m
BLUE := \033[0;34m
YELLOW := \033[1;33m
BOLD := \033[1m
END := \033[0m

# Default target
# --------------
.DEFAULT_GOAL := help

# Utility Functions
# -----------------
define log_info
printf '%b\n' "$(BLUE)ℹ️  $(1)$(END)"
endef

define log_success
printf '%b\n' "$(GREEN)✅ $(1)$(END)"
endef

define log_warning
printf '%b\n' "$(YELLOW)⚠️  $(1)$(END)"
endef

define log_error
printf '%b\n' "$(RED)❌ $(1)$(END)"
endef


################################################################################
# HELP
################################################################################
.PHONY: help
help: ## 📚 Show this help message
	@printf '%b\n' "$(BOLD)$(PROJECT_NAME) Development Makefile$(END)"
	@echo ""
	@printf '%b\n' "$(CYAN)📋 Available Commands:$(END)"
	@echo ""
	@printf '%b\n' "$(BOLD)🚀 Setup & Environment:$(END)"
	@grep -E '^(check-uv|sync|upgrade|install-hooks|setup|clean):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(CYAN)%-20s$(END) %s\n", $$1, $$2}'
	@echo ""
	@printf '%b\n' "$(BOLD)🎨 Code Quality:$(END)"
	@grep -E '^(format|format-md|lint|lint-check|type-check|quality):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(CYAN)%-20s$(END) %s\n", $$1, $$2}'
	@echo ""
	@printf '%b\n' "$(BOLD)🧪 Testing:$(END)"
	@grep -E '^(test[a-zA-Z_-]*|qa):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(CYAN)%-20s$(END) %s\n", $$1, $$2}'
	@echo ""
	@printf '%b\n' "$(BOLD)📦 Build & Release:$(END)"
	@grep -E '^(build|release-check|version|release-prepare|release):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(CYAN)%-20s$(END) %s\n", $$1, $$2}'
	@echo ""
	@printf '%b\n' "$(BOLD)📓 Development Tools:$(END)"
	@grep -E '^(setup-kernel|run-jupyter):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(CYAN)%-20s$(END) %s\n", $$1, $$2}'
	@echo ""
	@printf '%b\n' "$(BOLD)🔧 Utilities:$(END)"
	@grep -E '^(help):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(CYAN)%-20s$(END) %s\n", $$1, $$2}'
	@echo ""
	@printf '%b\n' "$(YELLOW)💡 Quick Start:$(END)"
	@printf '%b\n' "  $(CYAN)make setup$(END)          - Complete development environment setup"
	@printf '%b\n' "  $(CYAN)make qa$(END)             - Run full quality assurance (format + lint + test)"
	@printf '%b\n' "  $(CYAN)make test$(END)           - Run all tests with coverage"
	@echo ""
	@printf '%b\n' "$(YELLOW)🚀 Release Workflow:$(END)"
	@printf '%b\n' "  $(CYAN)make version$(END)        - Show current version"
	@printf '%b\n' "  $(CYAN)make release-prepare$(END) - Check if ready for release (runs QA)"
	@printf '%b\n' "  $(CYAN)make release VERSION=x.y.z$(END) - Create and push GitHub release tag"
	@echo ""


################################################################################
# PROJECT SETUP
################################################################################
.PHONY: check-uv
check-uv: ## 🔧 Verify UV package manager is available
	@printf '%b\n' "$(BLUE)ℹ️  Checking UV package manager...$(END)"
	@command -v uv >/dev/null 2>&1 || { \
		printf '%b\n' "$(RED)❌ UV is not installed$(END)"; \
		printf '%b\n' "$(RED)Please install UV from: https://docs.astral.sh/uv/getting-started/installation/$(END)"; \
		exit 1; \
	}
	@printf '%b\n' "$(GREEN)✅ UV package manager is available$(END)"

.PHONY: sync
sync: check-uv ## 📦 Sync dependencies and create virtual environment
	@printf '%b\n' "$(BLUE)ℹ️  Syncing dependencies with UV...$(END)"
	@$(UV) sync --all-packages --all-groups || { \
		printf '%b\n' "$(RED)❌ Failed to sync packages$(END)"; \
		exit 1; \
	}
	@printf '%b\n' "$(BLUE)ℹ️  Installing QS-Trader in editable mode...$(END)"
	@$(UV) pip install -e . --quiet || { \
		printf '%b\n' "$(RED)❌ Failed to install package$(END)"; \
		exit 1; \
	}
	@printf '%b\n' "$(GREEN)✅ Dependencies synced successfully$(END)"

.PHONY: upgrade
upgrade: check-uv ## 🔄 Upgrade all packages to latest versions
	@printf '%b\n' "$(BLUE)ℹ️  Upgrading all packages with UV...$(END)"
	@$(UV) lock --upgrade || { \
		printf '%b\n' "$(RED)❌ Failed to upgrade packages$(END)"; \
		exit 1; \
	}
	@printf '%b\n' "$(BLUE)ℹ️  Syncing upgraded dependencies...$(END)"
	@$(UV) sync --all-packages --all-groups || { \
		printf '%b\n' "$(RED)❌ Failed to sync upgraded packages$(END)"; \
		exit 1; \
	}
	@printf '%b\n' "$(GREEN)✅ All packages upgraded and synced successfully$(END)"

.PHONY: install-hooks
install-hooks: sync ## 🪝 Install pre-commit hooks
	@printf '%b\n' "$(BLUE)ℹ️  Installing pre-commit hooks...$(END)"
	@$(UV) run pre-commit install || { \
		printf '%b\n' "$(RED)❌ Failed to install pre-commit hooks$(END)"; \
		exit 1; \
	}
	@printf '%b\n' "$(GREEN)✅ Pre-commit hooks installed$(END)"

.PHONY: pre-commit
pre-commit: sync ## 🔍 Run pre-commit hooks manually
	@printf '%b\n' "$(BLUE)ℹ️  Running pre-commit hooks...$(END)"
	@$(UV) run pre-commit run --all-files || { \
		printf '%b\n' "$(RED)❌ Pre-commit hooks failed$(END)"; \
		exit 1; \
	}
	@printf '%b\n' "$(GREEN)✅ Pre-commit hooks passed$(END)"

.PHONY: setup
setup: sync install-hooks ## 🚀 Complete development environment setup
	@printf '%b\n' "$(GREEN)✅ Development environment setup complete!$(END)"
	@printf '%b\n' "$(BLUE)💡 Use 'uv run <command>' to run commands in the environment$(END)"
	@printf '%b\n' "$(BLUE)💡 Example: uv run python $(SRC_DIR)/main.py$(END)"

.PHONY: clean
clean: ## 🧹 Clean workspace (remove cache, temp files, and scaffolded project files)
	@printf '%b\n' "$(BLUE)ℹ️  Cleaning development environment...$(END)"
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@find . -type f -name "*.pyo" -delete 2>/dev/null || true
	@rm -rf build/ dist/ *.egg-info .pytest_cache/ .ruff_cache/ .mypy_cache/
	@rm -f .coverage coverage.xml
	@rm -rf htmlcov/ mypy-report/ .coverage.*
	@printf '%b\n' "$(BLUE)ℹ️  Removing scaffolded project files...$(END)"
	@rm -rf config/ library/ data/ output/ logs/ examples/ experiments/ QS_TRADER_README.md
	@printf '%b\n' "$(GREEN)✅ Workspace cleaned$(END)"


################################################################################
# CODE QUALITY
################################################################################

.PHONY: format
format: sync ## 🎨 Format code with ruff, isort, and markdown (matches pre-commit)
	@printf '%b\n' "$(BLUE)ℹ️  Formatting Python code with ruff (fix + format)...$(END)"
	@$(UV) run ruff check --fix --target-version py313 $(SRC_DIR)/
	@$(UV) run ruff format --target-version py313 $(SRC_DIR)/
	@printf '%b\n' "$(BLUE)ℹ️  Formatting imports with isort...$(END)"
	@$(UV) run isort $(SRC_DIR)/
	@printf '%b\n' "$(BLUE)ℹ️  Formatting Markdown files...$(END)"
	@$(UV) run mdformat . --wrap=no --end-of-line=lf || printf '%b\n' "$(YELLOW)⚠️  mdformat not installed, run 'uv add --dev mdformat mdformat-gfm mdformat-tables'$(END)"
	@printf '%b\n' "$(GREEN)✅ Code and markdown formatting completed$(END)"

.PHONY: lint
lint: sync ## 🔍 Lint code and fix auto-fixable issues (matches pre-commit)
	@printf '%b\n' "$(BLUE)ℹ️  Linting code...$(END)"
	@$(UV) run ruff check --fix --target-version py313 $(SRC_DIR)/
	@printf '%b\n' "$(GREEN)✅ Code linting completed$(END)"

.PHONY: lint-check
lint-check: sync ## 📋 Check code without making changes (matches pre-commit)
	@printf '%b\n' "$(BLUE)ℹ️  Checking code quality...$(END)"
	@$(UV) run ruff check --target-version py313 $(SRC_DIR)/
	@$(UV) run ruff format --target-version py313 --check $(SRC_DIR)/
	@$(UV) run isort --check-only $(SRC_DIR)/
	@printf '%b\n' "$(GREEN)✅ Code quality check passed$(END)"

.PHONY: format-md
format-md: sync ## 📝 Format Markdown files only
	@printf '%b\n' "$(BLUE)ℹ️  Formatting Markdown files...$(END)"
	@$(UV) run mdformat . --wrap=no --end-of-line=lf || printf '%b\n' "$(YELLOW)⚠️  mdformat not installed, run 'uv add --dev mdformat mdformat-gfm mdformat-tables'$(END)"
	@printf '%b\n' "$(GREEN)✅ Markdown formatting completed$(END)"

.PHONY: type-check
type-check: sync ## 🔬 Run type checking with MyPy
	@printf '%b\n' "$(BLUE)ℹ️  Running type checks with MyPy...$(END)"
	@$(UV) run mypy $(SRC_DIR)/ || { \
		printf '%b\n' "$(RED)❌ Type checking failed$(END)"; \
		exit 1; \
	}
	@printf '%b\n' "$(GREEN)✅ Type checking completed$(END)"

.PHONY: quality
quality: format lint-check type-check ## 🏆 Run all code quality checks
	@printf '%b\n' "$(GREEN)✅ All code quality checks passed$(END)"

.PHONY: qa
qa: quality test ## 🔍 Full quality assurance (code quality + tests)
	@printf '%b\n' "$(GREEN)✅ Quality assurance complete - ready for production!$(END)"


################################################################################
# BUILD & RELEASE
################################################################################

.PHONY: build
build: clean qa ## 📦 Build package (clean + qa + uv build)
	@printf '%b\n' "$(BLUE)ℹ️  Building package with uv...$(END)"
	@$(UV) build
	@printf '%b\n' "$(GREEN)✅ Package built successfully$(END)"
	@printf '%b\n' "$(CYAN)📦 Distribution files:$(END)"
	@ls -lh dist/

.PHONY: version
version: ## 📋 Show current version from pyproject.toml
	@grep '^version = ' pyproject.toml | sed 's/version = "\(.*\)"/\1/'

.PHONY: release-prepare
release-prepare: qa ## 🚀 Prepare release (run QA, show version, prompt for confirmation)
	@printf '%b\n' "$(CYAN)════════════════════════════════════════════════════════════════$(END)"
	@printf '%b\n' "$(BOLD)📦 Release Preparation$(END)"
	@printf '%b\n' "$(CYAN)════════════════════════════════════════════════════════════════$(END)"
	@echo ""
	@printf '%b\n' "$(BLUE)Current version:$(END) $$(grep '^version = ' pyproject.toml | sed 's/version = "\(.*\)"/\1/')"
	@echo ""
	@printf '%b\n' "$(YELLOW)⚠️  Before releasing:$(END)"
	@echo "  1. Update version in pyproject.toml if needed"
	@echo "  2. Update CHANGELOG.md with release notes"
	@printf '%b\n' "  3. Commit all changes: $(CYAN)git add -A && git commit -m 'chore: prepare release vX.Y.Z'$(END)"
	@printf '%b\n' "  4. Run: $(CYAN)make release VERSION=X.Y.Z$(END)"
	@echo ""
	@printf '%b\n' "$(GREEN)✅ QA checks passed - ready for release$(END)"

.PHONY: release
release: ## 🚀 Create GitHub release (usage: make release VERSION=x.y.z)
	@if [ -z "$(VERSION)" ]; then \
		printf '%b\n' "$(RED)❌ VERSION not specified$(END)"; \
		printf '%b\n' "$(YELLOW)Usage: make release VERSION=x.y.z$(END)"; \
		printf '%b\n' "$(YELLOW)Example: make release VERSION=0.2.0$(END)"; \
		exit 1; \
	fi
	@printf '%b\n' "$(CYAN)════════════════════════════════════════════════════════════════$(END)"
	@printf '%b\n' "$(BOLD)🚀 Creating GitHub Release v$(VERSION)$(END)"
	@printf '%b\n' "$(CYAN)════════════════════════════════════════════════════════════════$(END)"
	@echo ""
	@CURRENT_VERSION=$$(grep '^version = ' pyproject.toml | sed 's/version = "\(.*\)"/\1/'); \
	if [ "$$CURRENT_VERSION" != "$(VERSION)" ]; then \
		printf '%b\n' "$(RED)❌ Version mismatch!$(END)"; \
		printf '%b\n' "$(YELLOW)pyproject.toml has: $$CURRENT_VERSION$(END)"; \
		printf '%b\n' "$(YELLOW)You specified: $(VERSION)$(END)"; \
		printf '%b\n' "$(YELLOW)Update pyproject.toml first or use VERSION=$$CURRENT_VERSION$(END)"; \
		exit 1; \
	fi
	@printf '%b\n' "$(BLUE)ℹ️  Checking git status...$(END)"
	@if [ -n "$$(git status --porcelain)" ]; then \
		printf '%b\n' "$(RED)❌ Working directory is not clean$(END)"; \
		printf '%b\n' "$(YELLOW)Commit or stash changes before releasing$(END)"; \
		git status --short; \
		exit 1; \
	fi
	@printf '%b\n' "$(GREEN)✅ Working directory is clean$(END)"
	@echo ""
	@printf '%b\n' "$(BLUE)ℹ️  Checking if tag v$(VERSION) already exists...$(END)"
	@if git rev-parse "v$(VERSION)" >/dev/null 2>&1; then \
		printf '%b\n' "$(RED)❌ Tag v$(VERSION) already exists$(END)"; \
		exit 1; \
	fi
	@printf '%b\n' "$(GREEN)✅ Tag is available$(END)"
	@echo ""
	@printf '%b\n' "$(BLUE)ℹ️  Building release artifacts...$(END)"
	@$(MAKE) build
	@echo ""
	@printf '%b\n' "$(BLUE)ℹ️  Creating git tag v$(VERSION)...$(END)"
	@git tag -a "v$(VERSION)" -m "Release version $(VERSION)"
	@printf '%b\n' "$(GREEN)✅ Tag created$(END)"
	@echo ""
	@printf '%b\n' "$(BLUE)ℹ️  Pushing tag to GitHub...$(END)"
	@git push origin "v$(VERSION)"
	@printf '%b\n' "$(GREEN)✅ Tag pushed to GitHub$(END)"
	@echo ""
	@printf '%b\n' "$(CYAN)════════════════════════════════════════════════════════════════$(END)"
	@printf '%b\n' "$(GREEN)✅ Release v$(VERSION) created successfully!$(END)"
	@printf '%b\n' "$(CYAN)════════════════════════════════════════════════════════════════$(END)"
	@echo ""
	@printf '%b\n' "$(YELLOW)📋 Next steps:$(END)"
	@printf '%b\n' "  1. Go to: https://github.com/QuantSpaceGit/QS-Trader/releases/new?tag=v$(VERSION)"
	@echo "  2. GitHub will auto-detect the tag"
	@echo "  3. Add release notes from CHANGELOG.md"
	@echo "  4. Attach files from dist/ directory:"
	@printf '%b\n' "     - dist/qs_trader-$(VERSION)-py3-none-any.whl"
	@printf '%b\n' "     - dist/qs_trader-$(VERSION).tar.gz"
	@echo "  5. Click 'Publish release'"
	@echo ""
	@printf '%b\n' "$(BLUE)💡 Or use GitHub CLI if installed:$(END)"
	@printf '%b\n' "  $(CYAN)gh release create v$(VERSION) dist/* --title 'Release v$(VERSION)' --notes-file CHANGELOG.md$(END)"
	@echo ""


################################################################################
# TESTING
################################################################################

.PHONY: test
test: sync ## 🧪 Run all tests with coverage
	@printf '%b\n' "$(BLUE)ℹ️  Running all tests with coverage...$(END)"
	@$(UV) run pytest --cov --cov-report=term-missing --cov-report=html || { \
		printf '%b\n' "$(RED)❌ Tests failed$(END)"; \
		exit 1; \
	}
	@printf '%b\n' "$(GREEN)✅ All tests passed$(END)"

.PHONY: test-fast
test-fast: sync ## ⚡ Run tests without coverage (faster)
	@printf '%b\n' "$(BLUE)ℹ️  Running tests (fast mode)...$(END)"
	@$(UV) run pytest -v || { \
		printf '%b\n' "$(RED)❌ Tests failed$(END)"; \
		exit 1; \
	}
	@printf '%b\n' "$(GREEN)✅ All tests passed$(END)"
