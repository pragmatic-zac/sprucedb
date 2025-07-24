.PHONY: help install install-dev sync test test-verbose type-check lint format clean build run setup dev check all
.DEFAULT_GOAL := help

# Colors for output
CYAN = \033[36m
GREEN = \033[32m
YELLOW = \033[33m
RED = \033[31m
RESET = \033[0m

help: ## Show this help message
	@echo "$(CYAN)SpruceDB Development Commands$(RESET)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "$(GREEN)%-20s$(RESET) %s\n", $$1, $$2}'

# Environment Setup
setup: ## Initial project setup (run this first)
	@echo "$(CYAN)Setting up SpruceDB development environment...$(RESET)"
	uv venv
	uv sync --group dev
	@echo "$(GREEN)✓ Setup complete! Run 'make dev' to see common commands.$(RESET)"

install: ## Install production dependencies only
	@echo "$(CYAN)Installing production dependencies...$(RESET)"
	uv sync

install-dev: sync ## Install all dependencies including dev tools

sync: ## Sync dependencies from lock file
	@echo "$(CYAN)Syncing dependencies...$(RESET)"
	uv sync --group dev

# Development
dev: ## Show common development commands
	@echo "$(CYAN)Common Development Commands:$(RESET)"
	@echo "  $(GREEN)make test$(RESET)       - Run tests"
	@echo "  $(GREEN)make type-check$(RESET) - Run type checking"
	@echo "  $(GREEN)make check$(RESET)      - Run all checks (tests + types)"
	@echo "  $(GREEN)make run$(RESET)        - Run the database"
	@echo "  $(GREEN)make clean$(RESET)      - Clean up generated files"

run: ## Run the main database application
	@echo "$(CYAN)Starting SpruceDB...$(RESET)"
	uv run python main.py

# Testing
test: ## Run tests
	@echo "$(CYAN)Running tests...$(RESET)"
	uv run pytest

test-verbose: ## Run tests with verbose output
	@echo "$(CYAN)Running tests (verbose)...$(RESET)"
	uv run pytest -v

test-coverage: ## Run tests with coverage report
	@echo "$(CYAN)Running tests with coverage...$(RESET)"
	uv run pytest --cov=src --cov-report=html --cov-report=term

# Type Checking & Linting
type-check: ## Run type checking with mypy
	@echo "$(CYAN)Running type checking...$(RESET)"
	uv run mypy src/

lint: ## Run linting (add ruff later)
	@echo "$(YELLOW)Linting not configured yet. Consider adding ruff.$(RESET)"

format: ## Format code (add ruff later)
	@echo "$(YELLOW)Formatting not configured yet. Consider adding ruff.$(RESET)"

# Combined Checks
check: test type-check ## Run all checks (tests + type checking)
	@echo "$(GREEN)✓ All checks passed!$(RESET)"

all: clean sync check ## Full pipeline: clean, sync, and run all checks

# Utility
clean: ## Clean up generated files and caches
	@echo "$(CYAN)Cleaning up...$(RESET)"
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -rf build/ dist/ htmlcov/ .coverage 2>/dev/null || true
	@echo "$(GREEN)✓ Cleanup complete!$(RESET)"

build: ## Build the package
	@echo "$(CYAN)Building package...$(RESET)"
	uv build

# Lock file management
lock: ## Generate/update lock file
	@echo "$(CYAN)Updating lock file...$(RESET)"
	uv lock

# Add dependencies (examples)
add-dep: ## Add a production dependency (usage: make add-dep DEP=package-name)
	@if [ -z "$(DEP)" ]; then echo "$(RED)Usage: make add-dep DEP=package-name$(RESET)"; exit 1; fi
	uv add $(DEP)

add-dev-dep: ## Add a development dependency (usage: make add-dev-dep DEP=package-name)
	@if [ -z "$(DEP)" ]; then echo "$(RED)Usage: make add-dev-dep DEP=package-name$(RESET)"; exit 1; fi
	uv add --dev $(DEP)
