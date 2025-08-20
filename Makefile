.PHONY: help install run watch health test clean lint

# Default target
help:
	@echo "Google Sheets Reader - Secure CLI Tool"
	@echo ""
	@echo "Available commands:"
	@echo "  install    Install dependencies"
	@echo "  run        Run single extraction (example: --filter 'status=PUBLISHED' --limit 100)"
	@echo "  watch      Start continuous monitoring (example: --interval 300)"
	@echo "  health     Run health check"
	@echo "  test       Run tests"
	@echo "  lint       Run code linting"
	@echo "  clean      Clean temporary files"
	@echo ""
	@echo "Examples:"
	@echo "  make run ARGS='--filter \"status=PUBLISHED\" --limit 100'"
	@echo "  make run ARGS='--columns \"title,status,date\" --out results.json'"
	@echo "  make watch ARGS='--interval 300 --filter \"status=ACTIVE\"'"
	@echo "  make health"

# Install dependencies
install:
	@echo "Installing dependencies..."
	pip install -r requirements.txt
	@echo "Dependencies installed successfully"

# Check environment setup
check-env:
	@echo "Checking environment configuration..."
	@python3 -c "from config import check_environment; import sys; sys.exit(0 if check_environment() else 1)" || \
	(echo "Environment not configured. Please copy .env.example to .env and fill in your values." && exit 1)
	@echo "Environment configuration OK"

# Run single extraction
run: check-env
	@echo "Running single extraction..."
	python3 app.py run $(ARGS)

# Start watch mode
watch: check-env
	@echo "Starting watch mode..."
	python3 app.py watch $(ARGS)

# Run health check
health: check-env
	@echo "Running health check..."
	python3 app.py health $(ARGS)

# Run tests
test:
	@echo "Running tests..."
	python3 -m pytest tests/ -v $(ARGS)

# Run code linting
lint:
	@echo "Running code linting..."
	@which flake8 > /dev/null || (echo "flake8 not installed. Run: pip install flake8" && exit 1)
	flake8 *.py tests/ --max-line-length=100 --ignore=E501

# Clean temporary files
clean:
	@echo "Cleaning temporary files..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.log" -delete
	rm -rf .pytest_cache/
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	@echo "Cleanup complete"

# Development setup
dev-setup: install
	@echo "Setting up development environment..."
	pip install pytest flake8 black
	@echo "Development environment ready"

# Format code
format:
	@echo "Formatting code..."
	@which black > /dev/null || (echo "black not installed. Run: pip install black" && exit 1)
	black *.py tests/ --line-length=100

# Show logs
logs:
	@echo "Recent application logs:"
	@if [ -f "app.log" ]; then tail -50 app.log; else echo "No log file found"; fi

# Environment setup helper
setup-env:
	@echo "Setting up environment..."
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo ".env file created from template"; \
		echo "Please edit .env and add your credentials"; \
	else \
		echo ".env file already exists"; \
	fi

# Quick start guide
quickstart:
	@echo "Quick Start Guide:"
	@echo ""
	@echo "1. Setup environment:"
	@echo "   make setup-env"
	@echo "   # Edit .env file with your credentials"
	@echo ""
	@echo "2. Install dependencies:"
	@echo "   make install"
	@echo ""
	@echo "3. Test connection:"
	@echo "   make health"
	@echo ""
	@echo "4. Run extraction:"
	@echo "   make run"
	@echo ""
	@echo "5. Start monitoring:"
	@echo "   make watch"

# Show status
status:
	@echo "System Status:"
	@echo "=============="
	@echo -n "Environment: "
	@python3 -c "from config import check_environment; print('✓ Configured' if check_environment() else '✗ Not configured')" 2>/dev/null || echo "✗ Not configured"
	@echo -n "Dependencies: "
	@python3 -c "import gspread; print('✓ Installed')" 2>/dev/null || echo "✗ Missing"
	@echo -n "Connection: "
	@make health > /dev/null 2>&1 && echo "✓ Healthy" || echo "✗ Failed"

# Deploy to Vercel with environment variables
deploy:
	@echo "Deploying to Vercel with environment variables..."
	@if [ -z "$(VERCEL_TOKEN)" ]; then \
		echo "❌ VERCEL_TOKEN not set. Get it from https://vercel.com/account/tokens"; \
		exit 1; \
	fi
	@if [ ! -f .env ]; then \
		echo "❌ .env file not found. Run 'make setup-env' first"; \
		exit 1; \
	fi
	@echo "🚀 Deploying with DATABASE_URL..."
	vercel --prod --token=$(VERCEL_TOKEN) -e DATABASE_URL="$$(grep DATABASE_URL .env | cut -d '=' -f2-)"

# Setup Vercel project
setup-vercel:
	@echo "Setting up Vercel project..."
	@which vercel > /dev/null || (echo "❌ Vercel CLI not installed. Run: npm i -g vercel" && exit 1)
	vercel link
	@echo "✅ Vercel project linked"