#!/bin/bash

# Weather Data Pipeline - Lakehouse Plumber Setup Script
# This script installs dependencies and initializes the project

echo "🌦️  Setting up Weather Data Pipeline with Lakehouse Plumber Framework"
echo "=================================================================="

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 is required but not installed."
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "🔄 Activating virtual environment..."
source venv/bin/activate

# Install requirements
echo "📥 Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Validate the configuration
echo "✅ Validating Lakehouse Plumber configuration..."
lhp validate --env dev

if [ $? -eq 0 ]; then
    echo "✅ Configuration validation passed!"
else
    echo "❌ Configuration validation failed. Please check your YAML files."
    exit 1
fi

# Display next steps
echo ""
echo "🎉 Setup completed successfully!"
echo ""
echo "Next Steps:"
echo "1. Activate the virtual environment: source venv/bin/activate"
echo "2. Validate configuration: lhp validate --env dev"
echo "3. Generate Python code: lhp generate --env dev --cleanup"
echo "4. Deploy to Databricks: databricks bundle deploy --target dev"
echo ""
echo "For more details, see the README.md file." 