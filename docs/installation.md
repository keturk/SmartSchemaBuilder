# Installation

To install and set up the Smart Schema Builder tool, follow the steps below:

## Prerequisites

- Python 3.9 or higher
- pip (Python package installer)

## Installation Methods

### Method 1: Using pip (Recommended)

1. Clone the repository:
   ```bash
   git clone https://github.com/keturk/smart-schema-builder.git
   cd smart-schema-builder
   ```

2. Create a virtual environment (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Method 2: Development Installation

For development and testing:

```bash
pip install -r requirements-dev.txt
```

### Method 3: Using pip install (if published to PyPI)

```bash
pip install smart-schema-builder
```

## Environment Variables

Set up the following environment variables in a `.env` file:

- `DB_PASSWORD`: Database password (used by `export_to_csv.py` and `run_sql_files.py`)
- `AI_PROVIDER`: AI provider to use (`openai`, `ollama`, or leave empty for auto-detection)
- `OPENAI_API_KEY`: OpenAI API key (used when OpenAI is selected)
- `OPENAI_MODEL`: OpenAI model to use (default: "gpt-3.5-turbo")
- `OPENAI_MAX_TOKENS`: Maximum tokens for OpenAI API calls (default: 1024)
- `OLLAMA_BASE_URL`: Ollama server URL (default: "http://localhost:11434")
- `OLLAMA_MODEL`: Ollama model to use (default: "llama2:7b")
- `OLLAMA_MAX_TOKENS`: Maximum tokens for Ollama calls (default: 1024)

### Example .env file:
```env
# Database Configuration
DB_PASSWORD=your_database_password

# AI Provider Configuration (optional - auto-detects if not set)
AI_PROVIDER=ollama  # or 'openai' or leave empty for auto-detection

# OpenAI Configuration (if using OpenAI)
OPENAI_API_KEY=your_openai_api_key
OPENAI_MODEL=gpt-3.5-turbo
OPENAI_MAX_TOKENS=1024

# Ollama Configuration (if using Ollama)
OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_MODEL=llama2:7b
OLLAMA_MAX_TOKENS=1024
```

## AI Provider Setup

### Option 1: Ollama (Recommended - Free & Local)

1. **Install Ollama**: Download and install from [ollama.ai](https://ollama.ai)
2. **Pull a model**: 
   ```bash
   ollama pull llama2:7b
   # or for a smaller model:
   ollama pull llama2:7b-chat
   ```
3. **Start Ollama service**: 
   ```bash
   ollama serve
   ```
4. **Configure environment**: Set `AI_PROVIDER=ollama` in your `.env` file

### Option 2: OpenAI (Cloud-based)

1. **Get API key**: Sign up at [platform.openai.com](https://platform.openai.com)
2. **Configure environment**: Set `AI_PROVIDER=openai` and `OPENAI_API_KEY=your_key` in your `.env` file

### Auto-Detection

If you don't set `AI_PROVIDER`, the tool will automatically:
1. Try to use Ollama if available
2. Fall back to OpenAI if API key is provided
3. Continue without AI features if neither is available

## Database Drivers

The tool supports multiple database systems. Install the appropriate driver:

- **PostgreSQL**: `psycopg2-binary` (included in requirements.txt)
- **MySQL**: `mysql-connector-python` (included in requirements.txt)
- **SQL Server**: `pyodbc` (included in requirements.txt)

## Verification

To verify the installation, run:
```bash
python generate_sql_files.py --help
```

This should display the help message for the script.
