# Installation

To install and set up the Smart Schema Builder tool, follow the steps below:

1. Clone the repository:
   ```bash
   git clone https://github.com/keturk/smart-schema-builder.git
   ```

2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up the environment variables:
   - `DB_PASSWORD`: Specifies the password for the database. This environment variable is used by `export_to_csv.py` and `run_sql_files.py` scripts.
   - `OPENAI_API_KEY`: Specifies the API key for the OpenAI API. This environment variable is used by the `generate_sql_files.py` script. If this variable is not defined, the functionality dependent on the OpenAI API will be skipped.
   - `OPENAI_ENGINE`: Specifies the OpenAI engine to be used. This environment variable is used by the `generate_sql_files.py` script.
   - `OPENAI_MAX_TOKENS`: Specifies the maximum number of tokens to be used in the OpenAI API calls. This environment variable is used by the `generate_sql_files.py` script.

Make sure to set the appropriate values for these environment variables in the `.env` file before running the Smart Schema Builder tool.
