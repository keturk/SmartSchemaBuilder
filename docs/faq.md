# Frequently Asked Questions (FAQ)

Here are some frequently asked questions about the Smart Schema Builder tool:

1. **What databases are supported by the Smart Schema Builder tool?**
   - The tool supports PostgreSQL, MySQL, and SQL Server databases.


2. **Can I use the tool without AI for suggesting table names?**
   - Yes, the tool can be used without any AI provider. If no AI provider is available, the functionality dependent on AI will be skipped and the tool will continue with basic table name generation.

3. **What AI providers are supported?**
   - **Ollama** (recommended): Free, local AI processing. Install from [ollama.ai](https://ollama.ai) and run `ollama serve`.
   - **OpenAI**: Cloud-based AI service. Requires an API key from [platform.openai.com](https://platform.openai.com).
   - **Auto-detection**: The tool automatically tries Ollama first, then OpenAI, then continues without AI if neither is available.

4. **How do I set up Ollama?**
   - Install Ollama from [ollama.ai](https://ollama.ai)
   - Pull a model: `ollama pull llama2:7b`
   - Start the service: `ollama serve`
   - The tool will automatically detect and use Ollama if it's running


5. **How can I contribute to the Smart Schema Builder project?**
   - To contribute, please refer to the contributing guidelines available in the project's repository.


6. **How can I join the discussion?**
   - To join the discussion, please refer to the [discussions section](https://github.com/keturk/SmartSchemaBuilder/discussions) in the project's repository.


7. **Does the Smart Schema Builder tool handle complex database relationships, such as many-to-many relationships?**
   - No. The Smart Schema Builder tool is still in its early stages of development and should be used with caution. While the tool aims to automate the process of generating database schemas, it is essential to thoroughly review and validate the generated schema before deploying. It is recommended to perform extensive testing and validation to ensure the accuracy and integrity of the generated schema for your specific use case. Additionally, it's advisable to keep backups of your data.

 
8. **Can I exclude specific tables or columns from being included in the generated schema?**
   - Yes, you can exclude specific tables or columns by modifying the code in the `generate_sql_files.py` script. Look for the relevant sections where tables and columns are processed, and add the necessary logic to exclude the desired tables or columns based on your criteria.

