"""
MIT License

Smart Schema Builder

Copyright (c) 2023 Kamil Ercan Turkarslan

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import os
import logging
from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any
from enum import Enum

from common.library import get_str_from_env, get_int_from_env
from common.exceptions import AIProviderError, NetworkError, ConfigurationError, ValidationError
from common.error_handler import handle_errors, retry_on_failure, safe_execute
from common.validators import InputValidator


class AIProvider(Enum):
    """Enumeration of supported AI providers."""
    OPENAI = "openai"
    OLLAMA = "ollama"
    NONE = "none"


class AIProviderBase(ABC):
    """Abstract base class for AI providers."""
    
    def __init__(self, model: str, max_tokens: int = 1024):
        self.model = model
        self.max_tokens = max_tokens
    
    @abstractmethod
    def generate_text(self, prompt: str, **kwargs) -> str:
        """Generate text based on the given prompt."""
        pass
    
    @abstractmethod
    def is_available(self) -> bool:
        """Check if the AI provider is available and configured."""
        pass


class OpenAIProvider(AIProviderBase):
    """OpenAI API provider using the modern v1.x API."""
    
    def __init__(self, model: str = "gpt-3.5-turbo", max_tokens: int = 1024):
        super().__init__(model, max_tokens)
        self.api_key = get_str_from_env("OPENAI_API_KEY", "NOT_DEFINED")
        self._client = None
        
        # Validate inputs
        self.model = InputValidator.validate_table_name(model) if model else "gpt-3.5-turbo"
        self.max_tokens = InputValidator.validate_positive_integer(max_tokens, "max_tokens")
    
    def _get_client(self):
        """Get OpenAI client, creating it if necessary."""
        if self._client is None:
            try:
                import openai
                if self.api_key == "NOT_DEFINED":
                    raise ConfigurationError("OpenAI API key not configured")
                self._client = openai.OpenAI(api_key=self.api_key)
            except ImportError as e:
                raise AIProviderError(
                    "OpenAI package not installed. Install with: pip install openai",
                    provider="openai"
                ) from e
            except Exception as e:
                raise AIProviderError(
                    f"Failed to create OpenAI client: {str(e)}",
                    provider="openai"
                ) from e
        return self._client
    
    @retry_on_failure(max_retries=3, delay=1.0, exceptions=(NetworkError,))
    def generate_text(self, prompt: str, **kwargs) -> str:
        """Generate text using OpenAI API."""
        if not prompt or not isinstance(prompt, str):
            raise ValidationError("Prompt must be a non-empty string", field="prompt", value=prompt)
        
        try:
            client = self._get_client()
            response = client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "user", "content": prompt}
                ],
                max_tokens=self.max_tokens,
                temperature=kwargs.get('temperature', 0),
                **kwargs
            )
            return response.choices[0].message.content
        except Exception as e:
            if "rate limit" in str(e).lower():
                raise NetworkError(f"OpenAI rate limit exceeded: {e}", url="openai.com") from e
            elif "authentication" in str(e).lower():
                raise AIProviderError(f"OpenAI authentication failed: {e}", provider="openai") from e
            else:
                raise AIProviderError(f"OpenAI API error: {e}", provider="openai") from e
    
    def is_available(self) -> bool:
        """Check if OpenAI is available and configured."""
        return self.api_key != "NOT_DEFINED" and self.api_key is not None


class OllamaProvider(AIProviderBase):
    """Ollama local provider."""
    
    def __init__(self, model: str = "llama2:7b", max_tokens: int = 1024):
        super().__init__(model, max_tokens)
        self.base_url = get_str_from_env("OLLAMA_BASE_URL", "http://localhost:11434")
        self._client = None
        
        # Validate inputs
        self.model = InputValidator.validate_table_name(model) if model else "llama2:7b"
        self.max_tokens = InputValidator.validate_positive_integer(max_tokens, "max_tokens")
    
    def _get_client(self):
        """Get Ollama client, creating it if necessary."""
        if self._client is None:
            try:
                import ollama
                self._client = ollama.Client(host=self.base_url)
            except ImportError as e:
                raise AIProviderError(
                    "Ollama package not installed. Install with: pip install ollama",
                    provider="ollama"
                ) from e
            except Exception as e:
                raise AIProviderError(
                    f"Failed to create Ollama client: {str(e)}",
                    provider="ollama"
                ) from e
        return self._client
    
    @retry_on_failure(max_retries=3, delay=1.0, exceptions=(NetworkError,))
    def generate_text(self, prompt: str, **kwargs) -> str:
        """Generate text using Ollama."""
        if not prompt or not isinstance(prompt, str):
            raise ValidationError("Prompt must be a non-empty string", field="prompt", value=prompt)
        
        try:
            client = self._get_client()
            response = client.generate(
                model=self.model,
                prompt=prompt,
                options={
                    'num_predict': self.max_tokens,
                    'temperature': kwargs.get('temperature', 0),
                }
            )
            return response['response']
        except Exception as e:
            if "connection" in str(e).lower() or "refused" in str(e).lower():
                raise NetworkError(f"Ollama connection failed: {e}", url=self.base_url) from e
            elif "model" in str(e).lower() and "not found" in str(e).lower():
                raise AIProviderError(f"Ollama model not found: {e}", provider="ollama") from e
            else:
                raise AIProviderError(f"Ollama API error: {e}", provider="ollama") from e
    
    def is_available(self) -> bool:
        """Check if Ollama is available and running."""
        try:
            client = self._get_client()
            # Try to list models to check if Ollama is running
            models = client.list()
            # Check if the specific model is available
            available_models = models.models if hasattr(models, 'models') else models.get('models', [])
            model_available = any(getattr(model, 'model', model.get('model', '')) == self.model for model in available_models)
            if not model_available:
                available_model_names = [getattr(m, 'model', m.get('model', 'unknown')) for m in available_models]
                logging.warning(f"Model {self.model} not found in Ollama. Available models: {available_model_names}")
            return model_available
        except Exception as e:
            logging.warning(f"Ollama availability check failed: {e}")
            return False


class AIProviderFactory:
    """Factory class for creating AI providers."""
    
    @staticmethod
    def create_provider(provider_type: AIProvider, model: Optional[str] = None, max_tokens: int = 1024) -> AIProviderBase:
        """Create an AI provider instance."""
        if provider_type == AIProvider.OPENAI:
            model = model or get_str_from_env("OPENAI_MODEL", "gpt-3.5-turbo")
            return OpenAIProvider(model, max_tokens)
        elif provider_type == AIProvider.OLLAMA:
            model = model or get_str_from_env("OLLAMA_MODEL", "llama2:7b")
            return OllamaProvider(model, max_tokens)
        else:
            raise ValueError(f"Unsupported AI provider: {provider_type}")
    
    @staticmethod
    def get_available_providers() -> List[AIProvider]:
        """Get list of available AI providers."""
        available = []
        
        # Check OpenAI
        try:
            openai_provider = OpenAIProvider()
            if openai_provider.is_available():
                available.append(AIProvider.OPENAI)
        except Exception:
            pass
        
        # Check Ollama
        try:
            ollama_provider = OllamaProvider()
            if ollama_provider.is_available():
                available.append(AIProvider.OLLAMA)
                logging.info("Ollama provider is available")
            else:
                logging.info("Ollama provider is not available")
        except Exception as e:
            logging.warning(f"Ollama provider check failed: {e}")
            pass
        
        return available
    
    @staticmethod
    def get_preferred_provider() -> AIProvider:
        """Get the preferred AI provider based on availability and configuration."""
        # Check environment variable first
        preferred = get_str_from_env("AI_PROVIDER", "").lower()
        if preferred in ["openai", "ollama"]:
            try:
                provider_type = AIProvider(preferred)
                provider = AIProviderFactory.create_provider(provider_type)
                if provider.is_available():
                    return provider_type
            except Exception:
                pass
        
        # Fallback to available providers
        available = AIProviderFactory.get_available_providers()
        
        # Prefer Ollama if available (local, free)
        if AIProvider.OLLAMA in available:
            return AIProvider.OLLAMA
        
        # Fallback to OpenAI
        if AIProvider.OPENAI in available:
            return AIProvider.OPENAI
        
        # No providers available
        return AIProvider.NONE


def get_ai_provider() -> Optional[AIProviderBase]:
    """Get the configured AI provider."""
    provider_type = AIProviderFactory.get_preferred_provider()
    
    if provider_type == AIProvider.NONE:
        logging.warning("No AI providers available. Table name suggestions will be disabled.")
        return None
    
    try:
        return AIProviderFactory.create_provider(provider_type)
    except Exception as e:
        logging.error(f"Failed to create AI provider: {e}")
        return None


def generate_table_names(csv_filenames: List[str], folder: str) -> List[str]:
    """Generate table names using the configured AI provider."""
    provider = get_ai_provider()
    
    if provider is None:
        # Fallback to simple naming
        return [os.path.splitext(filename)[0].lower() for filename in csv_filenames]
    
    prompt = f"""
    Please suggest meaningful table names for the following CSV files without adding unnecessary repetitive 
    postfixes and prefixes:

    {", ".join(csv_filenames)}

    These files are located in the {folder} folder. Please provide your suggestions in a structured manner, 
    with one suggestion per line.    
    
    Your suggestions are highly appreciated.
    
    If suggestions are multiple words, please use underscores to separate the words.
    """
    
    try:
        response = provider.generate_text(prompt)
        # Parse the response - look for lines that look like table names
        lines = [line.strip() for line in response.split('\n') if line.strip()]
        suggestions = []
        
        for line in lines:
            # Look for patterns like "1. filename - TableName" or just "TableName"
            if ' - ' in line:
                # Extract the part after the dash
                suggestion = line.split(' - ')[-1].strip()
            elif line and not line.startswith(('1.', '2.', '3.', '4.', '5.', '6.', '7.', '8.', '9.')):
                # Take the line as-is if it doesn't start with a number
                suggestion = line
            else:
                continue
                
            # Clean up the suggestion
            suggestion = suggestion.replace('*', '').strip()
            if suggestion and len(suggestion) > 1:
                suggestions.append(suggestion)
        
        # If we don't have enough suggestions, fill with simple names
        while len(suggestions) < len(csv_filenames):
            filename = csv_filenames[len(suggestions)]
            suggestions.append(os.path.splitext(filename)[0].lower())
        
        return suggestions[:len(csv_filenames)]  # Ensure we don't exceed the number of files
    except Exception as e:
        logging.error(f"Failed to generate table names: {e}")
        # Fallback to simple naming
        return [os.path.splitext(filename)[0].lower() for filename in csv_filenames]
