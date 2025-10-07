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
import logging
from typing import List, Optional

# Import the new AI provider system
from common.ai_provider import get_ai_provider, generate_table_names


def ask_openai(prompt: str, file_ids: Optional[List[str]] = None, completions: int = 1) -> dict:
    """
    Generate a response using the configured AI provider.
    
    This function maintains backward compatibility with the old API while using the new AI provider system.

    Args:
        prompt (str): The prompt to send to the AI provider.
        file_ids (list, optional): List of IDs of files to include with the prompt (not supported in new system).
        completions (int, optional): Number of completions to generate.

    Returns:
        dict: The response in the old format for backward compatibility.
    """
    provider = get_ai_provider()
    
    if provider is None:
        logging.warning("No AI provider available. Returning empty response.")
        return {
            'choices': [{'text': '', 'finish_reason': 'stop'}]
        }
    
    try:
        response_text = provider.generate_text(prompt)
        return {
            'choices': [{'text': response_text, 'finish_reason': 'stop'}]
        }
    except Exception as e:
        logging.error(f"AI provider error: {e}")
        return {
            'choices': [{'text': '', 'finish_reason': 'error'}]
        }


def ask_openai_multipart(prompt: str, file_ids: Optional[List[str]] = None, completions: int = 1) -> str:
    """
    Generate a multipart response using the configured AI provider.

    Args:
        prompt (str): The prompt to send to the AI provider.
        file_ids (list, optional): List of IDs of files to include with the prompt (not supported).
        completions (int, optional): Number of completions to generate.

    Returns:
        str: The concatenated response from the AI provider.
    """
    provider = get_ai_provider()
    
    if provider is None:
        logging.warning("No AI provider available. Returning empty response.")
        return ""
    
    try:
        return provider.generate_text(prompt)
    except Exception as e:
        logging.error(f"AI provider error: {e}")
        return ""


def generate_text_with_large_prompt(prompt: str) -> str:
    """
    Generate text using the configured AI provider for large prompts.

    Args:
        prompt (str): The large prompt to send to the AI provider.

    Returns:
        str: The generated text from the AI provider.
    """
    provider = get_ai_provider()
    
    if provider is None:
        logging.warning("No AI provider available. Returning empty response.")
        return ""
    
    try:
        return provider.generate_text(prompt)
    except Exception as e:
        logging.error(f"AI provider error: {e}")
        return ""


# Legacy functions for backward compatibility
def openai_upload_files(jsonl_filenames: List[str], remove_files: bool = False) -> List[str]:
    """
    Legacy function - file uploads are not supported in the new AI provider system.
    
    Args:
        jsonl_filenames (list): List of filenames to upload.
        remove_files (bool, optional): Whether to remove the files locally after uploading.

    Returns:
        list: Empty list (file uploads not supported).
    """
    logging.warning("File uploads are not supported in the new AI provider system.")
    return []


def cleanup_files():
    """
    Legacy function - no cleanup needed in the new AI provider system.
    """
    pass


def register_file_for_cleanup(file_id: str):
    """
    Legacy function - no cleanup needed in the new AI provider system.
    
    Args:
        file_id (str): ID of the file to be registered.
    """
    pass
