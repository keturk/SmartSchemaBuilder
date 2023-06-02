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
import atexit
import logging
import openai
from common.library import get_str_from_env, get_int_from_env


# Get the OpenAI configuration from environment variables
openai_api_key = get_str_from_env("OPENAI_API_KEY", "NOT_DEFINED")
openai_engine = get_str_from_env("OPENAI_ENGINE", "text-davinci-003")
openai_max_tokens = get_int_from_env("OPENAI_MAX_TOKENS", 1024)

# Set the OpenAI API key
openai.api_key = openai_api_key

# List to store file IDs for cleanup
file_ids_to_delete = []


def register_file_for_cleanup(file_id):
    """
    Register a file ID for cleanup on program exit.

    Args:
        file_id (str): ID of the file to be registered.
    """
    file_ids_to_delete.append(file_id)


# Register cleanup function to be called on program exit
atexit.register(lambda: cleanup_files())


def cleanup_files():
    """
    Cleanup function to delete files registered for cleanup.
    """
    if file_ids_to_delete:
        logging.info("Cleaning up files...")
        for file_id in file_ids_to_delete:
            try:
                openai.File.delete(file_id)
                logging.info(f"File {file_id} deleted successfully.")
            except Exception as e:
                logging.error(f"Error deleting file {file_id}: {str(e)}")


def ask_openai(prompt, file_ids=None, completions=1):
    """
    Generate a response from OpenAI based on a given prompt.

    Args:
        prompt (str): The prompt to send to OpenAI.
        file_ids (list, optional): List of IDs of files to include with the prompt.
        completions (int, optional): Number of completions to generate.

    Returns:
        dict: The response from OpenAI.
    """
    payload = {
        "engine": openai_engine,
        "prompt": prompt,
        "max_tokens": openai_max_tokens,
        "temperature": 0,
        "n": completions,
        "stop": None,
        "echo": False
    }

    if file_ids:
        payload["files"] = file_ids

    try:
        response = openai.Completion.create(**payload)
        return response
    except openai.error.APIError as e:
        logging.error(f"OpenAI API Error: {str(e)}")
        raise


def ask_openai_multipart(prompt, file_ids=None, completions=1):
    """
    Generate a multipart response from OpenAI based on a given prompt.

    Args:
        prompt (str): The prompt to send to OpenAI.
        file_ids (list, optional): List of IDs of files to include with the prompt.
        completions (int, optional): Number of completions to generate.

    Returns:
        str: The concatenated response from OpenAI.
    """
    response = ask_openai(prompt)
    results = response['choices'][0]['text']

    while response['choices'][0]['finish_reason'] != 'stop':
        response = ask_openai(results, file_ids, completions)
        results += response['choices'][0]['text']

    return results


def openai_upload_files(jsonl_filenames, remove_files=False):
    """
    Upload a list of files to OpenAI and optionally delete them locally.

    Args:
        jsonl_filenames (list): List of filenames to upload.
        remove_files (bool, optional): Whether to remove the files locally after uploading.

    Returns:
        list: List of IDs of the uploaded files.
    """
    file_ids = []
    for filename in jsonl_filenames:
        with open(filename, "r") as file:
            try:
                response = openai.File.create(purpose="fine-tune", file=file)
                file_ids.append(response.id)
                register_file_for_cleanup(response.id)
            except openai.error.APIError as e:
                logging.error(f"OpenAI API Error while uploading file {filename}: {str(e)}")
                raise

        if remove_files:
            try:
                os.remove(filename)
            except OSError as e:
                logging.error(f"Error deleting file {filename}: {str(e)}")
                raise

    return file_ids


def generate_text_with_large_prompt(prompt):
    """
    Generate text from OpenAI based on a large prompt by splitting it into chunks.

    Args:
        prompt (str): The large prompt to send to OpenAI.

    Returns:
        str: The generated text from OpenAI.
    """
    # Split the prompt into chunks
    prompt_chunks = [prompt[i:i + openai_max_tokens] for i in range(0, len(prompt), openai_max_tokens)]

    # Generate text for each prompt chunk
    response_chunks = []
    for chunk in prompt_chunks:
        try:
            result = ask_openai(chunk)
            response_chunks.append(str(result))  # Convert dictionary to string
        except openai.error.APIError as e:
            logging.error(f"OpenAI API Error while generating text: {str(e)}")
            raise

    # Concatenate the generated text from all chunks
    generated_text = ' '.join(response_chunks)

    return generated_text
