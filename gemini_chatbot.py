import os
import json
import asyncio
import google.generativeai as genai
from dotenv import load_dotenv
from prompts import system_prompt

# Load environment variables
load_dotenv()
# Import the tools
from tools import tool_registry

try:
    genai.configure(api_key=os.environ["GEMINI_API_KEY"])
except KeyError:
    print("Error: GEMINI_API_KEY environment variable not found")
    print("Make sure you have a .env file with GEMINI_API_KEY=your_api_key")
    exit(1)

# Maximum number of consecutive tool calls to prevent infinite loops
MAX_CONSECUTIVE_TOOL_CALLS = 15

# Gemini model configuration
generation_config = {
    "temperature": 1,
    "top_p": 0.95,
    "top_k": 64,
    "max_output_tokens": 8192,
    "response_mime_type": "text/plain",
}

safety_settings = [
    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
]

try:
    # Initialize the model without starting a chat session
    model = genai.GenerativeModel(
        model_name="gemini-2.5-flash-latest",
        generation_config=generation_config,
        safety_settings=safety_settings,
        system_instruction=system_prompt # <-- Use the dedicated system instruction parameter
    )
except Exception as e:
    print(f"Error initializing Gemini model: {str(e)}")
    exit(1)


def send_to_gemini(history, stream=True):
    """
    Sends the conversation history to Gemini and returns the AI's response.
    This is now stateless and suitable for a server environment.
    When stream=True, yields chunks of the response as they arrive.
    """
    try:
        # The system prompt is now set in the model, so we can filter it out
        # from the conversational history sent to the API.
        # Gemini expects a specific schema: user -> model -> user -> model...
        gemini_history = []
        for msg in history:
            role = msg["role"]
            content = msg.get("content", "")
            
            # Skip the system message as it's passed separately
            if role == "system":
                continue

            # Convert roles to what Gemini expects ('user' or 'model')
            if role in ["human", "user"]:
                gemini_role = "user"
            elif role in ["assistant", "model", "tool"]:
                 # Treat tool results and assistant messages as 'model' context for the next turn
                gemini_role = "model"
            else:
                continue # Skip unknown roles

            gemini_history.append({"role": gemini_role, "parts": [content]})
            
        if stream:
            # For streaming response
            accumulated_response = ""
            response_stream = model.generate_content(gemini_history, stream=True)
            for chunk in response_stream:
                if chunk.text:
                    accumulated_response += chunk.text
                    yield chunk.text, accumulated_response
        else:
            # For non-streaming response
            response = model.generate_content(gemini_history)
            return response.text

    except Exception as e:
        error_message = f"Error communicating with Gemini API: {str(e)}"
        print(error_message)
        # Yield the error to be sent to the client
        if stream:
            yield error_message, error_message
        else:
            return error_message


async def process_tool_calls(ai_response, conversation_history): # <-- Pass history in
    """Process all tool calls in the AI response."""
    tool_calls = tool_registry.extract_tool_calls(ai_response)

    if not tool_calls:
        return False

    tool_results = []
    for i, (tool_type, tool_value) in enumerate(tool_calls):
        print(f"Executing tool call {i + 1}/{len(tool_calls)}: {tool_type} - {tool_value}")
        result = await tool_registry.execute(tool_type, tool_value)
        print(f"Tool {tool_type} completed.")
        tool_results.append((tool_type, tool_value, result))

    # Add all tool results to the conversation history that was passed in
    for tool_type, tool_value, result in tool_results:
        type_name = "Application" if tool_type == "app" else "Search" if tool_type == "search" else tool_type.capitalize()
        # For Gemini, the result of a tool call is associated with the 'tool' role
        conversation_history.append({
            "role": "tool",
            "content": f"Tool execution result for {tool_type} with args {tool_value}:\n\n{result}"
        })

    return True


async def conversation_loop():
    """Standalone conversation loop for direct command-line testing."""
    # This history is only for this loop, not used by the API server
    local_conversation_history = [
        # System prompt is handled by the model's system_instruction
    ]
    try:
        print("Gemini Assistant initialized. Type 'exit' or 'quit' to end the conversation.")
        while True:
            user_input = input("You: ").strip()
            if user_input.lower() in ["exit", "quit"]:
                print("Exiting gracefully...")
                break

            local_conversation_history.append({"role": "user", "content": user_input})

            accumulated_response = ""
            print("AI: ", end="", flush=True)

            # Pass the local history to the generator
            response_generator = send_to_gemini(local_conversation_history, stream=True)
            async for chunk, accumulated in response_generator:
                print(chunk, end="", flush=True)
                accumulated_response = accumulated
            print()

            local_conversation_history.append({"role": "assistant", "content": accumulated_response})

            tool_call_count = 0
            has_tool_calls = True
            while has_tool_calls and tool_call_count < MAX_CONSECUTIVE_TOOL_CALLS:
                # Pass local history to be modified
                has_tool_calls = await process_tool_calls(accumulated_response, local_conversation_history)
                if has_tool_calls:
                    tool_call_count += 1
                    print("AI: ", end="", flush=True)
                    # Get a new generator
                    response_generator = send_to_gemini(local_conversation_history, stream=True)
                    async for chunk, accumulated in response_generator:
                        print(chunk, end="", flush=True)
                        accumulated_response = accumulated
                    print()
                    local_conversation_history.append({"role": "assistant", "content": accumulated_response})

    except KeyboardInterrupt:
        print("\nGracefully exiting the conversation...")
    except Exception as e:
        print(f"\nAn error occurred: {str(e)}")


if __name__ == "__main__":
    asyncio.run(conversation_loop())