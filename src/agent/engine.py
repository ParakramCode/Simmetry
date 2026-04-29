import json
import logging
from typing import List, Dict, Any, Optional

try:
    import ollama
    OLLAMA_AVAILABLE = True
except ImportError:
    OLLAMA_AVAILABLE = False

try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False

from .prompts import RACE_ENGINEER_SYSTEM_PROMPT
from .tools import AGENT_TOOLS

logger = logging.getLogger(__name__)

class RaceEngineerAgent:
    """
    Orchestrates the AI agent.
    Primary: Ollama (local)
    Fallback: Gemini (cloud)
    """
    def __init__(self, ollama_model: str = "llama3", gemini_model: str = "gemini-1.5-pro-latest"):
        self.ollama_model = ollama_model
        self.gemini_model = gemini_model
        
        # Tools defined in OpenAI function calling schema for Ollama/Gemini natively.
        # In a fully fleshed out system, you'd auto-generate this from Pydantic.
        self.tools_schema = [
            {
                "type": "function",
                "function": {
                    "name": "get_last_session",
                    "description": "Returns metadata about the most recent telemetry session, including track, car, and best lap.",
                    "parameters": {"type": "object", "properties": {}}
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "compare_laps",
                    "description": "Returns delta metrics between two laps.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "lap_a": {"type": "integer"},
                            "lap_b": {"type": "integer"}
                        },
                        "required": ["lap_a", "lap_b"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "get_setup_recommendations",
                    "description": "Passes symptoms to the Setup Recommendation Engine and returns rules-based setup advice. Symptoms are boolean flags.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "symptoms_dict": {
                                "type": "object",
                                "description": "Dictionary of symptom booleans like mid_corner_understeer, exit_wheelspin, etc."
                            }
                        },
                        "required": ["symptoms_dict"]
                    }
                }
            }
        ]

    def chat(self, user_message: str, chat_history: List[Dict[str, str]] = None) -> str:
        messages = [{"role": "system", "content": RACE_ENGINEER_SYSTEM_PROMPT}]
        if chat_history:
            messages.extend(chat_history)
        messages.append({"role": "user", "content": user_message})

        try:
            if OLLAMA_AVAILABLE:
                return self._run_ollama(messages)
            else:
                logger.warning("Ollama not available. Falling back to Gemini.")
                return self._run_gemini(messages)
        except Exception as e:
            logger.error(f"Ollama failed: {e}. Attempting fallback to Gemini.")
            if GEMINI_AVAILABLE:
                return self._run_gemini(messages)
            return "Error: Both Ollama and Gemini failed. Please check your setup."

    def _run_ollama(self, messages: List[Dict[str, str]]) -> str:
        # Ollama v0.1.30+ supports tools
        response = ollama.chat(
            model=self.ollama_model,
            messages=messages,
            tools=self.tools_schema,
        )
        
        # Check if the model decided to call a tool
        if response.get("message", {}).get("tool_calls"):
            for tool_call in response["message"]["tool_calls"]:
                function_name = tool_call["function"]["name"]
                arguments = tool_call["function"]["arguments"]
                
                # Execute tool
                if function_name in AGENT_TOOLS:
                    logger.info(f"Ollama calling tool: {function_name}({arguments})")
                    tool_result = AGENT_TOOLS[function_name](**arguments)
                    
                    # Append result to messages and recurse
                    messages.append(response["message"])
                    messages.append({
                        "role": "tool",
                        "content": json.dumps(tool_result),
                        "name": function_name
                    })
            
            # Get final answer after tool execution
            final_response = ollama.chat(model=self.ollama_model, messages=messages)
            return final_response["message"]["content"]
            
        return response["message"]["content"]

    def _run_gemini(self, messages: List[Dict[str, str]]) -> str:
        if not GEMINI_AVAILABLE:
            return "Error: google-generativeai is not installed."
            
        # Simplified Gemini fallback (assuming tools handled similarly or handled purely via text)
        # For full implementation, map self.tools_schema to Gemini's `tools` parameter.
        # Here we do a standard text generation fallback for brevity.
        model = genai.GenerativeModel(self.gemini_model)
        
        # Convert standard OpenAI message format to Gemini format
        prompt = ""
        for m in messages:
            prompt += f"{m['role'].upper()}:\n{m['content']}\n\n"
            
        response = model.generate_content(prompt)
        return response.text
