import os
import time
from typing import cast

from dotenv import load_dotenv
from langchain_core.messages import HumanMessage        
from langchain_core.runnables.config import RunnableConfig

from .persistent_agent import (
    create_persistent_agent,
    list_available_threads,
    PersistentAgentState,
)

load_dotenv()


def main():
    """Main CLI interface for the persistent agent"""
    print("🤖 Persistent AI Agent with PostgreSQL")
    print("=" * 60)
    print("Commands:")
    print("  'quit'     - Exit the application")
    print("  'new'      - Start a new conversation thread")
    print("  'list'     - List all conversation threads")
    print("  'switch <thread_id>' - Switch to a specific thread")
    print("  'current'  - Show current thread information")
    print("  'debug'    - Show debug information")
    print("=" * 60)

    # Create the agent
    agent = create_persistent_agent()

    # Initialize conversation tracking
    conversations = {}  # Local tracking
    thread_id = "main_thread"
    user_name = input("What's your name? ").strip() or "User"

    # Add initial conversation
    conversations[thread_id] = {
        "name": "Main Conversation",
        "created_at": time.time(),
        "message_count": 0,
    }

    print(f"\n✅ Hello {user_name}! Session: session_{int(time.time())}")
    print(f"✅ Current thread: {thread_id}")
    print("How can I help you today?\n")

    while True:
        try:
            user_input = input(f"\n{user_name}: ").strip()
            
            # Handle empty input
            if not user_input:
                continue
                
            # Handle quit commands
            if user_input.lower() in ["quit", "exit", "bye"]:
                print("👋 Goodbye!")
                break
                
            # Handle new thread command
            elif user_input.lower() == "new":
                thread_id = f"thread_{int(time.time())}"
                conversations[thread_id] = {
                    "name": f"Conversation {len(conversations)}",
                    "created_at": time.time(),
                    "message_count": 0,
                }
                print(f"🆕 New conversation started: {thread_id}")
                continue
                
            # Handle list command
            elif user_input.lower() == "list":
                list_available_threads(agent)
                print("\n📋 Local tracked conversations:")
                for conv_id, conv_info in conversations.items():
                    status = "🟢" if conv_id == thread_id else "⚪"
                    print(f"{status} {conv_id}: {conv_info['name']} ({conv_info['message_count']} messages)")
                continue
                
            # Handle current command
            elif user_input.lower() == "current":
                print(f"📍 Current thread: {thread_id}")
                if thread_id in conversations:
                    info = conversations[thread_id]
                    print(f"   Name: {info['name']}")
                    print(f"   Messages: {info['message_count']}")
                    print(f"   Created: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(info['created_at']))}")
                continue
                
            # Handle switch command
            elif user_input.lower().startswith("switch "):
                new_thread_id = user_input[7:].strip()
                if new_thread_id in conversations:
                    thread_id = new_thread_id
                    print(f"🔄 Switched to conversation: {thread_id}")
                else:
                    print(f"❌ Conversation '{new_thread_id}' not found. Use 'list' to see available conversations.")
                continue
                
            # Handle debug command
            elif user_input.lower() == "debug":
                print(f"🔍 Debug Information:")
                print(f"   Thread ID: {thread_id}")
                print(f"   User Name: {user_name}")
                print(f"   Total Conversations: {len(conversations)}")
                print(f"   OpenAI API Key: {'Set' if os.getenv('OPENAI_API_KEY') else 'NOT SET'}")
                print(f"   Model: {os.getenv('OPENAI_MODEL', 'gpt-3.5-turbo')}")
                continue

            # Configuration for the current thread
            config: RunnableConfig = {"configurable": {"thread_id": thread_id}}

            # Get the current state from checkpoint or initialize
            try:
                current_state = agent.get_state(config)
                conversation_count = current_state.values.get("conversation_count", 0)
                print(f"📊 Loaded state: {conversation_count} previous messages in thread")
            except Exception as e:
                print(f"⚠️ Could not load previous state: {e}")
                conversation_count = conversations[thread_id]["message_count"]

            # Build the input state with only the new message
            initial_state: PersistentAgentState = {
                "messages": [HumanMessage(content=user_input)],
                "user_name": user_name,
                "conversation_count": conversation_count,
                "session_id": f"session_{int(time.time())}",
                "thread_metadata": {"thread_id": thread_id},
            }

            # Invoke the agent
            print(f"🤔 Processing your question...")
            result = agent.invoke(initial_state, config)

            # Update local conversation metadata
            conversations[thread_id]["message_count"] += 1

            # Display response
            response_content = result['messages'][-1].content
            if response_content and response_content.strip():
                print(f"Agent: {response_content}")
            else:
                print("Agent: ⚠️ (No response generated - this may indicate an API issue)")
            print("-" * 60)

        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"❌ Error: {e}")
            print("💡 Use 'debug' command to see configuration details")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    main()