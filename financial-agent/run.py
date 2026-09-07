import asyncio
import sys
from dotenv import load_dotenv
from analyst.graph import graph

load_dotenv()

# ANSI color codes for pretty terminal output
CYAN    = "\033[96m"
YELLOW  = "\033[93m"
GREEN   = "\033[92m"
MAGENTA = "\033[95m"
RESET   = "\033[0m"
BOLD    = "\033[1m"
DIM     = "\033[2m"


async def main():
    print(f"\n{BOLD}{'='*60}{RESET}")
    print(f"{BOLD}  💹 Financial Agent — Drift Inspector{RESET}")
    print(f"{BOLD}{'='*60}{RESET}\n")

    # Accept news text from terminal
    if len(sys.argv) > 1:
        # Passed as a CLI argument: python run.py "Apple reported record profits..."
        news = " ".join(sys.argv[1:])
    else:
        # Interactive prompt in terminal
        print(f"{DIM}Paste or type the financial news you want to analyse.{RESET}")
        print(f"{DIM}Press Enter twice (blank line) to submit, or Ctrl+C to quit.{RESET}\n")
        lines = []
        try:
            while True:
                line = input()
                if line == "" and lines:
                    break
                lines.append(line)
        except KeyboardInterrupt:
            print("\nAborted.")
            return
        news = "\n".join(lines)

    if not news.strip():
        print("No news provided. Exiting.")
        return

    initial_state = {"news": news}

    print(f"\n{BOLD}▶ Running agent…{RESET}\n{'─'*60}")

    # Use stream_mode="updates" — emits state updates from each node as it finishes
    async for event in graph.astream(initial_state, stream_mode="updates"):
        for node_name, state_update in event.items():
            print(f"\n{CYAN}{BOLD}┌─ Node: {node_name}{RESET}")

            if state_update is None:
                continue
            for key, value in state_update.items():
                if key == "messages":
                    for msg in value:
                        role = msg.__class__.__name__
                        content = msg.content or ""
                        # Flatten list content (Gemini sometimes returns list of dicts)
                        if isinstance(content, list):
                            content = "".join(
                                p.get("text", "") if isinstance(p, dict) else str(p)
                                for p in content
                            )
                        tool_calls = getattr(msg, "tool_calls", [])
                        if tool_calls:
                            print(f"{YELLOW}│  [{role}] Tool calls: {[tc['name'] for tc in tool_calls]}{RESET}")
                        elif content:
                            # Print content with line-wrapping indent
                            preview = content[:500] + ("…" if len(content) > 500 else "")
                            print(f"{GREEN}│  [{role}]:{RESET} {preview}")
                else:
                    print(f"{MAGENTA}│  {key}:{RESET} {value}")

            print(f"{CYAN}└{'─'*58}{RESET}")

    print(f"\n{'─'*60}\n{BOLD}✅ Agent run complete.{RESET}\n")


if __name__ == "__main__":
    asyncio.run(main())
