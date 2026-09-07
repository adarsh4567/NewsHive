import asyncio
import json
import uuid
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

from analyst.graph import graph

load_dotenv()

app = FastAPI(title="Financial Agent API", description="On-Demand AI Analysis Microservice")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class AnalyzeRequest(BaseModel):
    news: str

async def stream_analysis(news_text: str):
    """
    Generator function that runs the LangGraph agent and yields SSE events.
    """
    initial_state = {"news": news_text}
    
    # We yield a starting message
    yield f"data: {json.dumps({'event': 'start', 'message': 'Agent initialized. Starting analysis...'})}\n\n"

    try:
        async for event in graph.astream(initial_state, stream_mode="updates"):
            for node_name, state_update in event.items():
                
                # Let the client know which node is executing
                yield f"data: {json.dumps({'event': 'node_update', 'node': node_name})}\n\n"
                
                if state_update is None:
                    continue
                    
                if "messages" in state_update:
                    for msg in state_update["messages"]:
                        role = msg.__class__.__name__
                        content = msg.content or ""
                        
                        # Flatten list content if needed
                        if isinstance(content, list):
                            content = "".join(
                                p.get("text", "") if isinstance(p, dict) else str(p)
                                for p in content
                            )
                            
                        tool_calls = getattr(msg, "tool_calls", [])
                        
                        if tool_calls:
                            yield f"data: {json.dumps({'event': 'tool_call', 'tools': [tc['name'] for tc in tool_calls]})}\n\n"
                        elif content:
                            yield f"data: {json.dumps({'event': 'content', 'role': role, 'text': content})}\n\n"

        yield f"data: {json.dumps({'event': 'end', 'message': 'Analysis complete.'})}\n\n"
    except Exception as e:
        yield f"data: {json.dumps({'event': 'error', 'message': str(e)})}\n\n"

@app.post("/analyze")
async def analyze_news(req: AnalyzeRequest):
    """
    Accepts a news article and returns a StreamingResponse of the AI's execution.
    """
    return StreamingResponse(
        stream_analysis(req.news),
        media_type="text/event-stream"
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
