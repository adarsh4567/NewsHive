from analyst.state import AgentState
from analyst.sentiment_analyzer.node import sentiment_node
from analyst.entity_recognizer.node import entity_node
from langgraph.graph import StateGraph, START, END
from typing import Literal,Generator
from langgraph.prebuilt import ToolNode
from analyst.entity_recognizer.tools.search import search_tool
from analyst.entity_recognizer.tools.finsearch import stock_tool
from analyst.investment_analyzer.node import investment_node
# from langgraph.checkpoint.memory import MemorySaver
from langchain_core.messages import AIMessageChunk
from analyst.drift_wrapper.drift_detector import DriftTelemetryWrapper


def should_continue(state:AgentState) -> Literal["tools","investment_advice"]:
   """
    Routing function to determine next step:
    - If last message has tool calls -> go to tools node
    - Otherwise -> investment_advice
   """
   messages = state.get("messages", [])

   if not messages:
        return "investment_advice"
   
   last_message = messages[-1]

   if hasattr(last_message, 'tool_calls') and last_message.tool_calls:
        return "tools"
   return "investment_advice" 






class FinancialAgent:

    def __init__(self):
        self.runnable = self.build_graph()


    def build_graph(self):
        workflow = StateGraph(AgentState)
        
        workflow.add_node("sentiment_node",sentiment_node)
        workflow.add_node("entity_node",entity_node)
        workflow.add_node("tools",ToolNode([search_tool,stock_tool]))
        workflow.add_node("investment_node",investment_node)

        # parallel node execution
        workflow.add_edge(START,"sentiment_node")
        workflow.add_edge(START,"entity_node")


        workflow.add_conditional_edges(
        "entity_node",
        should_continue,
            {
                "tools": "tools",
                "investment_advice": "investment_node"
            }
        )
        workflow.add_edge("sentiment_node","investment_node")
        workflow.add_edge("investment_node", END)
        workflow.add_edge("tools", "entity_node")
        return workflow.compile()
    



agent = FinancialAgent()

raw_graph = agent.build_graph()

graph = DriftTelemetryWrapper(raw_graph)


