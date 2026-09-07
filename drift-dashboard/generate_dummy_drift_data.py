import json
import uuid
import random
from datetime import datetime, timedelta
import os

def generate_telemetry_logs(num_runs=150):
    # Schema matches drift_detector.py exactly:
    # { "run_id": str, "timestamp": float, "execution_sequence": list, "tool_calls": list, "state_mutations": list }
    
    logs = []
    base_time = datetime.now() - timedelta(days=30)
    
    for i in range(num_runs):
        run_id = f"run_{uuid.uuid4().hex[:8]}"
        timestamp = (base_time + timedelta(hours=i * (720 / num_runs))).timestamp()
        
        # Introduce drift over time (later runs have longer sequences and more tools)
        drift_factor = i / num_runs
        
        # 1. execution_sequence
        seq_len = int(random.gauss(5 + (drift_factor * 8), 2))
        seq_len = max(3, seq_len)
        execution_sequence = ["start"] + ["process_node"] * seq_len + ["end"]
        
        # 2. tool_calls
        tool_calls = []
        num_tools = int(random.gauss(2 + (drift_factor * 6), 1.5))
        num_tools = max(0, num_tools)
        for _ in range(num_tools):
            tool_name = random.choices(["tavily_search_results_json", "get_stock_data"], weights=[0.7, 0.3])[0]
            tool_calls.append({"name": tool_name, "args": {"query": "dummy"}})
            
        # 3. state_mutations
        state_mutations = []
        num_mutations = seq_len
        for j in range(num_mutations):
            state_mutations.append({"intermediate_data": f"step_{j}", "status": "processing"})
            
        logs.append({
            "run_id": run_id,
            "timestamp": timestamp,
            "execution_sequence": execution_sequence,
            "tool_calls": tool_calls,
            "state_mutations": state_mutations
        })
        
    data_dir = os.path.join(os.path.dirname(__file__), "dummy_data")
    os.makedirs(data_dir, exist_ok=True)
    
    log_file = os.path.join(data_dir, "telemetry_logs.jsonl")
    with open(log_file, "w") as f:
        for log in logs:
            f.write(json.dumps(log) + "\n")
            
    print(f"✅ Generated {num_runs} realistic JSONL records in {log_file}")

if __name__ == "__main__":
    generate_telemetry_logs(150)
