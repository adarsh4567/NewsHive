import json
from datetime import datetime
import asyncio
from zoneinfo import ZoneInfo
import os
import uuid
import boto3


class DriftTelemetryWrapper():

    def __init__(self,graph):
        self.graph = graph
        self.execution_sequence = []
        self.tool_calls = []
        self.state_mutations = []

    async def astream(self,state,stream_mode="updates",config=None,**kwargs):


        async for event in self.graph.astream(state,stream_mode="updates",config=config,**kwargs):

            for current_node, state_update in event.items():

                self.record_execution(current_node)

                if state_update is None:
                    continue

                if "messages" in state_update:
                    message = state_update["messages"]

                    if message:
                        last_msg = message[-1]

                        if hasattr(last_msg,"tool_calls") and last_msg.tool_calls:
                            for tc in last_msg.tool_calls:
                                tool_name = tc.get("name")
                                self.tool_calls.append(tc)
                                print(f"[Toolcall Added] {tool_name}")

                snapshot = {}

                for key, value in state_update.items():
                    if key != "messages":
                        snapshot[key] = value

                if len(snapshot) > 0:
                    self.state_mutations.append(snapshot)       


                previous_node = current_node

            yield event

        asyncio.create_task(self.flush_telemetry())        


    def record_execution(self,current_node):
        
        self.execution_sequence.append(current_node)
        print(f"[Telemetry Intercepted] {current_node}")

        

    async def flush_telemetry(self):
        # 1. Generate a unique Run ID
        run_id = f"run_{uuid.uuid4().hex[:8]}"
        now = datetime.now()
        
        # 2. Add run_id to payload for tracing
        run_data = {
            "run_id": run_id,
            "timestamp": now.timestamp(),
            "execution_sequence": self.execution_sequence,
            "tool_calls": self.tool_calls,
            "state_mutations": self.state_mutations
        }
        json_payload = json.dumps(run_data) + "\n"
        # 3. Keep writing locally for your local backup
        current_dir = os.path.dirname(__file__)
        log_file_path = os.path.join(current_dir, "telemetry_logs.jsonl")
        with open(log_file_path, "a") as f:
            f.write(json_payload) 
        
        # 4. Fire and forget S3 upload in a background thread
        await asyncio.to_thread(self._upload_to_s3, json_payload, run_id, now)
        print(f"[Telemetry] Flushed {len(self.execution_sequence)}")
    def _upload_to_s3(self, payload_str, run_id, time_obj):
        bucket_name = os.getenv("S3_BUCKET_NAME")
        if not bucket_name:
            print("[Telemetry] S3_BUCKET_NAME not set in .env! Skipping S3 upload.")
            return
            
        # Format: raw/agent_runs/year=2026/month=08/day=19/run_abc123.jsonl
        year = time_obj.strftime("%Y")
        month = time_obj.strftime("%m")
        day = time_obj.strftime("%d")
        
        s3_key = f"raw/agent_runs/year={year}/month={month}/day={day}/{run_id}.jsonl"
        
        try:
            s3_client = boto3.client('s3')
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=payload_str
            )
            print(f"[Telemetry] Successfully uploaded to S3: {s3_key}")
        except Exception as e:
            print(f"[Telemetry] Failed to upload to S3: {str(e)}")                       