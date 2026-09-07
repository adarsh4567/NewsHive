"""
AgentDrift Dashboard
====================
Reads processed Parquet data from S3 and renders a real-time
AI-agent telemetry and drift monitoring dashboard.

Run:
    cd dashboard
    streamlit run app.py
"""

import os
import boto3
import pandas as pd
import awswrangler as wr
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import streamlit as st
from datetime import datetime
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../financial-agent/.env"))

S3_BUCKET      = os.getenv("S3_BUCKET_NAME", "agent-eval-bucket")
AWS_REGION     = os.getenv("AWS_REGION", "ap-south-1")
DRIFT_PATH     = f"s3://{S3_BUCKET}/metrics/drift_scores/"
PROCESSED_PATH = f"s3://{S3_BUCKET}/processed/agent_fingerprints/"

BOTO_SESSION = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=AWS_REGION,
)

# Palette
CYAN   = "#00e5ff"
GREEN  = "#00ff88"
RED    = "#ff4c4c"
PURPLE = "#c084fc"
YELLOW = "#fbbf24"
BG     = "#0d1117"
CARD   = "#161b22"
BORDER = "#30363d"

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="AgentDrift | AI Telemetry Monitor",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─────────────────────────────────────────────────────────────────────────────
# CUSTOM CSS — premium glassmorphism dark theme
# ─────────────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
# CUSTOM CSS — minimalist, clean, spacious dark theme
# ─────────────────────────────────────────────────────────────────────────────
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime

st.set_page_config(page_title="Agent Drift Monitor", layout="wide", initial_sidebar_state="collapsed")

# ─────────────────────────────────────────────────────────────────────────────
# CUSTOM CSS — Extremely simple, clean, and explicit
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
* { font-family: 'Inter', sans-serif !important; }

.stApp { background: #f8fafc; color: #0f172a; }
.block-container { max-width: 1200px !important; padding-top: 3rem !important; }

/* Status Banner */
.status-banner {
    padding: 24px 32px;
    border-radius: 12px;
    margin-bottom: 32px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05);
}
.status-healthy { background: #dcfce7; border: 1px solid #bbf7d0; color: #166534; }
.status-warning { background: #fef9c3; border: 1px solid #fef08a; color: #854d0e; }
.status-critical { background: #fee2e2; border: 1px solid #fecaca; color: #991b1b; }

.status-title { font-size: 1.5rem; font-weight: 700; display: flex; align-items: center; gap: 12px; }
.status-subtitle { font-size: 0.95rem; margin-top: 4px; opacity: 0.9; }

/* Metric Cards */
.metric-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 24px; margin-bottom: 32px; }
.metric-card {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 24px;
    box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1);
}
.metric-title { font-size: 0.9rem; font-weight: 600; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 16px; }
.metric-comparison { display: flex; justify-content: space-between; align-items: flex-end; }
.metric-current { font-size: 2.5rem; font-weight: 700; color: #0f172a; line-height: 1; }
.metric-baseline { font-size: 0.9rem; color: #64748b; padding-bottom: 4px; }
.drift-up { color: #ef4444; font-weight: 600; }
.drift-down { color: #10b981; font-weight: 600; }

/* Sections */
.section-card {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 24px;
    margin-bottom: 24px;
    box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1);
}
.section-header { font-size: 1.2rem; font-weight: 600; color: #0f172a; margin-bottom: 8px; }
.section-desc { font-size: 0.9rem; color: #64748b; margin-bottom: 20px; }

/* Hide Streamlit Chrome */
footer { visibility: hidden; }
header { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# DATA PROCESSING
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=60)
def load_data():
    import os
    import json
    
    data_dir = os.path.join(os.path.dirname(__file__), "dummy_data")
    log_file = os.path.join(data_dir, "telemetry_logs.jsonl")
    
    if not os.path.exists(log_file):
        st.error("Dummy data not found. Please run `python generate_dummy_drift_data.py` first.")
        st.stop()
        
    rows = []
    with open(log_file, "r") as f:
        for line in f:
            if not line.strip(): continue
            data = json.loads(line)
            
            # Extract features from exact wrapper schema
            run_id = data.get("run_id")
            timestamp = data.get("timestamp")
            seq = data.get("execution_sequence", [])
            tools = data.get("tool_calls", [])
            mutations = data.get("state_mutations", [])
            
            # Aggregate data points for dashboard
            seq_length = len(seq)
            total_tool_calls = len(tools)
            tavily_count = sum(1 for t in tools if t.get("name") == "tavily_search_results_json")
            stock_count = sum(1 for t in tools if t.get("name") == "get_stock_data")
            num_mutations = len(mutations)
            
            rows.append({
                "run_id": run_id,
                "run_date": pd.to_datetime(timestamp, unit="s"),
                "seq_length": seq_length,
                "total_tool_calls": total_tool_calls,
                "tavily_tool_count": tavily_count,
                "stock_tool_count": stock_count,
                "state_mutations": num_mutations,
                "is_success": seq[-1] == "end" if seq else False
            })
            
    df = pd.DataFrame(rows)
    df = df.sort_values("run_date").reset_index(drop=True)
    return df

def main():
    try:
        df = load_data()
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        st.stop()

    # Calculate Baseline (first 30%)
    baseline_idx = int(len(df) * 0.3)
    
    # Calculate Z-scores dynamically to get a "Drift Score"
    for m in ["seq_length", "total_tool_calls", "tavily_tool_count"]:
        b_mean = df[m].iloc[:baseline_idx].mean()
        b_std = df[m].iloc[:baseline_idx].std() + 0.001
        df[f"z_{m}"] = (df[m] - b_mean) / b_std
        
    z_cols = [c for c in df.columns if c.startswith("z_")]
    df["max_z"] = df[z_cols].abs().max(axis=1)
    df["drift_percentage"] = (df["max_z"] / 3.0) * 100  # Cap at ~100% at Z=3
    df["drift_percentage"] = df["drift_percentage"].clip(0, 100)
    
    # Identify Baseline vs Current
    baseline_df = df.iloc[:baseline_idx]
    current_df = df.iloc[-20:]

    
    current_drift_score = current_df["drift_percentage"].mean()
    
    # ── HEADER & STATUS BANNER ────────────────────────────────────────────────
    st.markdown('<div style="font-size: 2rem; font-weight: 700; color: #0f172a; margin-bottom: 1rem;">🔍 Financial AI Agent Monitor</div>', unsafe_allow_html=True)
    
    if current_drift_score < 30:
        banner_class, icon, status, desc = "status-healthy", "✅", "System Healthy", "The agent is behaving according to baseline expectations. No significant drift detected."
    elif current_drift_score < 60:
        banner_class, icon, status, desc = "status-warning", "⚠️", "Behavioral Shift Detected", "The agent is showing signs of changing behavior (e.g., using more tools than usual)."
    else:
        banner_class, icon, status, desc = "status-critical", "🚨", "Critical Drift Detected!", "The agent has severely deviated from its baseline behavior. Immediate review required."

    st.markdown(f"""
    <div class="status-banner {banner_class}">
        <div>
            <div class="status-title">{icon} {status}</div>
            <div class="status-subtitle">{desc}</div>
        </div>
        <div style="text-align: right;">
            <div style="font-size: 0.85rem; opacity: 0.8; margin-bottom: 4px;">Current Drift Severity</div>
            <div style="font-size: 2rem; font-weight: 700;">{current_drift_score:.1f}%</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── EXPLICIT DRIFT METRICS ────────────────────────────────────────────────
    st.markdown("### How is the agent behaving compared to when it was first deployed?")
    
    b_search = baseline_df["tavily_tool_count"].mean()
    c_search = current_df["tavily_tool_count"].mean()
    search_diff = ((c_search - b_search) / b_search) * 100 if b_search else 0
    search_class = "drift-up" if search_diff > 20 else ("drift-down" if search_diff < -20 else "")
    
    b_steps = baseline_df["seq_length"].mean()
    c_steps = current_df["seq_length"].mean()
    steps_diff = ((c_steps - b_steps) / b_steps) * 100 if b_steps else 0
    steps_class = "drift-up" if steps_diff > 20 else ("drift-down" if steps_diff < -20 else "")

    b_success = baseline_df["is_success"].mean() * 100
    c_success = current_df["is_success"].mean() * 100
    success_diff = c_success - b_success
    success_class = "drift-up" if success_diff < -5 else ("drift-down" if success_diff > 5 else "")

    st.markdown(f"""
    <div class="metric-grid">
        <div class="metric-card">
            <div class="metric-title">Average Web Searches per Task</div>
            <div class="metric-comparison">
                <div class="metric-current {search_class}">{c_search:.1f}</div>
                <div class="metric-baseline">Baseline: {b_search:.1f} ({'+' if search_diff>0 else ''}{search_diff:.1f}%)</div>
            </div>
        </div>
        <div class="metric-card">
            <div class="metric-title">Reasoning Steps (Agent Depth)</div>
            <div class="metric-comparison">
                <div class="metric-current {steps_class}">{c_steps:.1f}</div>
                <div class="metric-baseline">Baseline: {b_steps:.1f} ({'+' if steps_diff>0 else ''}{steps_diff:.1f}%)</div>
            </div>
        </div>
        <div class="metric-card">
            <div class="metric-title">Task Success Rate</div>
            <div class="metric-comparison">
                <div class="metric-current {success_class}">{c_success:.1f}%</div>
                <div class="metric-baseline">Baseline: {b_success:.1f}% ({'+' if success_diff>0 else ''}{success_diff:.1f}%)</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── SIMPLE DRIFT TIMELINE ─────────────────────────────────────────────────
    st.markdown("""
    <div class="section-card">
        <div class="section-header">Drift Severity Timeline</div>
        <div class="section-desc">Shows how far the agent's behavior deviates from normal over time. A score above 50% indicates significant drift.</div>
    """, unsafe_allow_html=True)
    
    fig = go.Figure()
    
    # Smooth line
    fig.add_trace(go.Scatter(
        x=df["run_date"],
        y=df["drift_percentage"].rolling(window=5, min_periods=1).mean(),
        mode='lines',
        line=dict(color='#3b82f6', width=3),
        name="Average Drift"
    ))
    
    # Critical threshold line
    fig.add_hline(y=50, line_dash="dash", line_color="#ef4444", annotation_text="Danger Threshold (50%)", annotation_font_color="#ef4444")
    
    fig.update_layout(
        paper_bgcolor="white", plot_bgcolor="white",
        margin=dict(l=0, r=0, t=10, b=0),
        xaxis=dict(showgrid=True, gridcolor="#f1f5f9"),
        yaxis=dict(showgrid=True, gridcolor="#f1f5f9", range=[0, 100], title="Drift Severity %"),
        height=300, showlegend=False
    )
    st.plotly_chart(fig, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

    # ── HUMAN READABLE ANOMALIES ──────────────────────────────────────────────
    st.markdown("""
    <div class="section-card">
        <div class="section-header">Recent Anomalous Behavior</div>
        <div class="section-desc">Specific instances where the agent behaved poorly or unexpectedly.</div>
    """, unsafe_allow_html=True)

    # Filter for anomalous runs
    anomalies = df[df["drift_percentage"] > 50].sort_values("run_date", ascending=False).head(10)
    
    if anomalies.empty:
        st.success("No significant anomalies detected in recent runs.")
    else:
        display_data = []
        for _, row in anomalies.iterrows():
            date_str = row["run_date"].strftime("%Y-%m-%d %H:%M")
            
            # Determine plain English reason
            issues = []
            if row.get("z_seq_length", 0) > 2: issues.append("Agent got stuck in a long reasoning loop.")
            if row.get("z_tavily_tool_count", 0) > 2: issues.append("Agent performed excessive web searches.")
            if row.get("is_success") == False: issues.append("Agent failed to complete the analysis.")
            
            reason = " ".join(issues) if issues else "Unusual behavior patterns detected."
            
            display_data.append({
                "Date": date_str,
                "Run ID": row["run_id"][-8:],
                "Drift Severity": f"{row['drift_percentage']:.1f}%",
                "Observed Issue": reason
            })
            
        st.dataframe(pd.DataFrame(display_data), use_container_width=True, hide_index=True)
    
    st.markdown("</div>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()
