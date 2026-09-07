from transformers import BertTokenizer, BertForSequenceClassification
from analyst.state import AgentState
import torch

# Lazy-loaded singletons — avoids blocking LangGraph startup with a 400MB download
_tokenizer = None
_model = None

def _get_model():
    global _tokenizer, _model
    if _tokenizer is None:
        _tokenizer = BertTokenizer.from_pretrained("yiyanghkust/finbert-tone")
        _model = BertForSequenceClassification.from_pretrained("yiyanghkust/finbert-tone")
    return _tokenizer, _model


def sentiment_node(state: AgentState) -> AgentState:

    news = state["news"]

    tokenizer, model = _get_model()

    inputs = tokenizer(news, return_tensors="pt", padding=True, truncation=True, max_length=512)

    with torch.no_grad():
        outputs = model(**inputs)
        predictions = torch.nn.functional.softmax(outputs.logits, dim=-1)

    labels = ["Neutral", "Positive", "Negative"]
    predicted_class = torch.argmax(predictions, dim=1).item()

    return {
        "sentiment": labels[predicted_class],
        "sentimentflag": True
    }