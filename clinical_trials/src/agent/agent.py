"""Clinical trials RAG agent (MLflow ResponsesAgent).

Retrieves study documents from the Vector Search index on gold_study_documents and answers
with a Foundation Model chat endpoint. Its only data source is that gold index: it has no
SQL or table access, so silver (and its PII) is out of reach by construction.
"""

import uuid

import mlflow
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from mlflow.entities import SpanType
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import ResponsesAgentRequest, ResponsesAgentResponse

config = mlflow.models.ModelConfig(development_config={
    "vector_search_index": "workspace.clinical_trials.gold_study_documents_index",
    "llm_endpoint": "databricks-meta-llama-3-3-70b-instruct",
    "num_results": 5,
    "max_tokens": 800,
})

RETRIEVED_COLUMNS = ["nct_id", "brief_title", "overall_status", "document_text"]
SYSTEM_PROMPT = """You answer questions about clinical trials registered on ClinicalTrials.gov.
Use only the study documents provided in the context. Cite the NCT ID of every study you use,
for example (NCT03506373). If the context doesn't contain the answer, say so plainly instead of
guessing. Never provide names, emails, phone numbers or other personal contact details of
investigators, staff or participants; for contacts, point people to the study's
ClinicalTrials.gov page (https://clinicaltrials.gov/study/<NCT ID>)."""


class ClinicalTrialsAgent(ResponsesAgent):
    def __init__(self):
        self.index_name = config.get("vector_search_index")
        self.llm_endpoint = config.get("llm_endpoint")
        self.num_results = int(config.get("num_results"))
        self.max_tokens = int(config.get("max_tokens"))
        self.w = WorkspaceClient()

    @mlflow.trace(span_type=SpanType.RETRIEVER)
    def retrieve(self, query: str) -> list[dict]:
        result = self.w.vector_search_indexes.query_index(
            index_name=self.index_name,
            columns=RETRIEVED_COLUMNS,
            query_text=query,
            num_results=self.num_results,
        )
        names = [c.name for c in result.manifest.columns]
        return [dict(zip(names, row)) for row in (result.result.data_array or [])]

    @mlflow.trace(span_type=SpanType.LLM)
    def generate(self, question: str, docs: list[dict], history: list[dict]) -> str:
        context = "\n\n---\n\n".join(d["document_text"] for d in docs) or "(no matching studies)"
        messages = [ChatMessage(role=ChatMessageRole.SYSTEM, content=SYSTEM_PROMPT)]
        messages += [ChatMessage(role=ChatMessageRole(m["role"]), content=m["content"]) for m in history]
        messages.append(ChatMessage(
            role=ChatMessageRole.USER,
            content=f"Context:\n{context}\n\nQuestion: {question}",
        ))
        response = self.w.serving_endpoints.query(
            name=self.llm_endpoint, messages=messages, max_tokens=self.max_tokens, temperature=0.1,
        )
        return response.choices[0].message.content

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        turns = [_as_text_message(item.model_dump()) for item in request.input]
        turns = [t for t in turns if t]
        if not turns or turns[-1]["role"] != "user":
            raise ValueError("The last input item must be a user message")
        question, history = turns[-1]["content"], turns[:-1]
        docs = self.retrieve(question)
        answer = self.generate(question, docs, history)
        return ResponsesAgentResponse(
            output=[self.create_text_output_item(text=answer, id=str(uuid.uuid4()))],
            custom_outputs={"sources": [d["nct_id"] for d in docs]},
        )


def _as_text_message(item: dict):
    """Reduce a Responses input item to {role, content} text, or None for non-message items."""
    role, content = item.get("role"), item.get("content")
    if role not in ("user", "assistant"):
        return None
    if isinstance(content, list):
        content = "".join(part.get("text", "") for part in content if isinstance(part, dict))
    return {"role": role, "content": content or ""}


mlflow.models.set_model(ClinicalTrialsAgent())
