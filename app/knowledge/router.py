from typing import Dict, List

from app.knowledge.domains import KnowledgeDomain

OPS_TERMS = {
    "nginx",
    "docker",
    "cloudflare",
    "tunnel",
    "postgres",
    "qdrant",
    "minio",
    "redis",
    "429",
    "rate limit",
    "uvicorn",
}

REFERENCE_TERMS = {
    "how do i",
    "build",
    "implement",
    "pattern",
    "example",
    "docs",
    "documentation",
    "api",
    "endpoint",
    "schema",
}


def route_query(query: str) -> Dict[str, object]:
    normalized = " ".join(query.lower().split())
    if any(term in normalized for term in OPS_TERMS):
        return {
            "searched_domains": [KnowledgeDomain.OPS.value, KnowledgeDomain.PROJECT.value],
            "routing_reason": "matched ops/infra terms",
            "routing_mode": "router_prefer",
        }
    if any(term in normalized for term in REFERENCE_TERMS):
        return {
            "searched_domains": [KnowledgeDomain.PROJECT.value, KnowledgeDomain.REFERENCE.value],
            "routing_reason": "matched build/reference terms",
            "routing_mode": "router_prefer",
        }
    return {
        "searched_domains": [
            KnowledgeDomain.PROJECT.value,
            KnowledgeDomain.OPS.value,
            KnowledgeDomain.REFERENCE.value,
        ],
        "routing_reason": "default query routing",
        "routing_mode": "router_default",
    }
