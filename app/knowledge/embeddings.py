from abc import ABC, abstractmethod
from importlib.metadata import PackageNotFoundError, version
from threading import Lock
from typing import Dict, List

from sentence_transformers import SentenceTransformer


def _package_version(name: str) -> str:
    try:
        return version(name)
    except PackageNotFoundError:
        return "unknown"


class EmbeddingProvider(ABC):
    @abstractmethod
    def embed(self, texts: List[str]) -> List[List[float]]:
        raise NotImplementedError


class EmbeddingProviderInitError(RuntimeError):
    pass


class SentenceTransformersProvider(EmbeddingProvider):
    def __init__(self, model_name: str):
        self.model_name = model_name
        try:
            self.model = SentenceTransformer(model_name, device="cpu")
            self.embedding_revision = self._resolve_revision()
        except Exception as exc:
            raise EmbeddingProviderInitError(f"embedding_provider_init_failed:{model_name}:{exc}") from exc

    def _resolve_revision(self) -> str:
        package_version = _package_version("sentence-transformers")
        config_revision = getattr(getattr(self.model, "_model_config", None), "_commit_hash", None)
        if config_revision:
            return str(config_revision)
        return f"unknown:sentence-transformers={package_version}"

    def _prepare_text(self, text: str) -> str:
        cleaned = text.strip()
        if self.model_name.startswith("intfloat/e5-"):
            return f"passage: {cleaned}"
        return cleaned

    def embed(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []
        vectors = self.model.encode(
            [self._prepare_text(text) for text in texts],
            batch_size=min(32, max(1, len(texts))),
            show_progress_bar=False,
            convert_to_numpy=True,
            normalize_embeddings=True,
        )
        return vectors.tolist()


_PROVIDER_CACHE: Dict[str, SentenceTransformersProvider] = {}
_PROVIDER_LOCK = Lock()


def get_sentence_transformers_provider(model_name: str) -> SentenceTransformersProvider:
    with _PROVIDER_LOCK:
        provider = _PROVIDER_CACHE.get(model_name)
        if provider is not None:
            return provider
        provider = SentenceTransformersProvider(model_name)
        _PROVIDER_CACHE[model_name] = provider
        return provider


def warmup_sentence_transformers_provider(model_name: str) -> SentenceTransformersProvider:
    provider = get_sentence_transformers_provider(model_name)
    provider.embed(["warmup"])
    return provider
