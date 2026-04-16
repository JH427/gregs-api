import hashlib
import json
import mimetypes
import os
import tempfile
import uuid
from importlib.metadata import PackageNotFoundError, version
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException, UploadFile
from markitdown import MarkItDown
from sqlalchemy.orm import Session

from app.artifacts import create_artifact_record
from app.logging_utils import log_event
from app.storage import delete_object, get_client, get_object, put_bytes

SUPPORTED_FILE_MIME_TYPES = {
    "application/pdf",
    "text/plain",
    "text/markdown",
    "text/x-markdown",
}
MARKDOWN_EXTENSIONS = {".md", ".markdown"}
TEXT_EXTENSIONS = {".txt"}


def _package_version(name: str) -> str:
    try:
        return version(name)
    except PackageNotFoundError:
        return "unknown"


def compute_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def build_import_staging_path(source: str, suffix: str = "") -> str:
    return f"imports/staging/{source}/{uuid.uuid4()}{suffix}"


def detect_upload_mime(filename: Optional[str], declared_content_type: Optional[str]) -> str:
    normalized = (declared_content_type or "").split(";")[0].strip().lower()
    if normalized in SUPPORTED_FILE_MIME_TYPES:
        return normalized

    guessed, _ = mimetypes.guess_type(filename or "")
    guessed = (guessed or "").lower()
    if guessed in SUPPORTED_FILE_MIME_TYPES:
        return guessed

    ext = os.path.splitext((filename or "").lower())[1]
    if ext in MARKDOWN_EXTENSIONS:
        return "text/markdown"
    if ext in TEXT_EXTENSIONS:
        return "text/plain"
    return normalized or "application/octet-stream"


async def read_upload_limited(upload: UploadFile, max_bytes: int) -> bytes:
    data = await upload.read()
    if len(data) > max_bytes:
        raise HTTPException(status_code=413, detail="file too large")
    return data


def stage_import_bytes(source: str, data: bytes, content_type: str, suffix: str = "") -> str:
    path = build_import_staging_path(source, suffix=suffix)
    put_bytes(get_client(), path, data, content_type)
    return path


def read_staged_import_bytes(path: str) -> bytes:
    obj = get_object(get_client(), path)
    try:
        return obj.read()
    finally:
        obj.close()
        obj.release_conn()


def delete_staged_import(path: Optional[str]) -> None:
    if not path:
        return
    try:
        delete_object(get_client(), path)
    except Exception:
        pass


def _pdf_page_count(pdf_bytes: bytes) -> Optional[int]:
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=True) as handle:
        handle.write(pdf_bytes)
        handle.flush()
        for module_name in ("pypdf", "PyPDF2"):
            try:
                if module_name == "pypdf":
                    from pypdf import PdfReader
                else:
                    from PyPDF2 import PdfReader
                return len(PdfReader(handle.name).pages)
            except Exception:
                continue
    return None


def convert_pdf_to_markdown(pdf_bytes: bytes) -> Tuple[str, str, Optional[int]]:
    parser_version = _package_version("markitdown")
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=True) as handle:
        handle.write(pdf_bytes)
        handle.flush()
        result = MarkItDown().convert(handle.name)

    markdown = getattr(result, "text_content", None)
    if markdown is None:
        markdown = getattr(result, "markdown", None)
    if markdown is None:
        markdown = getattr(result, "text", None)
    if markdown is None:
        markdown = str(result)
    if not isinstance(markdown, str) or not markdown.strip():
        raise ValueError("empty_markdown_output")

    return markdown, parser_version, _pdf_page_count(pdf_bytes)


def create_import_error_artifact(
    db: Session,
    task_id: str,
    derived_from: Optional[str],
    error_code: str,
    message: str,
    logger,
) -> str:
    payload = {
        "error_code": error_code,
        "message": message,
        "derived_from": derived_from,
    }
    artifact = create_artifact_record(
        db=db,
        task_id=task_id,
        artifact_type="import_error",
        content_type="application/json",
        data=json.dumps(payload).encode("utf-8"),
        metadata=payload,
        event_logger=logger,
    )
    return artifact.id


def _extract_chatgpt_message(node: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    message = node.get("message")
    if not isinstance(message, dict):
        return None

    author = message.get("author") or {}
    return {
        "id": message.get("id") or node.get("id"),
        "node_id": node.get("id"),
        "parent": node.get("parent"),
        "role": author.get("role"),
        "create_time": message.get("create_time", node.get("create_time")),
        "update_time": message.get("update_time", node.get("update_time")),
        "status": message.get("status"),
        "recipient": message.get("recipient"),
        "content": message.get("content"),
        "metadata": message.get("metadata") or {},
    }


def _ordered_mapping_nodes(conversation: Dict[str, Any]) -> List[Dict[str, Any]]:
    mapping = conversation.get("mapping")
    if not isinstance(mapping, dict) or not mapping:
        return []

    children: Dict[Optional[str], List[Dict[str, Any]]] = {}
    for node in mapping.values():
        if not isinstance(node, dict):
            continue
        children.setdefault(node.get("parent"), []).append(node)

    def sort_key(node: Dict[str, Any]) -> Tuple[float, str]:
        create_time = node.get("create_time")
        if create_time is None and isinstance(node.get("message"), dict):
            create_time = node["message"].get("create_time")
        try:
            return float(create_time or 0), str(node.get("id") or "")
        except Exception:
            return 0.0, str(node.get("id") or "")

    ordered: List[Dict[str, Any]] = []
    seen_ids = set()

    def walk(parent_id: Optional[str]) -> None:
        for child in sorted(children.get(parent_id, []), key=sort_key):
            child_id = child.get("id")
            if child_id in seen_ids:
                continue
            seen_ids.add(child_id)
            ordered.append(child)
            walk(child_id)

    root_candidates = [
        node for node in mapping.values()
        if isinstance(node, dict) and (node.get("parent") not in mapping or node.get("parent") is None)
    ]
    for root in sorted(root_candidates, key=sort_key):
        root_id = root.get("id")
        if root_id in seen_ids:
            continue
        seen_ids.add(root_id)
        ordered.append(root)
        walk(root_id)
    return ordered


def extract_chatgpt_messages(conversation: Dict[str, Any]) -> List[Dict[str, Any]]:
    mapping_nodes = _ordered_mapping_nodes(conversation)
    if mapping_nodes:
        messages = []
        for node in mapping_nodes:
            message = _extract_chatgpt_message(node)
            if message is not None:
                messages.append(message)
        return messages

    raw_messages = conversation.get("messages")
    if isinstance(raw_messages, list):
        return [message for message in raw_messages if isinstance(message, dict)]
    return []


def normalize_chatgpt_export(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        conversations = payload.get("conversations")
        if isinstance(conversations, list):
            return [item for item in conversations if isinstance(item, dict)]
        if "mapping" in payload:
            return [payload]
    raise ValueError("invalid_chatgpt_export")


def run_import_file_task(db: Session, task_id: str, params: Dict[str, Any], logger) -> Dict[str, Any]:
    staging_path = params["staging_path"]
    filename = params.get("filename")
    mime = params.get("mime", "application/octet-stream")
    source = "files"
    artifact_ids: List[str] = []
    raw_artifact_id: Optional[str] = None

    try:
        raw_bytes = read_staged_import_bytes(staging_path)
        sha256 = compute_sha256(raw_bytes)
        raw_artifact = create_artifact_record(
            db=db,
            task_id=task_id,
            artifact_type="import_raw",
            content_type=mime,
            data=raw_bytes,
            metadata={
                "source": source,
                "filename": filename,
                "sha256": sha256,
                "size_bytes": len(raw_bytes),
            },
            event_logger=logger,
        )
        raw_artifact_id = raw_artifact.id
        artifact_ids.append(raw_artifact.id)

        if mime == "application/pdf":
            parsed_text, parser_version, page_count = convert_pdf_to_markdown(raw_bytes)
            parsed_artifact = create_artifact_record(
                db=db,
                task_id=task_id,
                artifact_type="import_parsed_markdown",
                content_type="text/markdown",
                data=parsed_text.encode("utf-8"),
                metadata={
                    "derived_from": raw_artifact.id,
                    "parser": "markitdown",
                    "parser_version": parser_version,
                    "confidence": "medium",
                },
                event_logger=logger,
            )
            parser_name = "markitdown"
        elif mime in {"text/plain", "text/markdown", "text/x-markdown"}:
            parsed_text = raw_bytes.decode("utf-8")
            parsed_artifact = create_artifact_record(
                db=db,
                task_id=task_id,
                artifact_type="import_parsed_text",
                content_type="text/plain",
                data=parsed_text.encode("utf-8"),
                metadata={
                    "derived_from": raw_artifact.id,
                    "parser": "plain",
                    "parser_version": "builtin",
                    "confidence": "medium",
                },
                event_logger=logger,
            )
            parser_name = "plain"
            parser_version = "builtin"
            page_count = None
        else:
            error_id = create_import_error_artifact(
                db=db,
                task_id=task_id,
                derived_from=raw_artifact.id,
                error_code="unsupported_file_type",
                message=f"unsupported import mime: {mime}",
                logger=logger,
            )
            artifact_ids.append(error_id)
            log_event(logger, "import_failed", task_id=task_id, artifact_ids=artifact_ids, source=source)
            return {
                "artifact_ids": artifact_ids,
                "summary": f"Import failed for file {filename}",
            }

        artifact_ids.append(parsed_artifact.id)
        metadata_payload = {
            "source": source,
            "filename": filename,
            "mime": mime,
            "size_bytes": len(raw_bytes),
            "sha256": sha256,
            "parser": parser_name,
            "parser_version": parser_version,
            "pages": page_count,
        }
        metadata_artifact = create_artifact_record(
            db=db,
            task_id=task_id,
            artifact_type="import_metadata",
            content_type="application/json",
            data=json.dumps({"type": "import_metadata", "payload": metadata_payload}).encode("utf-8"),
            metadata=metadata_payload,
            event_logger=logger,
        )
        artifact_ids.append(metadata_artifact.id)
        log_event(logger, "import_completed", task_id=task_id, artifact_ids=artifact_ids, source=source)
        return {
            "artifact_ids": artifact_ids,
            "summary": f"Imported file {filename}",
        }
    except UnicodeDecodeError as exc:
        error_id = create_import_error_artifact(
            db=db,
            task_id=task_id,
            derived_from=raw_artifact_id,
            error_code="invalid_utf8",
            message=str(exc),
            logger=logger,
        )
        artifact_ids.append(error_id)
        log_event(logger, "import_failed", task_id=task_id, artifact_ids=artifact_ids, source=source)
        return {"artifact_ids": artifact_ids, "summary": f"Import failed for file {filename}"}
    except Exception as exc:
        error_id = create_import_error_artifact(
            db=db,
            task_id=task_id,
            derived_from=raw_artifact_id,
            error_code="import_processing_failed",
            message=str(exc),
            logger=logger,
        )
        artifact_ids.append(error_id)
        log_event(logger, "import_failed", task_id=task_id, artifact_ids=artifact_ids, source=source)
        return {"artifact_ids": artifact_ids, "summary": f"Import failed for file {filename}"}
    finally:
        delete_staged_import(staging_path)


def run_import_chatgpt_task(db: Session, task_id: str, params: Dict[str, Any], logger) -> Dict[str, Any]:
    staging_path = params["staging_path"]
    source = "chatgpt"
    artifact_ids: List[str] = []
    raw_artifact_id: Optional[str] = None

    try:
        raw_bytes = read_staged_import_bytes(staging_path)
        sha256 = compute_sha256(raw_bytes)
        raw_artifact = create_artifact_record(
            db=db,
            task_id=task_id,
            artifact_type="import_raw",
            content_type="application/json",
            data=raw_bytes,
            metadata={
                "source": source,
                "sha256": sha256,
                "size_bytes": len(raw_bytes),
            },
            event_logger=logger,
        )
        raw_artifact_id = raw_artifact.id
        artifact_ids.append(raw_artifact.id)

        conversations = normalize_chatgpt_export(json.loads(raw_bytes.decode("utf-8")))
        error_ids: List[str] = []

        for conversation in conversations:
            try:
                conversation_id = conversation.get("id") or conversation.get("conversation_id")
                title = conversation.get("title")
                messages = extract_chatgpt_messages(conversation)
                parsed_payload = {
                    "conversation_id": conversation_id,
                    "title": title,
                    "messages": messages,
                }
                artifact = create_artifact_record(
                    db=db,
                    task_id=task_id,
                    artifact_type="import_parsed_text",
                    content_type="application/json",
                    data=json.dumps(parsed_payload).encode("utf-8"),
                    metadata={
                        "derived_from": raw_artifact.id,
                        "parser": "chatgpt_export",
                        "parser_version": "v1",
                        "confidence": "high",
                        "conversation_id": conversation_id,
                        "title": title,
                    },
                    event_logger=logger,
                )
                artifact_ids.append(artifact.id)
            except Exception as exc:
                error_ids.append(
                    create_import_error_artifact(
                        db=db,
                        task_id=task_id,
                        derived_from=raw_artifact.id,
                        error_code="conversation_parse_failed",
                        message=str(exc),
                        logger=logger,
                    )
                )

        metadata_payload = {
            "source": source,
            "num_conversations": len(conversations),
            "sha256": sha256,
        }
        metadata_artifact = create_artifact_record(
            db=db,
            task_id=task_id,
            artifact_type="import_metadata",
            content_type="application/json",
            data=json.dumps({"type": "import_metadata", "payload": metadata_payload}).encode("utf-8"),
            metadata=metadata_payload,
            event_logger=logger,
        )
        artifact_ids.extend(error_ids)
        artifact_ids.append(metadata_artifact.id)

        if error_ids and len(error_ids) == len(conversations):
            log_event(logger, "import_failed", task_id=task_id, artifact_ids=artifact_ids, source=source)
        elif error_ids:
            log_event(logger, "import_partial_failure", task_id=task_id, artifact_ids=artifact_ids, source=source)
        else:
            log_event(logger, "import_completed", task_id=task_id, artifact_ids=artifact_ids, source=source)

        return {
            "artifact_ids": artifact_ids,
            "summary": f"Imported ChatGPT export ({len(conversations)} conversations)",
        }
    except Exception as exc:
        error_id = create_import_error_artifact(
            db=db,
            task_id=task_id,
            derived_from=raw_artifact_id,
            error_code="chatgpt_import_failed",
            message=str(exc),
            logger=logger,
        )
        artifact_ids.append(error_id)
        log_event(logger, "import_failed", task_id=task_id, artifact_ids=artifact_ids, source=source)
        return {
            "artifact_ids": artifact_ids,
            "summary": "Imported ChatGPT export (0 conversations)",
        }
    finally:
        delete_staged_import(staging_path)
