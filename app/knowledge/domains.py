from enum import Enum


class KnowledgeDomain(str, Enum):
    BELIEF = "belief"
    PROJECT = "project"
    INFLUENCE = "influence"
    COGNITION = "cognition"
    REFERENCE = "reference"
    ARCHIVE = "archive"
    OPS = "ops"


DOMAIN_PRECEDENCE = [
    KnowledgeDomain.BELIEF,
    KnowledgeDomain.PROJECT,
    KnowledgeDomain.OPS,
    KnowledgeDomain.INFLUENCE,
    KnowledgeDomain.COGNITION,
    KnowledgeDomain.REFERENCE,
    KnowledgeDomain.ARCHIVE,
]
