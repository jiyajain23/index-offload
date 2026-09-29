"""Deterministic offline text vectors. This is a small lexical baseline, not an LLM."""

import hashlib
import math
import re

MODEL_VERSION = "local-hash-v1"


def embed(text: str, dimension: int = 384) -> tuple[list[float], list[int], list[float]]:
    tokens = re.findall(r"[a-z0-9]+", text.lower())
    dense = [0.0] * dimension
    sparse: dict[int, float] = {}
    for token in tokens:
        digest = hashlib.sha256(token.encode()).digest()
        dense_index = int.from_bytes(digest[:4], "big") % dimension
        sign = 1 if digest[4] % 2 else -1
        dense[dense_index] += sign
        sparse_index = int.from_bytes(digest[5:9], "big") % 100_000
        sparse[sparse_index] = sparse.get(sparse_index, 0.0) + 1.0
    norm = math.sqrt(sum(value * value for value in dense))
    if norm:
        dense = [value / norm for value in dense]
    indices = sorted(sparse)
    return dense, indices, [sparse[index] for index in indices]
