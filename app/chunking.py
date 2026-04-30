from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TextChunk:
    index: int
    start: int
    end: int
    text: str


def chunk_text(text: str, *, max_chars: int, overlap: int = 400) -> list[TextChunk]:
    """
    Simple character-based chunking with overlap.
    Keeps chunks reasonably aligned to newlines when possible.
    """
    if max_chars <= 0:
        raise ValueError("max_chars must be > 0")
    if overlap < 0:
        raise ValueError("overlap must be >= 0")

    n = len(text)
    if n == 0:
        return []

    chunks: list[TextChunk] = []
    i = 0
    idx = 0
    while i < n:
        end = min(n, i + max_chars)

        # try not to cut in the middle of a line
        if end < n:
            nl = text.rfind("\n", i, end)
            if nl != -1 and (end - nl) < 200:
                end = nl

        chunk = text[i:end].strip()
        if chunk:
            chunks.append(TextChunk(index=idx, start=i, end=end, text=chunk))
            idx += 1

        if end >= n:
            break
        i = max(0, end - overlap)

    return chunks

