"""Memory that learns: after a chat turn, suggest lasting facts about the user.

Only messages that look like they contain personal facts ("my manager is…", "I prefer…",
"mera…") are checked. The model proposes short third-person notes; the user accepts or
dismisses each one, and nothing is remembered without their OK.

The extraction request reuses JOI's own system prompt as its prefix, so the local model's
prompt cache stays warm and the user's next message isn't slowed down.
"""

import json
import logging
import re
from typing import Iterable, List

from llm_provider import astream_llm

logger = logging.getLogger("memory_learning")

HINT_RE = re.compile(
    r"\b(i am|i'm|im|i was|my|mine|i prefer|i like|i love|i hate|i don't like|i work|i live|"
    r"i study|call me|remember|always|never|mera|meri|mere|mujhe|main)\b",
    re.IGNORECASE,
)

INSTRUCTION = (
    "Do not call any tool and do not answer the message. Your only job now: list every lasting "
    "fact in the message below about the user or the people in their life: names, family, "
    "colleagues and managers, job or studies, where they live, preferences and habits. One-off "
    "requests, questions and today's plans are not lasting. Use only what the message says. "
    "Write each fact separately as a short English note in the third person, without 'I' or "
    "'my' (a Hindi message still gets English notes). For example, the message \"my brother "
    "Rohan is a doctor and I hate mornings\" gives [\"Brother is Rohan\", \"Rohan is a doctor\", "
    "\"Dislikes mornings\"]. Answer ONLY with a JSON array of at most 3 strings, or [] if there "
    "is nothing lasting.\n\nMessage:\n"
)

_STOP = {"the", "and", "user", "is", "a", "an", "of", "to", "in", "at", "as", "for", "with", "has", "are", "their", "they"}


def _grounded(fact: str, message: str) -> bool:
    """Keep a fact only if one of its key words appears in the message (blocks invented ones)."""
    words = [w for w in re.findall(r"[a-zऀ-ॿ]{3,}", fact.lower()) if w not in _STOP]
    msg = message.lower()
    return any(w in msg or w.rstrip("s") in msg for w in words)


def worth_checking(message: str) -> bool:
    return 6 <= len(message) <= 2000 and bool(HINT_RE.search(message))


async def extract_facts(system_prompt: str, message: str) -> List[str]:
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": INSTRUCTION + message},
    ]
    output = ""
    async for _chunk, accumulated in astream_llm(messages):
        output = accumulated
    items: list = []
    # Greedy first (handles nested arrays like ["fact", []]), then the first bracket pair.
    for pattern in (r"\[.*\]", r"\[.*?\]"):
        match = re.search(pattern, output, re.DOTALL)
        if not match:
            continue
        try:
            parsed = json.loads(match.group(0))
        except json.JSONDecodeError:
            continue
        items = parsed if isinstance(parsed, list) else []
        break
    else:
        # Not valid JSON at all: fall back to the quoted strings inside the brackets.
        items = re.findall(r'"([^"]{3,140})"', output)
    flat: list = []
    for item in items:
        flat.extend(item if isinstance(item, list) else [item])
    facts = []
    for item in flat:
        if not isinstance(item, str):
            continue
        text = str(item).strip().rstrip(".")
        if 3 <= len(text) <= 140 and "TOOL_CALL" not in text and _grounded(text, message):
            facts.append(text)
    return facts[:3]


def _normalize(text: str) -> str:
    return re.sub(r"[^a-z0-9ऀ-ॿ ]", "", text.lower()).strip()


def only_new(facts: Iterable[str], existing_memory: str, pending: Iterable[str]) -> List[str]:
    """Drop facts already in memory or already waiting for the user's decision."""
    known = _normalize(existing_memory)
    waiting = {_normalize(p) for p in pending}
    out = []
    for fact in facts:
        n = _normalize(fact)
        if n and n not in known and n not in waiting and n not in {_normalize(f) for f in out}:
            out.append(fact)
    return out
