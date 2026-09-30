"""Generate Fumadocs MDX API reference pages from blitzq docstrings.

Run from the repo root: python scripts/gen_api_docs.py
Output goes to docs-web/content/docs/reference/.
"""

from __future__ import annotations

import re
import shutil
from pathlib import Path

import griffe

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "docs-web" / "content" / "docs" / "reference"

PUBLIC_MODULES = [
    "blitzq",
    "blitzq.client",
    "blitzq.task",
    "blitzq.context",
    "blitzq.results",
    "blitzq.retries",
    "blitzq.routing",
    "blitzq.schedules",
    "blitzq.serialization",
    "blitzq.ratelimit",
    "blitzq.exceptions",
    "blitzq.worker",
    "blitzq.metrics",
]


def mdx_escape(text: str) -> str:
    """Escape braces so prose text doesn't get parsed as MDX expressions."""
    return text.replace("{", "\\{").replace("}", "\\}")


def render_signature(obj) -> str:
    parts = []
    for p in obj.parameters:
        if p.name in ("self", "cls"):
            continue
        piece = p.name
        if p.annotation is not None:
            piece += f": {p.annotation}"
        if p.default is not None:
            piece += f" = {p.default}"
        parts.append(piece)
    returns = f" -> {obj.returns}" if getattr(obj, "returns", None) is not None else ""
    return f"({', '.join(parts)}){returns}"


def render_docstring(obj) -> str:
    if obj.docstring is None:
        return ""
    parsed = obj.docstring.parse("numpy")
    out = []
    for section in parsed:
        kind = section.kind.value
        if kind == "text":
            out.append(mdx_escape(section.value))
        elif kind == "parameters":
            out.append("**Parameters**\n")
            for p in section.value:
                ann = f" `{p.annotation}`" if p.annotation else ""
                desc = mdx_escape(p.description or "")
                out.append(f"- `{p.name}`{ann} - {desc}")
        elif kind == "returns":
            out.append("**Returns**\n")
            for r in section.value:
                ann = f" `{r.annotation}`" if r.annotation else ""
                desc = mdx_escape(r.description or "")
                name = f"`{r.name}` " if r.name else ""
                out.append(f"- {name}{ann} - {desc}")
        elif kind == "raises":
            out.append("**Raises**\n")
            for r in section.value:
                out.append(f"- `{r.annotation}` - {mdx_escape(r.description or '')}")
        elif kind == "examples":
            out.append("**Examples**\n")
            for ex in section.value:
                out.append(mdx_escape(ex[1]) if isinstance(ex, tuple) else mdx_escape(str(ex)))
        out.append("")
    return "\n".join(out)


def render_function(func, heading_level: int) -> str:
    h = "#" * heading_level
    sig = render_signature(func)
    lines = [f"{h} `{func.name}{sig}`\n"]
    doc = render_docstring(func)
    if doc:
        lines.append(doc)
    return "\n".join(lines)


def render_class(cls) -> str:
    lines = [f"## `class {cls.name}`\n"]
    doc = render_docstring(cls)
    if doc:
        lines.append(doc)
    methods = [
        m
        for name, m in cls.members.items()
        if m.kind.value == "function" and not name.startswith("_") or name == "__init__"
    ]
    for method in sorted(methods, key=lambda m: (m.name != "__init__", m.name)):
        if method.name.startswith("_") and method.name != "__init__":
            continue
        lines.append(render_function(method, heading_level=3))
    return "\n".join(lines)


def render_module(mod) -> str:
    title = mod.name
    lines = [
        "---",
        f"title: {title}",
        f"description: API reference for {title}.",
        "---",
        "",
    ]
    if mod.docstring is not None:
        lines.append(mdx_escape(mod.docstring.value))
        lines.append("")

    classes = [m for m in mod.members.values() if m.kind.value == "class" and not m.name.startswith("_")]
    functions = [
        m for m in mod.members.values() if m.kind.value == "function" and not m.name.startswith("_")
    ]

    for cls in sorted(classes, key=lambda c: c.name):
        lines.append(render_class(cls))
        lines.append("")

    for func in sorted(functions, key=lambda f: f.name):
        lines.append(render_function(func, heading_level=2))
        lines.append("")

    return "\n".join(lines)


def slugify(module_name: str) -> str:
    return re.sub(r"[._]", "-", module_name)


def main() -> None:
    if OUT_DIR.exists():
        shutil.rmtree(OUT_DIR)
    OUT_DIR.mkdir(parents=True)

    package = griffe.load("blitzq", search_paths=[str(ROOT / "src")])

    pages = []
    for mod_name in PUBLIC_MODULES:
        mod = package if mod_name == "blitzq" else package[mod_name.removeprefix("blitzq.")]
        slug = "index" if mod_name == "blitzq" else slugify(mod_name.removeprefix("blitzq."))
        content = render_module(mod)
        (OUT_DIR / f"{slug}.mdx").write_text(content, encoding="utf-8")
        pages.append(slug)
        print(f"wrote reference/{slug}.mdx")

    meta = {
        "title": "API Reference",
        "pages": pages,
    }
    import json

    (OUT_DIR / "meta.json").write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
    print("wrote reference/meta.json")


if __name__ == "__main__":
    main()
