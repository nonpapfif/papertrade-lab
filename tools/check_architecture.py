"""Check local import direction and duplicated executable function bodies."""

from __future__ import annotations

import argparse
import ast
import json
from collections import defaultdict
from pathlib import Path
from typing import Any


def sources(root: Path) -> dict[str, Path]:
    return {
        ".".join(path.relative_to(root).with_suffix("").parts).removesuffix(".__init__"): path
        for package in ("papertrade", "tools")
        for path in sorted((root / package).rglob("*.py"))
    }


class _Imports(ast.NodeVisitor):
    def __init__(self, module: str, package: bool, known: set[str]) -> None:
        self.package = module if package else module.rpartition(".")[0]
        self.known = known
        self.edges: set[str] = set()

    def visit_If(self, node: ast.If) -> None:
        test = node.test
        if (
            isinstance(test, ast.Name) and test.id == "TYPE_CHECKING"
            or isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"
        ):
            for child in node.orelse:
                self.visit(child)
        else:
            self.generic_visit(node)

    def visit_Import(self, node: ast.Import) -> None:
        self.edges.update(alias.name for alias in node.names if alias.name in self.known)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        base = node.module or ""
        if node.level:
            parts = self.package.split(".")
            if node.level > 1:
                parts = parts[: -(node.level - 1)]
            base = ".".join([*parts, *([base] if base else [])])
        candidates = {base, *(f"{base}.{alias.name}" for alias in node.names)}
        self.edges.update(candidates & self.known)


def import_graph(root: Path) -> dict[str, set[str]]:
    modules = sources(root)
    result = {}
    for module, path in modules.items():
        visitor = _Imports(module, path.name == "__init__.py", set(modules))
        visitor.visit(ast.parse(path.read_text(), filename=str(path)))
        result[module] = visitor.edges - {module}
    return result


def cycles(graph: dict[str, set[str]]) -> list[list[str]]:
    indices: dict[str, int] = {}
    lows: dict[str, int] = {}
    stack: list[str] = []
    active: set[str] = set()
    result = []

    def visit(node: str) -> None:
        indices[node] = lows[node] = len(indices)
        stack.append(node)
        active.add(node)
        for target in sorted(graph[node]):
            if target not in indices:
                visit(target)
                lows[node] = min(lows[node], lows[target])
            elif target in active:
                lows[node] = min(lows[node], indices[target])
        if lows[node] == indices[node]:
            group = []
            while True:
                target = stack.pop()
                active.remove(target)
                group.append(target)
                if target == node:
                    break
            if len(group) > 1:
                result.append(sorted(group))

    for node in sorted(graph):
        if node not in indices:
            visit(node)
    return sorted(result)


def duplicate_bodies(root: Path) -> list[list[dict[str, Any]]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for path in sources(root).values():
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            body = node.body[:]
            if (
                body and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                body = body[1:]
            # Ignore declarations and trivial property access, not executable helper bodies.
            if sum(len(list(ast.walk(statement))) for statement in body) < 8:
                continue
            key = "|".join(ast.dump(statement, include_attributes=False) for statement in body)
            groups[key].append({
                "path": str(path.relative_to(root)), "line": node.lineno, "name": node.name,
            })
    return [rows for rows in groups.values() if len(rows) > 1]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    graph = import_graph(args.root)
    result = {
        "module_count": len(graph),
        "runtime_import_cycles": cycles(graph),
        "duplicate_function_bodies": duplicate_bodies(args.root),
    }
    print(json.dumps(result, indent=2))
    return int(bool(result["runtime_import_cycles"] or result["duplicate_function_bodies"]))


if __name__ == "__main__":
    raise SystemExit(main())
