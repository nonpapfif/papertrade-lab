"""Keep shared implementations single-owned and production imports acyclic."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tools.check_architecture import cycles, duplicate_bodies, import_graph, sources

ROOT = Path(__file__).resolve().parents[1]


class ArchitectureTests(unittest.TestCase):
    def test_production_imports_have_no_runtime_cycles_including_lazy_imports(self) -> None:
        self.assertEqual(cycles(import_graph(ROOT)), [])

    def test_production_function_bodies_have_one_implementation(self) -> None:
        self.assertEqual(duplicate_bodies(ROOT), [])

    def test_shared_modules_do_not_import_orchestrators(self) -> None:
        graph = import_graph(ROOT)
        leaves = {
            "papertrade.clock", "papertrade.jsonio", "tools.source_identity",
            "tools.strategy_catalog", "tools.capacity_admission", "tools.kr_comparison_account",
        }
        self.assertTrue(leaves <= graph.keys())
        for module in leaves:
            reached = set()
            pending = list(graph[module])
            while pending:
                target = pending.pop()
                if target not in reached:
                    reached.add(target)
                    pending.extend(graph[target])
            with self.subTest(module=module):
                self.assertFalse(any(
                    name.startswith(("tools.run_", "tools.autotest_", "tools.probe_"))
                    or name in {"tools.paper_capacity", "tools.local_validation"}
                    for name in reached
                ), reached)

    def test_frozen_runtime_manifest_covers_transitive_implementation_dependencies(self) -> None:
        from tools.run_async_live_validation import SOURCES

        graph = import_graph(ROOT)
        reached = set()
        pending = ["tools.run_async_live_validation"]
        while pending:
            module = pending.pop()
            if module not in reached:
                reached.add(module)
                pending.extend(graph[module])
        declared = set(SOURCES)
        modules = sources(ROOT)
        required = {str(modules[name].relative_to(ROOT)) for name in reached}
        self.assertFalse(required - declared, sorted(required - declared))

    def test_parser_includes_lazy_relative_edges_and_excludes_typing_only_edges(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            package = root / "papertrade"
            package.mkdir()
            (package / "__init__.py").write_text("")
            (package / "a.py").write_text(
                "from typing import TYPE_CHECKING\n"
                "if TYPE_CHECKING:\n    from . import c\n"
                "def load():\n    from .b import value\n    return value\n"
            )
            (package / "b.py").write_text("from .a import load\nvalue = 1\n")
            (package / "c.py").write_text("from . import a\n")
            graph = import_graph(root)
            self.assertNotIn("papertrade.c", graph["papertrade.a"])
            self.assertIn("papertrade.b", graph["papertrade.a"])
            self.assertEqual(cycles(graph), [["papertrade.a", "papertrade.b"]])

    def test_clones_ignore_names_docstrings_and_locations_but_preserve_behavior(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            package = root / "tools"
            package.mkdir()
            (package / "a.py").write_text(
                'def first(value):\n    """First name."""\n    return sorted(set(value))\n'
            )
            (package / "b.py").write_text(
                '\n\ndef second(value):\n    """Second name."""\n'
                '    return sorted(set(value))\n'
            )
            (package / "c.py").write_text("def different(value):\n    return sorted(value)\n")
            rows = duplicate_bodies(root)
            self.assertEqual(len(rows), 1)
            self.assertEqual({row["name"] for row in rows[0]}, {"first", "second"})
