# Domain docs

This repository uses one domain context for paper-trading experiments.

## Before exploring

Read root `GLOSSARY.md` if it exists. Read relevant decisions under `docs/adr/` if present.
Neither is required to exist; proceed silently when absent. The `domain-modeling` skill creates
terms and decisions only when they are resolved during actual work.

Use the glossary's terms in issues, hypotheses, proposals, and test names.
Surface conflicts with existing decisions explicitly before proposing a different direction.

## Layout

- `GLOSSARY.md`: project terms, without implementation details.
- `docs/adr/`: architectural decisions that justify a lasting trade-off.

Keep the experiment contract in `AGENTS.md` authoritative.
The free simulation, historical v1/v2 behavior, and separate live-data paper comparisons have
different assumptions. Read the guide for the affected experiment through `README.md` before
changing its behavior; the comparisons do not redefine the free simulation's rules.
