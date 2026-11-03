# Issue tracker: GitHub

Issues and specs live in `nonpapfif/papertrade-lab` on GitHub. Use the `gh` CLI.
Always pass `--repo nonpapfif/papertrade-lab` to issue and pull-request commands.
Follow the repository workflow and experiment contract in `AGENTS.md`.

## Operations

- Read: `gh issue view <number> --repo nonpapfif/papertrade-lab --json number,title,body,labels,comments`.
- List: `gh issue list --repo nonpapfif/papertrade-lab --state open --json number,title,body,labels`.
- Create: write the English body to a temporary file, then use
  `gh issue create --repo nonpapfif/papertrade-lab --title "..." --body-file <path>`.
- Comment: `gh issue comment <number> --repo nonpapfif/papertrade-lab --body-file <path>`.
- Label: `gh issue edit <number> --repo nonpapfif/papertrade-lab --add-label "..."`
  or `--remove-label "..."`. Use the roles in `docs/agents/triage-labels.md`.
- Close: post the explanation first, then use
  `gh issue close <number> --repo nonpapfif/papertrade-lab`.
- Sub-issues: use `gh issue create --parent <parent>` or
  `gh issue edit <parent> --add-sub-issue <child>` with the explicit repo.
  If unavailable, put `Part of #<parent>` in the child and a task link in the parent.
- Dependencies: use `gh issue edit <child> --add-blocked-by <blocker>` with the explicit repo.
  If unavailable, put `Blocked by: #<number>` at the top of the child body.
  A ticket is ready only when all blockers are closed.
- Count open blockers with
  `gh api repos/nonpapfif/papertrade-lab/issues/<number> --jq .issue_dependencies_summary.blocked_by`.
  For the text fallback, read each referenced blocker and check its state.

When a skill says "publish to the issue tracker", create a GitHub issue here.
When it says "fetch the relevant ticket", read the corresponding issue here.
Before the first issue write in a session, verify the target with
`gh repo view nonpapfif/papertrade-lab --json nameWithOwner`.

## Pull requests as a triage surface

**PRs as a request surface: no.**

GitHub shares issue and PR numbers. Resolve an ambiguous reference with `gh pr view`
and fall back to `gh issue view`, always with the explicit repo.

## Wayfinding operations

- Map: one issue labelled `wayfinder:map` containing Notes, Decisions-so-far, and Fog.
- Child tickets: link them as sub-issues, or use the parent task list and `Part of` fallback.
  Label each `wayfinder:<type>` where type is `research`, `prototype`, `grilling`, or `task`.
- Before first use, check that the required wayfinding labels exist; create missing labels
  only as part of an authorized wayfinding task.
- Frontier: read the map's open children in map order and exclude assigned or blocked tickets.
- Claim: assign the chosen ticket to `@me` before working on it.
- Resolve: comment with the answer, close the child, and add a summary and link to the map's
  Decisions-so-far.
