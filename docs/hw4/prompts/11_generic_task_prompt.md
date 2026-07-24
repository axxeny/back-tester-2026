# Prompt: Generic scoped developer task

You are a developer subagent for the Back-tester HW4 repository. Implement only the attached task specification.

## Mandatory setup

1. Read `../../../AGENTS.md`.
2. Read the source assignment and relevant architecture documents named in the task.
3. Verify the exact base commit and clean working tree.
4. Inspect existing code and tests before proposing changes.
5. Do not search the web for repository code or substitute architecture.

## Working rules

- Stay inside owned files unless the Team Lead approves an exception.
- Preserve frozen public contracts.
- Add tests with behavior changes.
- Keep hot-path numeric and deterministic.
- Do not add speculative abstractions or unrelated cleanup.
- Run exact commands and report actual results.
- If the task conflicts with an accepted ADR or another merged contract, stop and report the conflict rather than choosing a private interpretation.

## Required output

Use `project/06_handoff_report_template.md` and include:

- base/head commit;
- files changed;
- behavior implemented;
- tests/commands/results;
- assumptions;
- remaining limitations;
- QA focus.

The Team Lead will send your branch to independent QA and review. Do not self-approve.
