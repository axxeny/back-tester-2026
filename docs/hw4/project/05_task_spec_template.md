# Task specification template

Copy this template for every delegated implementation task. A task without these fields is not ready for an agent.

## Identity

- **Task ID:**
- **Title:**
- **Milestone:**
- **Base branch/commit:**
- **Assigned role prompt:**
- **Owner:**

## Context

Explain the current behavior, why this task exists, and which architecture decisions apply. Link exact docs and current code paths.

## Scope

### Must implement

- 

### Must not implement

- 

### Owned files/directories

- 

### Files that require Team Lead approval before editing

- public contract headers;
- root `../../../CMakeLists.txt` / `../../../pyproject.toml` unless explicitly owned;
- another active agent's owned files.

## Required behavior

List deterministic input → output/state examples, not only class names.

1. 

## Interfaces

List exact structs, methods, callback payloads, result columns, or stubs this task must consume/provide.

## Acceptance tests

1. 

## Required commands

```bash
# clean configure/build

# targeted tests

# full relevant test set
```

## Performance constraints

State the forbidden hot-path behavior and any benchmark required.

## Deliverables

- implementation;
- developer-authored tests;
- documentation update if a public behavior changes;
- handoff report using `06_handoff_report_template.md`.

## QA focus

List edge cases the independent QA agent should attack.

## Done condition

A concrete, binary list. Avoid “works correctly” without observables.
