# Agent prompt index

These prompts are designed for a Team Lead that can coordinate multiple subagents. Each prompt is self-contained enough to paste into an agent together with a task brief and repository path.

## Recommended invocation order

1. [`00_teamlead_orchestrator.md`](00_teamlead_orchestrator.md)
2. [`01_dev_baseline_packaging.md`](01_dev_baseline_packaging.md)
3. Team Lead freezes contracts, then may run in parallel:
   - [`02_dev_core_market.md`](02_dev_core_market.md)
   - [`03_dev_scheduler_concurrency.md`](03_dev_scheduler_concurrency.md)
   - [`05_dev_python_api.md`](05_dev_python_api.md) against a stub
4. [`04_dev_trading_matching.md`](04_dev_trading_matching.md)
5. [`06_dev_results_pnl.md`](06_dev_results_pnl.md)
6. [`07_dev_integration_benchmarks.md`](07_dev_integration_benchmarks.md)
7. Every implementation task is followed by:
   - [`08_qa_independent.md`](08_qa_independent.md)
   - [`09_reviewer_independent.md`](09_reviewer_independent.md)
8. Use [`10_bugfix_agent.md`](10_bugfix_agent.md) for concrete QA/review findings.
9. Use [`11_generic_task_prompt.md`](11_generic_task_prompt.md) for tasks that do not fit a specialization.

## Inputs Team Lead must append to a role prompt

- repository path;
- base branch and exact base commit;
- task spec from `project/05_task_spec_template.md`;
- owned and forbidden files;
- dependency commits/interfaces;
- expected validation commands.

Do not give an agent only a broad instruction such as “implement Group A.”
