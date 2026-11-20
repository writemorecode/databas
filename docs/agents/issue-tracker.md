# Issue tracker: GitHub

Issues and specs live in GitHub Issues for `writemorecode/databas`.
Use the `gh` CLI from this clone.

## Conventions

- Before the first issue creation of a session, check
  `gh repo view --json nameWithOwner`. The target must be
  `writemorecode/databas`; confirm unexpected targets with the user.
- Create: `gh issue create --title "..." --body "..."`.
  For multiline bodies, use a heredoc with `--body-file -`.
- Read: `gh issue view <number> --json number,title,body,labels,comments`.
- List: `gh issue list --state open --json number,title,body,labels`.
  Add label and state filters as needed.
- Comment: `gh issue comment <number> --body "..."`.
- Apply or remove labels: `gh issue edit <number> --add-label "..."`
  or `--remove-label "..."`.
- Close: post an explanatory comment, then `gh issue close <number>`.
  Comment separately even when the issue is already closed.

## Parent issues and dependencies

Use native GitHub sub-issues and blocking dependencies when available.
With gh 2.94+, use `gh issue edit <parent> --add-sub-issue <child>`
and `gh issue edit <child> --add-blocked-by <blocker>`.

On older CLI versions, use GitHub's REST API. Sub-issue and dependency
endpoints require numeric database IDs, not issue numbers or node IDs.

When native relationships are unavailable, put `Part of #<parent>` and
`Blocked by: #<n>, #<n>` at the top of the child body, and track children
in the parent's task list. An issue is unblocked when all blockers close.

## Pull requests as a triage surface

**PRs as a request surface: no.**

## Skill operations

- "Publish to the issue tracker": create a GitHub issue.
- "Fetch the relevant ticket": read the referenced GitHub issue.

## Wayfinding

- A map is one issue labelled `wayfinder:map`.
- Link child tickets as sub-issues, with the parent/task-list fallback above.
- Child labels: `wayfinder:research`, `wayfinder:prototype`,
  `wayfinder:grilling`, or `wayfinder:task`.
- The frontier is the map's open, unassigned children with no open
  blockers; choose the first in map order.
- Claim with `gh issue edit <n> --add-assignee @me` as the session's
  first write.
- Resolve by commenting with the answer, closing the child, and appending
  a summary and link to the map's Decisions-so-far.
