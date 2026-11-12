# Issue tracker: GitHub

Issues and specs live in `alexnham/helius-indexer`.
Use the authenticated `gh` CLI.

Verify the target with `gh repo view --json nameWithOwner`.
Specify `--repo alexnham/helius-indexer` when operating outside the checkout.

Read issue bodies and comments before acting.
Use `--body-file` for multiline issue bodies and comments.

Publish one issue per approved ticket. Use native blocking relationships;
if unavailable, list blocking issue references in the issue body.
A ticket is startable only when all blockers are complete.

Use native sub-issues when an existing issue is the parent.
Keep parent issues unchanged unless explicitly authorized.

Apply the vocabulary in `triage-labels.md`.
Publish tickets after the user approves their breakdown.

PRs as a request surface: no.
