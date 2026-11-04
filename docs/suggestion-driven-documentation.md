# Suggestion-Driven Documentation

| Field   | Value      |
| ------- | ---------- |
| Version | 0.1        |
| Date    | 2026-04-12 |

Instead of making documentation changes on the fly during a pull request review, ask LLM reviewers to write each actionable suggestion down in a suggestion document. Review those documents afterward, decide which suggestions to implement, and keep them as the record of what was proposed, accepted, rejected, or deferred.

The default is one suggestion per document. A single document may contain multiple suggestions only for small editorial-only batches such as typos, formatting fixes, or similar non-behavioral wording changes.

Store suggestion documents in `docs/suggestions/`.

Use the following filename format:

`S{number}_{title}_{tool-model-effort}.md`

Example: `S001_improve-heading-clarity_claude-sonnet-4-6-high.md`

- `number` must be sequential and zero-padded to three digits: `001`, `002`, `003`, and so on.
- Suggestion numbers are tracked per target file suggestion folder (for example `docs/suggestions/contracts/` or `docs/suggestions/components/`), not globally across all suggestion folders.
- Suggestion numbers are never reused within the same target file suggestion folder. Each new suggestion gets the next available number in that folder, and gaps from rejected or removed suggestions are acceptable.
- `title` must be a short lowercase kebab-case summary, for example `improve-heading-clarity`.
- `tool-model-effort` must be a lowercase hyphenated identifier for the authoring tool, model, and effort level, for example `claude-sonnet-4-6-high` or `codex-gpt-5-medium`.

Each suggestion document should use this structure:

```md
# S012 - Improve heading clarity

| Field                    | Value                                 |
|--------------------------|---------------------------------------|
| Priority                 | medium                                |
| File                     | `docs/example.md`                     |
| Decision                 | pending                               |
| Implementation reference |                                       |
| Created at               | 2026-04-12                            |
| Author                   | Claude Code, claude-sonnet-4-6, high  |
| Reviewer                 |                                       |

## Issue
Briefly describe the problem being pointed out.

## Suggestion
Describe the proposed change clearly and concretely.
```

- `Priority` records importance, such as `low`, `medium`, or `high`.
- `File` records the original file being reviewed, not the proposal copy.
- `Decision` records the current state of the suggestion: `pending`, `accepted`, `rejected`, or `deferred`.
- `Implementation reference` links to the commit, pull request, or document change that implemented the suggestion.
- `Created at` records when the suggestion was written.
- `Author` records who created the suggestion. For tools, include the tool name, model, and effort level.
- `Reviewer` records who reviewed the suggestion. Leave it blank while `Decision` is `pending`. Populate it with the reviewer identity when `Decision` is `accepted`, `rejected`, or `deferred`.

## Creating suggestions

When an LLM tool is given a file to review:

1. Review the target document and write suggestions into `docs/suggestions/{filename}` folder, where `{filename}` is the name of the file being reviewed without its extension or directory path (for example, suggestions for `docs/contracts.md` go into `docs/suggestions/contracts/`).
2. Commit the suggestion documents.

## Applying suggestions


1. Create a copy in `docs/proposals/` named `{filename}-proposal-v{number}.{extension}`. `{filename}` is the file's name without its extension or directory path. Version numbers start at `1`. If proposal versions already exist for that file, use the next number after the highest existing version.
2. Commit the proposal copy as-is.
3. Read all suggestion documents with `Decision` set to `pending`.
4. If there are any questions for engineering director, ask them.
5. For each suggestion, apply the change to the proposal copy, fill in the `Implementation reference` field, and commit before moving to the next suggestion.
6. Once every suggestion targeting a proposal has been resolved, copy proposal over original file.
