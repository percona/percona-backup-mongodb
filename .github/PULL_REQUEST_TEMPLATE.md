<!-- Thanks for contributing to Percona Backup for MongoDB! Please fill in the sections below. -->

## Jira issue

<!-- Link the related issue, e.g. https://jira.percona.com/browse/PBM-1234 -->
PBM-

## Description

<!-- What does this change do, and why? Note any behavior changes users will notice. -->

## Type of change

- [ ] Bug fix
- [ ] New feature or enhancement
- [ ] Refactoring / internal change (no user-facing change)
- [ ] Documentation / repository content
- [ ] Build, CI, or packaging

## Checklist

- [ ] I have followed the [contributing guidelines](https://github.com/percona/percona-backup-mongodb/blob/dev/CONTRIBUTING.md) and signed the [CLA](https://cla-assistant.percona.com/percona/percona-backup-mongodb).
- [ ] The branch name and commit messages include the Jira issue key (for example, `PBM-1234 - My subject`).
- [ ] I have added or updated tests, or explained below why none are needed.
- [ ] Tests pass locally (see [Running tests locally](https://github.com/percona/percona-backup-mongodb/blob/dev/CONTRIBUTING.md#running-tests-locally)).
- [ ] User-facing changes (new commands, flags, config options, or behavior) are documented, or a docs issue or PR is linked.

## Accessibility checklist

Accessibility matters for PBM. Please review our [Accessibility Statement](../ACCESSIBILITY.md) and check every item that applies to your change. Leave items unchecked and write **N/A** next to any section that doesn't apply.

### CLI output, help text, and error messages
<!-- Only if you changed anything `pbm` or `pbm-agent` prints or logs. -->
- [ ] Status and results are written in words. Meaning doesn't rely on color, symbols, or column position alone.
- [ ] `--out json` output stays complete and consistent with the text output.
- [ ] No new interactive-only flow. Any prompt has a flag-based alternative (such as `--yes`).
- [ ] Progress output is kept to a minimum (no new rapidly repeating characters or redrawn lines).
- [ ] Error messages say what failed, why (if known), and what you can do next.
- [ ] `--help` text is updated and complete for new or changed commands and flags.

### Accessibility notes
<!-- Optional: describe accessibility considerations, known limitations, or follow-up issues. -->

## Additional notes for reviewers

<!-- Optional: anything that helps review, such as risky areas, test setup, storage types tested, or MongoDB versions. -->
