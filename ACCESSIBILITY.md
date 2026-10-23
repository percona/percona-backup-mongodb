# Accessibility

Percona Backup for MongoDB (PBM) is used by database administrators, SREs, and developers to protect critical data. We want everyone — including people with disabilities and people who use assistive technology such as screen readers, screen magnifiers, braille displays, or voice control — to be able to install, operate, and contribute to PBM.

PBM has no graphical interface. You work with it through the `pbm` command-line tool, the `pbm-agent` service, the [official documentation](https://docs.percona.com/percona-backup-mongodb/), and this GitHub repository. This document explains what accessibility means for these parts, what we expect from contributors, and how to tell us about a barrier.

## Priorities

- **Readable command-line output.** Commands use plain text and say what happened and what to do next. Meaning never depends on color, symbols, or column position alone.
- **Machine-readable output.** Commands that show status or data support `--out json` (or `--out json-pretty`), so you can read results with a screen reader, a script, or another tool instead of parsing aligned tables.
- **Non-interactive operation.** Every action works without interactive prompts or a full-screen terminal interface. Commands that ask for confirmation also accept a flag (such as `--yes`) to skip the prompt.
- **Clear errors and logs.** Error messages and `pbm logs` output state the problem in words, name the affected node or operation, and suggest a next step where possible.
- **Accessible documentation.** We work toward [WCAG 2.2 Level AA](https://www.w3.org/TR/WCAG22/) for the PBM documentation and repository content. This is a goal, not a claim of verified conformance.

## Contributor expectations

If you are contributing code or documentation, please follow these guidelines so we don't make accessibility worse.

### CLI changes

- Don't use color, emoji, or special characters as the only way to convey status (for example, "error" vs. "ok"). Always include a word.
- When you add or change a command that prints status or data, keep the `--out json` output complete and consistent with the text output.
- Don't add interactive-only flows. Every prompt must have a flag-based alternative.
- Keep progress output to a minimum. Prefer a final summary line over frequently repeating characters, which screen readers may read aloud one at a time.
- Write error messages that say what failed, why (if known), and what you can do about it.
- Keep help text (`pbm <command> --help`) complete. It is often the most accessible reference, because it works over SSH with no browser.

### Documentation and repository content

- Use a logical heading hierarchy (don't skip levels).
- Use real Markdown lists and tables, with header rows for tables.
- Put commands and output in fenced code blocks, not screenshots of a terminal.
- Use unique, descriptive link text (avoid "click here" / "read more").
- Give images and diagrams meaningful alternative text. For complex diagrams (such as architecture diagrams), provide an equivalent text description nearby. See the [W3C alt decision tree](https://www.w3.org/WAI/tutorials/images/decision-tree/).
- Don't use color as the only way to convey meaning.
- Documentation lives in a separate repository. See the [documentation contribution guide](https://github.com/percona/pbm-docs).

### Pull requests

- If your change affects CLI output, help text, or documentation, mention any accessibility considerations in the PR description.
- Reviewers may ask for changes that affect accessibility, just as they would for correctness or usability.

## Reporting accessibility issues

If you run into an accessibility barrier with PBM, its documentation, or this repository, please tell us. We treat accessibility reports as valuable expertise, not as complaints.

1. **Open an issue** in the [PBM Jira project](https://jira.percona.com/projects/PBM) and label it `accessibility` <!-- TODO: confirm label/component -->. If you can't use Jira, post on the [Percona Forums](https://forums.percona.com/c/mongodb/percona-backup-for-mongodb) instead.
2. When you can, include:
   - What you were trying to do and what went wrong.
   - The command you ran (with credentials removed) or the documentation page URL.
   - Your PBM version (`pbm version`), operating system, terminal, and assistive technology (with versions).
   - A recording, screenshot, or copied terminal output, if you're comfortable sharing one.

You don't need to assign a severity or share any personal or medical information.

### Severity

Maintainers assign severity during triage.

- **Critical:** Blocks a core task, such as running or checking a backup or restore, with no workaround.
- **Serious:** Makes a core task significantly harder, but a workaround exists (for example, using `--out json` instead of text output).
- **Moderate:** Causes inconsistent or confusing output, but doesn't stop the task.
- **Minor:** Has little impact on completing a task.

### How we respond

- We will acknowledge your report respectfully and treat it as valuable input.
- We will not ask you to disclose a diagnosis or other personal information.
- We will explain next steps, known limitations, and any dependencies.
- When we can, we will suggest a workaround while a fix is in progress.
- We will update the issue when the status or expected timing changes.
- We may ask you to confirm that a fix resolves the barrier before closing the issue.

### Resolution expectations

After triage, each accessibility issue gets a severity and an owner. We aim to:

- **Critical:** Fix it in the next patch release.
- **Serious:** Fix it in one of the next two releases.
- **Moderate / Minor:** Plan it as part of regular release work.

<!-- TODO: confirm these targets, or replace them with day-based targets the team can reliably meet. -->

If we can't meet a target, we will explain why on the issue, share any workaround, and give a revised estimate.

## Ownership and maintenance

The **[PBM maintainers / role — TODO]** own accessibility for PBM. They are responsible for:

- Triaging accessibility reports.
- Tracking known barriers and accessibility work.
- Raising unresolved accessibility risks during release planning.
- Reviewing this document at least once a year and when ownership changes.

## Supported environments

PBM runs on Linux servers and is usually operated in a terminal, either locally or over SSH. We aim to support:

- **Terminals:** standard Linux terminal emulators, and SSH sessions from macOS Terminal, iTerm2, Windows Terminal, and PuTTY.
- **Screen readers used with terminals:** VoiceOver (macOS), NVDA (Windows), and Orca (Linux).
- **Documentation:** current versions of Chrome, Edge, Firefox, and Safari, on desktop and mobile.

We have not formally tested every combination above. If one doesn't work for you, please report it.

## Known limitations

- **Table-style text output.** Commands such as `pbm status` and `pbm list` arrange text output in aligned columns, which can be hard to follow with a screen reader. *Workaround:* use `--out json` or `--out json-pretty`.
- **Progress dots.** When a command waits for an operation to finish, it prints a `.` periodically. Some screen readers may read each dot aloud. *Workaround:* run the command without waiting, then check progress with `pbm status`.
- **Confirmation prompts.** Some destructive commands ask for `[y/N]` confirmation. *Workaround:* review the command, then pass `--yes` to skip the prompt.
- **Diagrams.** The architecture diagram in the README has only a short alt text. The [architecture documentation](https://docs.percona.com/percona-backup-mongodb/details/architecture.html) describes the components in text.

If you find a barrier that isn't listed here, please [report it](#reporting-accessibility-issues).

## Feedback

Accessibility is ongoing work, not a one-time fix. If you have suggestions for improving this document or our practices, open a Jira issue or a pull request. Use the reporting process above for active barriers. Thank you for helping make Percona Backup for MongoDB usable by everyone.
