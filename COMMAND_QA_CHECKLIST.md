# CLINX Command QA Checklist

Use this file to track live command verification against Discord screenshots.

Status legend:
- `[ ]` Not tested
- `[x]` Passed
- `[-]` Failed / needs fix

## Backup

- [ ] `/backup create`
  Expected: returns a new load ID and source guild info.
- [ ] `/backup load`
  Expected: opens action selector, then applies selected restore steps and returns summary stats.
- [ ] `/backup list`
  Expected: shows saved backup IDs.
- [ ] `/backup delete`
  Expected: deletes the selected backup ID and confirms removal.
- [ ] `/backupcreate`
  Expected: same behavior as `/backup create`.
- [ ] `/backupload`
  Expected: same behavior as `/backup load`.

## Restore / Cleanup

- [ ] `/restore_missing`
  Expected: creates only missing categories/channels from source guild.
- [ ] `/cleantoday`
  Expected: dry-run warning without `confirm=true`, deletes only today's channels with `confirm=true`.
- [ ] `/masschannels`
  Expected: opens modal, parses pasted layout, creates categories/channels correctly, skips duplicates, keeps topics.

## Panels

- [ ] `/panel suggestion`
  Expected: privately confirms posting, then CLINX posts a public suggestion board with v2 components and working intake buttons.

## Export

- [ ] `/export guild`
  Expected: sends full guild snapshot JSON file.
- [ ] `/export channels`
  Expected: sends channels export in selected format.
- [ ] `/export roles`
  Expected: sends roles export in selected format.
- [ ] `/export channel`
  Expected: sends one channel JSON export.
- [ ] `/export role`
  Expected: sends one role JSON export.
- [ ] `/export message`
  Expected: exports selected message as JSON.
- [ ] `/export reactions`
  Expected: exports message reactions in selected format.

## Import

- [ ] `/import guild`
  Expected: accepts snapshot JSON and starts background import job.
- [ ] `/import status`
  Expected: shows current import job state.
- [ ] `/import cancel`
  Expected: cancels running import job and confirms request.

## Utility

- [ ] `/help`
  Expected: opens the public CLINX command library with category tabs, paging, select menu, and invite/support buttons.
- [ ] `/invite`
  Expected: returns bot invite link.
- [ ] `/leave`
  Expected: asks bot to leave current server after confirmation message.

## Notes

- For each command test, attach:
  - screenshot of command input/result
  - short note: `pass` or `issue`
  - if issue: what broke, what you expected, and whether it is reproducible
