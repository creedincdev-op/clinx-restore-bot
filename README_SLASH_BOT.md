# CLINX Slash Bot Guide

## Run

```powershell
start.bat
```

## Core Commands

- `/backup create`
- `/backup load`
- `/backup list`
- `/backup delete`
- `/backupcreate` (alias)
- `/backupload` (alias)
- `/restore_missing`
- `/cleantoday`
- `/masschannels`

## Export Commands

- `/export guild`
- `/export channels`
- `/export roles`
- `/export channel`
- `/export role`
- `/export message`
- `/export reactions`

## Import Commands

- `/import guild`
- `/import status`
- `/import cancel`

## Panels

- `/panel suggestion`

## Utility

- `/help`
- `/invite`
- `/leave`

## Xenon-Style Load Flow

`/backup load` now opens an embed + action selector:
- Delete Roles
- Delete Channels
- Load Roles
- Load Channels
- Load Settings

Then Confirm/Cancel buttons execute the selected plan.

## Mass Channel Paste Format

`/masschannels` opens a modal so you can paste a larger layout in one or two chunks.

```text
[Start Here]
welcome | New members start here
roles | Pick your roles
rules | Read rules first

[Community]
chat
media | Image and video sharing
voice: Hangout VC

GENERAL
chat
clips

Support:
# tickets | Open a ticket here
```



## Terminal UI

- creed_terminal.html (standalone terminal page)
