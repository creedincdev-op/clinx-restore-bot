# CLINX Slash Bot Guide

## Run

```powershell
start.bat
```

## 24/7 Hosting

- Oracle Always Free: `ORACLE_ALWAYS_FREE.md`
- Hosting summary: `HOSTING_24_7.md`
- Render setup: `RENDER_UPTIME_ROBOT.md`

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
  Posts the public CLINX suggestion board and keeps the command acknowledgement private.

## Utility

- `/help`
  Opens the interactive CLINX command library.
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
