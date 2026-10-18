(notifications)=

# Notifications

omniload can send a short report when a load ends, successful or not, to any
target [Apprise] supports, such as Slack, ntfy, email or a webhook.
Unattended runs (cron, Kubernetes jobs, AWS Lambda) then report their outcome
without a wrapper script that parses logs.

## Install

The notification support is an optional extra:

```shell
pip install 'omniload[notify]'
```

It is part of `omniload[full]`. Without the extra, a run that asks for a
notification stops before the load starts and names the extra to install.

## Usage

Name an Apprise URL with `--notify`. Each URL receives one report per run, for
the run's final outcome:

```shell
omniload ingest \
    --source-uri 'postgresql://user:pass@localhost/app' \
    --source-table 'public.orders' \
    --dest-uri 'duckdb:///warehouse.duckdb' \
    --dest-table 'raw.orders' \
    --notify 'ntfys://ntfy.sh/nightly-loads'
```

`--notify` can be given more than once. Some targets, as Apprise URLs:

- email: `mailto://user:pass@example.com`
- ntfy: `ntfys://ntfy.sh/<topic>`
- Slack: `slack://<token-a>/<token-b>/<token-c>`

The [Apprise wiki] documents the URL format of every target. A notification URL
usually carries a secret (a webhook token, a password), so treat it like a
connection string.

`--notify-on failure` sends a report only when the run fails, for a job that
should stay quiet while it works. The default is `--notify-on always`.

From Python, the same options are keyword arguments of {ref}`run_ingest
<python-api>`:

```python
from omniload import run_ingest

run_ingest(
    source_uri="postgresql://user:pass@localhost/app",
    source_table="public.orders",
    dest_uri="duckdb:///warehouse.duckdb",
    dest_table="raw.orders",
    notify=["ntfys://ntfy.sh/nightly-loads"],
    notify_on="failure",
)
```

## Two inputs that add up

Inline URLs suit a one-off run. Routing, where different targets hear about
different outcomes, lives in an Apprise config file. omniload takes both:

- `--notify <apprise-url>`: repeatable, `OMNILOAD_NOTIFY`, `notify=[...]` on
  `run_ingest`.
- `--notify-config <path-or-url>`: repeatable, `OMNILOAD_NOTIFY_CONFIG`,
  `notify_config=[...]` on `run_ingest`. It takes whatever Apprise reads as a
  config source: a local YAML or TEXT file, or a remote one over `http(s)://`.

Given together, they add up: inline URLs and every config source go into one
Apprise object, and each run makes a single `notify()` call. A flag replaces its
own environment variable, as everywhere else in `ingest`, but `--notify` and
`--notify-config` never replace each other.

```shell
# Ad hoc: one target, every outcome.
omniload ingest --source-uri ... --source-table ... --dest-uri ... --dest-table ... \
    --notify 'ntfys://ntfy.sh/nightly-loads'

# Routed by a config file, plus an inline target for this run.
omniload ingest ... --notify-config notify.yml --notify 'mailto://user:pass@example.com'
```

The environment variables take several values separated by whitespace, for
example `OMNILOAD_NOTIFY='ntfys://ntfy.sh/a mailto://user:pass@example.com'`.

:::{note}
Like `--columns`, and unlike the other `ingest` options, these three have no bare
environment variable name (`NOTIFY`, `NOTIFY_CONFIG`, `NOTIFY_ON`), and omniload ignores Apprise's own
discovery: `APPRISE_URLS`, `APPRISE_CONFIG_PATH` and the default config paths
such as `~/.apprise.yml`. A notification is a side effect, so a run only sends
one when its own invocation asks for it.
:::

## Routing with a config file

omniload sends each report with the outcome as its Apprise tag, `success` or
`failure`, and as its notification type. Inline URLs receive every report that
goes out. Config entries route by their own tags:

- `always`: every report.
- `failure`: reports of failed runs.
- `success`: reports of successful runs.

```yaml
# notify.yml: Slack hears about every run, ntfy only about failures.
urls:
  - slack://T1JJ3T3L2/A1BRTD4JD/TIiajkdnlazkcOXrIdevi7FQ:
      tag: always
  - ntfys://ntfy.sh/nightly-failures:
      tag: failure
```

`--notify-on failure` still decides whether a report goes out at all, config
targets included, while tags decide where it goes. One shared config file can
then serve a job that only wants failures.

The rest of the format is Apprise's own, and the [Apprise config] pages describe
it. Three parts of it shape what omniload sends:

- **Includes**: `include:` is followed one level deep, as `apprise -c` does by
  default. A remote config cannot include a local file; Apprise refuses that
  include and keeps the remote file's own entries.
- **Priorities**: a tag with a priority (`1:failure`) escalates as it does in
  Apprise. Entries of the lowest priority go first, and the next group is only
  tried when one of them fails. Inline `--notify` URLs and `failure` entries
  without a priority are priority 0, so while they succeed, a `1:failure` entry
  receives nothing. Entries tagged `always` sit outside that escalation: each of
  them is sent to, whatever its priority, and none of them holds back a
  `1:failure` entry.
- **Untagged entries receive nothing**, since that is how Apprise matches a tag
  filter. Give every entry a tag.

### Checks before the load

Apprise accepts a missing file, broken YAML or an empty file without an error,
so omniload reads each config source on its own before the load starts. It
stops the run with an error naming the source by position and scheme, for
example `--notify-config #2 (file)`, when:

- An inline URL is not a URL Apprise knows.
- Apprise cannot read the config source at all.
- The config file does not exist.
- The source holds no usable entry tagged `success`, `failure` or `always`. That
  covers broken YAML, an empty file, a file of unknown schemes, a file of only
  untagged or differently tagged entries, and a `${NAME}` template that has no
  value.

A broken config therefore fails under `--dry-run`, which reads it and sends
nothing. A file with one bad entry among good ones runs, without the bad entry,
as it would under Apprise.

A remote config that yields nothing is the exception in a real run: it logs a
warning and the load goes ahead, so a config server being down does not stop a
nightly job. Under `--dry-run` the same config is an error. omniload reads each
remote config once, before the load, and sends to what it read: the
notification makes no second fetch, and a dry run still fetches the config.

To see why Apprise rejects a config, run Apprise's own check, outside omniload:

```shell
apprise -c notify.yml --dry-run
```

:::{warning}
`apprise --dry-run` prints the targets it would notify, and its output can
contain secrets: header values in particular (`+Authorization=...` in a
`json://` or `jsons://` URL) appear unmasked. Do not paste it into a ticket.
:::

## Opening a GitHub issue on failure

Apprise has no GitHub target yet. Its `jsons://` target can call the GitHub
REST API instead, as the Apprise maintainer [showed on the omniload
tracker][github recipe]:

```yaml
# notify.yml
template:
  - github_token
urls:
  - "jsons://api.github.com/repos/OWNER/REPO/issues?+Authorization=Bearer%20${github_token}&+Accept=application/vnd.github%2Bjson&:message=body&:version=&:attachments=&:type=":
      tag: failure
```

```shell
export APPRISE_TEMPLATE_GITHUB_TOKEN='<token>'
omniload ingest ... --notify-config notify.yml
```

The `+` parameters set the request headers, `:message=body` renames the report
body to the field GitHub expects, and the empty `:version=`, `:attachments=` and
`:type=` drop fields GitHub does not take. Each POST creates a new issue (the
[GitHub create-an-issue] endpoint), titled with the report's title.

:::{important}
- **Route it by `failure`.** Every report sent to it opens another issue. An
  inline `--notify` URL receives every outcome, so use this recipe from a config
  file, or inline only together with `--notify-on failure`.
- **Keep the token out of the file and the shell history.** The example reads it
  from Apprise's template variable `APPRISE_TEMPLATE_GITHUB_TOKEN`, set by your
  scheduler or secret store. A config file that holds the token needs tight file
  permissions. Without a value, the entry cannot resolve, and the check before
  the load fails.
:::

## What the report contains

A report is short, in Markdown, with a one-line title such as
`omniload failure at load: postgresql → duckdb`:

```text
Status: **success**

- Source: `file`
- Destination: `duckdb` / `out.t`
- Started: 2026-10-18T19:31:55+00:00
- Finished: 2026-10-18T19:31:55+00:00
- Duration: 0.59 seconds
- Load packages: 1
- Failed jobs: 0
- omniload: `0.20.0`

Rows normalized:
- `t`: 3 rows
```

A failed run adds the stage it stopped at (`setup`, `staging`, `extract`,
`normalize`, `load`, `destination_post_load` or `source_post_load`, or
`pipeline` for any other dlt step) and the type
of the exception, such as `NoSuchTableError`. Row counts are the rows dlt
normalized, not rows committed, without dlt's own state tables. A count omniload
does not know for the run is shown as unavailable, never as 0.

The report leaves out anything that can carry a credential:

- **Exception and job messages**: only the exception type appears. The full
  error stays in the run's log.
- **Table selector payloads**: a selector keeps its leading name only. A
  `query:` selector reads as `custom query`, another `<kind>:<payload>`
  selector as `<kind> (custom selector)`, and a `?` or `#` part is dropped.
- **URIs**: only the scheme appears, never credentials, hosts or paths.

Table names dlt produced are shown when they are plain identifiers. Nested
tables are named after the keys in the data, and a spreadsheet source names
one table per worksheet, so keys and sheet names can appear. When the
destination table selector itself had a part redacted, the report numbers the
tables (`table 1`) instead.

## When sending fails

The report goes out after the load has committed and the source has run its
own commit or cleanup, so a notification never delays an acknowledgement.

Once the load has run, a report that cannot be built or delivered (a target
that is down, a timeout, a partial delivery) logs one warning, which names a
failed target by its scheme at most. The return value, the exception and the exit status
stay what they would have been without the notification. Each `notify()` call
waits at most 30 seconds. A target still sending after that keeps going until
its own network timeouts end it, and the process waits for it before exiting.

A run stopped by `KeyboardInterrupt` or `SystemExit` sends no report. An
interrupt while the report is being sent interrupts as usual. A `SystemExit`
raised while the report is being sent, by a plugin or by a signal handler of a
program that embeds omniload, is logged as a failed send and does not end
the run.

### What omniload keeps out of the log

Apprise's own log lines, and those of the libraries it sends through, print
notification URLs. While omniload calls Apprise, it drops every log record
created in that call, its own excepted, including records a delivery thread
writes after a timeout. It does so with a hook on `logging.Logger.handle`,
installed the first time a run is given a notification target. The hook is inert outside
those calls, so everything else logs as usual. omniload's own messages name a
target by position and scheme only.

What the hook does not cover:

- A logger class that overrides `handle`, or code that kept a reference to
  `Logger.handle` from before omniload installed the hook.
- Output written with `warnings.warn`, which does not go through logging unless
  `logging.captureWarnings(True)` is set. `urllib3` warns this way, naming the
  host, when a target turns off certificate checks (`?verify=no`).
- Threads a plugin starts without the calling context, such as the XMPP
  plugin's own thread.

Of these, the XMPP thread and the `urllib3` warning occur with Apprise 2.0 and
the libraries it sends through.

[Apprise]: https://github.com/caronc/apprise
[Apprise config]: https://github.com/caronc/apprise/wiki/config
[Apprise wiki]: https://github.com/caronc/apprise/wiki
[GitHub create-an-issue]: https://docs.github.com/en/rest/issues/issues#create-an-issue
[github recipe]: https://github.com/panodata/omniload/issues/100#issuecomment-6243654375
