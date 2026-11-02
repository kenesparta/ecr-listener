# ecr-listener

A small Linux daemon that deploys container images when they are pushed to Amazon ECR. It
long-polls an SQS queue that an EventBridge rule fills with ECR push events and, for each
pushed `repository:tag` it is configured to watch, runs `systemctl start` on that project's
deploy unit. It deletes the event only if the unit succeeded.

```text
docker push ──► ECR ──► EventBridge rule ──► SQS queue ──► ecr-listener ──► systemctl start <unit>
                                                 │                              │
                                                 └──► DLQ (after 8 deliveries)  └──► docker compose pull && up -d
```

The daemon never talks to Docker or ECR itself. The unit does the pull with its own
credentials; `ecr-listener` only decides *when* to start it.

## Why

The host pulls; nothing pushes to it. `ecr-listener` makes outbound HTTPS calls to SQS only, so
the host needs no inbound port, no webhook endpoint and no CI credentials that can reach it.
Compared with a timer that pulls every few minutes, a push is deployed within seconds, and
nothing is pulled while nothing is pushed.

It was written for a single small host (2 GB RAM) running a handful of Docker Compose projects,
each deployed by a systemd oneshot unit, where it is the only deploy path. That shapes it:
deploys run one at a time, a message is deleted only after a successful deploy, and every
failure leaves the event on the queue to be retried.

## Configuration

`ecr-listener [--config <path>]` reads `/etc/ecr-listener/config.toml` by default.
`ecr-listener --version` prints the version.

```toml
queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/kenesparta-host-deploy-events"
region = "us-east-1"
deploy_timeout_secs = 360   # optional, default 360: client-side cap on one `systemctl start`

[[project]]
name = "blog"
repository = "kenespartadev"
tag = "latest"
unit = "personal-infra-deploy@blog.service"
```

| Key                   | Rule                                                                               |
|-----------------------|------------------------------------------------------------------------------------|
| `queue_url`           | Starts with `https://`.                                                            |
| `region`              | Not empty. The region is always taken from this file, never from the environment. |
| `deploy_timeout_secs` | 30 to 3600; 360 when absent.                                                       |
| `[[project]]`         | At least one.                                                                      |
| `name`                | Not empty; unique.                                                                 |
| `repository`, `tag`   | Not empty; each `(repository, tag)` pair unique. Compared verbatim with the event. |
| `unit`                | Matches `^[A-Za-z0-9][A-Za-z0-9@._:-]*\.service$`, so it can never start with `-`. |

Unknown keys are rejected. The file is validated before anything touches AWS; an invalid file
makes the daemon exit with status **2** and a message on stderr naming the file and the problem.
[`config.example.toml`](config.example.toml) is a starting point.

## Credentials and IAM

Credentials come from the standard AWS default chain. The reference unit below points it at a
static key with `AWS_SHARED_CREDENTIALS_FILE` and sets `AWS_EC2_METADATA_DISABLED=true`, so the
chain never falls through to instance metadata. Profiles that use SSO or `credential_process`
are not supported: those providers are compiled out to keep the binary small.

The key needs exactly these actions, on the one queue:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:ChangeMessageVisibility",
        "sqs:GetQueueAttributes"
      ],
      "Resource": "arn:aws:sqs:us-east-1:123456789012:kenesparta-host-deploy-events"
    }
  ]
}
```

`DeleteMessageBatch` and `ChangeMessageVisibilityBatch`, which the daemon uses, are authorized
by `sqs:DeleteMessage` and `sqs:ChangeMessageVisibility`. With SSE-SQS the key needs no KMS
permissions. Every SDK call is capped at 40 seconds, retries included, so a stalled connection
cannot hang the 20-second long poll.

## The queue it expects

| Setting                 | Value                                                                       |
|-------------------------|-----------------------------------------------------------------------------|
| Type                    | Standard                                                                    |
| Encryption              | SSE-SQS                                                                     |
| Visibility timeout      | 900 s                                                                       |
| Receive wait time       | 20 s (the daemon also asks for 20 s on every receive)                       |
| Redrive policy          | A dead-letter queue, `maxReceiveCount` 8                                    |
| Queue policy            | `sqs:SendMessage` for `events.amazonaws.com`, conditioned on the rule's ARN |

The EventBridge rule sends ECR image actions unchanged (no input transformer):

```json
{
  "source": ["aws.ecr"],
  "detail-type": ["ECR Image Action"],
  "detail": { "action-type": ["PUSH"], "result": ["SUCCESS"] }
}
```

Narrowing the rule further (by `repository-name`, for instance) is fine; the daemon filters on
its own anyway.

## What it does with each message

Each `ReceiveMessage` returns up to 10 messages. Every message body is an EventBridge envelope;
unknown fields are ignored.

| Message                                                                                                                     | Outcome                                                     |
|-----------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------|
| Not JSON, not `source` `aws.ecr` with `detail-type` `ECR Image Action`, or a malformed `detail`                            | Logged as a warning and **kept**, with the retry delay below, so it ends up in the DLQ for inspection |
| An ECR image action that is not ours: `action-type` not `PUSH`, `result` not `SUCCESS`, no `image-tag` (untagged pushes such as buildx attestation manifests), or a `repository:tag` no project watches | Logged as `ignored` with its repository and tag, and **deleted** |
| A successful tagged push of a watched `repository:tag`                                                                      | Deployed                                                    |

Within one batch, all pushes for the same project become **one** deploy. Projects are deployed
one at a time, in the order of the configuration file; concurrent pulls would compete for
memory on a small host. For each project the daemon:

1. sets its messages' visibility to `deploy_timeout_secs + 60` seconds, so they stay hidden while
   the deploy runs;
2. logs the pushed digests and runs `systemctl start --no-ask-password -- <unit>`, waiting at
   most `deploy_timeout_secs`. `systemctl start` blocks until a oneshot unit finishes, and exit
   status 0 means success;
3. on success, deletes the project's messages;
4. on failure, timeout or a spawn error, logs an error naming the unit, the exit status and the
   hint `journalctl -u <unit>`, and defers each message by its retry delay.

The message body never reaches the command line: it only selects one of the units listed in the
configuration. The worst a forged event can do is trigger a redeploy of a configured project.

## Failure semantics

A message is deleted only after the deploy it asked for succeeded, or when it is a well-formed
event that is not ours. Everything else stays on the queue and is delivered again, which makes
redeploys possible, so **deploy units must be idempotent**.

The retry delay of a message is `min(60 × 2^(receive_count − 1), 900)` seconds, where
`receive_count` is SQS's `ApproximateReceiveCount`:

| Delivery            | 1    | 2     | 3     | 4     | 5–8    |
|---------------------|------|-------|-------|-------|--------|
| Retried after       | 60 s | 120 s | 240 s | 480 s | 900 s  |

With `maxReceiveCount` 8, a message that fails every time moves to the dead-letter queue about
75 minutes after its first delivery. **The daemon never reads the DLQ**: messages there are kept
for a person to inspect, and a deploy that landed there is not retried until the next push (or
until the message is redriven).

Other failures:

- **A batch delete or visibility change fails**, whole or per entry: logged as a warning. An
  undeleted message reappears and causes an idempotent redeploy; a message whose visibility was
  not changed keeps its current timeout.
- **`ReceiveMessage` fails** (network, throttling, credentials revoked): logged as an error and
  retried with a backoff of 1 s doubling to 60 s, reset by the next successful receive. After
  startup the daemon never exits on an AWS error.
- **Startup**: the daemon calls `GetQueueAttributes` (`QueueArn`) to prove the key and the queue
  before telling systemd it is ready. If that fails it logs the error and exits with status 1,
  so a bad key or queue URL is a failed start, and `Restart=always` tries again.
- **SIGTERM or SIGINT**: the daemon sends `STOPPING=1`, cancels the long poll or the batch in
  progress, and exits 0. An abandoned `systemctl start` client is killed; the systemd job it
  queued runs to completion regardless, and its undeleted messages reappear later.

## Logging

Logs go to stderr as plain lines, without colours or timestamps (journald stamps them).
`RUST_LOG` filters them, defaulting to `info`.

The `aws_config` target is held at `warn` whatever `RUST_LOG` says, unless `RUST_LOG` names it
explicitly (`RUST_LOG=info,aws_config=info`). At `info` the SDK logs the credentials it
resolved, and their debug form includes the access key id. The secret is redacted and the id is
not a secret, but there is no reason to keep it in the journal.

```text
INFO ecr_listener::daemon: listening queue=https://sqs.us-east-1.amazonaws.com/123456789012/kenesparta-host-deploy-events queue_arn=arn:aws:sqs:us-east-1:123456789012:kenesparta-host-deploy-events projects=4
INFO deploy{project=blog}: ecr_listener::executor: deploying unit=personal-infra-deploy@blog.service digests=sha256:… messages=1
INFO deploy{project=blog}: ecr_listener::executor: deployed unit=personal-infra-deploy@blog.service
```

## Reference systemd unit

The canonical copies of this unit and of the polkit rule below are templated by the Ansible
`deploy` role of the (private) `personal-infra` repository; these are references.

```ini
# /etc/systemd/system/ecr-listener.service
[Unit]
Description=Deploy pushed ECR images from an SQS queue
Wants=network-online.target
After=network-online.target

[Service]
Type=notify
User=ecr-listener
ExecStart=/usr/local/bin/ecr-listener --config /etc/ecr-listener/config.toml
Environment=AWS_SHARED_CREDENTIALS_FILE=/etc/ecr-listener/credentials
Environment=AWS_EC2_METADATA_DISABLED=true
Restart=always
RestartSec=10

NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
PrivateDevices=yes
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectKernelLogs=yes
ProtectControlGroups=yes
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6
RestrictNamespaces=yes
LockPersonality=yes
MemoryDenyWriteExecute=yes
SystemCallArchitectures=native
CapabilityBoundingSet=
MemoryMax=64M

[Install]
WantedBy=multi-user.target
```

`AF_UNIX` is needed both for `sd_notify` and for `systemctl`, which talks to systemd over the
D-Bus system socket.

## Reference polkit rule

`ecr-listener` runs as an unprivileged system user. This rule lets that user `start`, and only
`start`, the listed units; `systemctl` runs with `--no-ask-password`, so anything else fails
instead of prompting.

```js
// /etc/polkit-1/rules.d/50-ecr-listener.rules
polkit.addRule(function (action, subject) {
    var units = [
        "personal-infra-deploy@blog.service"
    ];
    if (action.id == "org.freedesktop.systemd1.manage-units" &&
        subject.user == "ecr-listener" &&
        action.lookup("verb") == "start" &&
        units.indexOf(action.lookup("unit")) >= 0) {
        return polkit.Result.YES;
    }
});
```

## Building and testing

```bash
cargo build --release --locked
cargo fmt --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
```

The toolchain is pinned in `rust-toolchain.toml`. The tests need neither AWS nor systemd: the
batch planning, the retry policies, the executor and the poll loop run against hand-written fakes,
and the `systemctl` runner is tested with `/bin/sh` standing in for it.

## Releases

1. Bump `version` in `Cargo.toml`, run `cargo check` to refresh `Cargo.lock`, and commit both.
2. Tag the commit `v<version>` and push the tag: `git tag v0.2.0 && git push origin v0.2.0`.
3. The `release` workflow fails unless the tag equals `v` + the `Cargo.toml` version. It runs the
   tests, builds `--release` for `x86_64-unknown-linux-gnu` on Ubuntu 24.04, and publishes a
   GitHub release with generated notes and two assets:
   - `ecr-listener-x86_64-unknown-linux-gnu`: the raw, stripped binary;
   - `ecr-listener-x86_64-unknown-linux-gnu.sha256`: its `sha256sum` line.
4. Point the host at the new version and its checksum. The asset is downloaded from
   `https://github.com/kenesparta/ecr-listener/releases/download/v<version>/ecr-listener-x86_64-unknown-linux-gnu`,
   so the asset name is part of that contract.

Built on Ubuntu 24.04, the binary needs no newer glibc than 24.04's, and runs on any later release
as well.

## License

[MIT](LICENSE).
