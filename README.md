# SGJB Telehitch Insights — First-Time AWS EC2 Setup

> **Scope:** This is the repository's single setup guide. It is intentionally
> limited to a clean, first-time deployment for an interviewer or evaluator.
> It assumes a new EC2 instance and a clean Airflow installation; upgrade and
> migration procedures are intentionally outside this guide.

This guide creates a persistent Apache Airflow installation on one Ubuntu EC2
instance. Airflow remains online when your computer is off and schedules the
Telegram-to-Databricks DAG every 15 minutes.

The deployment uses:

- Apache Airflow 2.10.5 in a Python virtual environment;
- PostgreSQL for persistent Airflow metadata;
- `LocalExecutor`, limited to one active task for this single-machine deployment;
- one Airflow scheduler process;
- one Airflow webserver worker;
- systemd services that restart Airflow after a process failure or EC2 reboot;
- daily PostgreSQL metadata backups;
- an SSH tunnel for private access to the Airflow UI;
- paged historical Telegram ingestion followed by incremental ingestion;
- support for up to 100 Telegram channels and optional forum topics.

## Architecture

```text
AWS EC2 Ubuntu instance
├── systemd: telehitch-airflow-scheduler
├── systemd: telehitch-airflow-webserver
├── systemd timer: telehitch-airflow-backup
├── Python virtual environment: ~/airflow-venv
├── PostgreSQL metadata database: local port 5432
├── Airflow logs and backups: ~/airflow
└── repository DAGs: ~/sgjb-telehitch-insights/dags
    ├── telegram_to_databricks.py
    └── telegram_scraper.py
         │
         ├── Telegram API
         └── Databricks SQL warehouse and Delta table
```

The Airflow UI listens only on the EC2 instance's loopback interface at
`127.0.0.1:8080`. You access it from your computer through an encrypted SSH
tunnel; port 8080 does not need to be publicly exposed.

---

## Phase 1: create an AWS account

Skip this phase if you already have an AWS account you control.

1. Open <https://aws.amazon.com/>.
2. Click **Create an AWS Account**.
3. Enter an email address you control long-term.
4. Enter an account name, such as `Telehitch Airflow`.
5. Complete the email verification step.
6. Create a unique root-user password and save it in a password manager.
7. Choose the appropriate personal or business account type.
8. Enter the requested contact information.
9. Add a valid payment method.
10. Complete phone verification.
11. Select the **Basic Support** plan unless you intentionally require a paid
    support plan.
12. Wait for the account-activation email.
13. Sign in to the AWS Management Console as the root user.

### Enable multi-factor authentication

1. In the AWS console, open the account menu in the upper-right corner.
2. Select **Security credentials**.
3. Find **Multi-factor authentication (MFA)**.
4. Click **Assign MFA device**.
5. Select a passkey, security key, or authenticator application.
6. Complete the registration prompts.
7. Sign out and verify that MFA is required when you sign in again.

Do not create root-user access keys. The EC2 setup in this guide uses the AWS
console and SSH, not root API credentials.

### Create an everyday administrator

Use AWS IAM Identity Center or the guided administrative-user flow shown in your
account:

1. Search the AWS console for **IAM Identity Center**.
2. Enable it if your account has not already done so.
3. Create a user for your everyday AWS administration.
4. Assign administrative access to your AWS account.
5. Complete the invitation and configure MFA for this user.
6. Sign out of the root account.
7. Use the everyday administrator for the remaining phases.

Keep the root account only for account-level recovery and tasks that explicitly
require it.

---

## Phase 2: choose an AWS Region and create a budget

### Choose a Region

1. In the AWS console, open the Region selector in the upper-right corner.
2. Choose a Region near you and near the services the DAG connects to.
3. Record the Region name and code, for example:

   ```text
   Asia Pacific (Singapore)
   ap-southeast-1
   ```

4. Use this same Region for the EC2 instance, key pair, security group, and EBS
   volume.

### Create a monthly budget

1. Search for **Billing and Cost Management**.
2. Open **Budgets**.
3. Click **Create budget**.
4. Select **Cost budget**.
5. Enter a name such as:

   ```text
   telehitch-airflow-monthly-budget
   ```

6. Enter the monthly amount you are willing to spend.
7. Add alert thresholds such as 50%, 80%, and 100%.
8. Enter an email address you monitor.
9. Review and create the budget.
10. Open the AWS **Free Tier** and **Cost Explorer** pages to review the benefits
    and prices that apply to your account.

A budget sends alerts; it does not automatically stop the EC2 instance.

---

## Phase 3: choose the EC2 instance size

The installation is optimized for one DAG and one task at a time.

| Instance type | Memory | Use case |
|---|---:|---|
| `t3.micro` | 1 GiB | Low-cost interviewer evaluation; must be validated with Phase 17 |
| `t3.small` | 2 GiB | Recommended upgrade when memory or task duration is marginal |
| `t3.medium` | 4 GiB | More capacity for larger backfills and a more responsive UI |

This first-time guide uses `t3.micro` for the low-cost evaluation. It provides
2 vCPUs and 1 GiB memory. The installer creates a 2 GiB swap file and limits
Airflow to one active task, one DAG parser, and one webserver worker. Select it
only when the EC2 launch page marks it **Free tier eligible** or its displayed
price is acceptable for your account. T3 instances are burstable; this guide
selects **Standard** CPU credit mode to avoid surplus CPU-credit charges.

A successful installation alone does not prove that 1 GiB is sufficient. Phase
17 measures representative scheduled runs and gives explicit resize criteria.

Use these initial ingestion settings on a micro or small instance:

```text
TELEGRAM_BACKFILL_PAGE_LIMIT=100
DATABRICKS_BATCH_SIZE=50
```

They keep each historical page and Databricks SQL statement small. After the
historical backfills complete and the instance is stable, you can increase them
carefully.

---

## Phase 4: create an EC2 SSH key pair

1. Search the AWS console for **EC2**.
2. In the left navigation, open **Network & Security → Key Pairs**.
3. Click **Create key pair**.
4. Enter a name such as:

   ```text
   telehitch-airflow
   ```

5. Select key-pair type **ED25519** if available; otherwise select **RSA**.
6. Select private-key format **`.pem`** for macOS, Linux, OpenSSH, and VS Code
   Remote SSH.
7. Click **Create key pair**.
8. Save the downloaded file in your Mac SSH directory:

   ```bash
   mkdir -p ~/.ssh
   mv ~/Downloads/telehitch-airflow.pem ~/.ssh/
   chmod 400 ~/.ssh/telehitch-airflow.pem
   ```

9. Back up the private key in a secure location.

AWS does not provide another download of the same private key. Never commit it to
this repository.

If **Create key pair** is not available inside the launch wizard, do not select
**Proceed without a key pair**. Leave the launch wizard open in one tab and create
the key separately:

1. Open a second AWS console tab in the same Region.
2. Open **EC2 → Network & Security → Key Pairs**.
3. Click **Create key pair** and use the selections above.
4. Return to the launch-wizard tab and refresh the key-pair list or reload the
   page.
5. Select `telehitch-airflow`.

If the separate **Key Pairs** page also does not show **Create key pair**, or AWS
shows `AccessDenied`, the signed-in identity lacks `ec2:CreateKeyPair` permission.
Sign in with the administrator identity created during account setup or ask the
AWS account administrator to grant that permission. Do not launch this instance
without a key: this deployment relies on persistent SSH access for installation,
VS Code, maintenance, and recovery.

---

## Phase 5: launch the Ubuntu EC2 instance

Open **EC2 → Instances** and click **Launch instances**. Use the selections below
exactly unless your AWS account or Region does not offer an option.

### Name and tags

| Field | Select or enter |
|---|---|
| Name | `telehitch-airflow` |
| Additional tags | Leave empty for now |

### Application and OS Image

1. Under **Quick Start**, click **Ubuntu**.
2. Select the plain server image named similar to:

   ```text
   Ubuntu Server 24.04 LTS (HVM), SSD Volume Type
   ```

3. Select architecture:

   ```text
   64-bit (x86)
   ```

4. Confirm the provider is **Canonical**. Canonical's AWS owner ID is
   `099720109477`.
5. Confirm the image description does not include any of these:

   ```text
   SQL Server
   Ubuntu Pro
   Desktop
   Marketplace software
   ```

6. Do not select an image containing **SQL Server**. Those images include Microsoft
   SQL Server licensing and software that this project does not use. Additional
   AMI software charges can apply.
7. Do not select Ubuntu Server 26.04 for this deployment. Ubuntu 24.04 includes
   Python 3.12, which is supported by the pinned Airflow 2.10.5 installation.
   Ubuntu 26.04 uses a newer Python generation that is outside this deployment
   configuration.
8. If AWS shows multiple 24.04 choices, select the regular **Server** image, not
   Minimal, Desktop, Pro, or an image bundled with third-party software.
9. AMI IDs differ by AWS Region and image release, so select by the image name,
   architecture, and verified Canonical provider rather than copying an AMI ID
   from another Region.

### Instance type

| Field | Select |
|---|---|
| Instance type | `t3.micro` |
| vCPUs | 2 |
| Memory | 1 GiB |
| Purchase option | On-Demand; do not request Spot |

Confirm that the console displays **Free tier eligible** or that you accept the
shown hourly price.

### Key pair

Under **Key pair (login)**, select the key pair created earlier:

```text
telehitch-airflow
```

If the dropdown contains only **Proceed without a key pair**, stop here and use
the separate **EC2 → Network & Security → Key Pairs** page described in Phase 4.
Do not launch until `telehitch-airflow` appears in this dropdown.

### Network settings

Click **Edit** and use:

| Field | Select or enter |
|---|---|
| Network | Default VPC |
| Subnet | No preference (default subnet in any Availability Zone) |
| Auto-assign public IP | Enable |
| Firewall | Create security group |
| Security group name | `telehitch-airflow-ssh` |
| Description | `SSH access to Telehitch Airflow EC2` |

Delete every automatically proposed inbound rule, including MSSQL, HTTP, and
HTTPS. Then add exactly one inbound rule:

| Type | Protocol | Port | Source type | Source |
|---|---|---:|---|---|
| SSH | TCP | 22 | My IP | Your current public IPv4 `/32` |

The final inbound-rule list must not contain:

```text
MSSQL 1433
HTTP 80
HTTPS 443
Custom TCP 8080
SSH from 0.0.0.0/0
```

Keep the default outbound rule that permits outbound traffic. The EC2 instance
needs outbound HTTPS access to Ubuntu package repositories, GitHub, Telegram,
and Databricks.

If your home public IP changes later, edit the security group's SSH source to
your new **My IP** value.

### Configure storage

In **Storage (volumes)**, keep **Simple → EBS Volumes** selected. Configure
**Volume 1 (AMI Root)** as follows:

| Field | Select or enter |
|---|---|
| Storage type | `EBS` |
| Device name | Keep `/dev/sda1` |
| Snapshot | Keep the snapshot supplied by the selected Ubuntu AMI |
| Size | `30 GiB` |
| Volume type | `gp3` |
| IOPS | `3000` |
| Throughput | `125 MiB/s` |
| Delete on termination | `Yes` |
| Volume initialization rate | Leave blank; selecting a rate adds charges |

The **Encrypted** field initially displays **Not encrypted**. This value is a
selector:

1. Click **Not encrypted**.
2. Change it to **Encrypted**.
3. The **KMS key** field becomes available.
4. Open **KMS key → Select**.
5. Choose the AWS managed EBS key, displayed as `aws/ebs`, `(default) aws/ebs`,
   or an ARN ending in `alias/aws/ebs`.
6. Do not create a customer-managed KMS key for this deployment.

If the selector cannot be changed because of an AWS account policy, stop before
launching and ask the account administrator to permit encrypted EBS volume
creation. The separate account-wide **EBS encryption by default** setting is not
required when you encrypt this root volume directly in the launch wizard.

Under **File systems**, select **None**. Do not select S3 Files, EFS, or FSx, and
do not add another EBS or instance-store volume.

### Advanced details

Expand **Advanced details** and use:

| Field | Select |
|---|---|
| Domain join directory | None |
| IAM instance profile | None |
| Hostname type | IP name/default |
| DNS Hostname | Enable/default |
| Instance auto-recovery | Default |
| Shutdown behavior | Stop |
| Stop - Hibernate behavior | Disable |
| Termination protection | Enable |
| Stop protection | Enable if offered |
| Detailed CloudWatch monitoring | Disable |
| Tenancy | Shared |
| Credit specification | Standard |
| Placement group | None |
| Capacity reservation | Open/default |
| User data | Leave blank |

For **Metadata options**, keep the endpoint enabled and require IMDSv2 if the
wizard offers the choice:

```text
Metadata accessible: Enabled
Metadata version: V2 only / Required
Metadata response hop limit: 1
```

Termination and stop protection prevent accidental console actions. You can
disable the relevant protection later if you intentionally need to stop or
terminate the instance.

### Review the Summary and launch

Before clicking **Launch instance**, confirm the Summary shows:

```text
Number of instances: 1
Software Image: plain Canonical Ubuntu Server 24.04 LTS
Instance type: t3.micro
Firewall: telehitch-airflow-ssh
Inbound access: SSH 22 from My IP only
Storage: one 30 GiB gp3 root volume with EBS encryption enabled
```

If the Summary still says **SQL Server**, go back to the AMI section and select
the plain Canonical Ubuntu Server image.

Then:

1. Click **Launch instance**.
2. Click **View all instances**.
3. Wait until:

   ```text
   Instance state: Running
   Status checks: 2/2 checks passed
   ```

4. Select the instance.
5. Record its **Instance ID**, **Public IPv4 address**, and **Public IPv4 DNS**.
6. Open its **Security** tab and verify the only inbound permission is SSH port
   22 from your public `/32` address.

---

## Phase 6: connect to EC2

### Terminal method

On your Mac, run:

```bash
ssh -i ~/.ssh/telehitch-airflow.pem ubuntu@YOUR_EC2_PUBLIC_DNS
```

On the first connection:

1. Verify that the hostname matches your EC2 instance.
2. Type `yes` to save the host key.
3. Confirm the prompt changes to an Ubuntu prompt similar to:

   ```text
   ubuntu@ip-10-0-0-123:~$
   ```

Verify the connection:

```bash
whoami
hostname
```

`whoami` should print `ubuntu`. If your prompt remains similar to
`gerald@Geralds-MacBook-Pro`, you are still on your Mac and the EC2 login did
not succeed.

### If SSH says `Permission denied (publickey)`

This error means the instance was reachable on port 22, but it rejected the
private key offered by your Mac. It is not an Airflow problem and changing the
security group will not fix a public-key rejection.

First, open **EC2 → Instances**, select `telehitch-airflow`, and inspect the
instance **Details**. Find **Key pair name**.

- If it says `telehitch-airflow`, continue with the local-key checks below.
- If it shows another name, use the `.pem` file belonging to that exact key
  pair.
- If it is blank or says that no key pair is assigned, the instance was
  launched with **Proceed without a key pair**. Creating a key pair afterward
  does not install its public key on the existing instance.

For a new instance that contains no work, the safest recovery for a missing or
wrong launch key is:

1. Select the unusable instance in **EC2 → Instances**.
2. Choose **Instance state → Terminate instance**. If termination protection is
   enabled, first use **Actions → Instance settings → Change termination
   protection** to disable it.
3. Keep the `telehitch-airflow` key pair that you already downloaded.
4. Launch a replacement instance with the same Ubuntu, network, and storage
   selections.
5. In **Key pair (login)**, explicitly select `telehitch-airflow` before
   launching.
6. Wait for `2/2 checks passed` and connect to the replacement instance.

Do not repeatedly recreate private keys with the same intended purpose. The
private `.pem` file must correspond to the public key that EC2 installed when
the instance was launched.

On your Mac, validate the existing key:

```bash
ls -l ~/.ssh/telehitch-airflow.pem
chmod 400 ~/.ssh/telehitch-airflow.pem
ssh-keygen -y -f ~/.ssh/telehitch-airflow.pem >/dev/null \
  && echo "Private key is readable"
```

Then use the repository's diagnostic helper with the instance's actual
**Public IPv4 DNS** or **Public IPv4 address**:

```bash
cd /path/to/sgjb-telehitch-insights

./deploy/aws-ec2/check-ssh.sh \
  YOUR_ACTUAL_EC2_PUBLIC_DNS \
  ~/.ssh/telehitch-airflow.pem
```

The helper forces SSH to offer only this key, prints its derived public-key
fingerprint, and enables verbose authentication output. It must be run on your
Mac, where the `.pem` file is stored.

You can also run the equivalent command directly:

```bash
ssh -vvv \
  -o IdentitiesOnly=yes \
  -i ~/.ssh/telehitch-airflow.pem \
  ubuntu@YOUR_ACTUAL_EC2_PUBLIC_DNS
```

Use `ubuntu` for the plain Canonical Ubuntu AMI. Do not use your Mac username
`gerald`, and do not include `https://` in the hostname.

In the verbose output, a line similar to `Offering public key` followed by
`Permission denied (publickey)` confirms that the selected private key does not
match a public key accepted by the instance. Recheck **Key pair name** in EC2;
do not solve this by opening port 22 to the whole internet.

### VS Code Remote SSH method

1. Install Visual Studio Code.
2. Open **Extensions**.
3. Install **Remote - SSH** by Microsoft.
4. Press `Command+Shift+P`.
5. Select **Remote-SSH: Open SSH Configuration File**.
6. Open `~/.ssh/config`.
7. Add:

   ```sshconfig
   Host telehitch-airflow
       HostName YOUR_EC2_PUBLIC_DNS
       User ubuntu
       IdentityFile ~/.ssh/telehitch-airflow.pem
   ```

8. Save the file.
9. Press `Command+Shift+P` again.
10. Select **Remote-SSH: Connect to Host**.
11. Select `telehitch-airflow`.
12. Choose **Linux** if VS Code asks for the remote operating system.
13. Accept the verified host fingerprint.
14. Open **Terminal → New Terminal** in the remote VS Code window.
15. Run `whoami` and confirm it prints `ubuntu`.

---

## Phase 7: update Ubuntu and install Git

Run these commands on EC2:

```bash
sudo apt update
sudo apt -y upgrade
sudo apt install -y git
```

If Ubuntu reports that a reboot is required:

```bash
sudo reboot
```

Wait approximately one minute, then reconnect using SSH or VS Code.

Verify Git and Python:

```bash
git --version
python3 --version
```

---

## Phase 8: clone the repository

1. Open the GitHub repository in your browser.
2. Click **Code**.
3. Copy the HTTPS clone URL.
4. On EC2, run:

   ```bash
   cd ~
   git clone YOUR_GITHUB_REPOSITORY_HTTPS_URL
   cd sgjb-telehitch-insights
   ```

5. Confirm the deployment files exist:

   ```bash
   find deploy/aws-ec2 -maxdepth 2 -type f -print | sort
   ```

6. Confirm the DAG files exist:

   ```bash
   find dags -maxdepth 1 -type f -name '*.py' -print | sort
   ```

Expected DAG files:

```text
dags/telegram_scraper.py
dags/telegram_to_databricks.py
```

---

## Phase 9: create the protected Airflow environment file

From the repository root on EC2:

```bash
cp deploy/aws-ec2/airflow.env.example deploy/aws-ec2/airflow.env
chmod 600 deploy/aws-ec2/airflow.env
nano deploy/aws-ec2/airflow.env
```

Fill in every value containing `REPLACE_WITH`.

### Airflow administrator

```text
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=<long unique password>
AIRFLOW_ADMIN_FIRSTNAME=<first name>
AIRFLOW_ADMIN_LASTNAME=<last name>
AIRFLOW_ADMIN_EMAIL=<email address>
```

Generate a webserver secret key:

```bash
python3 -c 'import secrets; print(secrets.token_hex(32))'
```

Paste it into:

```text
AIRFLOW__WEBSERVER__SECRET_KEY=<generated value>
```

Generate a Fernet key:

```bash
python3 -c 'import base64,os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())'
```

Paste it into:

```text
AIRFLOW__CORE__FERNET_KEY=<generated value>
```

### Telegram credentials

Set:

```text
TELEGRAM_API_ID=<numeric Telegram API ID>
TELEGRAM_API_HASH=<Telegram API hash>
TELEGRAM_SESSION_STRING=<Telethon StringSession>
```

### Telegram sources

For one whole channel:

```text
TELEGRAM_CHANNEL=CarpoolSgJb
```

For an additional forum topic:

```text
TELEGRAM_CHANNEL_2=TeleHitch
TELEGRAM_CHANNEL_2_TOPIC_ID=1823745
```

For another whole channel:

```text
TELEGRAM_CHANNEL_3=AnotherChannel
```

Channel numbers can continue through `TELEGRAM_CHANNEL_100`. A topic variable
must use the same suffix as its channel:

```text
TELEGRAM_CHANNEL_4=AnotherForum
TELEGRAM_CHANNEL_4_TOPIC_ID=123456
```

### Schedule and page sizes

Use these initial values:

```text
TELEGRAM_AIRFLOW_SCHEDULE="*/15 * * * *"
TELEGRAM_BACKFILL_PAGE_LIMIT=100
TELEGRAM_PER_RUN_LIMIT=0
```

- A new source starts with paged historical ingestion.
- Each successful page saves that source's highest Telegram message ID.
- After the final historical page, the source switches to incremental ingestion.
- Existing sources remain incremental when another channel is added later.

### Databricks configuration

Set:

```text
DATABRICKS_SERVER_HOSTNAME=<hostname without https://>
DATABRICKS_HTTP_PATH=<SQL warehouse HTTP path>
DATABRICKS_TOKEN=<personal access token>
DATABRICKS_CATALOG=workspace
DATABRICKS_SCHEMA=default
DATABRICKS_TABLE=sgjb-telehitch-raw
DATABRICKS_BATCH_SIZE=50
```

The Databricks destination table should not already exist for a clean first-time
setup. The DAG creates it with the expected schema during its first successful
run.

Save Nano with:

1. `Control+O`
2. `Enter`
3. `Control+X`

Confirm the file is protected:

```bash
stat -c '%a %n' deploy/aws-ec2/airflow.env
```

Expected permissions:

```text
600 deploy/aws-ec2/airflow.env
```

Do not print or copy the completed file into logs, issues, screenshots, or Git.

---

## Phase 10: install Airflow

From the repository root:

```bash
./deploy/aws-ec2/install.sh
```

The installer performs these operations:

1. installs Ubuntu build tools, PostgreSQL, and Python virtual-environment support;
2. creates a 2 GiB swap file when the instance has less than 3 GiB RAM;
3. creates `~/airflow-venv`;
4. installs Apache Airflow 2.10.5 with PostgreSQL support and official constraints;
5. installs Telethon and the Databricks SQL connector;
6. creates a local PostgreSQL role and database with a generated password;
7. tunes PostgreSQL conservatively for a small EC2 instance;
8. configures Airflow with PostgreSQL and `LocalExecutor`;
9. limits Airflow to one parser, one active task, and one webserver worker;
10. migrates the Airflow metadata database and creates the administrator;
11. installs and starts the scheduler and webserver systemd services;
12. installs and starts the daily PostgreSQL backup timer.

The installation can take several minutes on a small instance.

When it finishes, verify the services:

```bash
sudo systemctl status telehitch-airflow-scheduler.service --no-pager
sudo systemctl status telehitch-airflow-webserver.service --no-pager
sudo systemctl status telehitch-airflow-backup.timer --no-pager
```

Each should report `active (running)` or, for the timer, `active (waiting)`.

Verify swap on a micro or small instance:

```bash
free -h
swapon --show
```

---

## Phase 11: verify the Airflow installation and DAG

Run the combined verification helper:

```bash
./deploy/aws-ec2/verify-install.sh
```

It checks the scheduler, webserver, backup timer, DAG registration, DAG import
errors, checkpoint Variable, swap, and recent scheduler logs. A first-time
installation may show a warning that `telegram_scraper_channel_state` is absent;
that is expected before the first successful historical page.

A healthy result includes:

```text
PASS: Airflow scheduler is active
PASS: Airflow webserver is active
PASS: DAG telegram_to_databricks_live_sync is registered
PASS: Airflow reports no DAG import errors
```

Airflow CLI commands can print a harmless warning that Python Graphviz is not
installed. Graphviz is needed only for CLI image rendering; it is not required
for scheduling, Grid view, logs, Telegram ingestion, or Databricks writes.

You can also run the underlying checks separately:

```bash
./deploy/aws-ec2/airflow-command.sh dags list
./deploy/aws-ec2/airflow-command.sh dags list-import-errors
```

`No data found` from `dags list-import-errors` means there are no import errors.
The terminal may wrap the long DAG ID across multiple display lines; this does
not change the registered ID.

If the DAG is missing, inspect scheduler logs:

```bash
sudo journalctl -u telehitch-airflow-scheduler.service -n 200 --no-pager
```

Common causes are an incomplete `airflow.env`, missing Python dependencies, or a
syntax/import error in one of the two DAG files.

---

## Phase 12: open the Airflow UI through an SSH tunnel

Keep your normal EC2 SSH session open. On your Mac, open a second Terminal window
and run:

```bash
ssh -i ~/.ssh/telehitch-airflow.pem \
  -L 8080:127.0.0.1:8080 \
  ubuntu@YOUR_EC2_PUBLIC_DNS
```

Leave this tunnel terminal open.

Open a browser on your Mac and visit:

```text
http://localhost:8080
```

Sign in with the `AIRFLOW_ADMIN_USERNAME` and `AIRFLOW_ADMIN_PASSWORD` values
from `airflow.env`.

In the DAG list:

1. Search for `telegram_to_databricks_live_sync`.
2. Confirm the DAG appears.
3. Keep it paused for the first manual test.
4. Open **Grid** or **Graph** and confirm the task `sync_messages` appears.
5. Confirm the schedule is `*/15 * * * *`.

If `localhost:8080` does not load, check:

```bash
sudo systemctl status telehitch-airflow-webserver.service --no-pager
sudo journalctl -u telehitch-airflow-webserver.service -n 100 --no-pager
```

---

## Phase 13: run the first historical ingestion

Before triggering the DAG, confirm:

- the Airflow DAG is paused;
- the Databricks destination table does not already contain an incompatible
  schema;
- the Telegram account represented by the StringSession can access every
  configured source;
- the Databricks token can use the selected SQL warehouse and write to the
  selected catalog and schema.

Trigger exactly one manual run:

1. Open `telegram_to_databricks_live_sync`.
2. Click **Trigger DAG**.
3. Confirm the trigger.
4. Open **Grid**.
5. Select the new run.
6. Select `sync_messages`.
7. Open **Logs**.

For each new source, expect a log line similar to:

```text
Starting Telegram sync source=CarpoolSgJb mode=full_history min_id=0 limit=100
```

A source that returns a full page remains in historical-backfill mode. Its next
run resumes after the highest successfully merged message ID. A source switches
to incremental mode after its final short or empty historical page.

A successful task turns green.

---

## Phase 14: verify Airflow state and Databricks data

### Check the Airflow state variable

In the Airflow UI:

1. Open **Admin → Variables**.
2. Find `telegram_scraper_channel_state`.
3. Confirm that it contains one state entry per source that completed a page.

Example:

```json
{
  "carpoolsgjb": {
    "initial_backfill_complete": false,
    "last_message_id": 1234
  },
  "telehitch#topic=1823745": {
    "initial_backfill_complete": true,
    "last_message_id": 1823999
  }
}
```

A nonzero `last_message_id` with `initial_backfill_complete=false` means a paged
historical backfill is still in progress.

### Check the Databricks table

In Databricks SQL Editor, run:

```sql
DESCRIBE TABLE `workspace`.`default`.`sgjb-telehitch-raw`;
```

Expected columns:

```text
channel
topic_id
id
message_date_gmt8
message
sender_id
sender_handle
scraped_at_gmt8
```

Inspect source counts:

```sql
SELECT
  channel,
  topic_id,
  COUNT(*) AS row_count,
  MIN(message_date_gmt8) AS oldest_message_gmt8,
  MAX(message_date_gmt8) AS newest_message_gmt8,
  MAX(id) AS maximum_message_id
FROM `workspace`.`default`.`sgjb-telehitch-raw`
GROUP BY channel, topic_id
ORDER BY channel, topic_id;
```

Check duplicate keys:

```sql
SELECT
  channel,
  id,
  COUNT(*) AS copies
FROM `workspace`.`default`.`sgjb-telehitch-raw`
GROUP BY channel, id
HAVING COUNT(*) > 1;
```

Expected result: no rows.

---

## Phase 15: enable the 15-minute schedule

After the first manual run succeeds:

1. Return to the DAG list.
2. Turn on the toggle for `telegram_to_databricks_live_sync`.
3. Confirm the DAG is unpaused.
4. Keep the SSH tunnel open only while you want to use the Airflow UI.
5. You may close your browser, close the tunnel, and turn off your computer.
6. The Airflow scheduler remains active on EC2 through systemd.
7. Return after 30–45 minutes and reconnect through the SSH tunnel.
8. Confirm that scheduled runs occurred at 15-minute intervals.

The default schedule is:

```text
*/15 * * * *
```

which runs at minute `00`, `15`, `30`, and `45` of each hour.

---

## Phase 16: verify reboot persistence and backups

### Verify daily backups

Run:

```bash
sudo systemctl list-timers telehitch-airflow-backup.timer
./deploy/aws-ec2/backup-airflow.sh
find ~/airflow/backups -maxdepth 1 -type f -ls
```

The helper creates a PostgreSQL custom-format dump and retains 14 days of local metadata backups.

### Verify service restart after reboot

Run:

```bash
sudo reboot
```

Wait approximately one minute, reconnect, and run:

```bash
sudo systemctl status telehitch-airflow-scheduler.service --no-pager
sudo systemctl status telehitch-airflow-webserver.service --no-pager
sudo systemctl status telehitch-airflow-backup.timer --no-pager
```

Reconnect the SSH tunnel and confirm the Airflow UI loads. Verify that the DAG
remains unpaused and that scheduled runs continue.

---

## Phase 17: validate EC2 capacity during real DAG runs


A successful run proves functional correctness, but not long-term capacity. Run
the resource sampler across at least three scheduled executions:

```bash
cd ~/sgjb-telehitch-insights
./deploy/aws-ec2/monitor-resources.sh 3600 5
```

The first argument is the monitoring duration in seconds and the second is the
sampling interval. The helper records total available RAM, swap use, CPU, load,
the scheduler and webserver cgroups, PostgreSQL RSS, and all Airflow process RSS.
It prints peak values and checks kernel logs for OOM kills. Detailed samples are
stored in a protected CSV under `~/airflow/`.

For a 1 GiB `t3.micro`, keep it only after several representative runs if there
are no OOM events or service restarts, minimum available RAM normally remains
above roughly 100 MiB, swap does not continuously climb from run to run, and
tasks complete comfortably within 15 minutes. Resize to `t3.small` if available
RAM repeatedly approaches zero, swap grows without recovering, the kernel kills
processes, services restart, or task duration approaches the schedule interval.
AWS CPU-credit capacity must be checked separately in the EC2 CloudWatch
**CPUCreditBalance** metric because guest Linux cannot report that balance.

---

## First-time setup checklist

- [ ] AWS account created and MFA enabled.
- [ ] Everyday AWS administrator configured.
- [ ] AWS Region selected.
- [ ] Monthly budget and alerts created.
- [ ] EC2 SSH key pair downloaded and protected.
- [ ] Ubuntu EC2 instance launched.
- [ ] Security group allows SSH from your IP only.
- [ ] Port 8080 is not open publicly.
- [ ] Repository cloned onto EC2.
- [ ] `deploy/aws-ec2/airflow.env` created with permission `600`.
- [ ] Telegram and Databricks credentials configured.
- [ ] Telegram channels and optional topic IDs configured.
- [ ] Airflow installation script completed.
- [ ] Scheduler, webserver, and backup timer are active.
- [ ] DAG appears with no import errors.
- [ ] Airflow UI works through the SSH tunnel.
- [ ] First manual historical page succeeded.
- [ ] Airflow state Variable contains source progress.
- [ ] Databricks table has the expected schema and rows.
- [ ] Duplicate-key query returns no rows.
- [ ] DAG is unpaused and runs every 15 minutes.
- [ ] Services restart after an EC2 reboot.
- [ ] Daily metadata backups are active.

- end -
