# Deploy Apache Airflow on AWS EC2

This guide creates a persistent Apache Airflow installation on one Ubuntu EC2
instance. Airflow remains online when your computer is off and schedules the
Telegram-to-Databricks DAG every 15 minutes.

The deployment uses:

- Apache Airflow 2.10.5 in a Python virtual environment;
- SQLite for Airflow metadata;
- `SequentialExecutor`, which runs one task at a time;
- one Airflow scheduler process;
- one Airflow webserver worker;
- systemd services that restart Airflow after a process failure or EC2 reboot;
- daily SQLite metadata backups;
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
├── Airflow metadata, logs, and backups: ~/airflow
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
| `t2.micro` or `t3.micro` | 1 GiB | Lowest-cost functional test; the installer creates swap |
| `t2.small` or `t3.small` | 2 GiB | Recommended small-instance starting point |
| `t3.medium` | 4 GiB | More capacity for backfills and a more responsive UI |

For a first small deployment, select `t3.small` when it is available at an
acceptable price. Select only an instance type whose displayed price and account
benefits you have reviewed in the EC2 launch page.

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

---

## Phase 5: launch the Ubuntu EC2 instance

1. Open **EC2 → Instances**.
2. Click **Launch instances**.
3. Enter the instance name:

   ```text
   telehitch-airflow
   ```

4. Under **Application and OS Images**, select **Ubuntu**.
5. Select **Ubuntu Server 22.04 LTS** with the **64-bit (x86)** architecture.
6. Confirm that the image publisher is Canonical.
7. Under **Instance type**, select the size chosen in Phase 3.
8. Under **Key pair**, select:

   ```text
   telehitch-airflow
   ```

### Configure networking

9. Click **Edit** under **Network settings**.
10. Keep the default VPC and public subnet for this beginner deployment.
11. Enable **Auto-assign public IP**.
12. Create a new security group named:

    ```text
    telehitch-airflow-ssh
    ```

13. Add one inbound rule:

    | Type | Port | Source |
    |---|---:|---|
    | SSH | 22 | My IP |

14. Do not add an inbound rule for port 8080.
15. Keep the default outbound rule so the instance can reach package repositories,
    Telegram, GitHub, and Databricks over HTTPS.

### Configure storage

16. Set the root volume to 25–30 GiB using `gp3`.
17. Keep encryption enabled.
18. Keep **Delete on termination** enabled unless you have a specific retention
    requirement.

### Launch

19. Review the Summary panel, including the instance price, EBS storage, public
    IPv4 pricing, architecture, and Region.
20. Click **Launch instance**.
21. Open **View all instances**.
22. Wait until:

    ```text
    Instance state: Running
    Status checks: 2/2 checks passed
    ```

23. Select the instance and record its **Public IPv4 DNS** and **Public IPv4
    address**.

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

`whoami` should print `ubuntu`.

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

1. installs Ubuntu build tools, Python virtual-environment support, and SQLite;
2. creates a 2 GiB swap file when the instance has less than 3 GiB RAM;
3. creates `~/airflow-venv`;
4. installs Apache Airflow 2.10.5 with the official constraint file;
5. installs Telethon and the Databricks SQL connector;
6. creates `~/airflow` for metadata, logs, and backups;
7. configures SQLite and `SequentialExecutor`;
8. configures one parser, one task at a time, and one webserver worker;
9. migrates the Airflow metadata database;
10. creates the administrator account;
11. installs and starts the scheduler and webserver systemd services;
12. installs and starts the daily backup timer.

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

## Phase 11: verify that Airflow parsed the DAG

Run:

```bash
./deploy/aws-ec2/airflow-command.sh dags list
```

Look for:

```text
telegram_to_databricks_live_sync
```

Check import errors:

```bash
./deploy/aws-ec2/airflow-command.sh dags list-import-errors
```

Expected result: no import errors.

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

The helper retains 14 days of local SQLite backups.

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

## Setup verification commands

Run these from the repository root on EC2:

```bash
# List parsed DAGs and import errors.
./deploy/aws-ec2/airflow-command.sh dags list
./deploy/aws-ec2/airflow-command.sh dags list-import-errors

# Check scheduler, webserver, and backup timer.
sudo systemctl status telehitch-airflow-scheduler.service --no-pager
sudo systemctl status telehitch-airflow-webserver.service --no-pager
sudo systemctl status telehitch-airflow-backup.timer --no-pager

# Inspect recent service logs.
sudo journalctl -u telehitch-airflow-scheduler.service -n 100 --no-pager
sudo journalctl -u telehitch-airflow-webserver.service -n 100 --no-pager

# Check memory and swap.
free -h
swapon --show

# Create and list an Airflow metadata backup.
./deploy/aws-ec2/backup-airflow.sh
find ~/airflow/backups -maxdepth 1 -type f -ls
```
