# Beginner setup: Telegram to Databricks every 15 minutes with Cloud Composer 3

This guide assumes no previous Google Cloud or Airflow experience. Complete the sections in order and do not upload a Telegram `.session` file, Databricks token, API hash, or `.env` file to GitHub or Cloud Composer's DAG folder.

## What this setup does

Cloud Composer is Google's managed Apache Airflow service. It stays online independently of your computer. Every 15 minutes, Airflow runs `telegram_to_databricks.py`; that DAG calls `telegram_scraper.py`, fetches Telegram messages after the last successful message ID, merges them into a Databricks Delta table, and records the new checkpoint.

The default schedule is `*/15 * * * *` (at minutes 00, 15, 30, and 45 each hour). Databricks can automatically start its stopped SQL warehouse when the connector opens a connection. Cloud Composer itself is not an always-free Google Cloud product and continues to incur charges while the environment exists.

## Values to choose and record first

Create a temporary note containing only non-secret setup labels:

| Label | Suggested example | Your value |
|---|---|---|
| Google Cloud project name | `Telegram Telehitch` | |
| Google Cloud project ID | `telegram-telehitch-12345` | |
| Google Cloud region | `asia-southeast1` | |
| Composer environment name | `telegram-airflow` | |
| Composer service account name | `composer-telegram-worker` | |
| Telegram channel | `CarpoolSgJb` | |
| Databricks catalog | Usually `workspace` | |
| Databricks schema | `default` or a schema you create | |
| Databricks table | `telegram_messages` | |

Use the same Google Cloud region as, or the closest available region to, the Databricks workspace. The examples use `asia-southeast1`; replace it everywhere if you choose a different region. Project IDs are globally unique, lowercase, and cannot be changed after creation.

Do **not** put secret values in this note.

---

## Part 1: secure the old Telegram session immediately

A Telegram session file was previously tracked in this repository. Removing it from the current revision does not erase it from old Git history. Treat that session as compromised.

1. Open Telegram on your phone.
2. Open **Settings**.
3. Open **Devices** (sometimes called **Active Sessions**).
4. Review the sessions.
5. Terminate the session used by the old scraper. If you cannot identify it, choose **Terminate All Other Sessions**.
6. Log back in to any legitimate devices that Telegram disconnected.
7. Never commit a file ending in `.session` again. This repository now ignores those files.

Complete this before generating the new StringSession later in this guide.

---

## Part 2: create or prepare your Google account

1. Open <https://accounts.google.com/>.
2. If you already have a Google account you control, sign in and continue.
3. Otherwise, click **Create account**.
4. Choose **For my personal use** unless this belongs to a company.
5. Enter your name, date of birth, desired address, and password.
6. Complete phone or email verification if Google requests it.
7. Add a recovery email and enable two-step verification:
   1. Open <https://myaccount.google.com/security>.
   2. Find **How you sign in to Google**.
   3. Open **2-Step Verification**.
   4. Follow the prompts.

Use an account you will keep. It will own the Google Cloud project and billing account.

---

## Part 3: register for Google Cloud and enable billing

> **Cost warning:** Cloud Composer is not included in Google Cloud's permanent Free Tier. A new eligible account may receive trial credit, but a Composer environment can consume that credit and becomes billable afterward. A budget alert warns you; it does not automatically stop spending.

1. Open <https://console.cloud.google.com/>.
2. Sign in with the Google account from Part 2.
3. Accept the Google Cloud terms if prompted.
4. If offered **Start free**, click it.
5. Select your country and account type.
6. Enter your legal name, address, and payment verification information.
7. Review the trial and billing terms before accepting them.
8. Google may place a temporary authorization hold; read the exact notice shown for your country.
9. Finish registration and wait for the Cloud Console home page.
10. Confirm a billing account exists:
    1. Open the navigation menu (`☰`) in the upper-left.
    2. Select **Billing**.
    3. Confirm that a billing account is displayed and active.
11. If Composer creation later says the free-trial account is restricted, you may need to upgrade the billing account to paid billing. Upgrading does not make Composer free; it permits normal billable use.

### Create a billing budget before any infrastructure

1. In **Billing**, choose the billing account.
2. Open **Budgets & alerts**.
3. Click **Create budget**.
4. Name it `telegram-composer-budget`.
5. Scope it to the project you will create, or return here after creating the project.
6. Enter a monthly amount you are willing to pay.
7. Keep alert thresholds such as 50%, 90%, and 100%.
8. Confirm your email receives billing alerts.
9. Click **Finish**.
10. Remember: reaching the budget does not shut resources down automatically.

---

## Part 4: create a Google Cloud project

1. At the top of Cloud Console, click the project selector (it may say **Select a project**).
2. Click **New Project**.
3. Enter `Telegram Telehitch` for the project name.
4. Note the automatically generated project ID, or replace it with a globally unique ID such as `telegram-telehitch-12345`.
5. If asked for an organization or location and this is a personal account, leave **No organization** selected.
6. Click **Create**.
7. Wait for the creation notification.
8. Open the project selector again.
9. Select the new project.
10. Verify the project name appears in the top bar before doing any later steps.
11. Return to **Billing → Budgets & alerts** and scope your budget to this project if you could not do so earlier.

---

## Part 5: enable the required Google APIs

1. In Cloud Console, confirm the correct project is selected.
2. Open `☰` → **APIs & Services** → **Library**.
3. Search for **Cloud Composer API**.
4. Open it and click **Enable**.
5. Return to the API Library.
6. Search for **Secret Manager API**.
7. Open it and click **Enable**.
8. Return to the API Library.
9. Search for **Cloud Build API** and enable it if Composer package installation asks for it.
10. Search for **Artifact Registry API** and enable it if Composer package installation asks for it.
11. Wait several minutes if a newly enabled API is not immediately recognized.

Composer enables or requests other dependent services during environment creation. Follow any explicit enablement prompt shown by Google.

---

## Part 6: create the Cloud Composer service account

A service account is the non-human identity used by Composer workers and DAG tasks.

1. Open `☰` → **IAM & Admin** → **Service Accounts**.
2. Click **Create service account**.
3. Enter:
   - Service account name: `composer-telegram-worker`
   - Service account ID: leave the generated `composer-telegram-worker`
   - Description: `Runs the Telegram to Databricks Cloud Composer DAG`
4. Click **Create and continue**.
5. In **Select a role**, search for and choose **Composer Worker**.
6. Click **Add another role**.
7. Search for and choose **Secret Manager Secret Accessor**.
8. Click **Continue**.
9. You normally do not need to grant individual users access in the final optional section.
10. Click **Done**.
11. Copy the service account email. It resembles:

   ```text
   composer-telegram-worker@YOUR_PROJECT_ID.iam.gserviceaccount.com
   ```

12. Do not create or download a service-account JSON key. Composer uses the service account directly.

If Google will not let you select this service account when creating Composer, your signed-in user may need the **Service Account User** role on it. For a personal project where you are Owner, you usually already have sufficient permission.

---

## Part 7: obtain Telegram API credentials

A Telegram API ID and API hash identify your Telegram client application. They are not the same as a bot token.

1. Open <https://my.telegram.org/> in a browser.
2. Enter the phone number attached to your Telegram account, including country code (for example, `+65...`).
3. Click **Next**.
4. Telegram sends a login code, usually inside the Telegram app—not necessarily by SMS.
5. Enter that code on the website.
6. Open **API development tools**.
7. If you have no application, fill in the form:
   - App title: `Telehitch Insights`
   - Short name: `telehitchinsights`
   - URL: leave blank if optional, or use your repository URL
   - Platform: choose **Desktop** or the closest suitable option
   - Description: `Private Telegram analytics ingestion`
8. Submit the form.
9. Record the numeric **api_id** in a secure password manager.
10. Record the **api_hash** in the password manager.
11. Never commit either value to Git.

Telegram may limit how often application credentials can be created. Reuse the application you just created.

---

## Part 8: prepare your computer and generate a cloud-safe Telegram StringSession

Cloud Composer workers are replaceable, so a local SQLite `.session` file is unsuitable. The included helper creates a Telethon StringSession that can be stored in Secret Manager.

### Install Git and Python

1. Install Python 3.10 or newer from <https://www.python.org/downloads/> if `python --version` does not work.
2. On Windows, select **Add Python to PATH** during installation.
3. Install Git from <https://git-scm.com/downloads> if `git --version` does not work.
4. Open Terminal (macOS/Linux) or PowerShell (Windows).
5. Run:

   ```bash
   python --version
   git --version
   ```

6. Both commands should print versions.

### Download this repository

If it is hosted on GitHub:

1. Open the repository page.
2. Click **Code**.
3. Copy its HTTPS URL.
4. In Terminal/PowerShell, move to a folder where you keep projects.
5. Run:

   ```bash
   git clone YOUR_REPOSITORY_HTTPS_URL
   cd sgjb-telehitch-insights
   ```

If you already have the repository, open a terminal in its directory and pull the latest committed version.

### Create a virtual environment and install the helper dependency

macOS/Linux:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install "telethon>=1.34"
```

Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install "telethon>=1.34"
```

### Generate the StringSession

1. Run:

   ```bash
   python scripts/generate_telegram_string_session.py
   ```

2. Enter your Telegram API ID.
3. Enter the API hash. The terminal deliberately does not display it.
4. Enter your Telegram phone number if Telethon requests it.
5. Enter the login code sent by Telegram.
6. Enter your Telegram two-step-verification password if requested.
7. The helper prints a value between `TELEGRAM_SESSION_STRING START` and `END` markers.
8. Copy only that value to a password manager temporarily.
9. Do not paste it into a source file, chat, issue, or Git commit.
10. Leave the virtual environment with `deactivate` when finished.

Anyone with the StringSession plus the API credentials may be able to act as your Telegram session. Protect it like a password.

---

## Part 9: collect Databricks Free Edition details

Databricks Free Edition is serverless-only, quota-limited, intended for non-commercial learning/prototyping, and has no SLA. It can stop compute after fair-use limits are exceeded. This means the overall pipeline is not guaranteed 24/7 even though Composer remains scheduled.

### Open the SQL warehouse

1. Sign in to your Databricks Free Edition workspace.
2. In the left sidebar, open **SQL Warehouses**. It may be nested under **SQL** or **Compute**, depending on the UI.
3. Free Edition normally provides one small serverless warehouse, often called **Starter Warehouse**.
4. Click the warehouse name.
5. If it is stopped, click **Start** for initial testing.
6. Open the **Connection details** tab.
7. Copy the **Server hostname** to your temporary setup note. It is not a password.
8. Copy the **HTTP path** to your temporary setup note. It usually begins with `/sql/1.0/warehouses/`.
9. Do not include `https://` in `DATABRICKS_SERVER_HOSTNAME`; copy the hostname exactly as shown.

A stopped warehouse should start when a SQL connection is established. Free Edition can still enforce daily/monthly quotas.

### Choose the catalog and schema

1. Open **Catalog** or **Catalog Explorer** in Databricks.
2. Identify a catalog you can write to. In Free Edition this is commonly `workspace`.
3. Expand it and identify a schema, commonly `default`.
4. Record both exact names.
5. The loader creates `telegram_messages` automatically if your account has permission.

### Generate a Databricks personal access token

The current connector uses a PAT. OAuth/service-principal automation is preferable in paid production environments, but Free Edition administrative options are limited.

1. Click your username/avatar in the top bar.
2. Open **Settings**.
3. Open **Developer**.
4. Next to **Access tokens**, click **Manage**.
5. Click **Generate new token**.
6. Use the comment/name `cloud-composer-telegram`.
7. Choose an expiration long enough for your test, but not unlimited if avoidable.
8. If scopes are offered, select the SQL/BI Tools scope needed to use a SQL warehouse.
9. Click **Generate**.
10. Copy the token immediately to your password manager; Databricks may show it only once.
11. If the Access tokens option is absent, stop here: your Free Edition workspace may not permit the external authentication method this code currently requires. Do not put your Google password into the pipeline.

---

## Part 10: create four secrets in Google Secret Manager

The DAG uses Airflow's Secret Manager backend. Airflow Variable `telegram_api_id` maps to Secret Manager secret `airflow-variables-telegram_api_id`; the same prefix rule applies to the other secrets.

Create each secret separately:

1. In Google Cloud Console, confirm your project is selected.
2. Open `☰` → **Security** → **Secret Manager**.
3. Click **Create secret**.
4. Enter the first secret name exactly as shown below.
5. Paste only its corresponding value into **Secret value**.
6. Leave replication on **Automatic** unless you have a specific compliance requirement.
7. Click **Create secret**.
8. Repeat until all four exist.

| Secret Manager name | Value |
|---|---|
| `airflow-variables-telegram_api_id` | Numeric Telegram API ID |
| `airflow-variables-telegram_api_hash` | Telegram API hash |
| `airflow-variables-telegram_session_string` | Generated Telethon StringSession |
| `airflow-variables-databricks_token` | Databricks PAT |

### Verify service-account access

1. Open each secret.
2. Open its **Permissions** tab.
3. Confirm `composer-telegram-worker@...` has **Secret Manager Secret Accessor** through project-level or secret-level inheritance.
4. Do not grant public principals such as `allUsers` or `allAuthenticatedUsers`.

The four active secret versions and one access every 15 minutes per secret are normally within Secret Manager's small free monthly allowance, but always check current Google pricing.

---

## Part 11: create the Cloud Composer 3 environment

Environment creation commonly takes around 25 minutes.

1. In Cloud Console, search for **Composer** in the top search bar.
2. Open **Cloud Composer**.
3. Open **Environments**.
4. Click **Create environment**.
5. Select **Cloud Composer 3**.
6. Choose the standard/non-highly-resilient environment option for this prototype. Highly resilient environments cost more.
7. Enter environment name `telegram-airflow`.
8. Select the region recorded earlier, for example `asia-southeast1`.
9. Select the latest supported Composer 3 image offered by the console. Do not manually choose an obsolete Airflow version.
10. For **Service account**, select `composer-telegram-worker@YOUR_PROJECT_ID.iam.gserviceaccount.com`.
11. Choose the smallest environment size/resources the console permits for this prototype.
12. Keep public networking/default networking unless your organization requires private networking. The worker must reach Telegram and your public Databricks hostname over HTTPS.
13. Do not enable high resilience for this first personal prototype unless you accept the added cost.

### Configure the Airflow Secret Manager backend during creation

In the **Airflow configuration overrides** section, add:

| Section | Key | Value |
|---|---|---|
| `secrets` | `backend` | `airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend` |

If the console requests a single combined key instead of separate Section and Key fields, use `secrets-backend` with that value.

The default secret prefixes are `airflow-variables-` and `airflow-connections-`, which match the secrets created above. Keep Airflow 2.10.2+ `backends_order` at its default, which includes `custom`.

### Add non-secret environment variables during creation

Find **Environment variables** and add each row:

| Name | Value |
|---|---|
| `TELEGRAM_AIRFLOW_SCHEDULE` | `*/15 * * * *` |
| `TELEGRAM_CHANNEL` | `CarpoolSgJb` |
| `TELEGRAM_INITIAL_LOOKBACK_MESSAGES` | `1000` |
| `TELEGRAM_PER_RUN_LIMIT` | `0` |
| `DATABRICKS_SERVER_HOSTNAME` | Your copied Databricks server hostname |
| `DATABRICKS_HTTP_PATH` | Your copied HTTP path |
| `DATABRICKS_CATALOG` | Your writable catalog, for example `workspace` |
| `DATABRICKS_SCHEMA` | Your writable schema, for example `default` |
| `DATABRICKS_TABLE` | `telegram_messages` |
| `DATABRICKS_BATCH_SIZE` | `100` |

Do not add secret values as environment variables. `TELEGRAM_PER_RUN_LIMIT=0` means no per-run limit after the initial run. The initial run is capped at the newest 1,000 messages unless you change `TELEGRAM_INITIAL_LOOKBACK_MESSAGES`.

14. Review the estimated cost shown by Google.
15. Click **Create**.
16. Wait until environment status becomes **Healthy** or **Running**.
17. If creation fails, open the operation details, read the first concrete error, correct it, and retry. Common causes are billing restrictions, missing service-account roles, disabled APIs, unavailable region capacity, and organization policies.

---

## Part 12: install Python packages into Composer

Do not install `apache-airflow` yourself; Composer supplies and manages Airflow.

1. On the Composer **Environments** page, click `telegram-airflow`.
2. Open the **PyPI packages** tab.
3. Click **Edit**.
4. Click **Add package**.
5. Add package `telethon` with version specifier `>=1.34`.
6. Add package `databricks-sql-connector` with version specifier `>=3.0`.
7. Click **Save**.
8. Wait for the environment update to finish and return to healthy status.
9. If package installation reports a dependency conflict, save the error text. Do not try to solve it by adding a different `apache-airflow` package.

The repository `requirements.txt` intentionally contains only these Composer custom packages.

---

## Part 13: upload the two Python files to Composer's DAG folder

Both files must be in the same Composer `/dags` folder because the DAG imports the scraper module.

### Browser method

1. In Google Cloud Console, open **Cloud Composer → Environments**.
2. Find `telegram-airflow`.
3. In the **DAGs folder** column, click the folder link.
4. Cloud Storage opens at the environment bucket's `/dags` directory.
5. Click **Upload files**.
6. Upload `dags/telegram_to_databricks.py` from this repository.
7. Click **Upload files** again.
8. Upload the repository-root `telegram_scraper.py`.
9. Confirm both files appear directly under `/dags`, not inside an extra nested local folder.
10. Do not upload `session_name.session`, CSV files, `.env`, `.git`, tests, or your password-manager note.
11. Wait at least two minutes for synchronization and DAG parsing.

### Optional Cloud Shell method

If the repository is available in Cloud Shell, run:

```bash
gcloud composer environments storage dags import \
  --environment telegram-airflow \
  --location asia-southeast1 \
  --source dags/telegram_to_databricks.py

gcloud composer environments storage dags import \
  --environment telegram-airflow \
  --location asia-southeast1 \
  --source telegram_scraper.py
```

Replace the location if necessary.

---

## Part 14: open Airflow and verify that the DAG parsed

1. Return to **Cloud Composer → Environments**.
2. Find `telegram-airflow`.
3. Click **Airflow UI**.
4. Allow pop-ups if your browser blocks the new tab.
5. Sign in with your Google account if prompted.
6. In the DAG list, search for `telegram_to_databricks_live_sync`.
7. If it appears, leave it paused for now.
8. If it does not appear after several minutes:
   1. Return to the Composer environment.
   2. Open **Logs**.
   3. Filter for DAG processor/import errors.
   4. Confirm both Python files are in the same `/dags` directory.
   5. Confirm both custom PyPI packages installed successfully.
9. Open the DAG details and verify the schedule displays every 15 minutes.

---

## Part 15: initialize the checkpoint

The checkpoint is the highest Telegram message ID loaded successfully. Starting at zero causes the first run to load up to the configured initial lookback.

1. In Airflow UI, open **Admin → Variables**. In some Airflow versions, it is under **Browse** or the security menu.
2. Click **Add** or `+`.
3. Set key `telegram_scraper_last_message_id`.
4. Set value `0`.
5. Save.

This checkpoint is not a secret. The DAG updates it only after the Databricks merge succeeds.

Do not create the four credential variables in the Airflow UI. They are resolved from Secret Manager using the backend configured earlier.

---

## Part 16: run the first test manually

1. In Databricks, open **SQL Warehouses** and confirm the Starter Warehouse exists. It may be stopped; a connection should start it.
2. In Airflow, open `telegram_to_databricks_live_sync`.
3. Unpause the DAG using its toggle.
4. Click the **Trigger DAG** button (play/triangle icon).
5. Confirm the trigger without changing configuration.
6. Open **Grid** or **Graph** view.
7. Click the `sync_messages` task square.
8. Open **Logs**.
9. Wait for the task to finish.
10. A successful run turns green and logs a result containing fetched, merged, and maximum message ID values.

The first connection can take longer because Databricks serverless compute may need to start.

### If the task fails

Match the error to this checklist:

| Error contains | Likely fix |
|---|---|
| `Missing required environment variable` | Add the named Composer variable or verify the corresponding Secret Manager secret/backend |
| `Telegram session is not authorized` | Generate a fresh StringSession and add a new secret version |
| `ApiIdInvalidError` | Correct Telegram API ID/API hash secrets |
| `AuthKeyDuplicatedError` or revoked session | Generate a new StringSession and terminate old Telegram sessions |
| Databricks `401`/`403` | Replace expired token and verify warehouse/catalog permissions |
| Host resolution/timeout | Correct hostname and confirm Composer has outbound internet access |
| Table permission error | Use a writable catalog/schema or grant create/use permissions |
| Import error for `telethon` or `databricks` | Repair Composer PyPI package installation |
| Import error for `telegram_scraper` | Put `telegram_scraper.py` directly beside the DAG file in `/dags` |
| Warehouse/quota unavailable | Wait for Databricks Free Edition quota reset or move to a paid supported workspace |

Never paste full logs publicly without removing tokens, session strings, phone numbers, and personal message content.

---

## Part 17: verify data in Databricks

1. Open Databricks.
2. Open **SQL Editor**.
3. Select the Starter Warehouse.
4. Run, replacing catalog/schema if needed:

   ```sql
   SELECT
     channel,
     id,
     message_date,
     message,
     sender_id,
     scraped_at
   FROM workspace.default.telegram_messages
   ORDER BY id DESC
   LIMIT 20;
   ```

5. Confirm Telegram rows appear.
6. Confirm recent IDs are ordered at the top.
7. Return to Airflow **Admin → Variables**.
8. Confirm `telegram_scraper_last_message_id` is now greater than zero.
9. Trigger the DAG again.
10. Confirm the second run does not duplicate existing rows. The Delta `MERGE` key is channel plus message ID.

---

## Part 18: allow unattended 15-minute operation

1. Keep the DAG unpaused.
2. Close the Airflow browser tab; the browser does not need to remain open.
3. Turn off your computer if desired; Composer runs in Google Cloud.
4. Return after at least 30 minutes.
5. In Airflow, verify approximately two scheduled runs occurred and succeeded.
6. In Databricks, verify new messages arrived if the channel had activity.
7. Confirm Databricks starts when queried after being stopped. Free Edition controls serverless lifecycle and quotas; there may not be a configurable auto-stop field.
8. Review Google Cloud Billing daily during the first week.

---

## Part 19: monitoring and routine maintenance

### Every day during initial testing

1. Open Airflow UI.
2. Check for red failed runs.
3. Open failed task logs and fix the first root error.
4. Check Databricks Free Edition quota notices.
5. Check Google Cloud billing reports.

### Every month

1. Review Google Cloud actual costs and forecast.
2. Confirm the Databricks token has not expired.
3. Review Telegram **Settings → Devices** for unknown sessions.
4. Update Composer to a supported image using Google's upgrade check.
5. Review PyPI dependency compatibility before upgrades.
6. Confirm the target table continues receiving recent rows.

### Token rotation

1. Generate a new Databricks token.
2. Open Secret Manager secret `airflow-variables-databricks_token`.
3. Click **New version**.
4. Paste the new token and create the version.
5. Trigger the DAG and confirm success.
6. Revoke the old token in Databricks.
7. Disable or destroy the old Secret Manager version after verification.

### Telegram StringSession rotation

1. Run `python scripts/generate_telegram_string_session.py` locally.
2. Add it as a new version of `airflow-variables-telegram_session_string`.
3. Trigger and verify the DAG.
4. Terminate the old session under Telegram **Settings → Devices**.
5. Disable the old secret version.

---

## Part 20: stopping charges or deleting the setup

Pausing the DAG stops runs but does not stop Composer environment charges.

To stop Composer charges:

1. Export any logs/configuration you need.
2. Open **Cloud Composer → Environments**.
3. Select `telegram-airflow`.
4. Click **Delete**.
5. Type/confirm the environment name if prompted.
6. Wait for deletion to complete.
7. Check related Cloud Storage buckets and logs before deleting them.
8. Open **Billing → Reports** and verify usage declines.

To remove the whole prototype:

1. Revoke the Databricks token.
2. Terminate the Telegram StringSession in Telegram Devices.
3. Delete the four Secret Manager secrets.
4. Delete the Composer environment.
5. Delete the Google Cloud project only if it contains nothing else you need.
6. Delete the Databricks table/workspace data if desired.

---

## Final expected configuration checklist

- [ ] Old committed Telegram session revoked.
- [ ] Google Cloud account and billing configured.
- [ ] Budget alerts configured.
- [ ] Google Cloud project selected.
- [ ] Composer and Secret Manager APIs enabled.
- [ ] Dedicated service account has Composer Worker and Secret Manager Secret Accessor.
- [ ] Telegram API ID/hash created.
- [ ] New Telethon StringSession generated.
- [ ] Databricks hostname, HTTP path, catalog, and schema recorded.
- [ ] Databricks PAT created.
- [ ] Four `airflow-variables-*` secrets created.
- [ ] Composer 3 environment is healthy.
- [ ] Secret Manager backend configured.
- [ ] Non-secret environment variables configured.
- [ ] `telethon` and `databricks-sql-connector` installed.
- [ ] DAG and scraper uploaded side by side.
- [ ] DAG schedule shows every 15 minutes.
- [ ] Checkpoint initialized to zero.
- [ ] First manual run succeeded.
- [ ] Delta table contains data.
- [ ] Two consecutive runs do not create duplicates.
- [ ] Scheduled runs continue while your computer is off.
- [ ] Billing and Databricks quota behavior are understood.
