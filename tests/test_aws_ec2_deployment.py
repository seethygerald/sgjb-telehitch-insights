from pathlib import Path
import subprocess

ROOT = Path(__file__).parents[1]
DEPLOY = ROOT / "deploy" / "aws-ec2"


def test_ec2_environment_uses_direct_airflow_config_and_low_memory_limits():
    environment = (DEPLOY / "airflow.env.example").read_text()
    installer = (DEPLOY / "install.sh").read_text()

    assert "AIRFLOW__WEBSERVER__SECRET_KEY=" in environment
    assert "AIRFLOW__CORE__FERNET_KEY=" in environment
    assert "TELEGRAM_BACKFILL_PAGE_LIMIT=0" in environment
    assert "DATABRICKS_BATCH_SIZE=50" in environment
    assert "AIRFLOW__CORE__EXECUTOR=LocalExecutor" in installer
    assert "postgresql+psycopg2://airflow:" in installer
    assert 'apache-airflow[postgres]==${AIRFLOW_VERSION}' in installer
    assert "postgresql-client" in installer
    assert "AIRFLOW__CORE__PARALLELISM=1" in installer
    assert "AIRFLOW__SCHEDULER__PARSING_PROCESSES=1" in installer
    assert "AIRFLOW__WEBSERVER__WORKERS=1" in installer
    assert "memory_kib < 3145728" in installer
    assert '--constraint "${constraint_url}"' in installer
    assert "sys.version_info[:2] <= (3, 12)" in installer
    assert "plain Canonical Ubuntu Server 24.04 LTS x86 image" in installer


def test_ec2_webserver_is_bound_to_loopback_only():
    unit = (DEPLOY / "systemd" / "telehitch-airflow-webserver.service").read_text()

    assert "--hostname 127.0.0.1 --port 8080" in unit
    assert "0.0.0.0" not in unit


def test_ec2_services_restart_and_backup_timer_is_persistent():
    scheduler = (DEPLOY / "systemd" / "telehitch-airflow-scheduler.service").read_text()
    webserver = (DEPLOY / "systemd" / "telehitch-airflow-webserver.service").read_text()
    timer = (DEPLOY / "systemd" / "telehitch-airflow-backup.timer").read_text()

    assert "Restart=always" in scheduler
    assert "Restart=always" in webserver
    assert "Requires=postgresql.service" in scheduler
    assert "Requires=postgresql.service" in webserver
    assert 'Environment="PATH=__VENV_ROOT__/bin:' in scheduler
    assert 'Environment="PATH=__VENV_ROOT__/bin:' in webserver
    assert "OnCalendar=daily" in timer
    assert "Persistent=true" in timer


def test_ec2_secret_and_state_files_are_gitignored():
    gitignore = (ROOT / ".gitignore").read_text()

    assert "deploy/aws-ec2/airflow.env" in gitignore
    assert "deploy/aws-ec2/channel-state.json" in gitignore


def test_root_readme_is_single_first_time_setup_guide():
    guide = (ROOT / "README.md").read_text()

    assert "## Phase 1: create an AWS account" in guide
    assert "## Phase 15: enable the 15-minute schedule" in guide
    assert "single setup guide" in guide
    assert "clean, first-time deployment" in guide
    assert "migration procedures are intentionally outside this guide" in guide
    assert "Cloud Composer" not in guide
    assert "Oracle" not in guide
    assert "import the existing Composer checkpoint" not in guide
    assert not (DEPLOY / "README.md").exists()


def test_root_readme_uses_plain_ubuntu_and_locked_down_small_instance():
    guide = (ROOT / "README.md").read_text()

    assert "Instance type | `t3.micro`" in guide
    assert "Ubuntu Server 24.04 LTS (HVM), SSD Volume Type" in guide
    assert "Do not select Ubuntu Server 26.04" in guide
    assert "Do not select an image containing **SQL Server**" in guide
    assert "Do not select" in guide
    assert "SSH from 0.0.0.0/0" in guide
    assert "Custom TCP 8080" in guide
    assert "Credit specification | Standard" in guide
    assert "EC2 → Network & Security → Key Pairs" in guide
    assert "ec2:CreateKeyPair" in guide
    assert "Do not launch until `telehitch-airflow` appears" in guide
    assert "Click **Not encrypted**" in guide
    assert "Change it to **Encrypted**" in guide
    assert "Volume initialization rate | Leave blank" in guide
    assert "`(default) aws/ebs`" in guide
    assert "Under **File systems**, select **None**" in guide
    assert "one 30 GiB gp3 root volume with EBS encryption enabled" in guide


def test_ec2_ssh_helper_and_publickey_recovery_are_documented():
    guide = (ROOT / "README.md").read_text()
    helper = (DEPLOY / "check-ssh.sh").read_text()

    assert "If SSH says `Permission denied (publickey)`" in guide
    assert "Creating a key pair afterward" in guide
    assert "does not install its public key" in guide
    assert "Key pair name" in guide
    assert "./deploy/aws-ec2/check-ssh.sh" in guide
    assert "-o IdentitiesOnly=yes" in guide
    assert "ssh-keygen -y -f" in helper
    assert "BatchMode=yes" in helper
    assert "IdentitiesOnly=yes" in helper
    assert '"ubuntu@$host"' in helper


def test_ec2_verification_helper_checks_runtime_health():
    helper_path = DEPLOY / "verify-install.sh"
    helper = helper_path.read_text()
    guide = (ROOT / "README.md").read_text()

    assert helper_path.stat().st_mode & 0o111
    assert "telehitch-airflow-scheduler.service" in helper
    assert "telehitch-airflow-webserver.service" in helper
    assert "telehitch-airflow-backup.timer" in helper
    assert "Airflow uses LocalExecutor" in helper
    assert "Airflow metadata uses PostgreSQL" in helper
    assert "PostgreSQL is active and accepting connections" in helper
    assert "Scheduler PATH includes the Airflow virtual environment" in helper
    assert "telegram_to_databricks_live_sync" in helper
    assert "dags list-import-errors --output json" in helper
    assert "telegram_scraper_channel_state" in helper
    assert "swapon --show" in helper
    assert "./deploy/aws-ec2/verify-install.sh" in guide
    assert "Graphviz" in guide
    assert "`No data found`" in guide


def test_ec2_postgres_backup_and_sqlite_migration_helpers():
    backup = (DEPLOY / "backup-airflow.sh").read_text()
    migration_path = DEPLOY / "migrate-metadata-to-postgres.sh"
    migration = migration_path.read_text()
    updater = (DEPLOY / "update.sh").read_text()

    assert migration_path.stat().st_mode & 0o111
    assert "pg_dump" in backup
    assert "--format=custom" in backup
    assert "airflow-*.dump" in backup
    assert "telegram_scraper_channel_state" in migration
    assert "pre-postgres-airflow-" in migration
    assert '"${DEPLOY_DIR}/install.sh"' in migration
    assert '"${DEPLOY_DIR}/import-channel-state.sh"' in migration
    assert "migrate-metadata-to-postgres.sh" in updater
    assert "Verified zero-means-unlimited initial backfill behavior." in updater
    assert "backfill_page_limit=0" in updater
    assert "an empty state will be imported" not in migration
    assert "Migration stopped before changing metadata" in migration

    recovery_path = DEPLOY / "recover-channel-state-from-sqlite.sh"
    recovery = recovery_path.read_text()
    assert recovery_path.stat().st_mode & 0o111
    assert "pre-postgres-airflow-*.db" in recovery
    assert "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN" in recovery
    assert "AIRFLOW__CORE__EXECUTOR=SequentialExecutor" in recovery
    assert "variables get" in recovery
    assert "airflow.env" in recovery


def test_ec2_scheduler_runtime_includes_virtualenv_path():
    installer = (DEPLOY / "install.sh").read_text()

    assert "PATH=${VENV_ROOT}/bin:" in installer


def test_ec2_verifier_detects_a_stale_backfill_implementation():
    verifier = (DEPLOY / "verify-install.sh").read_text()

    assert "--allow-stopped-scheduler" in verifier
    assert "intentionally allowed to be stopped for maintenance" in verifier
    assert "backfill_page_limit=0" in verifier
    assert '== ("full_history", None)' in verifier
    assert "Scraper supports unlimited initial backfills" in verifier
    assert "Scraper is stale" in verifier


def test_ec2_resource_monitor_captures_task_capacity_signals():
    monitor_path = DEPLOY / "monitor-resources.sh"
    monitor = monitor_path.read_text()
    guide = (ROOT / "README.md").read_text()

    assert monitor_path.stat().st_mode & 0o111
    assert "mem_available_mib" in monitor
    assert "swap_used_mib" in monitor
    assert "cpu_used_percent" in monitor
    assert "MemoryCurrent" in monitor
    assert "telehitch-airflow-scheduler.service" in monitor
    assert "postgres_rss_mib" in monitor
    assert "out of memory|oom-kill|killed process" in monitor
    assert "sudo -n true" in monitor
    assert "OOM check skipped" in monitor
    assert "sudo -v" not in monitor
    assert "never prompts for a sudo password" in guide
    assert "monitor-resources.sh 3600 5" in guide
    assert "CPUCreditBalance" in guide
    assert '-v load_value="${load_1m}"' in monitor
    assert '-v load="${load_1m}"' not in monitor


def test_ec2_resource_monitor_executes_and_writes_a_sample(tmp_path):
    output = tmp_path / "monitor.csv"
    result = subprocess.run(
        [str(DEPLOY / "monitor-resources.sh"), "2", "1", str(output)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )

    assert result.returncode == 0, result.stderr
    rows = output.read_text().splitlines()
    assert rows[0].startswith("timestamp_utc,mem_available_mib")
    assert len(rows) >= 2
    assert "Resource summary:" in result.stdout
