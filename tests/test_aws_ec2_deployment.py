from pathlib import Path

ROOT = Path(__file__).parents[1]
DEPLOY = ROOT / "deploy" / "aws-ec2"


def test_ec2_environment_uses_direct_airflow_config_and_low_memory_limits():
    environment = (DEPLOY / "airflow.env.example").read_text()
    installer = (DEPLOY / "install.sh").read_text()

    assert "AIRFLOW__WEBSERVER__SECRET_KEY=" in environment
    assert "AIRFLOW__CORE__FERNET_KEY=" in environment
    assert "TELEGRAM_BACKFILL_PAGE_LIMIT=100" in environment
    assert "DATABRICKS_BATCH_SIZE=50" in environment
    assert "AIRFLOW__CORE__EXECUTOR=SequentialExecutor" in installer
    assert "AIRFLOW__CORE__PARALLELISM=1" in installer
    assert "AIRFLOW__SCHEDULER__PARSING_PROCESSES=1" in installer
    assert "AIRFLOW__WEBSERVER__WORKERS=1" in installer
    assert "memory_kib < 3145728" in installer
    assert '--constraint "${constraint_url}"' in installer


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
    assert "OnCalendar=daily" in timer
    assert "Persistent=true" in timer


def test_ec2_secret_and_state_files_are_gitignored():
    gitignore = (ROOT / ".gitignore").read_text()

    assert "deploy/aws-ec2/airflow.env" in gitignore
    assert "deploy/aws-ec2/channel-state.json" in gitignore


def test_ec2_readme_is_first_time_setup_only():
    guide = (DEPLOY / "README.md").read_text()

    assert "## Phase 1: create an AWS account" in guide
    assert "## Phase 15: enable the 15-minute schedule" in guide
    assert "Migration from Google Cloud Composer" not in guide
    assert "import the existing Composer checkpoint" not in guide
