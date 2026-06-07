"""Interactively create the Telethon StringSession used by Cloud Composer."""

from __future__ import annotations

import getpass

from telethon.sync import TelegramClient
from telethon.sessions import StringSession


def main() -> None:
    print("Create a Telegram StringSession for the Cloud Composer pipeline.")
    print("Nothing is written to disk. Keep the resulting value secret.\n")
    api_id = int(input("Telegram API ID: ").strip())
    api_hash = getpass.getpass("Telegram API hash (input hidden): ").strip()

    with TelegramClient(StringSession(), api_id, api_hash) as client:
        session_string = client.session.save()

    print("\nAuthentication succeeded.")
    print("Copy only the value between the markers into Google Secret Manager.")
    print("--- TELEGRAM_SESSION_STRING START ---")
    print(session_string)
    print("--- TELEGRAM_SESSION_STRING END ---")
    print("\nDo not save this value in Git, a CSV file, or a shared document.")


if __name__ == "__main__":
    main()
