from telethon import TelegramClient
import csv

api_id = 20260532
api_hash = 'e72b2cd2aad873f208aab3f02cfca3a9'
channel = 'CarpoolSgJb'

output_file = 'telegram_data_2026_20000.csv'
max_messages = 20000

client = TelegramClient('session_name', api_id, api_hash)

async def main():
    await client.start()

    saved = 0

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'date', 'message'])

        async for msg in client.iter_messages(channel):
            # Telegram returns newest to oldest.
            # Stop once we reach messages before 2026.
            if msg.date.year < 2026:
                print("Reached messages before 2026. Stopping.")
                break

            writer.writerow([
                msg.id,
                msg.date,
                msg.message
            ])

            saved += 1

            if saved % 100 == 0:
                print(f"Saved {saved} messages...")

            if saved >= max_messages:
                print("Reached 20,000 messages. Stopping.")
                break

    print(f"Done. Saved {saved} messages to {output_file}.")

with client:
    client.loop.run_until_complete(main())