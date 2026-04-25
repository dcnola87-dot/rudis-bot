"""
Rudis Execution Bot (v1)
Purpose: Prepare Swap only
"""

import os

import requests
from dotenv import load_dotenv

load_dotenv()

BOT_MODE = "EXECUTION"


def start():
    print("Rudis Execution Bot starting...")
    print(f"Mode: {BOT_MODE}")

    while True:
        cmd = input("exec> ").strip()

        if cmd in ("quit", "exit"):
            print("Bye.")
            break

        if cmd.startswith("prepare "):
            parts = cmd.split()

            if len(parts) != 4:
                print("Usage: prepare <BASE> <QUOTE> <AMOUNT>")
                continue

            _, base, quote, amount = parts

            # Normalize symbols
            base = base.upper()
            quote = quote.upper()

            # Validate amount
            try:
                amount_value = float(amount)
                if amount_value <= 0:
                    print("Amount must be greater than 0")
                    continue
            except ValueError:
                print("Amount must be a number")
                continue

            print("\n--- Prepare Swap ---")

            try:
                response = requests.post(
                    "http://127.0.0.1:8000/swap-sessions",
                    json={
                        "base": base,
                        "quote": quote,
                        "amount": amount_value,
                    },
                    timeout=5,
                )

                if response.status_code != 200:
                    print("Core API error:", response.text)
                    continue

                data = response.json()

                print(f"Session ID: {data.get('session_id')}")
                # Support either key name returned by core
                print(f"Expires At: {data.get('expires_at') or data.get('expires_at')}")
                print(f"Sign URL:   {data.get('sign_url') or data.get('sign_url')}")
                print()

            except requests.exceptions.RequestException as e:
                print("Could not reach Rudis Core:", str(e))
        else:
            print("Commands: prepare <...> | exit")


if __name__ == "__main__":
    start()
