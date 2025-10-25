#!/usr/bin/env python3
"""
Install Playwright/Patchright browsers
Run this once to download Chromium browser for scraping
"""

import subprocess
import sys
from pathlib import Path

def install_patchright_browsers():
    """Install browsers for patchright"""
    print("="*80)
    print("INSTALLING PLAYWRIGHT BROWSERS")
    print("="*80)

    try:
        print("\n📦 Installing Chromium browser for Patchright...")
        print("This may take a few minutes...\n")

        # Run patchright install
        result = subprocess.run(
            [sys.executable, "-m", "patchright", "install", "chromium"],
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            print("✅ Chromium browser installed successfully!")
            print(result.stdout)
            return True
        else:
            print("❌ Installation failed:")
            print(result.stderr)
            return False

    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def check_browsers_installed():
    """Check if browsers are already installed"""
    try:
        result = subprocess.run(
            [sys.executable, "-m", "patchright", "install", "--help"],
            capture_output=True,
            text=True
        )
        return result.returncode == 0
    except:
        return False

if __name__ == "__main__":
    print("Checking if browsers are installed...")

    if check_browsers_installed():
        print("✓ Patchright is available")
        install_patchright_browsers()
    else:
        print("❌ Patchright is not installed")
        print("Run: pip install patchright")
        sys.exit(1)
