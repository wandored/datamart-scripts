import subprocess
import sys
from datetime import datetime

# List of modules to run (in order)
MODULES = [
    "src.menu-engineering-online",
    "src.recipe-ingredient-update",
    "src.item-conversion-update",
    "src.uofm-update",
    "src.odata-table-update",
]


def run_module(module: str) -> bool:
    """Run a module via subprocess and stream output live."""
    print(f"\n{'=' * 80}")
    print(f"🚀 Starting {module} at {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"{'=' * 80}\n")
    result = subprocess.run(
        [sys.executable, "-m", module],
        text=True,
        capture_output=False,
    )
    print(f"\n{'-' * 80}")
    if result.returncode == 0:
        print(f"✅ Completed {module} successfully.")
        return True
    else:
        print(f"❌ {module} failed with return code {result.returncode}.")
        return False


def main():
    print("\n🧠 DataMart Update Orchestrator\n")
    print(f"Start time: {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"Python executable: {sys.executable}")
    print("\nModules to run in order:")
    for m in MODULES:
        print(f"  • {m}")
    print("\n")

    for module in MODULES:
        success = run_module(module)
        if not success:
            print(f"\n⚠️ Stopping execution — {module} failed.")
            sys.exit(1)  # stop on first failure

    print("\n🎉 All modules executed successfully!")
    print(f"End time: {datetime.now():%Y-%m-%d %H:%M:%S}")


if __name__ == "__main__":
    main()
