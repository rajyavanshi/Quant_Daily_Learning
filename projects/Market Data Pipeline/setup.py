import os

# Root project directory
root = r"D:\Quant_Daily_Learning\projects\Market Data Pipeline"

# Folder structure
folders = [
    "data",
    "data\\raw",
    "data\\cleaned",
    "data\\processed",
    "scripts",
    "notebooks_project",
    "notebooks_learning",
    "outputs",
    "outputs\\figures",
    "outputs\\tables"
]

# Files inside folders
files = {
    "scripts": [
        "fetch_data.py",
        "clean_data.py",
        "compute_metrics.py",
        "visualize.py"
    ],
    "notebooks_project": [
        "01_fetch_data.ipynb",
        "02_clean_data.ipynb",
        "03_compute_metrics.ipynb",
        "04_visualize.ipynb"
    ],
    "notebooks_learning": [
        "01_financial_market_data.ipynb",
        "02_data_cleaning_theory.ipynb",
        "03_returns_and_volatility.ipynb",
        "04_drawdowns_and_risks.ipynb"
    ],
}

# Base files
base_files = [
    "README.md",
    "requirements.txt",
    "app.py"
]

def make_project_structure():
    print(f"Creating Market Data Pipeline at:\n{root}\n")

    # Create folders and their files
    for folder in folders:
        path = os.path.join(root, folder)
        os.makedirs(path, exist_ok=True)
        print(f"Folder created: {path}")

        for f in files.get(folder, []):
            file_path = os.path.join(path, f)
            if not os.path.exists(file_path):
                with open(file_path, "w", encoding="utf-8") as file:
                    file.write(
                        f"# =====================================================\n"
                        f"# File: {f}\n"
                        f"# Purpose:\n"
                        f"# Author: Suraj Prakash\n"
                        f"# =====================================================\n"
                    )
                print(f"File created: {file_path}")

    # Create base files
    for base in base_files:
        file_path = os.path.join(root, base)
        if not os.path.exists(file_path):
            with open(file_path, "w", encoding="utf-8") as file:
                file.write(
                    f"# =====================================================\n"
                    f"# File: {base}\n"
                    f"# Purpose:\n"
                    f"# Author: Suraj Prakash\n"
                    f"# =====================================================\n"
                )
            print(f"Base file created: {file_path}")

    print("\n Project structure setup complete!")

if __name__ == "__main__":
    make_project_structure()
