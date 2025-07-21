# test_packages.py
packages = [
    'win32com.client',
    'mysql.connector',
    'numpy',
    'telegram',
    'dotenv'
]

for package in packages:
    try:
        __import__(package)
        print(f"✅ {package}")
    except ImportError:
        print(f"❌ {package}")