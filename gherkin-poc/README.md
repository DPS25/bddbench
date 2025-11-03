This folder contins a proof of concept to run Gherkin features with Python (behave) for InfluxDB-like benchmarks  

**How to install:**

```guide
PowerShell (Windows):

git clone https://github.com/DPS25/bddbench.git

cd .\bddbench\gherkin-poc

python -m venv .venv

.\.venv\Scripts\Activate.ps1

pip install -r .\requirements.txt

behave


  
Bash (Linx/macOS/WSL):

git clone https://github.com/DPS25/bddbench.git

cd bddbench/gherkin-poc

python3 -m venv .venv

source .venv/bin/activate

pip install -r requirements.txt

behave
