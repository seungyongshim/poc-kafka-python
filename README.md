![CI](../../workflows/CI/badge.svg)

## 1. use setup-dev.ps1 (simple)
```powershell
Set-ExecutionPolicy RemoteSigned
./setup-dev.ps1
```

## 2. manual setup
```powershell
python3 -m venv venv
& venv/Scripts/Activate.ps1
pip install -r requirements-dev.txt
pip install -r requirements.txt
```

## 3. run kafka
```powershell
cd kafka
docker-compose up -d
```

## 4. kafdrop
[http://localhost:9000](http://localhost:9000)
