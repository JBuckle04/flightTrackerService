# flightTrackerService

Basic Flask web app scaffolded for Azure App Service.

## Local run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

## Azure App Service

- Dependencies are provided in `requirements.txt`.
- Startup command is defined in `Procfile` and serves `app:app` with Gunicorn.