FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir requests beautifulsoup4 lxml python-dotenv

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

COPY scrapers/ /app/scrapers/

# env.local is gitignored. For local image builds, copy it into the build context yourself.
# Railway: set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (and any optional table overrides)
# in the service environment instead.

CMD ["python", "-m", "scrapers.llm_data_verifier.run_verifier", "--vendor-type", "all"]
