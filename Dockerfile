FROM python:3.12-slim
ARG FINANCE_COMMIT
ARG DAY_TRADER_COMMIT=3d0f4ef6986f4984b7c6bd46f7ff4f87744c3abd
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates git procps tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir websockets==15.0.1
RUN --mount=type=bind,source=state/docker-build,target=/bundles,readonly \
    git clone --no-checkout /bundles/finance.bundle /app \
    && git -C /app checkout --detach "$FINANCE_COMMIT" \
    && git -C /app remote remove origin \
    && git clone --no-checkout /bundles/day-trader.bundle /day-trader \
    && git -C /day-trader checkout --detach "$DAY_TRADER_COMMIT" \
    && git -C /day-trader remote remove origin \
    && git config --system --add safe.directory /app \
    && git config --system --add safe.directory /day-trader
WORKDIR /app
ENV HOME=/runner PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "-m", "tools.docker_strategy_cycle"]
