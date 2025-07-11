FROM quantconnect/lean:latest

# install Python packages
RUN pip install --no-cache-dir \
      finnhub-python \
      openai \
      vectorbt

# set workdir
WORKDIR /lean

# copy code in
COPY . .

# make sure env vars are available at runtime
# (we’ll mount a .env or use –env in CI)

ENTRYPOINT ["lean"]
CMD ["backtest", "BasicTemplate"]
