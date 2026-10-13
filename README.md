# Carriero

Code for **Carriero — Algorithmic Trading in US Markets**.

This repository holds two independent projects:

| Folder | What it is | Stack |
|---|---|---|
| [`CARRIERO_WEBSITE/`](CARRIERO_WEBSITE/) | The company website, [carriero.com](https://carriero.com) — landing page, team, contact form | Node.js · Express · Nodemailer · static HTML/CSS/JS |
| [`MACD_BOT_STRATEGY/`](MACD_BOT_STRATEGY/) | The QTI MACD intraday trading bot (model `!JFMX3X_NONCORR_MACD_V3.x`) — indicators, entry/exit logic, IBKR execution, shadow broker, dashboards | Python 3.12 · Interactive Brokers TWS API |

Each folder has its own README with setup and run instructions.

```
.
├── CARRIERO_WEBSITE/        # carriero.com
│   ├── server.js            # Express server + contact form API
│   ├── public/              # the site (index.html, css, js, images)
│   └── docs/                # static mirror of public/ (GitHub Pages)
└── MACD_BOT_STRATEGY/
    ├── MACD_ENTRY_EXIT/     # bot source code + config.json
    └── scripts/             # start / stop / dashboard shell scripts (Linux VM)
```

## Quick start

**Website**

```bash
cd CARRIERO_WEBSITE
npm install
node server.js          # http://localhost:3000
```

**Trading bot**

```bash
cd MACD_BOT_STRATEGY/MACD_ENTRY_EXIT
pip install ibapi pandas numpy openpyxl requests
python macd_bot.py      # needs a running IB Gateway / TWS
```

See [`MACD_BOT_STRATEGY/README.md`](MACD_BOT_STRATEGY/README.md) before running the bot.

## Secrets

No credentials are stored in this repository. Account numbers, the server address and
Discord webhooks have been replaced with placeholders or environment variables
(`DISCORD_WEBHOOK_URL`, `<VM_HOST>`, `DUXXXXXXX`). Supply your own via environment
variables or local, untracked config files.

## Disclaimer

The trading code is published for reference only. It is not investment advice, and running
it against a live brokerage account can lose money.
