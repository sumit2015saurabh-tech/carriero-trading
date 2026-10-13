# Carriero Website

Source for [carriero.com](https://carriero.com) — a single-page site (hero, about, team,
contact) served by a small Express server.

## Structure

| Path | Purpose |
|---|---|
| `server.js` | Express server: serves `public/`, plus `POST /api/contact` and `POST /api/subscribe` |
| `public/index.html` | The page, including the team section |
| `public/css/style.css` | Styles |
| `public/js/main.js` | Front-end behaviour (navigation, form submit) |
| `public/images/` | Team photos |
| `docs/` | Static copy of `public/` for GitHub Pages |
| `netlify/functions/` | Serverless versions of the contact/subscribe endpoints (Netlify) |
| `netlify.toml`, `render.yaml`, `vercel.json` | Alternative hosting configs (not used in production) |

## Run locally

Requires Node.js 18+.

```bash
npm install
node server.js
```

Open <http://localhost:3000>. Set `PORT` to use a different port.

## Hosting

Production runs on **Azure App Service**. Pushing to GitHub does **not** deploy the site;
deploy by zip-deploying this folder (without `node_modules`) to the App Service, using
its publish profile.

## Updating the team section

Edit the team cards in `public/index.html`, add the photo to `public/images/`, then copy
the same changes into `docs/` so both versions stay identical.
