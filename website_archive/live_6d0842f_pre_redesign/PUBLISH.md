# Publish CLINX Website (Public Link)

## Fastest (No Coding) - Netlify Drop
1. Open [https://app.netlify.com/drop](https://app.netlify.com/drop)
2. Drag the `website` folder from this project.
3. Netlify gives instant URL like `https://<name>.netlify.app`
4. Share that link in Discord.

## Vercel (Also Easy)
1. Push project to GitHub.
2. In Vercel: New Project -> import repo.
3. Set **Root Directory** to `website`.
4. Deploy.
5. Share URL like `https://<name>.vercel.app`.

## Cloudflare Pages
1. Create Pages project.
2. Connect repo.
3. Root directory: `website`
4. Build command: *(leave empty)*
5. Output directory: `.`
6. Deploy and share `https://<name>.pages.dev`.

## Optional free vanity domain
Use `is-a.dev` for names like:
- `clinx.is-a.dev`
- `creed-clinx.is-a.dev`

Then point it to your deployed URL.
