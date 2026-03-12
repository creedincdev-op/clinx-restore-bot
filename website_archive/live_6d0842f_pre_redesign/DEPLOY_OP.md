# OP Proper Deploy Stack (Recommended)

## Best choice: Cloudflare Pages + custom subdomain

Why this is better than Netlify for your use:
- Fast global edge network
- Free SSL + DDoS protection
- Reliable static hosting for landing pages
- Easy custom domain mapping

## Deploy steps (5 min)

1. Push this project to GitHub.
2. In Cloudflare Dashboard -> Pages -> Create Project.
3. Connect your repo.
4. Build settings:
   - Framework preset: `None`
   - Build command: *(leave empty)*
   - Build output directory: `website`
5. Deploy.

You will get a URL like:
- `https://<project>.pages.dev`

## Put your free vanity domain

Use one of these:
- `clinx.is-a.dev`
- `creed-clinx.is-a.dev`

Point it to your Cloudflare Pages domain using CNAME, then add that custom domain in Cloudflare Pages.

## Share in Discord

Send this single link:
- `https://your-final-domain`

Everyone can open it on mobile + desktop.

## Notes

- Security headers are already added in `website/_headers`
- SPA route fallback is already added in `website/_redirects`
