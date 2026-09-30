# docs-web

BlitzQ's documentation site: a Next.js application generated with
[Create Fumadocs](https://github.com/fuma-nama/fumadocs).

## Deploying to Vercel

Point a Vercel project at the `ainest-labs/BlitzQ` repo with **Root
Directory** set to `docs-web`. Everything else auto-detects (Next.js
framework preset, `npm install`, `npm run build`).

- The build's `prebuild` step (`npm run gen-api`, see `scripts/gen-api-safe.mjs`)
  regenerates `content/docs/reference/*.mdx` from `blitzq` docstrings using
  the Python script in `../scripts/gen_api_docs.py`. It's best-effort: if
  Python/pip aren't available in the build image, it warns and leaves the
  reference docs as whatever's already committed, rather than failing the
  build.
- Set `NEXT_PUBLIC_SITE_URL` in the Vercel project's environment variables to
  the production URL (e.g. `https://blitzq.ainest.in`) once the custom domain
  is attached, so Open Graph image URLs resolve to it instead of falling
  back to `VERCEL_URL`/localhost (see `src/app/layout.tsx`).
- No `vercel.json` needed — this is a standard Next.js app (no static
  export), so `app/api/search` deploys as a serverless function
  automatically.

## Development

Run development server:

```bash
npm run dev
# or
pnpm dev
# or
yarn dev
```

Open http://localhost:3000 with your browser to see the result.

## Explore

In the project, you can see:

- `lib/source.ts`: Code for content source adapter, [`loader()`](https://fumadocs.dev/docs/headless/source-api) provides the interface to access your content.
- `lib/layout.shared.tsx`: Shared options for layouts, optional but preferred to keep.

| Route                     | Description                                            |
| ------------------------- | ------------------------------------------------------ |
| `app/(home)`              | The route group for your landing page and other pages. |
| `app/docs`                | The documentation layout and pages.                    |
| `app/api/search/route.ts` | The Route Handler for search.                          |

### Fumadocs MDX

Collections are defined with the [Macro API](https://fumadocs.dev/docs/mdx/macro) in `lib/source.ts`.

Read the [Introduction](https://fumadocs.dev/docs/mdx) for further details.

## Learn More

To learn more about Next.js and Fumadocs, take a look at the following
resources:

- [Next.js Documentation](https://nextjs.org/docs) - learn about Next.js
  features and API.
- [Learn Next.js](https://nextjs.org/learn) - an interactive Next.js tutorial.
- [Fumadocs](https://fumadocs.dev) - learn about Fumadocs
