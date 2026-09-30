// Regenerates content/docs/reference/*.mdx from blitzq docstrings before the
// Next.js build, but never fails the build if Python isn't available (e.g.
// Vercel's Next.js build image doesn't guarantee one) — falls back to
// whatever's already committed in content/docs/reference/.
import { spawnSync } from 'node:child_process';
import path from 'node:path';

const root = path.resolve(import.meta.dirname, '..', '..');
const script = path.join(root, 'scripts', 'gen_api_docs.py');
const requirements = path.join(root, 'scripts', 'requirements.txt');

function run(cmd, args) {
  const res = spawnSync(cmd, args, { stdio: 'inherit', cwd: root });
  return res.status === 0 && res.error === undefined;
}

function findPython() {
  for (const cmd of ['python3', 'python']) {
    const res = spawnSync(cmd, ['--version'], { stdio: 'ignore' });
    if (res.status === 0) return cmd;
  }
  return null;
}

const python = findPython();
if (!python) {
  console.warn(
    '[gen-api] no python interpreter found; using the committed content/docs/reference/*.mdx as-is.',
  );
  process.exit(0);
}

const installed = run(python, ['-m', 'pip', 'install', '-q', '-r', requirements]);
if (!installed) {
  console.warn('[gen-api] pip install failed; using the committed content/docs/reference/*.mdx as-is.');
  process.exit(0);
}

const generated = run(python, [script]);
if (!generated) {
  console.warn('[gen-api] generation failed; using the committed content/docs/reference/*.mdx as-is.');
  process.exit(0);
}
