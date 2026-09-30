// One-off migration: copy docs/*.md into content/docs/*.mdx with frontmatter.
import fs from "node:fs";
import path from "node:path";

const root = path.resolve(import.meta.dirname, "..", "..");
const srcDir = path.join(root, "docs");
const outDir = path.join(root, "docs-web", "content", "docs");

const files = [
  { src: "installation.md", out: "installation.mdx", title: "Installation" },
  { src: "architecture.md", out: "architecture.mdx", title: "Architecture" },
  { src: "delivery_guarantees.md", out: "delivery-guarantees.mdx", title: "Delivery Guarantees" },
  { src: "framework_integration.md", out: "framework-integration.mdx", title: "Framework Integration" },
  { src: "operations.md", out: "operations.mdx", title: "Operations" },
  { src: "performance_tuning.md", out: "performance-tuning.mdx", title: "Performance Tuning" },
  { src: "benchmarking.md", out: "benchmarking.mdx", title: "Benchmarking" },
  { src: "COMPARISON_REPORT.md", out: "comparison-report.mdx", title: "Comparison Report" },
];

function stripLeadingH1(body, title) {
  const lines = body.split("\n");
  if (lines[0]?.trim().startsWith("# ")) {
    lines.shift();
    while (lines[0]?.trim() === "") lines.shift();
  }
  return lines.join("\n");
}

// MDX treats {, <, > specially outside of code fences; escape stray ones in prose.
function escapeMdx(body) {
  const lines = body.split("\n");
  let inFence = false;
  return lines
    .map((line) => {
      if (line.trim().startsWith("```")) {
        inFence = !inFence;
        return line;
      }
      if (inFence) return line;
      return line.replace(/\{/g, "\\{").replace(/\}/g, "\\}");
    })
    .join("\n");
}

fs.mkdirSync(outDir, { recursive: true });

const order = [];
for (const f of files) {
  const raw = fs.readFileSync(path.join(srcDir, f.src), "utf8");
  const body = escapeMdx(stripLeadingH1(raw, f.title));
  const frontmatter = `---\ntitle: ${f.title}\ndescription: ${f.title} for BlitzQ.\n---\n\n`;
  fs.writeFileSync(path.join(outDir, f.out), frontmatter + body);
  order.push(f.out.replace(/\.mdx$/, ""));
  console.log("wrote", f.out);
}

const meta = {
  title: "BlitzQ",
  pages: ["index", ...order, "reference"],
};
fs.writeFileSync(path.join(outDir, "meta.json"), JSON.stringify(meta, null, 2) + "\n");
console.log("wrote meta.json");
