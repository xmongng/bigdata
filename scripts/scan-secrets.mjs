import { readdirSync, readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { resolve } from 'node:path';

const root = resolve(import.meta.dirname, '..');
const staged = process.argv.includes('--staged');
const forbidden = /(?:^|\/)(?:legacy-import|checkpoints?|spark-events|data|backups?|backup-[^/]+|\.venv|node_modules|\.ssh|\.aws)(?:\/|$)|(?:^|\/)(?:\.env(?:\.(?!example$)[^/]+)?|client\.env|credentials(?:\.json)?|id_(?:rsa|ed25519)[^/]*|service-account[^/]*\.json)$|\.(?:pem|key|p12|pfx|crt|cer|log|dump|sqlite\d*|db|parquet|avro|zip|tgz|tar(?:\.gz)?)$/i;
const patterns = [
  ['private-key', /-----BEGIN (?:[A-Z]+ )*PRIVATE KEY-----/],
  ['provider-token', /\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,}|AKIA[A-Z0-9]{16}|xox[baprs]-[A-Za-z0-9-]{15,})\b/],
  ['embedded-url-auth', /\b[a-z]+:\/\/[^\s/:@]+:[^\s/@]+@/i],
  ['literal-secret', /(?:^\s*|[{,]\s*)(?:["']?[\w.-]*(?:password|secret(?:_key)?|access[_-]?key|api[_-]?key|token)["']?)\s*[:=]\s*["']([^"']{4,})["']/i],
];
function git(args) {
  const result = spawnSync('git', args, { cwd: root, maxBuffer: 8 * 1024 * 1024 });
  if (result.status !== 0) throw new Error('Git index could not be read.');
  return result.stdout;
}
function walk(dir, prefix = '') {
  return readdirSync(dir, { withFileTypes: true }).flatMap(entry => {
    if (entry.name === '.git') return [];
    const path = prefix + entry.name;
    if (entry.isDirectory() && !forbidden.test(path)) return walk(resolve(dir, entry.name), path + '/');
    return [{ path, link: entry.isSymbolicLink() }];
  });
}
try {
  let findings = 0;
  const paths = staged ? git(['ls-files', '-z', '--stage']).toString().split('\0').filter(Boolean).map(line => {
    const tab = line.indexOf('\t');
    return { path: line.slice(tab + 1), link: /^(120000|160000) /.test(line) };
  }) : walk(root);
  for (const { path, link } of paths) {
    const report = (line, rule) => { findings++; console.log(path + ':' + line + ' ' + rule); };
    if (link || forbidden.test(path)) { report(0, link ? 'link-or-submodule' : 'local-artifact'); continue; }
    const content = staged ? git(['show', ':' + path]) : readFileSync(resolve(root, path));
    if (content.length > 2 * 1024 * 1024 || content.includes(0)) { report(0, 'binary-or-oversized'); continue; }
    content.toString('utf8').split(/\r?\n/).forEach((line, index) => {
      for (const [rule, regex] of patterns) {
        const match = regex.exec(line);
        if (!match || (match[1] && /^(?:\${|\$[A-Z_]|CHANGE_ME|REPLACE_ME|placeholder|dummy|example|test-only)/i.test(match[1]))) continue;
        report(index + 1, rule);
      }
    });
  }
  console.log('Scanned ' + paths.length + ' paths; findings: ' + findings + '.');
  process.exitCode = findings ? 1 : 0;
} catch (error) {
  console.error(error.message);
  process.exitCode = 2;
}
