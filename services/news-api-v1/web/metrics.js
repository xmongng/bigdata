export const element = id => document.getElementById(id);
export const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
const number = new Intl.NumberFormat('vi-VN', { maximumFractionDigits: 2 });
const scenarioNames = new Map([
  ['replay-original-30', 'Nạp lại bộ gốc · 30 file, 30 bài'],
  ['solo-1', '1 yêu cầu · 1 file, 1 bài'],
  ['solo-5', '1 yêu cầu · 1 file, 5 bài'],
  ['solo-10', '1 yêu cầu · 1 file, 10 bài'],
  ['concurrent-2x5', '2 yêu cầu đồng thời · 2 file, 10 bài'],
  ['concurrent-3x5', '3 yêu cầu đồng thời · 3 file, 15 bài'],
  ['concurrent-3x5-files', '3 yêu cầu đồng thời · 15 file, 15 bài'],
]);

export const scenarioName = id => scenarioNames.get(id) ?? id;

export function node(tag, value) {
  const item = document.createElement(tag);
  item.textContent = value;
  return item;
}

export function duration(ms) {
  if (ms == null) return '—';
  return ms < 1000 ? number.format(ms) + ' ms' : number.format(ms / 1000) + ' giây';
}

export function row(values) {
  const result = document.createElement('tr');
  for (const value of values) result.insertCell().textContent = value;
  return result;
}

export function bars(container, entries, format = value => number.format(value)) {
  const maximum = Math.max(1, ...entries.map(entry => entry.value));
  container.replaceChildren(...entries.map(({ label, value, tone }) => {
    const line = node('div', '');
    line.className = 'bar';
    const progress = document.createElement('progress');
    progress.max = maximum;
    progress.value = value;
    progress.setAttribute('aria-label', label);
    if (tone) progress.classList.add(tone);
    line.append(node('span', label), progress, node('strong', format(value)));
    return line;
  }));
}

function readPercentile(samples, fraction) {
  const values = samples.filter(sample => sample.status === 200).map(sample => sample.ms).sort((a, b) => a - b);
  return values.length ? values[Math.ceil(values.length * fraction) - 1] : null;
}

export function renderBenchmark(data) {
  if (!data?.scenarios?.length) return;
  element('benchmark').hidden = false;
  element('benchmark-summary').textContent = 'Lượt ' + data.run + ' · ' +
    data.scenarios.filter(item => item.complete).length + '/' + data.scenarios.length + ' kịch bản đạt.';
  element('benchmark-rows').replaceChildren(...data.scenarios.map(item =>
    row([scenarioName(item.name), item.requests, item.files, item.seen + '/' + item.expected, duration(item.ack),
      duration(item.last), item.complete && item.last > 0 ? number.format(item.expected * 1000 / item.last) : '—'])));
  bars(element('benchmark-chart'), data.scenarios.flatMap(item => [
    { label: scenarioName(item.name) + ' · API tiếp nhận', value: item.ack },
    { label: scenarioName(item.name) + ' · Trino đọc đủ bài', value: item.last, tone: 'commit' },
  ]).filter(item => item.value != null), duration);
  bars(element('read-chart'), data.reads.flatMap(item => [
    { label: item.concurrency + ' lượt đọc cùng lúc · Thời gian điển hình', value: readPercentile(item.samples, .5) },
    { label: item.concurrency + ' lượt đọc cùng lúc · Mốc 95% hoàn tất', value: readPercentile(item.samples, .95), tone: 'commit' },
  ]).filter(item => item.value != null), duration);
}
