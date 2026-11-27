import { bars, duration, element, node, pause, renderBenchmark, row } from './metrics.js';

const labels = { cong_nghe: 'Công nghệ', kinh_te: 'Kinh tế', giao_duc: 'Giáo dục', khoa_hoc: 'Khoa học' };
const categoryLabel = value => Object.hasOwn(labels, value) ? labels[value] : value || 'Chưa phân loại';
const fields = ['title', 'source', 'category'];
let run = [];
let analysisRequest = 0;

async function request(url, options) {
  const response = await fetch(url, { signal: AbortSignal.timeout(35000), ...options });
  const body = await response.json();
  if (!response.ok) {
    throw new Error(typeof body.detail === 'string' ? body.detail : 'HTTP ' + response.status);
  }
  return { body, response };
}

function busy(value) {
  for (const id of ['send', 'preset', 'reset', 'verify', 'files']) element(id).disabled = value;
}

async function refresh() {
  const current = ++analysisRequest;
  const filter = element('filter');
  const selected = filter.value;
  element('analysis-status').textContent = 'Đang đọc từ Trino…';
  try {
    const { body: data } = await request('/api/dashboard/analysis?' + new URLSearchParams({ category: selected }));
    if (current !== analysisRequest) return;
    filter.replaceChildren(new Option('Tất cả', ''), ...data.categories.map(group =>
      new Option(categoryLabel(group.name), group.name)));
    if (selected && ![...filter.options].some(option => option.value === selected)) {
      filter.add(new Option(categoryLabel(selected), selected));
    }
    filter.value = selected;
    const origins = [...data.sources].sort((a, b) => b.count - a.count);
    const top = origins[0];
    const summary = top ? origins.length + ' nguồn · ' + data.categories.length + ' chuyên mục · ' +
      top.name + ' chiếm ' + Math.round(100 * top.count / data.total) + '%.' : 'Bảng hiện hành đang trống.';
    element('data-summary').textContent = summary;
    bars(element('categories'), data.categories.map(group => ({ label: categoryLabel(group.name), value: group.count })));
    bars(element('sources'), origins.map(group => ({ label: group.name, value: group.count, tone: 'commit' })));
    element('articles').replaceChildren(...data.articles.map(article => {
      const line = document.createElement('tr');
      const detail = document.createElement('details');
      detail.append(node('summary', article.title || article.article_id));
      if (article.clean_content) detail.append(node('p', article.clean_content));
      detail.append(node('small', article.article_id));
      line.insertCell().append(detail);
      for (const value of [article.source, categoryLabel(article.category), article.published_at]) {
        line.insertCell().textContent = value || '—';
      }
      return line;
    }));
    element('row-count').textContent = data.articles.length
      ? 'Hiển thị ' + data.articles.length + ' bài.' : 'Chưa có bài phù hợp.';
    element('analysis-status').textContent = data.limited ? 'Thống kê dựa trên tối đa 100 bài API trả về.' : '';
  } catch (error) {
    if (current === analysisRequest) element('analysis-status').textContent = 'Lỗi đọc Trino · ' + error.message;
  }
}

async function parse(file) {
  if (file.size > 10 * 1024 * 1024) throw new Error('File vượt 10 MB.');
  const text = new TextDecoder('utf-8', { fatal: true }).decode(await file.arrayBuffer());
  const payload = JSON.parse(text);
  const articles = Array.isArray(payload) ? payload : (payload?.articles ?? [payload]);
  if (!Array.isArray(articles) || !articles.length) throw new Error('Không có danh sách bài hợp lệ.');
  const byId = new Map();
  for (const article of articles) {
    const id = article?.article_id;
    if (typeof id !== 'string' || !id.trim() || id.trim().length > 256) {
      throw new Error('Mỗi bài cần article_id dạng chuỗi, tối đa 256 ký tự.');
    }
    const values = Object.fromEntries(fields.map(key => {
      const value = article[key];
      if (value != null && typeof value !== 'string') throw new Error(key + ' phải là chuỗi.');
      return [key, (value || '').trim()];
    }));
    byId.set(id.trim(), values);
  }
  return byId;
}

function parseTimings(header) {
  return Object.fromEntries([...String(header || '').matchAll(/([a-z]+);dur=(\d+(?:\.\d+)?)/g)]
    .map(([, name, value]) => [name, Number(value)]));
}

async function lookup(entries) {
  const ids = [...new Set(entries.flatMap(item => [...item.expected.keys()]))];
  const found = new Map();
  for (let offset = 0; offset < ids.length; offset += 20) {
    const parameters = new URLSearchParams();
    ids.slice(offset, offset + 20).forEach(id => parameters.append('article_id', id));
    const { body } = await request('/api/dashboard/visible?' + parameters);
    body.articles.forEach(article => found.set(article.article_id, article));
  }
  return found;
}

function matches(record, expected) {
  return record && fields.every(key => (record[key] || '') === expected[key]);
}

const statuses = {
  preparing: 'Đang kiểm tra JSON', sending: 'Đang gửi lên API', waiting: 'Đã gửi Kafka · đang đợi Trino',
  done: 'Đã xác nhận qua Trino', existing: 'Trino thấy dữ liệu · có ID đã khớp từ trước',
  unknown: 'Trino thấy dữ liệu · thiếu đối chiếu trước upload', pending: 'Hết thời gian chờ xác nhận',
  mismatch: 'Trino thấy ID nhưng nội dung khác', error: 'Lỗi',
};

function renderRun() {
  element('run-results').hidden = !run.length;
  const count = status => run.filter(item => item.status === status).length;
  const queued = run.filter(item => item.ack != null).length;
  const groups = [
    ['Đã chuẩn bị', run.filter(item => item.expected).length],
    ['Bronze + Kafka xác nhận', queued],
    ['Trino thấy đủ', count('done') + count('existing') + count('unknown')],
    ['Lỗi hoặc cần kiểm tra', count('error') + count('pending') + count('mismatch')],
  ];
  element('stages').replaceChildren(...groups.map(([name, total]) => {
    const card = node('div', '');
    card.append(node('span', name), node('strong', total + '/' + run.length));
    return card;
  }));
  element('queue').replaceChildren(...run.map(item => {
    const progress = item.matched != null ? ' · ' + item.matched + '/' + item.expected.size + ' bài khớp' : '';
    const line = row([item.file.name, statuses[item.status] + progress + (item.error ? ' · ' + item.error : ''),
      duration(item.ack), duration(item.timings?.minio), duration(item.timings?.kafka), duration(item.commit)]);
    line.className = 'state-' + item.status;
    return line;
  }));
  bars(element('run-chart'), run.flatMap(item => [
    { label: item.file.name + ' · API tiếp nhận', value: item.ack },
    { label: item.file.name + ' · Trino đọc đủ bài', value: item.commit, tone: 'commit' },
  ]).filter(item => item.value != null), duration);
}

async function verify(attempts) {
  const active = run.filter(item => ['waiting', 'pending', 'mismatch'].includes(item.status));
  if (!active.length) return;
  active.forEach(item => { item.status = 'waiting'; item.error = ''; });
  const message = element('verification');
  for (let attempt = 0; attempt < attempts; attempt++) {
    const pending = active.filter(item => item.status === 'waiting');
    if (!pending.length) break;
    if (attempt) await pause(2500);
    message.textContent = 'Đối chiếu dữ liệu trên Trino · lần ' + (attempt + 1) + '/' + attempts;
    try {
      const current = await lookup(pending);
      for (const item of pending) {
        const rows = [...item.expected];
        item.matched = rows.filter(([id, expected]) => matches(current.get(id), expected)).length;
        item.mismatched = rows.filter(([id, expected]) => current.has(id) && !matches(current.get(id), expected))
          .map(([id]) => id);
        if (item.matched !== rows.length) continue;
        if (!item.baseline) item.status = 'unknown';
        else if (rows.some(([id, expected]) => matches(item.baseline.get(id), expected))) item.status = 'existing';
        else {
          item.status = 'done';
          item.commit = performance.now() - item.started;
        }
      }
    } catch (error) {
      if (attempt === attempts - 1) {
        pending.forEach(item => { item.error = error.message; });
      }
    }
    renderRun();
  }
  for (const item of active.filter(item => item.status === 'waiting')) {
    item.status = item.mismatched?.length ? 'mismatch' : 'pending';
    if (item.status === 'mismatch') item.error = item.mismatched.join(', ');
  }
  const needsCheck = run.some(item => ['pending', 'mismatch'].includes(item.status));
  element('verify').hidden = !needsCheck;
  message.textContent = needsCheck ? 'Các file cần kiểm tra đã được đánh dấu. Có thể đối chiếu lại.' : 'Đã kết thúc đối chiếu.';
  renderRun();
  await refresh();
}

async function upload(files) {
  if (!files.length) throw new Error('Chưa có file JSON.');
  element('verify').hidden = true;
  element('verification').textContent = '';
  run = files.map(file => ({ file, status: 'preparing' }));
  renderRun();
  for (const item of run) {
    try {
      item.expected = await parse(item.file);
    } catch (error) {
      item.status = 'error';
      item.error = error.message;
    }
  }
  let baseline;
  try {
    baseline = await lookup(run.filter(item => item.expected));
  } catch {
    // An unavailable baseline prevents measuring a confirmed change for this run.
  }
  for (const [index, item] of run.entries()) {
    if (item.status === 'error') continue;
    const form = new FormData();
    form.append('files', item.file);
    item.baseline = baseline;
    item.status = 'sending';
    element('upload-status').textContent = 'Đang gửi file ' + (index + 1) + '/' + run.length;
    renderRun();
    item.started = performance.now();
    try {
      const { response } = await request('/upload-files', { method: 'POST', body: form });
      if (response.status !== 202) throw new Error('Phản hồi upload không xác nhận xếp hàng.');
      item.ack = performance.now() - item.started;
      item.timings = parseTimings(response.headers.get('server-timing'));
      item.status = 'waiting';
    } catch (error) {
      item.status = 'error';
      item.error = error.message;
    }
    renderRun();
  }
  element('upload-status').textContent = run.filter(item => item.ack != null).length + '/' + run.length +
    ' file được API xác nhận xếp hàng.';
  await verify(25);
}

async function reset() {
  const status = element('upload-status');
  const { body: snapshot } = await request('/articles?limit=100');
  if (snapshot.count >= 100) throw new Error('Reset dừng vì API chỉ liệt kê tối đa 100 bài.');
  const ids = [...new Set(snapshot.articles.map(article => article.article_id))];
  if (!ids.length) {
    status.textContent = 'Bảng đã trống. Có thể nạp lại bộ 30 bài.';
    return;
  }
  if (!window.confirm('Xóa ' + ids.length + ' bài hiện hành khỏi Iceberg? Các file JSON nguồn và dữ liệu Bronze được giữ lại.')) return;
  const accepted = new Set();
  const rejected = [];
  for (const id of ids) {
    status.textContent = 'Đang gửi DELETE · ' + (accepted.size + rejected.length + 1) + '/' + ids.length;
    try {
      const { response } = await request('/articles/' + encodeURIComponent(id), { method: 'DELETE' });
      if (response.status !== 202) throw new Error('DELETE chưa được xác nhận.');
      accepted.add(id);
    } catch {
      rejected.push(id);
    }
  }
  let remaining = [...accepted];
  let checked = false;
  const deadline = performance.now() + Math.max(120000, accepted.size * 3000);
  status.textContent = 'Kafka đã xác nhận ' + accepted.size + '/' + ids.length +
    ' lệnh DELETE. Đang kiểm tra kết quả trên Trino…';
  while (remaining.length && performance.now() < deadline) {
    try {
      const { body } = await request('/articles?limit=100');
      if (body.count >= 100) break;
      checked = true;
      remaining = body.articles.map(article => article.article_id).filter(id => accepted.has(id));
      const removed = accepted.size - remaining.length;
      status.textContent = 'Trino đã xác nhận xóa ' + removed + '/' + ids.length +
        ' bài · ' + remaining.length + ' bài đang chờ.' +
        (rejected.length ? ' ' + rejected.length + ' lệnh DELETE lỗi.' : '');
    } catch {
      status.textContent = 'Trino tạm thời chưa phản hồi. Đang thử kiểm tra lại…';
    }
    if (remaining.length) await pause(2500);
  }
  const removed = checked ? accepted.size - remaining.length : 0;
  status.textContent = 'Trino xác nhận đã xóa ' + removed + '/' + ids.length + ' bài.' +
    (rejected.length ? ' DELETE lỗi: ' + rejected.join(', ') + '.' : '') +
    (remaining.length ? ' Còn ' + remaining.length + ' bài chưa xác nhận. Bấm Làm mới để xem dữ liệu hiện tại.' : '') +
    (removed === ids.length ? ' Có thể nạp lại bộ 30 bài.' : '');
  if (removed === ids.length) {
    run = [];
    renderRun();
    element('verify').hidden = true;
    element('verification').textContent = '';
  }
  await refresh();
}

async function action(callback) {
  busy(true);
  try {
    await callback();
  } catch (error) {
    element('upload-status').textContent = error.message;
  } finally {
    busy(false);
  }
}

element('upload').addEventListener('submit', event => {
  event.preventDefault();
  action(() => upload([...element('files').files]));
});
element('preset').addEventListener('click', () => action(async () => {
  const { body } = await request('/api/dashboard/preset');
  await upload(body.map(file => new File([file.content], file.name, { type: 'application/json' })));
}));
element('reset').addEventListener('click', () => action(reset));
element('verify').addEventListener('click', () => action(() => verify(1)));
element('refresh').addEventListener('click', refresh);
element('filter').addEventListener('change', refresh);
refresh();
request('/api/dashboard/benchmark').then(({ body }) => renderBenchmark(body)).catch(() => {});
