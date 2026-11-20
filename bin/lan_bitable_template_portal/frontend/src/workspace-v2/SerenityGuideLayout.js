// Installed inside the isolated guide document. The offline reader owns its corpus,
// filter state, citations, term definitions and persisted to-do list.
function build(sections) {
  const element = (tag, text, className) => {
    const node = document.createElement(tag);
    if (text != null) node.textContent = text;
    if (className) node.className = className;
    return node;
  };
  const list = document.getElementById('list');
  const toc = document.getElementById('f-sec');
  const quick = document.getElementById('quick-section');
  const buttons = new Map();
  function addChapterButton(value, label) {
    const button = element('button', label, value ? 'sec-link' : 'sec-link all');
    button.type = 'button';
    button.dataset.v = value;
    toc.append(button);
    buttons.set(value, button);
  }
  addChapterButton('', '全部章节');
  const dimensions = [
    ['ratio', '性价比'], ['lens', '收益口径'], ['grade', '证据等级'],
    ['money', '金钱成本'], ['time', '时间成本'], ['will', '毅力成本'],
  ];
  for (const [key, label] of dimensions) {
    const values = new Set(sections.flatMap(section => section.entries.map(entry => entry[key]).filter(Boolean)));
    const row = element('div', null, 'filterrow');
    row.dataset.dim = key;
    row.append(element('strong', label));
    if (key === 'ratio') row.title = '性价比由收益和成本合成，仅在同一收益口径内比较。';
    for (const value of values) {
      const text = LABEL[key]?.[value] || (key === 'lens' && LENS_LABEL[value]) || value;
      const button = element('button', text);
      button.dataset.v = value;
      row.append(button);
    }
    document.getElementById('facets').append(row);
  }
  for (const path of Object.keys(window.__CORPUS__.docs || {})) {
    const link = element('a', path.slice(5,-3));
    link.href = REPO_BLOB + path;
    document.getElementById('long-reads').append(link);
  }
  for (const section of sections) {
    SECS.set(section.n, section);
    const option = element('option', section.n + '. ' + section.title);
    option.value = section.n;
    quick.append(option);
    addChapterButton(section.n, option.textContent);
    const sub = element('div', null, 'toc-sub');
    sub.hidden = true;
    toc.append(sub);
    TOC_SEC.set(section.n, sub);

    const block = element('section', null, 'sec-block');
    block.id = 'sec-' + section.n;
    block.dataset.sec = section.n;
    const heading = element('header', null, 'sec-h');
    const count = element('span', null, 'metric');
    const number = element('b');
    count.append(number, ' 条');
    heading.append(element('h2', section.title), count);
    block.append(heading);
    CUR_SEC = section.n;
    for (const text of section.intro) {
      const intro = element('p', null, 'intro');
      renderText(intro, text, [], false);
      block.append(intro);
    }
    for (const entry of section.entries) {
      const key = entry.sec + '-' + entry.n;
      ITEMS.set(key, entry);
      const article = element('article', null, 'card entry');
      article.id = 'e-' + key;
      const jump = element('a', entry.n + '. ' + entry.title);
      jump.href = '#' + article.id;
      jump.dataset.go = key;
      sub.append(jump);
      TOC.set(article.id, jump);

      const metadata = element('div', null, 'entry-meta');
      const labels = [section.n + '.' + entry.n, entry.grade && '证据 ' + entry.grade + ' 级',
        LENS_LABEL[entry.lens] || entry.lens, entry.ratio && '性价比 ' + entry.ratio,
        ...['money','time','will'].map(dim => LABEL[dim]?.[entry[dim]] || entry[dim]),
        entry.dispute && '有争议', entry.todo && '待核实'];
      for (const label of labels.filter(Boolean)) metadata.append(element('span', label));
      const title = element('h3');
      const add = element('button', '加入待办', 'todo-btn');
      const head = element('div', null, 'entry-head');
      head.append(title, add);
      const human = element('p', null, 'human');
      const rows = element('dl');
      const field = label => {
        const value = element('dd');
        rows.append(element('dt', label), value);
        return value;
      };
      const gain = field('收益'), cost = field('成本'), note = field('备注');
      const details = element('details');
      const summary = element('summary','证据与来源');
      const srcN = element('span'), src = element('div');
      summary.append(srcN);
      details.append(summary, src);
      article.append(metadata, head, human, rows, details);
      block.append(article);
      const card = {e:entry, el:article, key:null, vis:false, f:{t:title, human, cost, gain, note, details, srcN, src}};
      // Give offscreen headings meaningful text before the full body is painted.
      title.textContent = entry.title;
      CARDS.push(card);
      CARD_OF.set(article, card);
      CARD_IO.observe(article);
    }
    sub.append(element('p','没有符合当前筛选的条目'));
    list.append(block);
    BLOCKS.push({n:section.n, el:block, k:number});
  }
  quick.addEventListener('change', () => buttons.get(quick.value)?.click());
  todoSyncAll();
}
