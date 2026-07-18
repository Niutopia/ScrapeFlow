const directions = [
  { id: "01", name: "深海控制台", en: "Midnight Command", note: "三栏任务控制中心", className: "mini-command" },
  { id: "02", name: "纸张编辑部", en: "Paper Studio", note: "杂志式线性表单", className: "mini-paper" },
  { id: "03", name: "极光向导", en: "Aurora Wizard", note: "沉浸式分步操作", className: "mini-aurora" },
  { id: "04", name: "终端机", en: "Operator Terminal", note: "命令行工作台", className: "mini-terminal" },
  { id: "05", name: "媒体便当", en: "Media Bento", note: "彩色模块化面板", className: "mini-bento" },
];

export default function Home() {
  return (
    <main className="selector-page">
      <header className="selector-header">
        <a className="selector-brand" href="#top"><i>SF</i><span>ScrapeFlow</span></a>
        <span className="selector-status"><i /> DESIGN EXPLORATION · 05 DIRECTIONS</span>
      </header>
      <section className="selector-intro" id="top">
        <span>ALIST MEDIA AUTOMATION</span>
        <h1>五套真正不同的<br />刮削工作台</h1>
        <p>每一套都有独立的信息架构、操作路径和视觉语言。点击进入完整界面，体验输入路径、启动任务与查看结果。</p>
      </section>
      <section className="direction-grid">
        {directions.map((item) => (
          <a className="direction-card" href={`/ui/${item.id}`} key={item.id}>
            <div className={`mini-ui ${item.className}`} aria-hidden="true">
              <i className="mini-a" /><i className="mini-b" /><i className="mini-c" /><i className="mini-d" /><i className="mini-e" />
            </div>
            <div className="direction-meta">
              <span>{item.id}</span>
              <div><strong>{item.name}</strong><small>{item.en} · {item.note}</small></div>
              <b>↗</b>
            </div>
          </a>
        ))}
      </section>
      <footer className="selector-footer"><span>SCRAPEFLOW / UI DIRECTIONS</span><span>选择后接入本机刮削引擎</span></footer>
    </main>
  );
}
