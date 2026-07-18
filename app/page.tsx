const directions = [
  { id: "01", code: "INFUSE", name: "沉浸式任务卡", note: "大幅媒体艺术 · 极简悬浮控制", tone: "infuse" },
  { id: "02", code: "TV", name: "影院式工作台", note: "Apple TV 式内容层次 · 横向浏览", tone: "tv" },
  { id: "03", code: "NAS", name: "媒体文件管家", note: "群晖式可靠管理 · 清晰表格流程", tone: "nas" },
  { id: "04", code: "EMBY", name: "媒体服务器中心", note: "高效侧栏 · 海报与任务并行", tone: "emby" },
  { id: "05", code: "SF", name: "ScrapeFlow Cinema", note: "影音沉浸与专业控制的原创融合", tone: "fusion" },
];

export default function Home() {
  return (
    <main className="pick-page">
      <header className="pick-nav">
        <a className="pick-brand" href="#top"><i>SF</i><b>ScrapeFlow</b></a>
        <span>MEDIA AUTOMATION · DESIGN ROUND 02</span>
      </header>
      <section className="pick-hero" id="top">
        <div><span>五套全新方向</span><h1>像打开影音库一样，<br />开始一次刮削。</h1></div>
        <p>重新从 Infuse、Apple TV、群晖与 Emby 的产品语言出发。它们不只是配色不同，而是五种不同的使用方式。</p>
      </section>
      <section className="pick-grid">
        {directions.map((item, index) => (
          <a className={`pick-card ${item.tone}`} href={`/ui/${item.id}`} key={item.id}>
            <div className="pick-visual" aria-hidden="true">
              <div className="pv-backdrop" />
              <div className="pv-sidebar" />
              <div className="pv-window" />
              <div className="pv-poster" />
              <div className="pv-line one" />
              <div className="pv-line two" />
              <div className="pv-action" />
            </div>
            <div className="pick-copy"><span>0{index + 1} / {item.code}</span><h2>{item.name}</h2><p>{item.note}</p><b>进入体验&nbsp; →</b></div>
          </a>
        ))}
      </section>
      <footer className="pick-footer"><span>ALIST × TMDB × INFUSE</span><span>所有方案目前使用安全模拟数据</span></footer>
    </main>
  );
}
