(() => {
  const KEY = 'wkb_theme'; // 'auto' | 'light' | 'dark'
  const VALID = ['auto', 'light', 'dark'];

  function getTheme() {
    const v = localStorage.getItem(KEY);
    return VALID.includes(v) ? v : 'auto';
  }

  function setTheme(v) {
    const theme = VALID.includes(v) ? v : 'auto';
    document.documentElement.setAttribute('data-theme', theme);
    localStorage.setItem(KEY, theme);
    updateButtons();
  }

  function cycleTheme() {
    const cur = getTheme();
    const next = VALID[(VALID.indexOf(cur) + 1) % VALID.length];
    setTheme(next);
  }

  function updateButtons() {
    const cur = getTheme();
    const label = `Theme: ${cur[0].toUpperCase()}${cur.slice(1)}`;
    document.querySelectorAll('[data-role="theme-toggle"]').forEach(el => {
      el.textContent = label;
    });
  }

  function init() {
    // Initialize from storage
    const initTheme = getTheme();
    document.documentElement.setAttribute('data-theme', initTheme);
    updateButtons();

    // Bind buttons
    document.querySelectorAll('[data-role="theme-toggle"]').forEach(el => {
      el.addEventListener('click', (e) => { e.preventDefault(); cycleTheme(); });
    });

    // If in auto mode, reflect system theme changes in label (colors adapt via CSS)
    try {
      const mq = window.matchMedia('(prefers-color-scheme: light)');
      mq.addEventListener('change', () => { if (getTheme() === 'auto') updateButtons(); });
    } catch (_) { /* older browsers */ }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();

