(function () {
  function highlightSignals(root) {
    root.querySelectorAll('tr[data-signal="true"]').forEach((row) => {
      row.classList.add('bg-emerald-50');
    });
  }

  document.addEventListener('DOMContentLoaded', () => {
    const fragment = document.getElementById('summary-fragment');
    if (fragment) {
      highlightSignals(fragment);
    }
  });

  document.addEventListener('htmx:afterSwap', (event) => {
    if (event.target && event.target.id === 'summary-fragment') {
      highlightSignals(event.target);
    }
  });
})();
