// ============================================================
// Tabs — moderation mode switcher
// ============================================================

(function () {
  'use strict';

  var tabsContainer = document.getElementById('moderation-tabs');
  if (!tabsContainer) return;

  var tabBtns = tabsContainer.querySelectorAll('.tab-btn');
  var tabPanels = tabsContainer.querySelectorAll('.tab-panel');

  tabBtns.forEach(function (btn) {
    btn.addEventListener('click', function () {
      var targetId = this.getAttribute('aria-controls');

      // Deactivate all
      tabBtns.forEach(function (b) {
        b.classList.remove('active');
        b.setAttribute('aria-selected', 'false');
      });
      tabPanels.forEach(function (p) {
        p.classList.remove('active');
      });

      // Activate target
      this.classList.add('active');
      this.setAttribute('aria-selected', 'true');
      var panel = document.getElementById(targetId);
      if (panel) panel.classList.add('active');
    });
  });

})();
