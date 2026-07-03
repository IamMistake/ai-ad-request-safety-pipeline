// ============================================================
// Main JS — TOC toggle, active tracking, scroll fade-in
// ============================================================

(function () {
  'use strict';

  // --- TOC toggle ---
  var tocRail = document.getElementById('toc-rail');
  var tocToggle = document.getElementById('toc-toggle');
  var tocLinks = document.getElementById('toc-links');

  if (tocRail && tocToggle) {
    tocToggle.addEventListener('click', function () {
      tocRail.classList.toggle('is-collapsed');
    });
  }

  // --- TOC active section tracking (IntersectionObserver) ---
  if (tocLinks && 'IntersectionObserver' in window) {
    var links = tocLinks.querySelectorAll('.toc-link');
    var sections = [];
    links.forEach(function (link) {
      var href = link.getAttribute('href');
      if (href && href.startsWith('#')) {
        var section = document.getElementById(href.substring(1));
        if (section) sections.push({ el: section, link: link });
      }
    });

    if (sections.length > 0) {
      var observer = new IntersectionObserver(function () {
        var current = null;
        var currentTop = Infinity;

        sections.forEach(function (item) {
          var rect = item.el.getBoundingClientRect();
          // Find the section closest to the top of viewport (or just above it)
          if (rect.top <= 200 && rect.top < currentTop) {
            currentTop = rect.top;
            current = item;
          }
        });

        sections.forEach(function (item) {
          item.link.classList.remove('is-active');
        });
        if (current) {
          current.link.classList.add('is-active');
        }
      }, {
        threshold: [0, 0.25, 0.5, 0.75, 1],
        rootMargin: '-80px 0px -50% 0px'
      });

      sections.forEach(function (item) {
        observer.observe(item.el);
      });
    }
  }

  // --- Intersection Observer for fade-in animations ---
  function observeFadeIn() {
    var elements = document.querySelectorAll(
      '.pillars, .flow-steps, .pipeline-container, .rules-explorer, ' +
      '.results-grid, .status-grid, .quickstart-steps, .spark-grid, ' +
      '.data-sources, .fraud-table-wrap, .abstract-box, .metrics-strip, ' +
      '.flink-diagram, .spark-diagram'
    );

    elements.forEach(function (el) {
      el.classList.add('fade-in');
    });

    if (!('IntersectionObserver' in window)) {
      document.querySelectorAll('.fade-in').forEach(function (el) {
        el.classList.add('visible');
      });
      return;
    }

    var observer = new IntersectionObserver(function (entries) {
      entries.forEach(function (entry) {
        if (entry.isIntersecting) {
          entry.target.classList.add('visible');
          observer.unobserve(entry.target);
        }
      });
    }, {
      threshold: 0.08,
      rootMargin: '0px 0px -40px 0px'
    });

    document.querySelectorAll('.fade-in').forEach(function (el) {
      observer.observe(el);
    });
  }

  // Wait for SVG objects to load, then observe
  function waitForSvgs(callback) {
    var objects = document.querySelectorAll('object');
    var remaining = objects.length;
    if (remaining === 0) {
      callback();
      return;
    }
    objects.forEach(function (obj) {
      obj.addEventListener('load', function () {
        remaining -= 1;
        if (remaining === 0) callback();
      });
      if (obj.contentDocument || obj.type === 'failed') {
        remaining -= 1;
      }
    });
    setTimeout(callback, 1500);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () {
      waitForSvgs(observeFadeIn);
    });
  } else {
    waitForSvgs(observeFadeIn);
  }

  // --- BibTeX copy button ---
  var bibBtn = document.getElementById('bibtex-copy');
  if (bibBtn) {
    bibBtn.addEventListener('click', function () {
      var pre = this.parentElement.querySelector('pre');
      if (pre) {
        var text = pre.textContent || '';
        if (navigator.clipboard && navigator.clipboard.writeText) {
          navigator.clipboard.writeText(text.trim()).then(function () {
            bibBtn.innerHTML = '<i class="fa-regular fa-check-circle"></i> Copied!';
            bibBtn.classList.add('copied');
            setTimeout(function () {
              bibBtn.innerHTML = '<i class="fa-regular fa-clipboard"></i> Copy';
              bibBtn.classList.remove('copied');
            }, 2000);
          });
        }
      }
    });
  }

})();
