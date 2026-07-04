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
      '.results-grid, .quickstart-steps, .spark-grid, ' +
      '.data-sources, .fraud-table-wrap, .abstract-box, .metrics-strip, ' +
      '.flink-diagram, .spark-diagram, .moderation-diagram, .moderation-flow, ' +
      '.moderation-tfidf'
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

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', observeFadeIn);
  } else {
    observeFadeIn();
  }

  // --- Other Works dropdown (global functions for onclick= attributes) ---
  window.toggleMoreWorks = function () {
    var dropdown = document.getElementById('moreWorksDropdown');
    var button = document.querySelector('.more-works-btn');
    if (!dropdown || !button) return;
    dropdown.classList.toggle('show');
    button.classList.toggle('active');
  };

  document.addEventListener('click', function (event) {
    var container = document.querySelector('.more-works-container');
    var dropdown = document.getElementById('moreWorksDropdown');
    var button = document.querySelector('.more-works-btn');
    if (container && !container.contains(event.target)) {
      if (dropdown) dropdown.classList.remove('show');
      if (button) button.classList.remove('active');
    }
  });

  document.addEventListener('keydown', function (event) {
    if (event.key === 'Escape') {
      var dropdown = document.getElementById('moreWorksDropdown');
      var button = document.querySelector('.more-works-btn');
      if (dropdown) dropdown.classList.remove('show');
      if (button) button.classList.remove('active');
    }
  });

  // --- Interactive Pipeline Animation ---
  var prefersReducedMotion = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  var animState = {
    running: false,
    requestNum: 1,
    cancelToken: 0,
    steps: [
      { node: 'source', link: 'sourceKafka', status: 'Real traffic data enters with fraud injected', trace: 'ingest: WildChat request + fraud profile attached', delay: 760 },
      { node: 'kafka', link: 'kafkaFlink', status: 'Kafka records the event in requests.raw', trace: 'topic: requests.raw -> partitioned event stream', delay: 760 },
      { node: 'flink', link: 'flinkRfc', status: 'Flink evaluates stateful fraud signals', trace: 'flink: user/session/publisher risk signals updated', delay: 900 },
      { node: 'rfc', link: 'rfcModeration', status: 'RFC scoring resolves suspicious requests', trace: 'rfc: RandomForest context scores borderline traffic', delay: 780 },
      { node: 'moderation', link: 'moderationAd', status: 'Moderation checks prompt safety', trace: 'moderation: TF-IDF gate + OpenAI escalation when needed', delay: 920 },
      { node: 'ad', link: null, status: 'Safe request approved for ad injection', trace: 'ad.injection: approved request ready for sponsored suggestion', delay: 820 }
    ]
  };

  var animStatusText = document.getElementById('anim-status-text');
  var animPlayBtn = document.getElementById('anim-play-btn');
  var animResetBtn = document.getElementById('anim-reset-btn');
  var animRoundBadge = document.getElementById('anim-round-badge');
  var animRoundNum = document.getElementById('anim-round-num');
  var animLoopChip = document.getElementById('anim-loop-chip');
  var animOutputText = document.getElementById('anim-output-text');
  var animCursor = document.getElementById('anim-cursor');
  var animSpark = document.getElementById('anim-unit-spark');

  var animNodes = {
    source: document.getElementById('anim-unit-source'),
    kafka: document.getElementById('anim-unit-kafka'),
    flink: document.getElementById('anim-unit-flink'),
    rfc: document.getElementById('anim-unit-rfc'),
    moderation: document.getElementById('anim-unit-moderation'),
    ad: document.getElementById('anim-unit-ad')
  };

  var animLinks = {
    sourceKafka: document.getElementById('anim-link-source-kafka'),
    kafkaFlink: document.getElementById('anim-link-kafka-flink'),
    flinkRfc: document.getElementById('anim-link-flink-rfc'),
    rfcModeration: document.getElementById('anim-link-rfc-moderation'),
    moderationAd: document.getElementById('anim-link-moderation-ad')
  };

  function sleep(ms) {
    return new Promise(function (resolve) {
      window.setTimeout(resolve, prefersReducedMotion ? Math.min(ms, 80) : ms);
    });
  }

  function setRoundBadge(num) {
    if (animRoundNum) animRoundNum.textContent = '#' + num;
    if (!animRoundBadge) return;
    animRoundBadge.classList.remove('pulsing');
    void animRoundBadge.offsetWidth;
    animRoundBadge.classList.add('pulsing');
  }

  function clearActiveState() {
    Object.keys(animNodes).forEach(function (key) {
      if (animNodes[key]) animNodes[key].classList.remove('active');
    });
    Object.keys(animLinks).forEach(function (key) {
      if (animLinks[key]) animLinks[key].classList.remove('active');
    });
    if (animSpark) animSpark.classList.remove('active');
    if (animLoopChip) animLoopChip.classList.remove('show');
  }

  function pulseLink(linkKey) {
    var link = animLinks[linkKey];
    if (!link) return;
    link.classList.remove('active');
    void link.offsetWidth;
    link.classList.add('active');
  }

  async function streamTrace(text, token) {
    if (!animOutputText) return;
    if (animCursor) animCursor.classList.add('show');
    animOutputText.textContent = '';

    if (prefersReducedMotion) {
      animOutputText.textContent = text;
      if (animCursor) animCursor.classList.remove('show');
      return;
    }

    for (var i = 0; i < text.length; i += 2) {
      if (token !== animState.cancelToken) return;
      animOutputText.textContent += text.slice(i, i + 2);
      await sleep(12);
    }
    if (animCursor) animCursor.classList.remove('show');
  }

  async function runRequest(token) {
    setRoundBadge(animState.requestNum);
    if (animStatusText) animStatusText.textContent = 'Request #' + animState.requestNum + ' entering pipeline';
    if (animLoopChip) animLoopChip.classList.remove('show');
    if (animSpark) animSpark.classList.add('active');
    await sleep(220);

    for (var i = 0; i < animState.steps.length; i++) {
      if (!animState.running || token !== animState.cancelToken) return;
      var step = animState.steps[i];
      var node = animNodes[step.node];

      clearActiveState();
      if (animSpark && (step.node === 'flink' || step.node === 'rfc')) animSpark.classList.add('active');
      if (node) node.classList.add('active');
      if (step.link) pulseLink(step.link);
      if (animStatusText) animStatusText.textContent = step.status;

      await Promise.all([
        streamTrace('request #' + animState.requestNum + ' / ' + step.trace, token),
        sleep(step.delay)
      ]);
    }

    if (!animState.running || token !== animState.cancelToken) return;
    clearActiveState();
    if (animStatusText) animStatusText.textContent = 'Pipeline complete - request approved';
    await streamTrace('complete: request #' + animState.requestNum + ' safely routed to ad.injection', token);
    if (animLoopChip) animLoopChip.classList.add('show');
    animState.requestNum++;
    await sleep(950);
  }

  function resetAnimation() {
    animState.cancelToken++;
    animState.running = false;
    animState.requestNum = 1;
    if (animPlayBtn) {
      animPlayBtn.disabled = false;
      animPlayBtn.innerHTML = '<i class="fa-solid fa-play"></i> Play';
    }
    clearActiveState();
    if (animRoundNum) animRoundNum.textContent = '#1';
    if (animRoundBadge) animRoundBadge.classList.remove('pulsing');
    if (animStatusText) animStatusText.textContent = 'Ready';
    if (animOutputText) animOutputText.textContent = 'Waiting for a request...';
    if (animCursor) animCursor.classList.remove('show');
  }

  async function startAnimation() {
    if (animState.running) return;
    resetAnimation();
    animState.running = true;
    var token = ++animState.cancelToken;
    if (animPlayBtn) {
      animPlayBtn.disabled = true;
      animPlayBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Playing';
    }

    while (animState.running && token === animState.cancelToken) {
      await runRequest(token);
    }
  }

  if (animPlayBtn) {
    animPlayBtn.addEventListener('click', startAnimation);
  }
  if (animResetBtn) {
    animResetBtn.addEventListener('click', function () {
      resetAnimation();
    });
  }

})();
