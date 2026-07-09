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
      '.results-grid, .quickstart-steps, .spark-grid, .spark-flow-text, ' +
      '.data-sources, .abstract-box, .metrics-strip, ' +
      '.flink-diagram, .moderation-diagram, .moderation-flow, ' +
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
    scenarioIndex: 0,
    cancelToken: 0,
    scenarios: [
      {
        title: 'Clean Request',
        example: 'Normal IP, normal session, safe prompt.',
        final: 'Final result: approved for ad injection.',
        done: 'Clean request approved for ad injection',
        steps: [
          { node: 'user', link: 'userRaw', status: 'User request enters the pipeline', trace: 'clean: user request received', delay: 1250 },
          { node: 'raw', link: 'rawFlink', status: 'Kafka writes the event to requests.raw', trace: 'clean: requests.raw receives the event', delay: 1250 },
          { node: 'flink', link: 'flinkClean', status: 'Flink checks fraud rules and returns clean', trace: 'clean: fraud score stays below clean threshold', delay: 1650 },
          { node: 'clean', link: 'cleanModeration', status: 'Clean fraud verdict routes to requests.clean', trace: 'clean: requests.clean forwards prompt to moderation', delay: 1350 },
          { node: 'moderation', link: 'moderationAd', status: 'Moderation checks prompt and returns safe', trace: 'clean: prompt passes content safety checks', delay: 1650 },
          { node: 'ad', link: 'adProcess', status: 'Safe request moves to ad.injection', trace: 'clean: ad.injection accepts the request', delay: 1350 },
          { node: 'process', link: null, status: 'Finding Ad Process starts', trace: 'clean: finding the best sponsored suggestion', delay: 1500 }
        ]
      },
      {
        title: 'Fraud Request',
        example: 'Bad IP, abusive traffic pattern, obvious fraud signal.',
        final: 'Final result: blocked as fraud.',
        done: 'Fraud request blocked before moderation',
        steps: [
          { node: 'user', link: 'userRaw', status: 'User request enters the pipeline', trace: 'fraud: request received from risky source', delay: 1250 },
          { node: 'raw', link: 'rawFlink', status: 'Kafka writes the event to requests.raw', trace: 'fraud: requests.raw receives the event', delay: 1250 },
          { node: 'flink', link: 'flinkFraud', status: 'Flink checks fraud rules and returns fraud', trace: 'fraud: bad IP and abusive traffic pattern detected', delay: 1750 },
          { node: 'fraud', link: 'fraudDb', status: 'Fraud verdict routes to requests.fraud', trace: 'fraud: requests.fraud receives the blocked event', delay: 1400 },
          { node: 'db', link: null, status: 'Blocked event is stored for analysis', trace: 'fraud: request blocked as fraud', delay: 1500 }
        ]
      },
      {
        title: 'Suspicious Request',
        example: 'Not clearly fraud, but risky enough for model scoring.',
        final: 'Final result: RFC decides clean or fraud.',
        done: 'Suspicious request resolved by RFC scoring',
        steps: [
          { node: 'user', link: 'userRaw', status: 'User request enters the pipeline', trace: 'suspicious: borderline request received', delay: 1250 },
          { node: 'raw', link: 'rawFlink', status: 'Kafka writes the event to requests.raw', trace: 'suspicious: requests.raw receives the event', delay: 1250 },
          { node: 'flink', link: 'flinkSus', status: 'Flink checks fraud rules and returns suspicious', trace: 'suspicious: score enters model-scoring band', delay: 1750 },
          { node: 'sus', link: 'susRfc', status: 'Suspicious verdict routes to requests.sus', trace: 'suspicious: requests.sus queues the event for RFC', delay: 1400 },
          { node: 'rfc', link: 'rfcClean', status: 'RFC Scoring Service decides this case is clean', trace: 'suspicious: RFC scoring can return clean', delay: 1750 },
          { node: 'clean', link: 'cleanModeration', status: 'Clean RFC result routes to requests.clean', trace: 'suspicious: clean branch continues to moderation', delay: 1350 },
          { node: 'moderation', link: 'moderationAd', status: 'Moderation checks prompt before ad injection', trace: 'suspicious: safe prompt reaches ad.injection', delay: 1550 },
          { node: 'ad', link: null, status: 'Alternative RFC result would route to requests.fraud', trace: 'suspicious: RFC can instead choose fraud and block the request', delay: 1550 }
        ]
      }
    ]
  };

  var animStatusText = document.getElementById('anim-status-text');
  var animPlayBtn = document.getElementById('anim-play-btn');
  var animResetBtn = document.getElementById('anim-reset-btn');
  var animRoundBadge = document.getElementById('anim-round-badge');
  var animLoopChip = document.getElementById('anim-loop-chip');
  var animOutputText = document.getElementById('anim-output-text');
  var animCursor = document.getElementById('anim-cursor');
  var animSpark = document.getElementById('anim-unit-spark');
  var animScenarioExample = document.getElementById('anim-scenario-example');
  var animScenarioResult = document.getElementById('anim-scenario-result');

  var animNodes = {
    user: document.getElementById('anim-unit-user'),
    raw: document.getElementById('anim-unit-raw'),
    flink: document.getElementById('anim-unit-flink'),
    sus: document.getElementById('anim-unit-sus'),
    clean: document.getElementById('anim-unit-clean'),
    fraud: document.getElementById('anim-unit-fraud'),
    rfc: document.getElementById('anim-unit-rfc'),
    moderation: document.getElementById('anim-unit-moderation'),
    db: document.getElementById('anim-unit-db'),
    ad: document.getElementById('anim-unit-ad'),
    process: document.getElementById('anim-unit-process')
  };

  var animLinks = {
    userRaw: document.getElementById('anim-link-user-raw'),
    rawFlink: document.getElementById('anim-link-raw-flink'),
    flinkSus: document.getElementById('anim-link-flink-sus'),
    flinkClean: document.getElementById('anim-link-flink-clean'),
    flinkFraud: document.getElementById('anim-link-flink-fraud'),
    susRfc: document.getElementById('anim-link-sus-rfc'),
    rfcClean: document.getElementById('anim-link-rfc-clean'),
    rfcFraud: document.getElementById('anim-link-rfc-fraud'),
    cleanModeration: document.getElementById('anim-link-clean-moderation'),
    moderationAd: document.getElementById('anim-link-moderation-ad'),
    fraudDb: document.getElementById('anim-link-fraud-db'),
    adProcess: document.getElementById('anim-link-ad-process')
  };

  function sleep(ms) {
    return new Promise(function (resolve) {
      window.setTimeout(resolve, prefersReducedMotion ? Math.min(ms, 80) : ms);
    });
  }

  function setScenario(scenario) {
    if (animRoundBadge) animRoundBadge.textContent = scenario.title;
    if (animScenarioExample) animScenarioExample.textContent = 'Example:';
    if (animScenarioResult) animScenarioResult.textContent = scenario.example + ' ' + scenario.final;
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

  async function runScenario(scenario, token) {
    setScenario(scenario);
    if (animStatusText) animStatusText.textContent = scenario.title + ' entering pipeline';
    if (animLoopChip) animLoopChip.classList.remove('show');
    await sleep(500);

    for (var i = 0; i < scenario.steps.length; i++) {
      if (!animState.running || token !== animState.cancelToken) return;
      var step = scenario.steps[i];
      var node = animNodes[step.node];

      clearActiveState();
      if (animSpark && (step.node === 'sus' || step.node === 'rfc')) animSpark.classList.add('active');
      if (node) node.classList.add('active');
      if (step.link) pulseLink(step.link);
      if (animStatusText) animStatusText.textContent = step.status;

      await Promise.all([
        streamTrace(scenario.title.toLowerCase() + ' / ' + step.trace, token),
        sleep(step.delay)
      ]);
    }

    if (!animState.running || token !== animState.cancelToken) return;
    clearActiveState();
    if (animStatusText) animStatusText.textContent = scenario.done;
    await streamTrace('final: ' + scenario.final, token);
    if (animLoopChip) animLoopChip.classList.add('show');
    await sleep(1700);
  }

  function resetAnimation() {
    animState.cancelToken++;
    animState.running = false;
    animState.scenarioIndex = 0;
    if (animPlayBtn) {
      animPlayBtn.disabled = false;
      animPlayBtn.innerHTML = '<i class="fa-solid fa-play"></i> Play';
    }
    clearActiveState();
    if (animRoundBadge) animRoundBadge.textContent = 'Clean Request';
    if (animRoundBadge) animRoundBadge.classList.remove('pulsing');
    if (animStatusText) animStatusText.textContent = 'Ready';
    if (animScenarioResult) animScenarioResult.textContent = 'Normal IP, normal session, safe prompt. Final result: approved for ad injection.';
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

    for (var i = 0; i < animState.scenarios.length; i++) {
      if (!animState.running || token !== animState.cancelToken) break;
      animState.scenarioIndex = i;
      await runScenario(animState.scenarios[i], token);
    }

    if (token === animState.cancelToken) {
      animState.running = false;
      clearActiveState();
      if (animStatusText) animStatusText.textContent = 'Three request paths complete';
      if (animLoopChip) animLoopChip.classList.remove('show');
      if (animPlayBtn) {
        animPlayBtn.disabled = false;
        animPlayBtn.innerHTML = '<i class="fa-solid fa-play"></i> Play';
      }
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
