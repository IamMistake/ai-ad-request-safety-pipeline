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
  var animState = {
    running: false,
    step: 0,
    timer: null,
    steps: [
      { node: 'source', connector: 'sourceKafka', status: 'Real traffic enters with injected fraud requests', delay: 900 },
      { node: 'kafka', connector: 'kafkaFlink', status: 'Kafka stores the event in requests.raw', delay: 900 },
      { node: 'flink', connector: 'flinkRouting', status: 'Flink evaluates stateful fraud signals', delay: 1100 },
      { node: 'spark', connector: 'sparkRouting', status: 'Spark-trained model context supports suspicious scoring', delay: 800 },
      { node: 'routing', connector: 'routingModeration', status: 'Clean fraud verdict moves to moderation', delay: 850 },
      { node: 'moderation', connector: 'moderationAd', status: 'Moderation checks prompt safety', delay: 1100 },
      { node: 'ad', connector: null, status: 'Safe request is approved for ad injection', delay: 900 },
      { node: null, status: 'Pipeline complete - request processed', delay: 500 }
    ]
  };

  var animReq = document.getElementById('anim-request');
  var animStatusText = document.getElementById('anim-status-text');
  var animPlayBtn = document.getElementById('anim-play-btn');
  var animResetBtn = document.getElementById('anim-reset-btn');

  var animNodes = {
    source: document.getElementById('anim-node-source'),
    kafka: document.getElementById('anim-node-kafka'),
    flink: document.getElementById('anim-node-flink'),
    routing: document.getElementById('anim-node-routing'),
    moderation: document.getElementById('anim-node-moderation'),
    ad: document.getElementById('anim-node-ad'),
    spark: document.getElementById('anim-node-spark')
  };

  var animConnectors = {
    sourceKafka: document.querySelector('.connector-source-kafka'),
    kafkaFlink: document.querySelector('.connector-kafka-flink'),
    flinkRouting: document.querySelector('.connector-flink-routing'),
    routingModeration: document.querySelector('.connector-routing-moderation'),
    moderationAd: document.querySelector('.connector-moderation-ad'),
    sparkRouting: document.querySelector('.connector-spark-routing')
  };

  function resetAnimation() {
    if (animState.timer) {
      clearTimeout(animState.timer);
      animState.timer = null;
    }
    animState.running = false;
    animState.step = 0;
    if (animPlayBtn) {
      animPlayBtn.disabled = false;
      animPlayBtn.innerHTML = '<i class="fa-solid fa-play"></i> Play';
    }
    if (animReq) {
      animReq.className = 'anim-request';
      animReq.style.opacity = '0';
    }
    if (animStatusText) animStatusText.textContent = 'Ready';
    Object.keys(animNodes).forEach(function (key) {
      if (animNodes[key]) animNodes[key].classList.remove('active');
    });
    Object.keys(animConnectors).forEach(function (key) {
      if (animConnectors[key]) animConnectors[key].classList.remove('active');
    });
  }

  function runAnimationStep() {
    if (!animState.running) return;

    var stepData = animState.steps[animState.step];
    if (!stepData) {
      if (animStatusText) animStatusText.textContent = 'All requests processed successfully';
      if (animPlayBtn) {
        animPlayBtn.disabled = false;
        animPlayBtn.innerHTML = '<i class="fa-solid fa-play"></i> Play';
      }
      animState.running = false;
      return;
    }

    if (animStatusText) animStatusText.textContent = stepData.status;

    if (animReq && stepData.node) {
      animReq.className = 'anim-request active';
      animReq.style.opacity = '1';
    }

    if (stepData.node && animNodes[stepData.node]) {
      animNodes[stepData.node].classList.add('active');
    }
    if (stepData.connector && animConnectors[stepData.connector]) {
      animConnectors[stepData.connector].classList.add('active');
    }

    animState.step++;
    animState.timer = setTimeout(runAnimationStep, stepData.delay);
  }

  function startAnimation() {
    if (animState.running) return;
    resetAnimation();
    animState.running = true;
    animState.step = 0;
    if (animPlayBtn) {
      animPlayBtn.disabled = true;
      animPlayBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Playing';
    }
    if (animReq) {
      animReq.className = 'anim-request';
      animReq.style.opacity = '0';
    }
    runAnimationStep();
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
