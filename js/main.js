// ============================================================
// Main JS — Scroll animations, nav toggle, Intersection Observer
// ============================================================

(function () {
  'use strict';

  // --- Mobile Nav Toggle ---
  const navToggle = document.querySelector('.nav-toggle');
  const navLinks = document.querySelector('.nav-links');

  if (navToggle && navLinks) {
    navToggle.addEventListener('click', function () {
      const expanded = this.getAttribute('aria-expanded') === 'true';
      this.setAttribute('aria-expanded', !expanded);
      navLinks.classList.toggle('open');
    });

    // Close nav on link click
    navLinks.querySelectorAll('a').forEach(function (link) {
      link.addEventListener('click', function () {
        navToggle.setAttribute('aria-expanded', 'false');
        navLinks.classList.remove('open');
      });
    });
  }

  // --- Sticky nav background on scroll ---
  const nav = document.getElementById('site-nav');
  if (nav) {
    window.addEventListener('scroll', function () {
      if (window.scrollY > 50) {
        nav.style.background = 'rgba(10, 10, 15, 0.95)';
      } else {
        nav.style.background = 'rgba(10, 10, 15, 0.85)';
      }
    }, { passive: true });
  }

  // --- Intersection Observer for fade-in animations ---
  function observeSections() {
    const elements = document.querySelectorAll('.section, .hero-metrics, .problem-grid, .steps, .pipeline-container, .rules-explorer, .results-showcase, .status-grid, .quickstart-steps, .spark-details, .data-grid');

    elements.forEach(function (el) {
      el.classList.add('fade-in');
    });

    if (!('IntersectionObserver' in window)) {
      // Fallback: show all
      document.querySelectorAll('.fade-in').forEach(function (el) {
        el.classList.add('visible');
      });
      return;
    }

    const observer = new IntersectionObserver(function (entries) {
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
      // If already loaded or fails, count it
      if (obj.contentDocument || obj.type === 'failed') {
        remaining -= 1;
      }
    });
    // Fallback timeout
    setTimeout(callback, 1500);
  }

  // Wait for DOM + SVGs before observing
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () {
      waitForSvgs(observeSections);
    });
  } else {
    waitForSvgs(observeSections);
  }

})();
