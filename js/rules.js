// ============================================================
// Rule Explorer — toggle rules on/off, see score update
// ============================================================

(function () {
  'use strict';

  var rulesData = {
    stateless: [
      { name: 'Negative Prompt', reason: 'negative_prompt', score: 0.15 },
      { name: 'Bad User Agent', reason: 'bad_user_agent', score: 0.30 },
      { name: 'ASN Risk', reason: 'asn_risk', score: 0.20 },
      { name: 'Geo-Language Mismatch', reason: 'geo_language_mismatch', score: 0.45 }
    ],
    user: [
      { name: 'IP Burst', reason: 'ip_burst', score: 0.35 }
    ],
    session: [
      { name: 'Session Burst', reason: 'session_burst', score: 0.40 },
      { name: 'Session IP Churn', reason: 'session_ip_churn', score: 0.40 },
      { name: 'Session UA Churn', reason: 'session_ua_churn', score: 0.35 },
      { name: 'Session Country Hop', reason: 'session_country_hop', score: 0.50 },
      { name: 'Session ASN Churn', reason: 'session_asn_churn', score: 0.40 },
      { name: 'Prompt Replay', reason: 'prompt_replay', score: 0.45 },
      { name: 'Regular Cadence', reason: 'regular_cadence', score: 0.25 }
    ],
    publisher: [
      { name: 'Publisher Burst', reason: 'publisher_burst', score: 0.50 },
      { name: 'Publisher Burst Volume', reason: 'publisher_burst_volume', score: 0.35 },
      { name: 'Publisher Suspicious Rate', reason: 'publisher_suspicious_rate', score: 0.25 },
      { name: 'Publisher Bad UA Rate', reason: 'publisher_bad_ua_rate', score: 0.30 },
      { name: 'Publisher Dispersed Farm', reason: 'publisher_dispersed_farm', score: 0.20 },
      { name: 'Publisher Prompt Replay', reason: 'publisher_prompt_replay', score: 0.10 },
      { name: 'Publisher Geo Diversity', reason: 'publisher_geo_diversity', score: 0.25 },
      { name: 'Publisher UA Rotation', reason: 'publisher_ua_rotation', score: 0.20 },
      { name: 'Publisher Slow Prompt Replay', reason: 'publisher_slow_prompt_replay', score: 0.10 }
    ]
  };

  var activeRules = {};
  var totalScoreEl = document.getElementById('total-score-value');
  var verdictEl = document.getElementById('rules-verdict');

  function renderRules() {
    Object.keys(rulesData).forEach(function (scope) {
      var container = document.getElementById('rules-' + scope);
      if (!container) return;

      rulesData[scope].forEach(function (rule) {
        var key = rule.reason;
        activeRules[key] = false;

        var toggle = document.createElement('div');
        toggle.className = 'rule-toggle';
        toggle.setAttribute('role', 'checkbox');
        toggle.setAttribute('aria-checked', 'false');
        toggle.setAttribute('tabindex', '0');
        toggle.dataset.key = key;
        toggle.dataset.score = rule.score;

        var checkbox = document.createElement('span');
        checkbox.className = 'rule-checkbox';

        var label = document.createElement('span');
        label.textContent = rule.name;

        var scoreSpan = document.createElement('span');
        scoreSpan.className = 'rule-score';
        scoreSpan.textContent = '+' + rule.score.toFixed(1);

        toggle.appendChild(checkbox);
        toggle.appendChild(label);
        toggle.appendChild(scoreSpan);

        toggle.addEventListener('click', function () {
          var isActive = this.classList.toggle('active');
          this.setAttribute('aria-checked', isActive);
          activeRules[this.dataset.key] = isActive;
          updateScore();
        });

        toggle.addEventListener('keydown', function (e) {
          if (e.key === 'Enter' || e.key === ' ') {
            e.preventDefault();
            this.click();
          }
        });

        container.appendChild(toggle);
      });
    });
  }

  function updateScore() {
    var total = 0;
    Object.keys(rulesData).forEach(function (scope) {
      rulesData[scope].forEach(function (rule) {
        if (activeRules[rule.reason]) {
          total += rule.score;
        }
      });
    });
    total = Math.round(total * 100) / 100;

    totalScoreEl.textContent = total.toFixed(1);

    // Color
    totalScoreEl.className = 'rules-total-value';
    if (total >= 0.8) totalScoreEl.classList.add('high');
    else if (total >= 0.5) totalScoreEl.classList.add('medium');

    // Verdict
    var badge = verdictEl.querySelector('.verdict-badge');
    if (!badge) {
      badge = document.createElement('span');
      badge.className = 'verdict-badge';
      verdictEl.appendChild(badge);
    }

    badge.className = 'verdict-badge';
    if (total >= 0.8) {
      badge.textContent = 'Fraud';
      badge.classList.add('fraud');
    } else if (total >= 0.5) {
      badge.textContent = 'Suspicious';
      badge.classList.add('suspicious');
    } else {
      badge.textContent = 'Clean';
    }
  }

  renderRules();
  updateScore();

})();
