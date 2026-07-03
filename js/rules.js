// ============================================================
// Rule Explorer — toggle rules on/off, see score update
// ============================================================

(function () {
  'use strict';

  var rulesData = {
    stateless: [
      { name: 'Negative Prompt', reason: 'negative_prompt', score: 0.2 },
      { name: 'Bad User Agent', reason: 'bad_user_agent', score: 0.2 },
      { name: 'ASN Risk', reason: 'asn_risk', score: 0.2 },
      { name: 'Language/Country Mismatch', reason: 'language_mismatch_country', score: 0.1 }
    ],
    user: [
      { name: 'IP Burst', reason: 'ip_burst', score: 0.6 }
    ],
    session: [
      { name: 'Session Burst', reason: 'session_burst', score: 0.4 },
      { name: 'Session IP Churn', reason: 'session_ip_churn', score: 0.4 },
      { name: 'Session Country Hop', reason: 'session_country_hop', score: 0.5 },
      { name: 'Session ASN Churn', reason: 'session_asn_churn', score: 0.4 },
      { name: 'Prompt Replay', reason: 'prompt_replay', score: 0.4 },
      { name: 'Regular Cadence', reason: 'regular_cadence', score: 0.3 }
    ],
    publisher: [
      { name: 'Publisher Burst', reason: 'publisher_burst', score: 0.3 },
      { name: 'Publisher Suspicious Rate', reason: 'publisher_suspicious_rate', score: 0.3 },
      { name: 'Publisher Bad UA Rate', reason: 'publisher_bad_ua_rate', score: 0.3 }
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
        activeRules[key] = true;

        var toggle = document.createElement('div');
        toggle.className = 'rule-toggle active';
        toggle.setAttribute('role', 'checkbox');
        toggle.setAttribute('aria-checked', 'true');
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
