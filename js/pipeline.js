// ============================================================
// Pipeline diagram — click component legend to show detail
// ============================================================

(function () {
  'use strict';

  var detailEl = document.getElementById('component-detail');
  if (!detailEl) return;

  var components = {
    simulator: {
      title: 'Request Simulator',
      desc: 'Generates synthetic ad request traffic from the WildChat dataset (175k real GPT conversations). Each request is enriched with session context, user agent, IP geolocation (GeoLite2), and ASN. Supports normal and fraud traffic profiles for controlled experiments.',
      file: 'kafka/producers/request_simulator.py'
    },
    kafka: {
      title: 'Kafka Topics',
      desc: 'Five topics decouple all pipeline stages: <code>requests.raw</code> (ingress), <code>requests.sus</code> (suspicious awaiting RFC scoring), <code>requests.clean</code> (fraud-clean for moderation), <code>requests.fraud</code> (blocked events), and <code>ad.injection</code> (approved requests). Single-broker dev mode via Docker Compose.',
      file: 'docker-compose.yml'
    },
    flink: {
      title: 'Flink Fraud Detection',
      desc: 'Real-time stream processor consuming <code>requests.raw</code>. Assigns event-time watermarks, then keys the stream through three detectors: UserDetector (IP burst) &rarr; SessionDetector (6 rules: burst, IP churn, country hop, ASN churn, prompt replay, regular cadence) &rarr; PublisherDetector (3 rules: burst, suspicious rate, bad UA rate). Plus 4 stateless rules. Routes by score threshold.',
      file: 'flink_service/fraud_detection.py',
      rules: '14 rules total'
    },
    rfc: {
      title: 'RFC Scoring Service',
      desc: 'Planned service that consumes <code>requests.sus</code> and applies the Spark-trained RandomForest model to score suspicious requests. Not yet implemented. Cleared requests go to <code>requests.clean</code>; confirmed fraud goes to <code>requests.fraud</code>.',
      planned: true
    },
    moderation: {
      title: 'Moderation Service',
      desc: 'Consumes <code>requests.clean</code> and analyzes prompt content for safety. Supports two modes: <strong>mock</strong> (rule-based keyword analysis, TF-IDF gating) and <strong>OpenAI</strong> (calls <code>omni-moderation-latest</code> API). Prompt caching via SHA256 hash. Routes clean to <code>ad.injection</code>, unsafe to <code>requests.fraud</code>.',
      file: 'pipeline_consumers/moderation_consumer.py'
    },
    spark: {
      title: 'Spark Analytics & ML',
      desc: 'Batch analytics engine that reads historical request logs, computes risk rollups (per IP, publisher, ASN, session), and trains a RandomForestClassifier (100 estimators) on features: scam keyword presence, ASN, and prior fraud score. Writes serialized model + metrics for the RFC scoring service.',
      file: 'spark_service/spark_training.py'
    }
  };

  var legendButtons = document.querySelectorAll('.legend-item');

  legendButtons.forEach(function (btn) {
    btn.addEventListener('click', function () {
      var comp = this.getAttribute('data-component');
      var data = components[comp];
      if (!data) {
        detailEl.innerHTML = '<p class="component-detail-empty">No details available.</p>';
        return;
      }

      var html = '';
      if (data.planned) {
        html += '<span class="badge badge-planned" style="margin-bottom:12px;display:inline-block">Planned</span>';
      }
      html += '<h3 style="font-size:1.05rem;font-weight:700;margin-bottom:8px;color:var(--rm-ink)">' + data.title + '</h3>';
      html += '<p style="color:var(--rm-sub);font-size:0.9rem;line-height:1.7">' + data.desc + '</p>';
      if (data.file) {
        html += '<p style="margin-top:10px;font-size:0.82rem;color:#94a3b8">';
        html += '<code style="font-family:var(--font-mono)">' + data.file + '</code>';
        html += '</p>';
      }
      if (data.rules) {
        html += '<p style="margin-top:4px;font-size:0.82rem;color:var(--rm-primary-soft);font-weight:600">' + data.rules + '</p>';
      }
      detailEl.innerHTML = html;
    });
  });

})();
