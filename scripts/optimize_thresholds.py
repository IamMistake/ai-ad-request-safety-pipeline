import json, re, sys, time
from collections import defaultdict

FLINK_LOG = sys.argv[1] if len(sys.argv) > 1 else "/tmp/flink_output.log"
LABELS = "datasets/labeled_requests/train.jsonl"

print(f"Reading: {FLINK_LOG}")

pat = re.compile(r'\[flink-fraud\] (CLEAN|SUSPICIOUS|FRAUD) req_id=(\S+) score=([\d.]+) reasons=(.*?) -> (\S+)')
events_raw = []
for line in open(FLINK_LOG):
    m = pat.search(line)
    if m:
        events_raw.append((m.group(2), json.loads(m.group(4))))

labels = {}
with open(LABELS) as f:
    for line in f:
        row = json.loads(line)
        labels[row["event"]["req_id"]] = row

# Reason names in fixed order
R = [
    "publisher_new_ip",       # 0
    "publisher_new_session",  # 1
    "publisher_dispersed_farm",# 2
    "prompt_replay",          # 3
    "publisher_suspicious_rate",# 4
    "session_burst",          # 5
    "publisher_burst_volume", # 6
    "regular_cadence",        # 7
    "publisher_burst",        # 8
    "session_ip_churn",       # 9
    "session_asn_churn",      # 10
    "geo_language_mismatch",  # 11
    "session_country_hop",    # 12
    "negative_prompt",        # 13
]
ridx = {r:i for i,r in enumerate(R)}
NR = len(R)

# Group events by reason signature → (fraud_count, clean_count)
# Signature = tuple of 0/1 for each reason
sig_counts = defaultdict(lambda: [0, 0])  # [fraud, clean]
for req_id, reasons in events_raw:
    row = labels.get(req_id)
    if row is None: continue
    sig = tuple(1 if r in ridx else 0 for r in R)  # wrong — need to check if reason is in the event's reason list
    # Actually I need to build the signature from the reasons list
    sig = [0]*NR
    for r in reasons:
        idx = ridx.get(r)
        if idx is not None:
            sig[idx] = 1
    sig_counts[tuple(sig)][row["is_fraud"]] += 1

clean_total = sum(c[0] for c in sig_counts.values())
fraud_total = sum(c[1] for c in sig_counts.values())
print(f"Events: {clean_total + fraud_total} "
      f"(clean={clean_total}, fraud={fraud_total})")
print(f"Unique reason signatures: {len(sig_counts)}")

# Baseline scores
BASE = [0.05, 0.03, 0.45, 0.15, 0.15, 0.40, 0.35, 0.30, 0.50, 0.40, 0.40, 0.15, 0.50, 0.15]

def evaluate_fast(scores, sus_thr, fraud_thr, sig_counts):
    TP=FP=FN=TN=sus_f=sus_c=0
    for sig, (clean_cnt, fraud_cnt) in sig_counts.items():
        s = sum(sig[i] * scores[i] for i in range(NR))
        if s >= fraud_thr:
            TP += fraud_cnt
            FP += clean_cnt
        elif s >= sus_thr:
            sus_f += fraud_cnt
            sus_c += clean_cnt
        else:
            FN += fraud_cnt
            TN += clean_cnt
    ft=TP+FN+sus_f; ct=TN+FP+sus_c
    tpr=TP/ft if ft else 0; fpr=FP/ct if ct else 0
    prec=TP/(TP+FP) if TP+FP else 0
    f1=2*TP/(2*TP+FP+FN) if 2*TP+FP+FN else 0
    return TP, FP, FN, sus_f, sus_c, tpr, fpr, prec, f1

print("\n=== BASELINE ===")
r = evaluate_fast(BASE, 0.5, 0.7, sig_counts)
print(f"FRAUD=0.70 SUS=0.50  TP={r[0]} FP={r[1]} FN={r[2]} F->S={r[3]} C->S={r[4]}  "
      f"TPR={r[5]:.1%} FPR={r[6]:.2%} PREC={r[7]:.1%} F1={r[8]:.1%}")

# Sweep
I_GEO=11; I_SR=4; I_PR=3; I_RC=7; I_DIS=2; I_PBV=6

results = []
start_t = time.time()

for geo in [0.15, 0.25, 0.35, 0.50]:
 for sr in [0.15, 0.20, 0.25, 0.35]:
  for pr in [0.15, 0.25, 0.35]:
   for rc in [0.30, 0.40, 0.50]:
    for dis in [0.45, 0.30, 0.20, 0.10]:
     for pbv in [0.35, 0.40, 0.50]:
        scores = list(BASE)
        scores[I_GEO]=geo; scores[I_SR]=sr; scores[I_PR]=pr
        scores[I_RC]=rc; scores[I_DIS]=dis; scores[I_PBV]=pbv
        for st in [0.35, 0.40, 0.45]:
         for ft in [0.50, 0.55, 0.60, 0.65, 0.70]:
            if st >= ft: continue
            TP, FP, FN, sus_f, sus_c, tpr, fpr, prec, f1 = evaluate_fast(
                scores, st, ft, sig_counts)
            results.append((tpr, FP, sus_c, f1, geo, sr, pr, rc, dis, pbv, st, ft, TP, sus_f))

elapsed = time.time() - start_t
print(f"\nSwept {len(results)} combos in {elapsed:.2f}s")

results.sort(key=lambda x: (-x[0], x[1], x[2]))

print(f"\n{'Geo':>5} {'SusR':>5} {'ProR':>5} {'RegC':>5} {'DispF':>6} {'BurstV':>6} "
      f"{'SUSth':>5} {'FRAUDth':>7} {'TP':>5} {'FP':>5} {'F->S':>5} "
      f"{'C->S':>6} {'TPR':>5} {'PREC':>5} {'F1':>5}")
print("-"*95)

for r in results[:40]:
    tpr, FP, sus_c, f1, geo, sr, pr, rc, dis, pbv, st, ft, TP, sus_f = r
    prec = TP/(TP+FP)*100 if TP+FP else 0
    print(f"{geo:>4.2f} {sr:>4.2f} {pr:>4.2f} {rc:>4.2f} {dis:>5.2f} {pbv:>5.2f} "
          f"{st:>4.2f} {ft:>6.2f} {TP:>5} {FP:>5} {sus_f:>5} "
          f"{sus_c:>6} {tpr:>4.1%} {prec:>4.1f}% {f1:>4.1f}%")

print(f"\n=== CONFIGS NEAR TARGET (TPR>=65%, FP<3000, C->S<12000) ===")
print(f"{'Geo':>5} {'SusR':>5} {'ProR':>5} {'RegC':>5} {'DispF':>6} {'BurstV':>6} "
      f"{'SUSth':>5} {'FRAUDth':>7} {'TP':>5} {'FP':>5} {'F->S':>5} {'C->S':>6} {'TPR':>5}")
print("-"*85)
count = 0
for r in results:
    tpr, FP, sus_c, f1, geo, sr, pr, rc, dis, pbv, st, ft, TP, sus_f = r
    if tpr >= 0.65 and FP < 3000 and sus_c < 12000:
        print(f"{geo:>4.2f} {sr:>4.2f} {pr:>4.2f} {rc:>4.2f} {dis:>5.2f} {pbv:>5.2f} "
              f"{st:>4.2f} {ft:>6.2f} {TP:>5} {FP:>5} {sus_f:>5} {sus_c:>6} {tpr:>4.1%}")
        count += 1
if count == 0:
    print("(none — lowering threshold)")
    for r in results:
        tpr, FP, sus_c, f1, geo, sr, pr, rc, dis, pbv, st, ft, TP, sus_f = r
        if tpr >= 0.60 and FP < 5000 and sus_c < 15000:
            print(f"{geo:>4.2f} {sr:>4.2f} {pr:>4.2f} {rc:>4.2f} {dis:>5.2f} {pbv:>5.2f} "
                  f"{st:>4.2f} {ft:>6.2f} {TP:>5} {FP:>5} {sus_f:>5} {sus_c:>6} {tpr:>4.1%}")
            count += 1
            if count >= 10: break

print("\nDone.")
