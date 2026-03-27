# DDoS Detection Demo Script

## Overview

This demo shows how Timeplus builds a real-time DDoS detection pipeline using streaming SQL — from raw Cisco ASA firewall logs to automated alerting — with no external tooling, no batch jobs, and no fixed thresholds.

**Duration**: ~5 minutes  
**Key message**: Timeplus turns streaming firewall logs into dynamic, self-adjusting DDoS alerts using pure SQL.

**Pre-requisite**: All streams, materialized views, baselines, and the alert are already created and running. The attack MV is paused by default. Normal traffic at 5 EPS has been running long enough to establish baselines.

---

## Act 1 — Show the Pipeline (1 min)

**Talking point**: Walk through the architecture — 4 stages, all streaming SQL, no batch jobs.

```
Raw ASA Logs → Live 5s HOP Window → Mutable Baselines → Spike Detection → Alert
```

### Show the baseline has been learned

```sql
SELECT * FROM table(cisco_observability_ddos.sig_overall_baseline_mut)
WHERE src_ip = '203.0.113.66';

SELECT * FROM table(cisco_observability_ddos.sig_hourly_baseline_mut)
WHERE src_ip = '203.0.113.66';
```

**Talking point**: The mutable stream holds exactly one row per key, continuously updated in place. This IP has been sending normal traffic at 5 EPS — the baseline reflects that.

### Confirm everything is calm

```sql
SELECT src_ip, live_bytes, overall_spike_ratio, hourly_spike_ratio
FROM cisco_observability_ddos.cxt_ddos_stream
WHERE src_ip = '203.0.113.66';
```

**Talking point**: Spike ratios are hovering around 1.0× — traffic matches the baseline. No alerts firing.

---

## Act 2 — Launch the Attack (1 min)

### Resume the attack MV — this is the only command needed

```sql
SYSTEM RESUME MATERIALIZED VIEW cisco_asa_simulator.mv_asa_logs_attack;
```

**Talking point**: One command. We just went from 5 EPS to 505 EPS for this IP — a 100× traffic spike. The pipeline should detect this within seconds.

### Watch the spike ratios explode

```sql
SELECT src_ip, live_bytes, overall_spike_ratio, hourly_spike_ratio
FROM cisco_observability_ddos.cxt_ddos_stream
WHERE src_ip = '203.0.113.66';
```

**Talking point**: Watch the spike ratios jump well above 10×. Point out that both overall and hourly ratios are elevated — they may differ slightly since the hourly baseline only reflects traffic from this hour of the day.

---

## Act 3 — Show the Alert (1 min)

### Open the alert viewer

Open `http://34.168.13.2/` in the browser.

**Talking point**: The alert fired within seconds of the attack starting. The message includes the source IP, human-readable traffic volume, both spike ratios with visual indicators showing which thresholds were exceeded, and a timestamp. Rate-limited to 1 per 30 seconds so we don't flood the ops channel.

---

## Act 4 — Stop the Attack & Observe Recovery (1 min)

### Pause the attack MV

```sql
SYSTEM PAUSE MATERIALIZED VIEW cisco_asa_simulator.mv_asa_logs_attack;
```

### Watch ratios return to normal

```sql
SELECT src_ip, live_bytes, overall_spike_ratio, hourly_spike_ratio
FROM cisco_observability_ddos.cxt_ddos_stream
WHERE src_ip = '203.0.113.66';
```

**Talking point**: Within seconds, the spike ratios drop back toward 1.0×. The alerts stop. No manual intervention, no reset — the pipeline is self-adjusting. The baseline also gradually absorbs the attack period, making the system slightly more tolerant of similar spikes in the future — a natural damping effect.

---

## Act 5 — Wrap Up (1 min)

### Key takeaways

1. **Pure SQL, no external tooling** — the entire pipeline from raw logs to alerts is streaming SQL
2. **Dynamic thresholds** — each IP is measured against its own history, not a static number
3. **Two baselines** — overall average catches global anomalies; hourly baseline catches time-of-day patterns
4. **Mutable streams as dimension tables** — continuously updated baselines without unbounded storage growth
5. **Sub-second detection** — from traffic spike to webhook alert in under 5 seconds
6. **Self-recovering** — when the attack stops, ratios drop automatically

### The entire attack simulation was controlled by two commands

```sql
-- Start attack
SYSTEM RESUME MATERIALIZED VIEW cisco_asa_simulator.mv_asa_logs_attack;

-- Stop attack
SYSTEM PAUSE MATERIALIZED VIEW cisco_asa_simulator.mv_asa_logs_attack;
```