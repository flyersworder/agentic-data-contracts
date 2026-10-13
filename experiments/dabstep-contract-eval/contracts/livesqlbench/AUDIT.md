# Compile audit: livesqlbench

| database | entries | calculation | predicate | illustration | metrics | untranslated | prose | execution | queries | cost |
|---|---|---|---|---|---|---|---|---|---|---|
| archeology_scan | 54 | 20 | 24 | 10 | 21 | 23 | 10 | 48% | 41 | $1.27 |
| cold_chain_pharma_compliance | 60 | 17 | 25 | 18 | 7 | 35 | 18 | 17% | 40 | $1.28 |
| cross_border | 79 | 31 | 38 | 10 | 68 | 1 | 10 | 99% | 54 | $1.61 |
| crypto_exchange | 53 | 21 | 22 | 10 | 15 | 28 | 10 | 35% | 47 | $1.30 |
| cybermarket_pattern | 30 | 10 | 10 | 10 | 20 | 0 | 10 | 100% | 23 | $0.68 |
| disaster_relief | 58 | 25 | 23 | 10 | 46 | 2 | 10 | 96% | 62 | $1.35 |
| exchange_traded_funds | 89 | 43 | 36 | 10 | 74 | 5 | 10 | 94% | 77 | $2.30 |
| fake_account | 87 | 39 | 37 | 11 | 40 | 36 | 11 | 53% | 53 | $1.87 |
| households | 45 | 11 | 19 | 15 | 14 | 16 | 15 | 47% | 40 | $0.60 |
| hulushows | 92 | 45 | 36 | 11 | 66 | 15 | 11 | 81% | 94 | $1.88 |
| insider_trading | 74 | 31 | 32 | 11 | 55 | 8 | 11 | 87% | 56 | $1.58 |
| labor_certification_applications | 60 | 29 | 21 | 10 | 33 | 17 | 10 | 66% | 43 | $1.16 |
| mental_health | 96 | 43 | 43 | 10 | 66 | 20 | 10 | 77% | 84 | $2.63 |
| museum_artifact | 62 | 23 | 28 | 11 | 23 | 28 | 11 | 45% | 59 | $1.79 |
| organ_transplant | 55 | 22 | 23 | 10 | 28 | 17 | 10 | 62% | 31 | $1.23 |
| planets_data | 52 | 20 | 22 | 10 | 38 | 4 | 10 | 90% | 24 | $0.79 |
| polar_equipment | 58 | 21 | 28 | 9 | 38 | 11 | 9 | 78% | 63 | $1.88 |
| reverse_logistics | 30 | 10 | 10 | 10 | 8 | 12 | 10 | 40% | 11 | $0.47 |
| robot_fault_prediction | 65 | 23 | 28 | 14 | 38 | 13 | 14 | 75% | 39 | $1.20 |
| solar_panel | 50 | 20 | 20 | 10 | 34 | 6 | 10 | 85% | 16 | $0.74 |
| sports_events | 56 | 21 | 25 | 10 | 26 | 20 | 10 | 57% | 29 | $0.94 |
| virtual_idol | 64 | 19 | 27 | 18 | 36 | 10 | 18 | 78% | 36 | $1.33 |
| **total** | 1369 | 544 | 577 | 248 | 794 | 327 | 248 | 71% | 1022 | $29.90 |

## Sample: 30 metric(s), seed 2026

Hand check: 19 agree, 7 disagree, 4 unclear (reviewed independently, then spot-checked against the KB and the data; the compiler was frozen before this review and not changed after it).

### cybermarket_pattern / platform_liquidity_rate

Entry: Platform Liquidity Rate (calculation)

```text
Liquidity as a function of transaction flow and price.

PLR = \frac{\text{successful\_txns} \times \text{average\_price}}{\text{days\_active}}

Compiler note: PLR = successful_txns * average_price / days_active, computed at vendor level: successful_txns = vendors.CompletedTxns, average_price = average of product_availability->>'price_amt' over all product listings pointing to this vendor (products.SellerPointer), days_active = vendors.DaysActive. Checked that this does not exactly reproduce the stored vendor_compliance_ratings->>'liq_rate' value (ratios are not constant across vendors), so liq_rate appears to use a different/undocumented scaling; this expression encodes the formula as stated in the documentation.
```

Over `main.vendors`:

```sql
("CompletedTxns"::double * (SELECT AVG((p.product_availability->>'price_amt')::double) FROM products p WHERE p."SellerPointer" = vendors."SellerKey")) / "DaysActive"
```

Verdict: unclear -- note: KB 6 does not say whether average price is over listings or realized sales; the stored liq_rate does not settle it.

### hulushows / limited_series

Entry: Limited Series (predicate)

```text
Defines content with very few seasons, often a single story arc.

If a show consists of only a single part and no more than ten installments, it's called a limited series.

Built on: User Score Formats, Movie Identifier Formats.

Compiler note: Interpreted "single part" as Seasons_Total = 1 and "no more than ten installments" as Episode_Total <= 10, both from content_info.mediacounts.content_volumes.
```

Over `main.content_info`:

```sql
(mediacounts->'content_volumes'->>'Seasons_Total')::int = 1 AND (mediacounts->'content_volumes'->>'Episode_Total')::int <= 10
```

Verdict: agrees -- note: one season and at most ten episodes is the stated rule.

### mental_health / suicide_risk_prevalence_srp

Entry: Suicide Risk Prevalence (SRP) (calculation)

```text
Calculates the percentage of assessments indicating high or severe suicide risk at a facility.

SRP = rac{	ext{Number of assessments with a 'High' suicide risk}}{	ext{Total number of assessments}} 	imes 100 	ext{ for a given facility.}

Compiler note: SRP computed per facility: percent of assessments (joined via encounters.ab_ref = assessmentsymptomsandrisk.asr_key, encounters.fac_id = facilities.fac_key) with suic_risk='High' out of all assessments at that facility. Returns NULL if facility has no assessments to avoid division by zero.
```

Over `main.facilities`:

```sql
CASE WHEN (SELECT COUNT(*) FROM encounters e JOIN assessmentsymptomsandrisk r ON e.ab_ref = r.asr_key WHERE e.fac_id = facilities.fac_key) = 0 THEN NULL ELSE 100.0 * (SELECT COUNT(*) FROM encounters e JOIN assessmentsymptomsandrisk r ON e.ab_ref = r.asr_key WHERE e.fac_id = facilities.fac_key AND r.suic_risk = 'High') / (SELECT COUNT(*) FROM encounters e JOIN assessmentsymptomsandrisk r ON e.ab_ref = r.asr_key WHERE e.fac_id = facilities.fac_key) END
```

Verdict: unclear -- note: KB 3's formula counts High, its description says high or severe; both values occur.

### mental_health / well_resourced_high_support_environment

Entry: Well-Resourced High-Support Environment (predicate)

```text
Identifies facilities that are well-resourced and serve a patient population with generally high levels of social support effectiveness.

A facility where the Facility Resource Adequacy Index is 2.0 or greater and the average Social Support Effectiveness score of its patients is 4.5 or greater.

Built on: facility_resource_adequacy_index_frai, social_support_effectiveness_sse.

Compiler note: FRAI computed directly from facilities.com_res (single value per facility, so AVG is trivial). SSE averaged over distinct patient assessments (assessmentsocialanddiagnosis rows) linked to the facility via assessmentbasics.pat_owner_ref -> patients -> encounters.fac_id, deduplicated by asd_key to avoid double counting from multiple encounters per patient. Facilities with no com_res or no linked patients yield NULL and are excluded (not >= threshold).
```

Over `main.facilities`:

```sql
(CASE com_res WHEN 'Comprehensive' THEN 3 WHEN 'Extensive' THEN 3 WHEN 'Adequate' THEN 2 WHEN 'Limited' THEN 1 END) >= 2.0
AND (
  SELECT AVG(sse) FROM (
    SELECT DISTINCT asd.asd_key,
      (CASE asd.funcassess->>'Soc_Sup' WHEN 'Strong' THEN 3 WHEN 'Moderate' THEN 2 WHEN 'Limited' THEN 1 WHEN 'Weak' THEN 0 END) +
      (CASE asd.funcassess->>'Rel_Qual' WHEN 'Good' THEN 3 WHEN 'Fair' THEN 2 WHEN 'Poor' THEN 1 WHEN 'Conflicted' THEN 0 END) AS sse
    FROM assessmentsocialanddiagnosis asd
    JOIN assessmentbasics ab ON asd.asd_key = ab.ab_key
    JOIN patients p ON ab.pat_owner_ref = p.pat_key
    JOIN encounters e ON e.pat_ref = p.pat_key
    WHERE e.fac_id = facilities.fac_key
  ) t
) >= 4.5
```

Verdict: disagrees -- note: SSE scores Moderate=2 though KB 8 says Adequate=2, without saying so; averages assessments, not patients.

### robot_fault_prediction / average_joint_temperature

Entry: Average Joint Temperature (calculation)

```text
Calculates the average temperature across all of the robot's joints.

The mean temperature across all six joints (J1 to J6). Formula: (J1_temp + ... + J6_temp) / 6.

Compiler note: Averages temperature_C across J1-J6 extracted from the joint_health JSONB column.
```

Over `main.joint_condition`:

```sql
((joint_health->'J1'->>'temperature_C')::float + (joint_health->'J2'->>'temperature_C')::float + (joint_health->'J3'->>'temperature_C')::float + (joint_health->'J4'->>'temperature_C')::float + (joint_health->'J5'->>'temperature_C')::float + (joint_health->'J6'->>'temperature_C')::float) / 6
```

Verdict: agrees -- note: sum of the six documented joint temperatures over six.

### crypto_exchange / smart_money_flow

Entry: Smart Money Flow (predicate)

```text
Identifies directional bias of sophisticated traders.

Market conditions where smart-money force exceeds both retail-flow share and institutional-flow share by at least 20%, indicating strong directional bias from sophisticated traders.

Compiler note: Interpreted 'exceeds by at least 20%' as smart_money > retail_flow*1.2 AND smart_money > institutional_flow*1.2, consistent with the Flow Dominance definition's 'Smart Money Dominant' condition. Fields accessed via indicator_bundle->participant_flow->{smart_money,retail_flow,institutional_flow}, cast from jsonb to double precision. Rows with NULL retail_flow or institutional_flow yield NULL (excluded).
```

Over `main.analyticsindicators`:

```sql
(indicator_bundle->'participant_flow'->>'smart_money')::double precision > (indicator_bundle->'participant_flow'->>'retail_flow')::double precision * 1.2 AND (indicator_bundle->'participant_flow'->>'smart_money')::double precision > (indicator_bundle->'participant_flow'->>'institutional_flow')::double precision * 1.2
```

Verdict: disagrees -- note: the x1.2 comparison fails on negative flows: 9 rows pass with smart money below retail or institutional flow.

### exchange_traded_funds / positive_return_consistency

Entry: Positive Return Consistency (calculation)

```text
Calculates the percentage of a fund's historical years that have yielded positive returns.

	ext{Consistency} = rac{	ext{Number of years with positive returns}}{	ext{number of positive years with positive returns} + 	ext{number if years with negative returns}} 	imes 100

Compiler note: Uses histstats.Positive_Years and Negative_Years fields; division guarded against zero denominator with NULLIF.
```

Over `main.performance`:

```sql
(histstats->>'Positive_Years')::numeric / NULLIF((histstats->>'Positive_Years')::numeric + (histstats->>'Negative_Years')::numeric, 0) * 100
```

Verdict: agrees -- note: KB 7's positive/(positive+negative) percentage with a zero guard.

### polar_equipment / cabin_habitability_standard

Entry: Cabin Habitability Standard (predicate)

```text
Defines the minimum standards for habitable cabin conditions in polar environments.

A cabin meets 'Habitability Standards' when it maintains internal temperature (°C) between 18-24°C, oxygen level (%) above 19.5%, CO2 level (ppm) below 1000 ppm, functioning ventilation systems, and operational heating systems.

Compiler note: Used cabin_env_snapshot JSON fields: air_metrics.temperature_c, air_metrics.o2_pct, air_metrics.co2_ppm for the numeric thresholds; hvac.vent_state != 'Off' as 'functioning ventilation' and hvac.heater_state != 'Off' as 'operational heating'.
```

Over `main.CabinEnvironment`:

```sql
((cabin_env_snapshot->'air_metrics'->>'temperature_c')::real BETWEEN 18 AND 24)
AND ((cabin_env_snapshot->'air_metrics'->>'o2_pct')::real > 19.5)
AND ((cabin_env_snapshot->'air_metrics'->>'co2_ppm')::real < 1000)
AND ((cabin_env_snapshot->'hvac'->>'vent_state') != 'Off')
AND ((cabin_env_snapshot->'hvac'->>'heater_state') != 'Off')
```

Verdict: agrees -- note: thresholds and JSON paths match; observed HVAC states are On/Auto/Off.

### polar_equipment / polar_transportation_efficiency_coefficient_ptec

Entry: Polar Transportation Efficiency Coefficient (PTEC) (calculation)

```text
Measures vehicle transportation efficiency in polar conditions, considering vehicle performance and energy sustainability

PTEC = VPC \times (0.6 + 0.4 \times ESI \div 100)

Built on: energy_sustainability_index_esi, vehicle_performance_composite_vpc.

Compiler note: PTEC = VPC * (0.6 + 0.4*ESI/100). VPC computed inline from ground_vehicle_status JSON plus engine load via engine_link->EngineAndFluids. ESI computed via engine_link->batt_link->PowerBattery.battery_telemetry, using conversion_eff_pct as energy efficiency percent and primary_source as power source. Verified engine_link->batt_link->PowerBattery join is unique per chassis.
```

Over `main.ChassisAndVehicle`:

```sql
((1 - ((ground_vehicle_status->'brake_system'->>'pad_wear_pct')::numeric + (ground_vehicle_status->'tracks'->>'wear_pct')::numeric) / 200.0) * ((CAST(REPLACE(ground_vehicle_status->'vehicle_motion'->>'speed_kmh', ' m/s', '') AS numeric) * 3.6) / 50.0) * ((SELECT (e.engine_fluids_snapshot->'engine_core'->>'load_pct')::numeric FROM EngineAndFluids e WHERE e.ENGINE_ID = ChassisAndVehicle.engine_link) / 100.0)) * (0.6 + 0.4 * (SELECT (p.battery_telemetry->'power_state'->>'conversion_eff_pct')::double precision * CASE WHEN p.battery_telemetry->'power_state'->>'primary_source' IN ('Solar','Wind','Hybrid') THEN 1.5 WHEN p.battery_telemetry->'power_state'->>'primary_source' = 'Battery' THEN 1.0 WHEN p.battery_telemetry->'power_state'->>'primary_source' = 'Diesel' THEN 0.7 ELSE 0 END FROM EngineAndFluids e JOIN PowerBattery p ON p.PWR_BATT_ID = e.batt_link WHERE e.ENGINE_ID = ChassisAndVehicle.engine_link) / 100.0)
```

Verdict: agrees -- note: VPC, ESI and their combination match; speeds are stored in m/s, so x3.6 is needed.

### organ_transplant / panel_reactive_antibody_pra

Entry: Panel Reactive Antibody (PRA) (predicate)

```text
A test that measures the level of pre-existing antibodies in a recipient's blood against common HLA antigens, indicating their sensitization level.

A measure of a recipient's sensitization to foreign HLA antigens, represented by the `pra_score`[cite: 36]. For matching purposes, a recipient with a `pra_score` of 80 or higher is defined as having a 'high PRA score', indicating a state of high immunological sensitization.

Built on: human_leukocyte_antigen_hla_typing.

Compiler note: High PRA score defined as pra_score >= 80, per definition.
```

Over `main.recipients_immunology`:

```sql
pra_score >= 80
```

Verdict: agrees -- note: KB 23 states pra_score >= 80.

### labor_certification_applications / approval_rate_ar

Entry: Approval Rate (AR) (calculation)

```text
Calculates the percentage of certified applications out of total applications.

AR = (Number of Certified Applications / Total Number of Applications) × 100%. Application status is tracked as part of the application data.

Compiler note: statustag has inconsistent casing (Certified, certified, CERTIFIED); matched case-insensitively to compute the percentage of certified applications over all applications.
```

Over `main.cases`:

```sql
100.0 * SUM(CASE WHEN UPPER(statustag) = 'CERTIFIED' THEN 1 ELSE 0 END) / COUNT(*)
```

Verdict: agrees -- note: population-wide certified share with case normalization.

### planets_data / habitable_zone_outer_boundary

Entry: Habitable Zone Outer Boundary (calculation)

```text
Calculates the outer edge of a star's habitable zone, where a planet would be too cold for liquid water.

The outer boundary of the habitable zone ($r_o$) in AU is estimated based on the star's luminosity ($L_{\star}$): $r_o \approx \sqrt{L_{\star} / 0.53}$. This relies on the Stellar Luminosity.

Built on: stellar_luminosity.

Compiler note: Computes r_o = sqrt(L_star/0.53), reusing the stellar_luminosity formula inline from Radius_Value and Temp_Value JSON fields in stellarprops.
```

Over `main.stars`:

```sql
SQRT((POWER((stellarprops->'physical'->>'Radius_Value')::double precision, 2) * POWER((stellarprops->'physical'->>'Temp_Value')::double precision / 5778.0, 4)) / 0.53)
```

Verdict: agrees -- note: reuses stellar luminosity; units are solar radii and Kelvin.

### organ_transplant / expected_graft_survival_egs_score

Entry: Expected Graft Survival (EGS) Score (calculation)

```text
Predicts the probability of a transplanted organ functioning successfully over a specific period.

A predictive score based on factors like the Immunological Compatibility Score and donor age. Formula: $EGS = \frac{1}{1 + e^{-(-0.5 + 1.5 S_{immune} - 0.02 Age_{donor})}}$

Built on: immunological_compatibility_score.

Compiler note: EGS = 1/(1+exp(-(-0.5+1.5*S_immune-0.02*Age_donor))), reusing Immunological Compatibility Score formula from compatibility_metrics; donor age parsed as leading integer from demographics.age_count text (e.g. '57 years, mature donor' -> 57).
```

Over `main.compatibility_metrics`:

```sql
1.0 / (1.0 + exp(-(
  -0.5
  + 1.5 * (
      0.6 * (CASE WHEN (
          CASE
            WHEN left((SELECT d.blood_class FROM demographics d WHERE d.contrib_registry = compatibility_metrics.donor_ref_reg),2) = 'AB' THEN TRUE
            WHEN left((SELECT d.blood_class FROM demographics d WHERE d.contrib_registry = compatibility_metrics.donor_ref_reg),1) = 'O' THEN TRUE
            WHEN left((SELECT r.blood_class FROM recipients_demographics r WHERE r.recip_registry = compatibility_metrics.recip_ref_reg),2) = 'AB' THEN TRUE
            WHEN left((SELECT r.blood_class FROM recipients_demographics r WHERE r.recip_registry = compatibility_metrics.recip_ref_reg),1) =
                 left((SELECT d.blood_class FROM demographics d WHERE d.contrib_registry = compatibility_metrics.donor_ref_reg),1) THEN TRUE
            ELSE FALSE
          END
      ) THEN 1 ELSE 0 END)
      + 0.4 * (1 - (
          (CASE WHEN (SELECT h.hla_a_val FROM hla_info h WHERE h.immu_don_registry = compatibility_metrics.donor_ref_reg) <> (SELECT (r.hlaprofile->>'Hla_A_Val')::real FROM recipients_immunology r WHERE r.immu_recip_registry = compatibility_metrics.recip_ref_reg) THEN 1 ELSE 0 END)
        + (CASE WHEN (SELECT h.hla_b_val FROM hla_info h WHERE h.immu_don_registry = compatibility_metrics.donor_ref_reg) <> (SELECT (r.hlaprofile->>'Hla_B_Val')::real FROM recipients_immunology r WHERE r.immu_recip_registry = compatibility_metrics.recip_ref_reg) THEN 1 ELSE 0 END)
        + (CASE WHEN (SELECT h.hla_dr_val FROM hla_info h WHERE h.immu_don_registry = compatibility_metrics.donor_ref_reg) <> (SELECT (r.hlaprofile->>'Hla_Dr_Val')::real FROM recipients_immunology r WHERE r.immu_recip_registry = compatibility_metrics.recip_ref_reg) THEN 1 ELSE 0 END)
        ) / 6.0)
  )
  - 0.02 * (SELECT CAST(split_part(d.age_count, ' ', 1) AS INT) FROM demographics d WHERE d.contrib_registry = compatibility_metrics.donor_ref_reg)
)))
```

Verdict: disagrees -- note: the ABO parent treats AB as a universal donor (KB 20: AB is the universal recipient); a missing HLA lookup counts as no mismatch.

### sports_events / pole_based_race_win_probability

Entry: Pole-Based Race Win Probability (predicate)

```text
Defines the likelihood of a driver winning a race based on their starting position being pole.

Assume a race win probability of 35% if the driver started from Pole Position, and 5% otherwise.

Built on: pole_position, race_winner.

Compiler note: Encoded as a CASE expression over qualifying.px_pos assigning a probability value per row: 0.35 if px_pos=1 (Pole Position), else 0.05, per the entry's stated assumption.
```

Over `main.qualifying`:

```sql
CASE WHEN px_pos = 1 THEN 0.35 ELSE 0.05 END
```

Verdict: disagrees -- note: a probability compiled as a predicate: both branches are truthy, so all 2101 rows qualify, not the 86 poles.

### virtual_idol / professional_fan

Entry: Professional Fan (predicate)

```text
An adult fan who identifies with a professional occupation, representing a key demographic segment.

A fan is a Professional Fan if they are in the 'Adult' or 'Senior' Fan Age Category and their occupation path is 'Professional'.

Built on: Fan Age Category.

Compiler note: Adult/Senior = Age_Count >= 30 (covers both categories, upper bound unspecified). Occu_Path has random case noise, matched case-insensitively against 'professional'.
```

Over `main.fans`:

```sql
(demo_profile->>'Age_Count')::int >= 30 AND lower(demo_profile->>'Occu_Path') = 'professional'
```

Verdict: agrees -- note: KB 16's Adult and Senior categories are exactly age >= 30.

### mental_health / patient_with_high_crisis_low_support_profile

Entry: Patient with High Crisis & Low Support Profile (predicate)

```text
Identifies patients characterized by frequent crisis interventions and weak social support systems.

A patient with a total crisis intervention count greater than 2 AND a personal Social Support Effectiveness score of less than 3.

Built on: crisis_intervention_frequency_cif, social_support_effectiveness_sse.

Compiler note: Total crisis intervention count = sum of treatmentbasics.crisis_int over all encounters for the patient. Personal Social Support Effectiveness score (SSE) uses the defined Soc_Sup/Rel_Qual scoring; since a patient may have multiple assessment records (verified via assessmentbasics.pat_owner_ref grouping), the personal SSE is averaged across all their assessments.
```

Over `main.patients`:

```sql
(SELECT COALESCE(SUM(tb.crisis_int),0) FROM encounters e JOIN treatmentbasics tb ON tb.enc_ref = e.enc_key WHERE e.pat_ref = patients.pat_key) > 2
AND
(SELECT AVG((CASE asd.funcassess->>'Soc_Sup' WHEN 'Strong' THEN 3 WHEN 'Moderate' THEN 2 WHEN 'Limited' THEN 1 WHEN 'Weak' THEN 0 END) + (CASE asd.funcassess->>'Rel_Qual' WHEN 'Good' THEN 3 WHEN 'Fair' THEN 2 WHEN 'Poor' THEN 1 WHEN 'Conflicted' THEN 0 END))
 FROM assessmentsocialanddiagnosis asd JOIN assessmentbasics ab ON asd.asd_key = ab.ab_key WHERE ab.pat_owner_ref = patients.pat_key) < 3
```

Verdict: disagrees -- note: the same undisclosed Moderate-for-Adequate SSE scoring as the well-resourced metric.

### virtual_idol / event_enthusiast

Entry: Event Enthusiast (predicate)

```text
A fan who actively and frequently participates in a wide range of platform events, especially high-value ones like meet-and-greets.

A fan is an Event Enthusiast if their Event Participation Score (EPS) is greater than 25.

Built on: event_participation_score_eps.

Compiler note: Applies EPS formula directly over eventsandclub.evt_participation JSON fields; predicate is EPS > 25.
```

Over `main.eventsandclub`:

```sql
((evt_participation->>'On_Evt_Att')::numeric * 1 + (evt_participation->>'Off_Evt_Att')::numeric * 1.5 + (evt_participation->>'Meet_Att')::numeric * 2) > 25
```

Verdict: agrees -- note: KB 11's weights and the stated EPS > 25 threshold.

### virtual_idol / support_load_score_sls

Entry: Support Load Score (SLS) (calculation)

```text
Quantifies the amount of support resources a fan utilizes, based on their submission of tickets and issue reports.

SLS = \text{Support Tickets} + \text{Bug Reports} + \text{Technical Issue Reports}

Compiler note: SLS = Supp_Tix + Bug_Subs + Tech_Issue_Rpt, all fields from feedback_data jsonb in supportandfeedback.
```

Over `main.supportandfeedback`:

```sql
(feedback_data->>'Supp_Tix')::numeric + (feedback_data->>'Bug_Subs')::numeric + (feedback_data->>'Tech_Issue_Rpt')::numeric
```

Verdict: agrees -- note: sum of the three support counts KB 9 names.

### planets_data / planet_radius_in_earth_units

Entry: Planet Radius in Earth Units (calculation)

```text
Converts a planet's radius from Jupiter radius units to Earth radius units.

Given a planet's radius in Jupiter radii ($R_J$), its radius in Earth radii ($R_E$) is: $R_E = R_J \times 11.209$

Compiler note: Direct conversion of radjup (Jupiter radii) to Earth radii using factor 11.209.
```

Over `main.physical_properties`:

```sql
radjup * 11.209
```

Verdict: agrees -- note: Jupiter radii times KB 2's 11.209.

### labor_certification_applications / visa_class_distribution_vcd

Entry: Visa Class Distribution (VCD) (calculation)

```text
Measures the percentage distribution of different visa classifications.

VCD = (Number of Applications for Visa Class / Total Number of Applications) × 100%. Visa classification is tracked as part of the application data.

Compiler note: Computed as a per-row window function giving, for each case's visa class, the percentage of total applications that belong to that visa class (VCD = count of applications in that visacls / total applications * 100).
```

Over `main.cases`:

```sql
COUNT(*) OVER (PARTITION BY visacls) * 100.0 / COUNT(*) OVER ()
```

Verdict: agrees -- note: each class's share of all applications.

### exchange_traded_funds / total_value_score

Entry: Total Value Score (calculation)

```text
A holistic performance score combining risk-adjusted returns, cost-efficiency, and historical consistency.

	ext{TVS} = \sqrt[3]{	ext{Return on Cost} 	imes 	ext{Information Ratio} 	imes 	ext{Positive Return Consistency}}

Built on: return_on_cost_roc, information_ratio_simplified, positive_return_consistency.

Compiler note: TVS = cube root of (Return on Cost * Information Ratio (Simplified) * Positive Return Consistency). ROC uses performance.returnmetrics.fund_returns.Return_1Y divided by funds.fundmetrics.Expense_Net (matched via tickersym). Information Ratio uses risk_metrics.risk3y.Avg_Return_3Y minus performance.returnmetrics.benchmark_returns.Bench_Return_3Y, divided by risk_metrics.risk3y.Volatility_3Y (risk_metrics.investmentref matched to performance.productref, verified 1:1 unique join). Positive Return Consistency uses performance.histstats Positive_Years/(Positive_Years+Negative_Years)*100. Used cbrt() to handle potential negative products of the cube root as DuckDB's cbrt handles negative inputs correctly, unlike POWER with fractional exponent.
```

Over `main.performance`:

```sql
cbrt(
  ( (returnmetrics->'fund_returns'->>'Return_1Y')::double precision /
    (SELECT (f.fundmetrics->>'Expense_Net')::double precision FROM funds f WHERE f.tickersym = performance.productref) )
  *
  ( (SELECT (rm.risk3y->'risk_measures_3y'->>'Avg_Return_3Y')::double precision FROM risk_metrics rm WHERE rm.investmentref = performance.productref)
    -
    (returnmetrics->'benchmark_returns'->>'Bench_Return_3Y')::double precision )
  /
  (SELECT (rm.risk3y->'risk_measures_3y'->>'Volatility_3Y')::double precision FROM risk_metrics rm WHERE rm.investmentref = performance.productref)
  *
  ( (histstats->>'Positive_Years')::numeric / NULLIF((histstats->>'Positive_Years')::numeric + (histstats->>'Negative_Years')::numeric, 0) * 100 )
)
```

Verdict: agrees -- note: ROC, IR and consistency match KB 2, 6 and 7; lookups are unique.

### archeology_scan / environmental_condition_classification_system_eccs

Entry: Environmental Condition Classification System (ECCS) (predicate)

```text
A comprehensive classification system for archaeological site environments based on their suitability for scanning operations.

A four-tier classification where 'Optimal Scanning Conditions' have \text{ESI} > 85, 'Good Scanning Conditions' have ESI between 70-85, 'Acceptable Scanning Conditions' have ESI between 50-70, and 'Challenging Scanning Conditions' have ESI < 50. This classification guides scanning schedule planning and equipment selection to maximize data quality.

Built on: environmental_suitability_index_esi, optimal_scanning_conditions.

Compiler note: ECCS classifies rows of environment by ESI computed from ambient_cond JSON fields (Ambic_Temp, Hume_Pct, Illume_Lux). Tiers: >85 Optimal, [70,85] Good, [50,70) Acceptable, <50 Challenging, matching the boundary definitions given (treating 70 and 50 as inclusive lower bounds for Good/Acceptable per the 'between' wording).
```

Over `main.environment`:

```sql
CASE
  WHEN (100 - 2.5*ABS((ambient_cond->>'Ambic_Temp')::double - 20) - POWER(ABS(((ambient_cond->>'Hume_Pct')::double - 50)/2), 1.5) - 600.0/((ambient_cond->>'Illume_Lux')::double + 100)) > 85 THEN 'Optimal Scanning Conditions'
  WHEN (100 - 2.5*ABS((ambient_cond->>'Ambic_Temp')::double - 20) - POWER(ABS(((ambient_cond->>'Hume_Pct')::double - 50)/2), 1.5) - 600.0/((ambient_cond->>'Illume_Lux')::double + 100)) >= 70 THEN 'Good Scanning Conditions'
  WHEN (100 - 2.5*ABS((ambient_cond->>'Ambic_Temp')::double - 20) - POWER(ABS(((ambient_cond->>'Hume_Pct')::double - 50)/2), 1.5) - 600.0/((ambient_cond->>'Illume_Lux')::double + 100)) >= 50 THEN 'Acceptable Scanning Conditions'
  ELSE 'Challenging Scanning Conditions'
END
```

Verdict: unclear -- note: KB 50's Good and Acceptable ranges both include 70; the SQL gives 70 to Good. No row sits at 70.

### polar_equipment / life_support_reliability_classification_lsrc

Entry: Life Support Reliability Classification (LSRC) (predicate)

```text
Categorizes life support systems into reliability classes based on their LSSR score for operational decision-making.

Life support systems are classified into three reliability categories: 'High Reliability' WHEN (LSSR >= 0.8), 'Moderate Reliability' WHEN (LSSR >= 0.6 AND LSSR < 0.8), and 'Low Reliability' WHEN (LSSR < 0.6).

Built on: life_support_system_reliability_lssr.

Compiler note: LSRC built from LSSR formula (0.7*ORS + 0.3*TIE), joining OperationMaintenance.equipRef -> PowerBattery.equip_ref -> ThermalSolarWindAndGrid.batt_link, verified unique join. Thresholds: >=0.8 High, >=0.6 and <0.8 Moderate, else Low.
```

Over `main.OperationMaintenance`:

```sql
CASE
  WHEN (0.7 * (CASE
      WHEN "OPER_status" = 'Active' THEN 10 * (1 - "OPER_hours" / "maintCycleHrs")
      WHEN "OPER_status" = 'Standby' THEN 5 * (1 - "OPER_hours" / "maintCycleHrs")
      ELSE 0
    END) + 0.3 * (
      SELECT CASE t."insulationStat"
        WHEN 'Good' THEN 0.9 - t."heatLoss_kwh"/10
        WHEN 'Fair' THEN 0.6 - t."heatLoss_kwh"/10
        WHEN 'Poor' THEN 0.3 - t."heatLoss_kwh"/10
      END
      FROM "PowerBattery" pb
      JOIN "ThermalSolarWindAndGrid" t ON t."batt_link" = pb."PWR_BATT_ID"
      WHERE pb."equip_ref" = "OperationMaintenance"."equipRef"
    )) >= 0.8 THEN 'High Reliability'
  WHEN (0.7 * (CASE
      WHEN "OPER_status" = 'Active' THEN 10 * (1 - "OPER_hours" / "maintCycleHrs")
      WHEN "OPER_status" = 'Standby' THEN 5 * (1 - "OPER_hours" / "maintCycleHrs")
      ELSE 0
    END) + 0.3 * (
      SELECT CASE t."insulationStat"
        WHEN 'Good' THEN 0.9 - t."heatLoss_kwh"/10
        WHEN 'Fair' THEN 0.6 - t."heatLoss_kwh"/10
        WHEN 'Poor' THEN 0.3 - t."heatLoss_kwh"/10
      END
      FROM "PowerBattery" pb
      JOIN "ThermalSolarWindAndGrid" t ON t."batt_link" = pb."PWR_BATT_ID"
      WHERE pb."equip_ref" = "OperationMaintenance"."equipRef"
    )) >= 0.6 THEN 'Moderate Reliability'
  ELSE 'Low Reliability'
END
```

Verdict: disagrees -- note: ELSE 'Low Reliability' labels unknown LSSR as Low; KB 51 defines Low only as LSSR < 0.6.

### cross_border / sensitive_data_exposure

Entry: Sensitive Data Exposure (predicate)

```text
Highlights situations where sensitive information is not well-protected.

A case where data sensitivity index is above 100 and security robustness score is less than 2

Built on: data_sensitivity_index_dsi, security_robustness_score_srs.

Compiler note: DSI computed via scalar subquery joining DataProfile on unique profile_key FK (1:1, verified count matches); SRS computed inline from SecurityProfile.enc_state/acl_state per definition.
```

Over `main.SecurityProfile`:

```sql
(SELECT dp.vol_gb * CASE dp."dataSense" WHEN 'High' THEN 3 WHEN 'Medium' THEN 2 WHEN 'Low' THEN 1 ELSE NULL END FROM DataProfile dp WHERE dp.profileTrace = SecurityProfile.profile_key) > 100
AND
(CASE WHEN SecurityProfile.enc_state = 'Full' AND SecurityProfile.acl_state = 'Strong' THEN 3 WHEN SecurityProfile.enc_state = 'Full' OR SecurityProfile.acl_state = 'Strong' THEN 2 ELSE 1 END) < 2
```

Verdict: agrees -- note: DSI and SRS parents and thresholds match; profile lookups are unique.

### cybermarket_pattern / buyer_risk_dollar_ratio

Entry: Buyer Risk Dollar Ratio (calculation)

```text
Risk per dollar spent by a buyer.

BRDR = \frac{\text{risk\_score}}{\text{total\_spending\_usd}}

Built on: transaction_velocity_rate, anonymity_cost_index.

Compiler note: risk_dollar_ratio field already stores BRDR = risk_score/total_spending_usd as text like '0.0644 RiskScore/USD'; parsed numeric prefix.
```

Over `main.buyers`:

```sql
CAST(split_part(buyer_risk_profile->>'risk_dollar_ratio', ' ', 1) AS DOUBLE)
```

Verdict: unclear -- note: the stored ratio's column meaning says payment amount, KB 5 says total spending; the stored value is unverified.

### households / dwelling_type_score

Entry: Dwelling Type Score (calculation)

```text
Assigns a numerical score to different dwelling types based on a predefined quality ranking.

A scoring system where 'Brickwork house' receives 4 points, 'Apartment' receives 3 points, and all other types receive 1 point. This score is used in broader calculations like the Living Condition Score.

Built on: Dwelling Type.

Compiler note: Dwelling_Class values have inconsistent casing/whitespace/tab chars (e.g. 'Brickwork house', 'bRiCKwoRk housE', 'brick\thouse', 'Apt', 'ApARTMEnT'). Normalized by stripping non-letters and lowercasing: any variant matching brick...house pattern scores 4, apartment/apt scores 3, everything else (including 'other', 'shack...') scores 1, per entry definition (ignoring Condominium since not mentioned here).
```

Over `main.properties`:

```sql
CASE
  WHEN lower(regexp_replace(trim(dwelling_specs->>'Dwelling_Class'), '[^a-z]', '', 'g')) LIKE '%brick%house%' OR lower(regexp_replace(trim(dwelling_specs->>'Dwelling_Class'), '[^a-z]', '', 'g')) = 'brickworkhouse' THEN 4
  WHEN lower(regexp_replace(trim(dwelling_specs->>'Dwelling_Class'), '[^a-z]', '', 'g')) IN ('apartment','apt') THEN 3
  ELSE 1
END
```

Verdict: disagrees -- note: [^a-z] strips capitals before lower(), so 'Brickwork house' and 'Apartment' score 1.

### crypto_exchange / liquidation_risk_level

Entry: Liquidation Risk Level (predicate)

```text
Categorizes positions based on their proximity to liquidation.

Positions are categorized as 'Safe' or 'High Risk' based on how close the current market price is to the liquidation price. A position is 'High Risk' when liquidation price >= market price * 0.95.

Compiler note: liquidation_price stored as text with $ and commas, stripped and cast to double; market price taken as mark_price from marketdata joined via orders.exchSpot (unique match through ordStamp -> ORD_STAMP -> exchSpot -> EXCH_SPOT PK). Categorical label per definition: 'High Risk' if liquidation_price >= mark_price*0.95, else 'Safe'.
```

Over `main.riskandmargin`:

```sql
CASE WHEN CAST(REPLACE(REPLACE(margin_risk_profile->>'liquidation_price', '$', ''), ',', '') AS DOUBLE PRECISION) >= (SELECT CAST(md.orderbook_metrics->>'mark_price' AS DOUBLE PRECISION) FROM orders o JOIN marketdata md ON md."EXCH_SPOT" = o."exchSpot" WHERE o."ORD_STAMP" = riskandmargin."ordStamp") * 0.95 THEN 'High Risk' ELSE 'Safe' END
```

Verdict: agrees -- note: the entry's inequality and labels; lookups use unique keys.

### mental_health / annual_goal_achievement

Entry: Annual Goal Achievement (calculation)

```text
The projected number of recovery goals a patient is expected to achieve over a 12-month period, based on their current monthly velocity.

	ext{Annual Goals} = 	ext{Numeric value of recovery goal velocity} 	imes 12

Compiler note: recovery_goal_vel stored as text like '0.3 goals/month'; extracted the leading numeric value by splitting on space, then multiplied by 12 to annualize.
```

Over `main.assessmentbasics`:

```sql
CAST(split_part(recovery_goal_vel, ' ', 1) AS DOUBLE PRECISION) * 12
```

Verdict: agrees -- note: the goals/month prefix times 12, per KB 81.

### archeology_scan / premium_quality_scan

Entry: Premium Quality Scan (predicate)

```text
Defines the criteria for a premium quality archaeological scan suitable for conservation planning and scholarly publication.

A scan that is both a High Resolution Scan and has Comprehensive Coverage with \text{SQS} > 7.5, where SQS is the Scan Quality Score, producing data suitable for detailed analysis and conservation planning.

Built on: high_resolution_scan, comprehensive_coverage, scan_quality_score_sqs.

Compiler note: Combines High Resolution Scan (Scan_Resol_Mm<=1.0 and Point_Dense>=1000), Comprehensive Coverage (Cover_Pct>=95 and Lap_Pct>=30), and SQS>7.5 computed from cloud_metrics fields.
```

Over `main.pointcloud`:

```sql
(cloud_metrics->>'Scan_Resol_Mm')::numeric <= 1.0 AND (cloud_metrics->>'Point_Dense')::numeric >= 1000 AND (cloud_metrics->>'Cover_Pct')::numeric >= 95 AND (cloud_metrics->>'Lap_Pct')::numeric >= 30 AND POWER(10.0 / (LOG(10, (cloud_metrics->>'Scan_Resol_Mm')::numeric * 1000) / LOG(10, (cloud_metrics->>'Point_Dense')::numeric) * 5), 1.5) * (((cloud_metrics->>'Cover_Pct')::numeric * (1 + ((cloud_metrics->>'Lap_Pct')::numeric/100) * (1 - (cloud_metrics->>'Cover_Pct')::numeric/100))) / 100) * POWER(1 - (cloud_metrics->>'Noise_Db')::numeric / 30, 2) > 7.5
```

Verdict: agrees -- note: SRI, SCE and the parent formulas match KB 10, 11 and 3.

### solar_panel / critical_mechanical_health

Entry: Critical Mechanical Health (predicate)

```text
Indicates that an asset is suffering from severe and concurrent mechanical failures.

A state of critical mechanical health is reached when there is an active Structural Integrity Warning and a simultaneous Tracker Malfunction.

Built on: structural_integrity_warning, tracker_malfunction.

Compiler note: Combines Structural Integrity Warning and Tracker Malfunction predicates via AND, both evaluated on mech_health_snapshot JSONB fields.
```

Over `main.mechanical_condition`:

```sql
(mech_health_snapshot->'mount_cleaning'->>'mount_structure_status' IN ('Unstable', 'Check Required')) AND (mech_health_snapshot->'tracker'->>'tracker_state' IN ('Error', 'Maintenance'))
```

Verdict: agrees -- note: KB 23's structural warning AND KB 21's tracker malfunction.


## Refusals

All 206 untranslated entries in the 16 sampled databases were reviewed as well. 184 refusals stand: the KB leaves a mapping, weight, threshold or pairing undefined, the schema lacks the input, or the entry is a report or a write rather than a value or condition. 22 are over-strict: a faithful expression exists that the compiler missed.

| database | over-strict entry ids | what was missed |
|---|---|---|
| archeology_scan | 16, 33, 41, 44 | a composite key (arcref, crewref) that is unique; a conjunct that is false for every row; an existing-scan prerequisite the KB does not state |
| crypto_exchange | 5, 8, 16 | another source table where the lookup is one-to-one |
| exchange_traded_funds | 82, 87 | a population-wide aggregate (regression slope); a top sector per family with no ties |
| hulushows | 54 | an aggregate over six named boolean fields |
| labor_certification_applications | 47, 59 | explicit status labels; a dataset-level category |
| organ_transplant | 12 | a documented stored USD/QALY column |
| planets_data | 17 | a window over planets of the same host |
| polar_equipment | 10, 36, 40, 49, 50 | a unique weather -> maintenance -> equipment chain; null links were read as non-unique joins |
| sports_events | 25, 35 | a stored constructor race total; correlated per-season averages |
| virtual_idol | 63 | a list-valued calculation |

Most over-strict refusals follow from one rule: the compiler reads a non-unique join or a needed aggregate as a reason to refuse. The usual fix is a different source table or a key it did not check.
