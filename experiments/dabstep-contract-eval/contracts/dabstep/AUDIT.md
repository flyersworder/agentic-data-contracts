# Compile audit: dabstep

| database | entries | section | metrics | untranslated | prose | execution | queries | cost |
|---|---|---|---|---|---|---|---|---|
| dabstep | 23 | 23 | 3 | 0 | 20 | - | 2 | $0.20 |
| **total** | 23 | 23 | 3 | 0 | 20 | - | 2 | $0.20 |

## Sample: 3 metric(s), seed 2026

Hand check: 3 agree, 0 disagree, 0 unclear (reviewed independently against the manual).

### dabstep / fraud_rate

Entry: 7 Reducing Fraud-Related Fees (section)

```text
Fraud rate defined as the ratio of fraudulent volume (sum of eur_amount for transactions with has_fraudulent_dispute = true) over total volume (sum of eur_amount for all transactions).

From the manual section "7 Reducing Fraud-Related Fees".
```

Over `main.payments`:

```sql
SUM(CASE WHEN has_fraudulent_dispute THEN eur_amount ELSE 0 END) / SUM(eur_amount)
```

Verdict: agrees -- note: manual section 7: fraudulent volume over total volume.

### dabstep / is_intracountry_transaction

Entry: 5 Understanding Payment Processing Fees (section)

```text
A transaction is intracountry (domestic) if the issuing country equals the acquiring country; otherwise it is international/cross-border.

From the manual section "5 Understanding Payment Processing Fees".
```

Over `main.payments`:

```sql
issuing_country = acquirer_country
```

Verdict: agrees -- note: manual section 5: issuer country equals acquirer country.

### dabstep / is_local_acquiring

Entry: 5.1.1 Optimizing Transactions through Local Acquiring (section)

```text
A transaction is considered local acquiring when the card-issuing country matches the acquiring bank's country.

From the manual section "5.1.1 Optimizing Transactions through Local Acquiring".
```

Over `main.payments`:

```sql
issuing_country = acquirer_country
```

Verdict: agrees -- note: manual section 5.1.1: the same country equality.

