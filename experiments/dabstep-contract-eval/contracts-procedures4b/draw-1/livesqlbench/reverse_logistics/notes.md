When several charge rules match a return, the contract applies the matching rule with the LOWEST rule_id (first rule in the schedule). The documentation specifies no tie-break. In this data, 1,188 returns match more than one rule, up to nine, and matching rules usually have different charges.

For all five text criteria in the Return Handling Charge Schedule, the literal 'ANY' means “no restriction” as well as NULL. No text criterion is NULL in this data; the bound columns do use NULL. A specific criterion does not match a missing return value; customers.seg_category is NULL for 93 of 206 customers. The schedule has 400 rules, and 91 of 1,500 returns take the Standard Handling Charge. The contract reports a total RHC over all returns of 109208.30.

Only 995 of the 1,500 returns have a financial_management record, one per casetag. The contract joins that record to the return by casetag and casenum and computes TRC, RPI, SAL and RCP only for returns with a financial record. Recovery Rate per Day likewise uses returns with a financial record. For Disposal Cost, it reads the disposal_cost entry in financial_management.cost_breakdown, not financial_management.dispcost, which is 0 on every row.

The cost-component storage is specified as follows: Shipping Fee is the shipping.fee entry in returns.return_details and is never NULL; Restocking Fee and Relabeling Cost are the restocking_fee and relabeling_cost entries under fees in financial_management.cost_breakdown; Repair Estimate is under repair_costs.repair_estimate; Recovery Value is under valuation.recovery_value; and Carbon Footprint is under sustainability.carbon_footprint. The JSON numbers are stored as integers or doubles and are read as decimal numbers.

For Return Channel Cost Index, the contract divides a return's shipping fee by the average shipping fee of ALL returns in the same return_channel, rather than specifying a historical subset as the documentation does. It returns no quotient if that average is zero. For Recovery Rate per Day it uses returns.dayslapsed and returns no quotient when Days Lapsed is zero; the current data has neither NULL nor zero dayslapsed.

The contract locates Regulatory Compliance Status in products.product_traceability under compliance.regulatory_compliance, joined by returns.itemlink and products.itemcode. A NULL status means not available and, like any status other than 'Non-compliant', produces a zero Regulatory Compliance Penalty.

The contract defines an authorised warranty claim as a return for which the warranty_claim entry in returns.return_details authorization is not NULL AND auth_status there is 'Approved'; 270 of 1,500 returns meet this test. WCR divides that count by the number of returns in the population, and yields no quotient for an empty population. The documentation does not specify this claim test.

For Fraud Flag Severity Score, fraud_flags and risk_level are read from the fraud section of returns.return_details; the flag count ranges from 0 to 5. The contract supplies the undocumented weights Low=1, Medium=2, High=3. A NULL risk level means not assessed and gives a NULL score.

For Average Processing Time, return_processing has one row per loccode, which identifies a facility, and proctime supplies the processing time. There is no key linking return_processing to individual returns: a facility's APT is its single proctime, and APT across facilities is the average of their proctime values.

Total Returns is stored in the total_returns entry of customers.return_behavior_profile and is used as the numerator of CRFI, rather than recounted from returns. The same profile also holds return_frequency_score and similar_previous_returns; NULL similar_previous_returns means no data.

For Customer Tenure, the contract takes the earliest orders.txndate for a customer's orders, parsing text dates in year-month-day with hyphens or slashes or abbreviated/full English month-name formats, including ordinal day suffixes. All 1,488 orders have NULL txndate in this data, so tenure and CRFI are NULL for every customer. The computation retains customers without orders and produces no CRFI quotient for zero tenure.

The aesthetic score used for IRV is read as a decimal from quality_assessment.assessment_summary. Although documented as 1–10, the observed range is 2 to 11; the contract uses it as stored, without clipping. All 1,500 returns have a product and one inspection, so it computes RVL for every return. The contract reports 397 customer return months: 195 Contained, 123 Elevated and 79 Severe. CLR produces no quotient if summed item value is zero.

Return-channel data in returns.return_channel contains Store, Courier, Mail and the undocumented 'Collection Point'; it contains no 'Locker' or 'Pickup'. A NULL fraud risk level means not assessed. Text flag fields return_processing.needsrelabel, case_management.trainident and quality_assessment.assessment_summary documentation.has_photos mix the tokens 0/1/F/False/N/NO/P/True/Y/YES. The contract treats 1, True, Y and YES as truthy; 0, F, False, N and NO as falsy; and 'P' as undefined.

The contract locates return data in one row per case in returns, identified by casenum. Its srctxn points to orders.txnnum, itemlink to products.itemcode, and revdate is processed-date text consistently in 'YYYY-MM-DD' form. logtime, when the return was logged, instead has mixed date-text formats. Every return has an order and a customer, whose identifier is orders.buyerlink. The contract reports an observed CMRC range of 1–13.

In returns.return_details, reasoning contains primary_reason ('Changed Mind', 'Quality Issue', 'Size/Fit', 'Wrong Item'), secondary_reason, reason_notes and client_notes. Authorization contains auth_status ('Approved', 'Pending', 'Rejected'), warranty_status and warranty_claim (NULL means none). Shipping contains fee, carrier, insurance_amount, estimated_arrival and tracking_reference; fraud contains risk_level and fraud_flags.

In quality_assessment, inspectref identifies its return and assessment_summary stores condition.item_condition and condition.completeness, along with package_condition and usage_signs (NULL means none). The JSON also holds defects.defect_type and defect_severity; results.qa_result, functional_test_result, aesthetic_score and tech_review_status; and documentation.documentation_status, has_photos, qa_alert and needs_investigation.

The contract describes customers as one row per profilenum, with seg_category taking Wholesale, Retail or Individual and geographyzone holding a country name. Products have one row per itemcode: itemcategory takes Apparel, Accessories, Electronics or Home Goods; subcat takes Laptops, Shirts, Shoes or Smartphones; and unit_value is stored as a real number and read as a decimal for RHC, IRV, RVL and CLR. Orders have one row per txnnum; their transaction_value is 0 on every row.

The contract locates product regulatory status in product_traceability, which also contains trace batch_reference, lot_reference, serial_number and manufacture_date, plus compliance.hazardous_material and compliance.recall_flag ('Yes' or 'No').

In financial_management, creditref identifies the record and casetag uniquely identifies its return. cost_breakdown also stores fees.repackaging_cost and qa_fee; refund_amount, method, status and processing_days under refund; parts_fee and labor_fee under repair_costs; disposal_method, environmental_impact and recycling_category under disposal; value_loss_pct under valuation; and sustainability_score, efficiency_score and cost_efficiency under sustainability.

In return_processing, besides proctime and loccode, procprio holds the documented priorities; procstate holds Completed, Inspecting, Processing or Received; handreq holds Fragile, Hazardous, Special or Standard; needsquar holds Yes/No; and quardays holds quarantine days. dispaction holds Refurbish, Repair, Resell or Scrap; dispreason holds Good Condition, Repairable, Too Costly or Unsalvageable (NULL means none provided); apprlevel holds the documented approval levels; and excepttype holds Customer Value, Error or Goodwill (NULL means none provided).

case_management has one row per return, linked by casetie; satisfscore stores the documented 1–5 satisfaction scale. Its closestate holds Closed, Open or Pending.

The contract's JSON extraction convention is to read numeric entries as decimal numbers and to group a JSON text extraction before comparing it with a value: in DuckDB the extraction operator otherwise binds more loosely than comparisons and logical conditions.
