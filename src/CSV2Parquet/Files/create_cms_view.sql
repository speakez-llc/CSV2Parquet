CREATE OR REPLACE VIEW "cms_view" AS 
SELECT 
    *,
    total_drug_cost_under_65 / total_day_supply_under_65 as total_drug_cost_per_day_under_65,
    total_drug_cost_65_or_older / total_day_supply_65_or_older as total_drug_cost_per_day_65_or_older,
    total_drug_cost_under_65 / total_claims_under_65 as total_cost_per_claim_under_65,
    total_drug_cost_65_or_older / total_claims_65_or_older as total_cost_per_claim_65_or_older,
    total_drug_cost_under_65 / total_beneficiaries_under_65 as total_cost_per_beneficiary_under_65,
    total_drug_cost_65_or_older / total_beneficiaries_65_or_older as total_cost_per_beneficiary_65_or_older,
    total_30_day_fills_under_65 / total_beneficiaries_under_65 as total_30_day_fills_per_beneficiary_under_65,
    total_30_day_fills_65_or_older / total_beneficiaries_65_or_older as total_30_day_fills_per_beneficiary_65_or_older
FROM 
    cms;