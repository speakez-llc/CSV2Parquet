CREATE OR REPLACE VIEW "CMS_VIEW" AS 
SELECT 
    *,
    Total_Drug_Cost / Total_Day_Supply AS Total_Drug_Cost_Per_Day,
    Total_Drug_Cost_65_OrOlder / Total_Day_Supply_65_OrOlder AS Total_Drug_Cost_Per_Day_65_OrOlder,
    Total_Drug_Cost / Total_Claims AS Total_Cost_Per_Claim,
    Total_Drug_Cost_65_OrOlder / Total_CLaims_65_OrOlder AS Total_Cost_Per_Claim_65_OrOlder,
    Total_Drug_Cost / Total_Beneficiaries AS Total_Cost_Per_Beneficiary,
    Total_Drug_Cost_65_OrOlder / Total_Beneficiaries_65_OrOlder AS Total_Cost_Per_Beneficiary_65_OrOlder,
    Total_30_Day_Fills / Total_Beneficiaries AS Total_30_Day_Fills_Per_Beneficiary,
    Total_30_Day_Fills_65_OrOlder / Total_Beneficiaries_65_OrOlder AS Total_30_Day_Fills_Per_Beneficiary_65_OrOlder
FROM 
    CMS;