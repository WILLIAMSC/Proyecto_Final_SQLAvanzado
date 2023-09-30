--Creando tabla ivr_summary

CREATE TABLE `keepcoding.ivr_summary` AS
SELECT
  d.calls_ivr_id
  , calls_phone_number
  , calls_ivr_result
  , CASE WHEN STARTS_WITH(calls_vdn_label,"ATC") THEN "FRONT"
      WHEN STARTS_WITH(calls_vdn_label,"TECH") THEN "TECH"
      WHEN calls_vdn_label = "ABSORPTION" THEN "ABSORPTION"
    ELSE "RESTO"
    END AS vdn_aggregation
  , calls_start_date
  , calls_end_date
  , calls_total_duration
  , calls_customer_segment
  , calls_ivr_language
  , calls_steps_module
  , calls_module_aggregation
  , COALESCE(d.document_type, s.document_type) AS document_type
  , COALESCE(d.document_identification, s.document_identification) AS document_identification
  , COALESCE(d.customer_phone, s.customer_phone) AS customer_phone
  , COALESCE(d.billing_account_id, s.billing_account_id) AS billing_acount_id
  , IF(CONTAINS_SUBSTR(calls_module_aggregation, "AVERIA_MASIVA"),1,0) AS masiva_lg
  , IF(d.step_name = "CUSTOMERINFOBYPHONE.TX" AND d.step_description_error IS NULL, 1,0) AS info_by_phone_lg
  , IF(d.step_name = "CUSTOMERINFOBYDNI.TX" AND d.step_description_error IS NULL, 1,0) AS info_by_dni_lg
  FROM `keepcoding.ivr_detail` d
  LEFT JOIN `keepcoding.ivr_steps` s 
  ON s.ivr_id = d.calls_ivr_id
  QUALIFY ROW_NUMBER() OVER(PARTITION BY CAST(ivr_id AS STRING) ORDER BY ivr_id) = 1
   