--Creando tabla ivr_summary

CREATE OR REPLACE TABLE keepcoding.ivr_summary AS
WITH documentation
  AS (SELECT CAST(calls_ivr_id AS STRING) AS ivr_id
           , document_identification
           , document_type
           , module_sequence
           , step_sequence
        FROM keepcoding.ivr_detail
       WHERE document_identification NOT IN ('NULL', 'DESCONOCIDO')
     QUALIFY ROW_NUMBER() OVER(PARTITION BY ivr_id ORDER BY module_sequence DESC, step_sequence DESC) = 1)

SELECT calls.calls_ivr_id
     , calls.calls_phone_number
     , calls.calls_ivr_result
     , CASE WHEN LEFT(calls.calls_vdn_label, 3) = 'ATC' THEN 'FRONT'
           WHEN LEFT(calls.calls_vdn_label, 4) = 'TECH' THEN 'TECH'
           WHEN calls.calls_vdn_label = 'ABSORPTION' THEN 'ABSORPTION'
           ELSE 'RESTO'
       END AS vdn_aggregation
     , calls.calls_start_date
     , calls.calls_end_date
     , calls.calls_total_duration
     , calls.calls_customer_segment
     , calls.calls_ivr_language
     , calls.calls_steps_module
     , calls.calls_module_aggregation
     , IFNULL(documentation.document_type, 'DESCONOCIDO') AS document_type
     , IFNULL(documentation.document_identification, 'DESCONOCIDO') AS document_identification
     , IFNULL(MAX(NULLIF(calls.customer_phone, 'NULL')), 'DESCONOCIDO') AS customer_phone
     , IFNULL(MAX(NULLIF(calls.billing_account_id, 'NULL')), 'DESCONOCIDO') AS billing_account_id
     , MAX(IF(calls.module_name = "AVERIA_MASIVA", 1, 0)) AS masiva_lg
     , MAX(IF(calls.step_name = 'CUSTOMERINFOBYPHONE.TX' AND calls.step_description_error = 'NULL', 1, 0)) AS info_by_phone_lg
     , MAX(IF(calls.step_name = 'CUSTOMERINFOBYDNI.TX' AND calls.step_description_error = 'NULL', 1, 0)) AS info_by_dni_lg
     , MAX(IF(DATE_DIFF(calls.calls_start_date, recalls.calls_start_date, SECOND) BETWEEN 1 AND 24*60*60, 1, 0)) AS repeated_phone_24H
     , MAX(IF(DATE_DIFF(calls.calls_start_date, recalls.calls_start_date, SECOND) BETWEEN -24*60*60  AND -1, 1, 0)) AS cause_recall_phone_24H 
  FROM keepcoding.ivr_detail calls
  LEFT 
  JOIN documentation
    ON CAST(calls_ivr_id AS STRING) = documentation.ivr_id
  LEFT 
  JOIN keepcoding.ivr_detail recalls
    ON calls.calls_phone_number <> 'NULL'
   AND calls.calls_phone_number = recalls.calls_phone_number
   AND calls.calls_ivr_id <> recalls.calls_ivr_id
 GROUP BY calls_ivr_id
     , calls_phone_number
     , calls_ivr_result
     , vdn_aggregation
     , calls_start_date
     , calls_end_date
     , calls_total_duration
     , calls_customer_segment
     , calls_ivr_language
     , calls_steps_module
     , calls_module_aggregation
     , document_type
     , document_identification;
