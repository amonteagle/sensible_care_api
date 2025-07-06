-- View: transformed.clients_view

-- DROP VIEW transformed.clients_view;

CREATE OR REPLACE VIEW transformed.clients_view
 AS
 WITH temp_cte AS (
         SELECT clients.clientid AS client_id,
            TRIM(BOTH FROM clients.clientcode) AS client_code,
            TRIM(BOTH FROM clients.firstname) AS first_name,
            TRIM(BOTH FROM clients.lastname) AS last_name,
            TRIM(BOTH FROM clients.preferredname) AS preferred_name,
                CASE
                    WHEN lower(TRIM(BOTH FROM clients.gender)) ~~ 'm%'::text THEN 'Male'::text
                    WHEN lower(TRIM(BOTH FROM clients.gender)) ~~ 'f%'::text THEN 'Female'::text
                    ELSE 'Unknown'::text
                END AS gender,
                CASE
                    WHEN lower(TRIM(BOTH FROM clients.maritalstatus)) ~~ '%mar%ed%'::text THEN 'Married'::text
                    WHEN lower(TRIM(BOTH FROM clients.maritalstatus)) ~~ '%de%facto%'::text THEN 'De Facto'::text
                    WHEN lower(TRIM(BOTH FROM clients.maritalstatus)) ~~ '%wid%ow%'::text THEN 'Widowed'::text
                    WHEN lower(TRIM(BOTH FROM clients.maritalstatus)) ~~ '%divorce%'::text THEN 'Divorced'::text
                    WHEN lower(TRIM(BOTH FROM clients.maritalstatus)) ~~ '%never%mar%ed%'::text THEN 'Never Married'::text
                    WHEN lower(TRIM(BOTH FROM clients.maritalstatus)) ~~ '%partner%'::text THEN 'Partnered'::text
                    WHEN lower(TRIM(BOTH FROM clients.maritalstatus)) ~~ '%separ%'::text THEN 'Separated'::text
                    WHEN lower(TRIM(BOTH FROM clients.maritalstatus)) ~~ '%single%'::text THEN 'Single'::text
                    ELSE 'Unknown'::text
                END AS marital_status,
                CASE
                    WHEN clients.dateofbirth = '0000-00-00'::text THEN NULL::date
                    ELSE to_date(clients.dateofbirth, 'YYYY-MM-DD'::text)
                END AS birth_date,
            TRIM(BOTH FROM clients.fundingtype) AS funding_type,
            TRIM(BOTH FROM clients.clienttype) AS client_type,
                CASE
                    WHEN lower(clients.clienttype) ~~ 'hcp%sm%'::text THEN 'HCP Self-managed'::text
                    WHEN lower(clients.clienttype) ~~ 'hcp%brokerage%'::text THEN 'HCP Brokerage'::text
                    WHEN lower(clients.clienttype) ~~ 'hcp%'::text THEN 'HCP Care-managed'::text
                    WHEN lower(clients.clienttype) ~~ 'chsp%'::text THEN 'CHSP'::text
                    WHEN lower(clients.clienttype) ~~ 'ndis%'::text THEN 'NDIS'::text
                    ELSE 'Other'::text
                END AS client_subgroup,
                CASE
                    WHEN lower(clients.clienttype) ~~ 'hcp%'::text THEN 'HCP'::text
                    WHEN lower(clients.clienttype) ~~ 'chsp%'::text THEN 'CHSP'::text
                    WHEN lower(clients.clienttype) ~~ 'ndis%'::text THEN 'NDIS'::text
                    ELSE 'Other'::text
                END AS client_group,
            clients.hcplevel AS hcp_level,
            clients.payerid AS payer_id,
            upper(TRIM(BOTH FROM clients.suburb)) AS suburb,
            upper(TRIM(BOTH FROM clients.state)) AS state_code,
            TRIM(BOTH FROM clients.postcode) AS post_code,
                CASE
                    WHEN clients.longitude = ''::text THEN NULL::double precision
                    ELSE clients.longitude::double precision
                END AS longitude,
                CASE
                    WHEN clients.latitude = ''::text THEN NULL::double precision
                    ELSE clients.latitude::double precision
                END AS latitude,
            TRIM(BOTH FROM clients.area) AS area,
            TRIM(BOTH FROM clients.division) AS division,
            TRIM(BOTH FROM clients.casemanager) AS casemanager,
            TRIM(BOTH FROM clients.casemanager2) AS casemanager2,
                CASE
                    WHEN NULLIF(clients.languageenglish::text, ''::text) IS NULL THEN NULL::integer
                    ELSE clients.languageenglish::integer
                END AS language_english_flag,
            TRIM(BOTH FROM clients.languageother) AS other_languages,
            clients.interpreterrequired AS interpreter_required_flag,
            clients.referral AS referral_flag,
            TRIM(BOTH FROM clients.referer) AS referer_name,
            TRIM(BOTH FROM clients.referercode) AS referer_code,
            TRIM(BOTH FROM clients.currentstatus) AS current_status,
            clients.servicestatus AS service_status,
                CASE
                    WHEN clients.servicestart = '0000-00-00'::text THEN NULL::date
                    ELSE to_date(clients.servicestart, 'YYYY-MM-DD'::text)
                END AS service_start_date,
                CASE
                    WHEN clients.serviceend = '0000-00-00'::text THEN NULL::date
                    ELSE to_date(clients.serviceend, 'YYYY-MM-DD'::text)
                END AS service_end_date,
            clients.current AS current_flag,
            clients.reasonserviceended AS reason_service_ended,
            clients.preferedworker AS prefered_workers,
            clients.nonpreferedworker AS non_prefered_workers,
            clients.modifiedtime AS modified_datetime,
                CASE
                    WHEN EXTRACT(month FROM CURRENT_DATE) >= 7::numeric THEN EXTRACT(year FROM CURRENT_DATE) + 1::numeric
                    ELSE EXTRACT(year FROM CURRENT_DATE)
                END AS current_fical_year,
            make_date(
                CASE
                    WHEN EXTRACT(month FROM CURRENT_DATE) >= 7::numeric THEN EXTRACT(year FROM CURRENT_DATE)::integer
                    ELSE EXTRACT(year FROM CURRENT_DATE)::integer - 1
                END, 7, 1) AS current_fical_year_start
           FROM raw.clients clients
        ), stage_cte AS (
         SELECT temp_cte.client_id,
            (temp_cte.first_name || ' '::text) || temp_cte.last_name AS client_name,
            date_part('year'::text, age(temp_cte.birth_date::timestamp with time zone)) AS age_years,
            round((CURRENT_DATE - temp_cte.service_start_date)::numeric / 365.25, 1) AS tenure_years,
            temp_cte.gender,
            temp_cte.marital_status,
            temp_cte.area,
            temp_cte.state_code,
            temp_cte.suburb,
            temp_cte.post_code,
            (temp_cte.latitude::text || ','::text) || temp_cte.longitude::text AS encoded_location,
            temp_cte.casemanager,
            temp_cte.casemanager2,
                CASE
                    WHEN temp_cte.casemanager = ''::text AND temp_cte.casemanager2 = ''::text THEN 'unknown'::text
                    WHEN temp_cte.casemanager = ''::text THEN replace(temp_cte.casemanager2, '@sensiblecare.com.au'::text, ''::text)
                    WHEN temp_cte.casemanager2 = ''::text THEN replace(temp_cte.casemanager, '@sensiblecare.com.au'::text, ''::text)
                    ELSE (replace(temp_cte.casemanager, '@sensiblecare.com.au'::text, ''::text) || ', '::text) || replace(temp_cte.casemanager2, '@sensiblecare.com.au'::text, ''::text)
                END AS case_managers,
            temp_cte.language_english_flag,
            temp_cte.interpreter_required_flag,
            replace(replace(temp_cte.other_languages, '/'::text, ' '::text), '.'::text, ''::text) AS other_languages,
            temp_cte.referral_flag,
            temp_cte.referer_name,
            temp_cte.referer_code,
            temp_cte.funding_type,
            temp_cte.client_group,
            temp_cte.client_subgroup,
            temp_cte.client_type,
            temp_cte.hcp_level,
                CASE
                    WHEN temp_cte.hcp_level = ANY (ARRAY[1, 2, 3, 4]) THEN 'HCP '::text || temp_cte.hcp_level::text
                    ELSE 'Other'::text
                END AS hcp_level_name,
            temp_cte.current_flag,
            temp_cte.service_status,
                CASE
                    WHEN (temp_cte.service_end_date IS NULL OR temp_cte.service_end_date > CURRENT_DATE) AND temp_cte.service_status = 'Current'::text THEN 'Active'::text
                    ELSE 'Inactive'::text
                END AS active_flag,
            temp_cte.current_fical_year,
            temp_cte.current_fical_year_start,
            temp_cte.service_start_date,
            (date_trunc('month'::text, temp_cte.service_start_date::timestamp with time zone) + '1 mon -1 days'::interval)::date AS service_start_month,
                CASE
                    WHEN temp_cte.service_start_date IS NULL THEN NULL::text
                    WHEN EXTRACT(month FROM temp_cte.service_start_date) >= 7::numeric THEN (EXTRACT(year FROM temp_cte.service_start_date)::text || '-'::text) || ((EXTRACT(year FROM temp_cte.service_start_date) - 1999::numeric)::text)
                    ELSE (((EXTRACT(year FROM temp_cte.service_start_date) - 1::numeric)::text) || '-'::text) || ((EXTRACT(year FROM temp_cte.service_start_date) - 2000::numeric)::text)
                END AS service_start_year,
                CASE
                    WHEN temp_cte.service_start_date IS NULL THEN NULL::numeric
                    WHEN EXTRACT(month FROM temp_cte.service_start_date) >= 7::numeric THEN EXTRACT(year FROM temp_cte.service_start_date)
                    ELSE EXTRACT(year FROM temp_cte.service_start_date)
                END AS service_start_fiscal_year,
                CASE
                    WHEN EXTRACT(month FROM temp_cte.service_start_date) >= 7::numeric THEN EXTRACT(year FROM temp_cte.service_start_date)
                    ELSE EXTRACT(year FROM temp_cte.service_start_date)
                END = temp_cte.current_fical_year AS service_start_current_fiscal_year,
                CASE
                    WHEN EXTRACT(month FROM temp_cte.service_start_date) >= 7::numeric THEN EXTRACT(year FROM temp_cte.service_start_date)
                    ELSE EXTRACT(year FROM temp_cte.service_start_date)
                END = (temp_cte.current_fical_year - 1::numeric) AS service_start_prior_fiscal_year,
            temp_cte.service_start_date >= temp_cte.current_fical_year_start AND temp_cte.service_start_date <= CURRENT_DATE AS service_start_current_fytd,
            temp_cte.service_start_date >= (temp_cte.current_fical_year_start - '1 year'::interval) AND temp_cte.service_start_date <= (CURRENT_DATE - '1 year'::interval) AS service_start_prior_fytd,
            temp_cte.service_end_date,
            (date_trunc('month'::text, temp_cte.service_end_date::timestamp with time zone) + '1 mon -1 days'::interval)::date AS service_end_month,
                CASE
                    WHEN temp_cte.service_end_date IS NULL THEN NULL::text
                    WHEN EXTRACT(month FROM temp_cte.service_end_date) >= 7::numeric THEN (EXTRACT(year FROM temp_cte.service_end_date)::text || '-'::text) || ((EXTRACT(year FROM temp_cte.service_end_date) - 1999::numeric)::text)
                    ELSE (((EXTRACT(year FROM temp_cte.service_end_date) - 1::numeric)::text) || '-'::text) || ((EXTRACT(year FROM temp_cte.service_end_date) - 2000::numeric)::text)
                END AS service_end_year,
                CASE
                    WHEN temp_cte.service_end_date IS NULL THEN NULL::numeric
                    WHEN EXTRACT(month FROM temp_cte.service_end_date) >= 7::numeric THEN EXTRACT(year FROM temp_cte.service_end_date)
                    ELSE EXTRACT(year FROM temp_cte.service_end_date)
                END AS service_end_fiscal_year,
                CASE
                    WHEN EXTRACT(month FROM temp_cte.service_end_date) >= 7::numeric THEN EXTRACT(year FROM temp_cte.service_end_date)
                    ELSE EXTRACT(year FROM temp_cte.service_end_date)
                END = temp_cte.current_fical_year AS service_end_current_fiscal_year,
                CASE
                    WHEN EXTRACT(month FROM temp_cte.service_end_date) >= 7::numeric THEN EXTRACT(year FROM temp_cte.service_end_date)
                    ELSE EXTRACT(year FROM temp_cte.service_end_date)
                END = (temp_cte.current_fical_year - 1::numeric) AS service_end_prior_fiscal_year,
            temp_cte.service_end_date >= temp_cte.current_fical_year_start AND temp_cte.service_end_date <= CURRENT_DATE AS service_end_current_fytd,
            temp_cte.service_end_date >= (temp_cte.current_fical_year_start - '1 year'::interval) AND temp_cte.service_end_date <= (CURRENT_DATE - '1 year'::interval) AS service_end_prior_fytd,
            temp_cte.reason_service_ended,
                CASE
                    WHEN temp_cte.service_end_date IS NULL THEN NULL::text
                    WHEN lower(temp_cte.service_status) = 'deceased'::text OR lower(temp_cte.reason_service_ended) ~~ '%death%'::text OR lower(temp_cte.reason_service_ended) ~~ '%died%'::text OR lower(temp_cte.reason_service_ended) ~~ '%pass%away%'::text OR lower(temp_cte.reason_service_ended) ~~ '%decease%'::text OR lower(temp_cte.reason_service_ended) ~~ '%passing%'::text THEN 'Deceased'::text
                    WHEN lower(temp_cte.reason_service_ended) ~~ '%perm%care%'::text OR lower(temp_cte.reason_service_ended) ~~ '%resi%care%'::text OR lower(temp_cte.reason_service_ended) ~~ '%nurs%home%'::text OR lower(temp_cte.reason_service_ended) ~~ '%full%time%care%'::text OR lower(temp_cte.reason_service_ended) ~~ '%transition%care%'::text THEN 'Permanent Care'::text
                    WHEN (lower(temp_cte.reason_service_ended) ~~ '%chang%provider%'::text OR lower(temp_cte.reason_service_ended) ~~ '%different%provider%'::text OR lower(temp_cte.reason_service_ended) ~~ '%other%provider%'::text OR lower(temp_cte.reason_service_ended) ~~ '%move%provider%'::text OR lower(temp_cte.reason_service_ended) ~~ '%switch%provider%'::text OR lower(temp_cte.reason_service_ended) ~~ '%transfer%'::text OR lower(temp_cte.reason_service_ended) ~~ '%regis%hcp%'::text OR lower(temp_cte.reason_service_ended) ~~ '%uniting%ageing%'::text) AND lower(temp_cte.reason_service_ended) !~~ '%chsp%'::text THEN 'Provider Change'::text
                    WHEN lower(temp_cte.reason_service_ended) ~~ '%referral%complete%'::text THEN 'Referral Completed'::text
                    ELSE 'Other'::text
                END AS reason_service_ended_group
           FROM temp_cte
        )
 SELECT client_id,
    client_name,
    age_years,
    tenure_years,
    gender,
    marital_status,
    area,
    state_code,
    suburb,
    post_code,
    encoded_location,
    casemanager,
    casemanager2,
    case_managers,
    language_english_flag,
    interpreter_required_flag,
    other_languages,
    referral_flag,
    referer_name,
    referer_code,
    funding_type,
    client_group,
    client_subgroup,
    client_type,
    hcp_level,
    hcp_level_name,
    current_flag,
    service_status,
    active_flag,
    current_fical_year,
    current_fical_year_start,
    service_start_date,
    service_start_month,
    service_start_year,
    service_start_fiscal_year,
    service_start_current_fiscal_year,
    service_start_prior_fiscal_year,
    service_start_current_fytd,
    service_start_prior_fytd,
    service_end_date,
    service_end_month,
    service_end_year,
    service_end_fiscal_year,
    service_end_current_fiscal_year,
    service_end_prior_fiscal_year,
    service_end_current_fytd,
    service_end_prior_fytd,
    reason_service_ended,
    reason_service_ended_group,
        CASE
            WHEN age_years IS NULL THEN NULL::text
            WHEN age_years < 60::double precision THEN 'a. <60'::text
            WHEN age_years < 65::double precision THEN 'b. 60-65'::text
            WHEN age_years < 70::double precision THEN 'c. 65-70'::text
            WHEN age_years < 75::double precision THEN 'd. 70-75'::text
            WHEN age_years < 80::double precision THEN 'e. 75-80'::text
            WHEN age_years < 85::double precision THEN 'f. 80-85'::text
            WHEN age_years < 90::double precision THEN 'g. 85-90'::text
            WHEN age_years < 95::double precision THEN 'h. 90-95'::text
            ELSE 'i. 95+'::text
        END AS age_band,
        CASE
            WHEN tenure_years IS NULL THEN NULL::text
            WHEN tenure_years <= 0.5 THEN 'a. 0-6 months'::text
            WHEN tenure_years <= 1::numeric THEN 'b. 6-12 months'::text
            WHEN tenure_years <= 2::numeric THEN 'c. 1-2 years'::text
            WHEN tenure_years <= 3::numeric THEN 'd. 2-3 years'::text
            WHEN tenure_years <= 4::numeric THEN 'e. 3-4 years'::text
            WHEN tenure_years <= 5::numeric THEN 'f. 4-5 years'::text
            ELSE 'g. 5+ years'::text
        END AS tenure_band
   FROM stage_cte;

ALTER TABLE transformed.clients_view
    OWNER TO database_owner;

