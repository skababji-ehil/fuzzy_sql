-- FILTER QUERIES: Finding the child records belonging to specific patient 
-- # Method 1
-- using ON AND
SELECT *
FROM sample_r_b
JOIN sample_r_l
    ON sample_r_b.PNUM_R = sample_r_l.PNUM_R
    AND (sample_r_b.PNUM_R=291 AND sample_r_l.TOTCHG > 10000)

-- # Method 2
-- using WHERE
SELECT *
FROM sample_r_b
JOIN sample_r_l
    ON sample_r_b.PNUM_R = sample_r_l.PNUM_R
WHERE (sample_r_b.PNUM_R=291 AND sample_r_l.TOTCHG > 10000)

--------------------------------------------------------------------------------------------------------
-- CONCLUSION: For FILTER queries
-- METHOD 1 and 2 above will result in the same results, so choose one of them
--1) FIX SELECT *
--2) FIX FROM  the base table
--3) FIX JOIN  longitudinal table ON the key variable  
--3) use WHERE with mix of CAT, CNT, DT variables from both PARENT and CHILD. Let WHERE have a flat strunture with ANDs or ORs and a maximum number of TERMS equals to the sum of all CAT + CNT + DT variables from both parent and child
---------------------------------------------------------------------------------------------------------


-- AGGREGATE QUERIES: Finding the sum of total charges (SUM(TOTCHG)) for all patinets 
-- Grouping by parent norminal variables  WITHOUT aggrgeation function (this is not so useful in longitudinal queries since the JOIN will have no imapct)
SELECT *,COUNT(*)
FROM sample_r_b
JOIN sample_r_l
    ON sample_r_b.PNUM_R = sample_r_l.PNUM_R
GROUP BY sample_r_b.FEMALE, sample_r_b.HISPANIC

-- Grouping by parent AND child norminal variables  WITHOUT aggrgeation function
SELECT *,COUNT(*)
FROM sample_r_b
JOIN sample_r_l
    ON sample_r_b.PNUM_R = sample_r_l.PNUM_R
GROUP BY sample_r_b.FEMALE, sample_r_b.HISPANIC, sample_r_l.ASCHED

-- -- Grouping by parent norminal variables  with aggrgeation function over child variable
SELECT *,COUNT(*), AVG(sample_r_b.AGE)
FROM sample_r_b
JOIN sample_r_l
    ON sample_r_b.PNUM_R = sample_r_l.PNUM_R
GROUP BY sample_r_b.FEMALE, sample_r_b.HISPANIC

SELECT *,COUNT(*), SUM(sample_r_l.TOTCHG)
FROM sample_r_b
JOIN sample_r_l
    ON sample_r_b.PNUM_R = sample_r_l.PNUM_R
GROUP BY sample_r_b.FEMALE, sample_r_b.HISPANIC

SELECT *,COUNT(*),SUM(sample_r_l.TOTCHG)
FROM sample_r_b
JOIN sample_r_l
    ON sample_r_b.PNUM_R = sample_r_l.PNUM_R
GROUP BY sample_r_b.FEMALE, sample_r_b.HISPANIC, sample_r_l.ASCHED


--------------------------------------------------------------------------------------------------------
-- CONCLUSION: For AGGREGATE queries
--1) FIX SELECT *, COUNT(*) VARY AGG_FNTC and AGG_VAR 
--2) FIX FROM  the base table
--3) FIX JOIN  longitudinal table ON the key variable  
--3) Use GROUP BY  with mix of CAT, DT variables from both PARENT and CHILD.

-- TEMPLATE II-A)
-- SELECT *,COUNT(*)
-- FROM sample_r_b
-- JOIN sample_r_l
--     ON sample_r_b.PNUM_R = sample_r_l.PNUM_R
-- GROUP BY select(bag_of_cat_dt_vars_from_parent_and_child)


-- TEMPLATE II-B)
-- SELECT *,COUNT(*), glue_expression(random_select(bag of [AGG_FNTN]) + random_select(bag of [PRNT_CNT_VAR / bag of  PRNT_DT_VAR/bag of CHLD_CNT_VAR / bag of  CHLD_DT_VAR]))
-- FROM sample_r_b
-- JOIN sample_r_l
--     ON sample_r_b.PNUM_R = sample_r_l.PNUM_R
-- GROUP BY random_select(bag of [PRNT_CAT_VAR, PRNT_DT_VAR, CHLD_CAT_VAR, CHLD_DT_VAR])
---------------------------------------------------------------------------------------------------------


-- Filter - AGGREGATE QUERIES: Finding the sum of total charges (SUM(TOTCHG)  per patient 

-- Filter before aggregating but before joining
SELECT *,COUNT(*)
FROM sample_r_b
JOIN sample_r_l
    ON sample_r_b.PNUM_R = sample_r_l.PNUM_R
    AND (sample_r_b.PNUM_R=291 AND sample_r_l.TOTCHG > 10000)
GROUP BY sample_r_b.FEMALE, sample_r_b.HISPANIC


-- Filetr before aggregating but after the joining
SELECT *,COUNT(*)
FROM sample_r_b
JOIN sample_r_l
    ON sample_r_b.PNUM_R = sample_r_l.PNUM_R
WHERE (sample_r_b.PNUM_R=291 AND sample_r_l.TOTCHG > 10000)
GROUP BY sample_r_b.FEMALE, sample_r_b.HISPANIC


-- Filter before aggregating with agg_fntn
SELECT *,COUNT(*), SUM(sample_r_l.TOTCHG)
FROM sample_r_b
JOIN sample_r_l
    ON sample_r_b.PNUM_R = sample_r_l.PNUM_R
    AND (sample_r_b.PNUM_R=291 AND sample_r_l.TOTCHG > 10000)
GROUP BY sample_r_b.FEMALE, sample_r_b.HISPANIC


-- Filetr before aggregating but after the joining with agg_fntn
SELECT *,COUNT(*), SUM(sample_r_l.TOTCHG)
FROM sample_r_b
JOIN sample_r_l
    ON sample_r_b.PNUM_R = sample_r_l.PNUM_R
WHERE (sample_r_b.PNUM_R=291 AND sample_r_l.TOTCHG > 10000)
GROUP BY sample_r_b.FEMALE, sample_r_b.HISPANIC


-- The above four seem to have same answer, but the article on the web highlights a tiny difference


-- Filter after aggregating (this is IGNORED in our package)
SELECT *,COUNT(*)
FROM sample_r_b
JOIN sample_r_l
    ON sample_r_b.PNUM_R = sample_r_l.PNUM_R
GROUP BY sample_r_b.FEMALE, sample_r_b.HISPANIC
HAVING ( sample_r_l.TOTCHG > 10000 and COUNT(*)>100)

-- Note: see how having can take aggregate variables 

--------------------------------------------------------------------------------------------------------
-- CONCLUSION: For FILTER-AGGREGATE queries
-- The FILTER-AGGREGATE is carried our by combining the abvove two types of queries
-- HAVING is NOT considered in our project. 
-- Pay special ATTENTION to the brackets encoding the value comprrison expression
---------------------------------------------------------------------------------------------------------


-- OVERALL CONCLUSION
-- We have three fundamental expressions:
-- 1) agg_fntn_expression
-- 2) value_comparions_expresssion (after AND in JOIN , or after WHERE and it has to be between brackets ())
-- 3) group_by_expression (after group by and is shall NOT be between bracket)
