----  Types of JOINS
---- https://www.youtube.com/watch?v=KTvYHEntvn8
---  JOIN (aka INNER JOIN) -child- ON ---=---: matches entries from parenet and child table and ignores non-matching enteries
---- LEFT JOIN (aka LEFT OUTER JOIN) -- ON ---=---: shows all data from left (usually PARENT) even if there is no matching record on the right table (CHILD). 
---- (NOT SUPPORTED in SQLITE) RIGHT JOIN -- ON ---=--- : Opposite of left above
---- (NOT SUPPORTED in SQLITE )FULL OUTER JOIN --- ON ---=---: It will show all macthing and non-macthing records 
---- (IGNORED BY FUZZY_SQL) CROSS JOIN -child- : It shows all possible combiinations from  from both tables. Note that you do not pass ON. The output length of the query is the product of the lenegths of both input tables 



--------------------------------------------------------------------

--- AGG Queries without AGG FNTN:

SELECT s1_carrier_1a.LINE_PRCSG_IND_CD_4,  COUNT(*)
FROM s1_ben_sum_2008
JOIN s1_carrier_1a ON s1_ben_sum_2008.DESYNPUF_ID = s1_carrier_1a.DESYNPUF_ID
JOIN s1_inpatient ON s1_ben_sum_2008.DESYNPUF_ID = s1_inpatient.DESYNPUF_ID
GROUP BY s1_carrier_1a.LINE_PRCSG_IND_CD_4


-- SELECT (RANDOM GROUP BY VARS), COUNT(*)
-- FROM (RANDOM PARENT TBL)
-- (RANDOM JOIN or LEFT JOIN) (RANDOM CHILD TBL) ON (repsective PARENT KEY) = (repsective CHILD KEY)
-- (RANDOM JOIN or LEFT JOIN) (RANDOM CHILD TBL exclduing selected above) ON (repsective PARENT KEY) = (repsective CHILD KEY)
-- ...
-- GROUP BY (RANDOM GROUP BY VARS)

-- NOTE: GROUP{ BY vars van be from PARENT/CHILD/CAT/DT 


----------------------------------------------------------------------------------
--------- Possible FROM -- JOIN --- ON combinations:

---------- Case I: One parent to many multiple tables
--- SELECT (whatever) 
--- FROM parent (one side)
--- JOIN child1 (many side) on (parent.key = child1.key)
--- JOIN child2 (many side) on (parent.key = child2.key)
--- ....

---------- Case II (Ignored by fuzzy_sql) : One parent to many multiple tables (https://learnsql.com/blog/how-to-join-3-tables-or-more-in-sql/)
--- SELECT (whatever) 
--- FROM parent1 (one side)
--- JOIN junction (many side) on (parent1.key = junction.key)
--- JOIN parenet2 (one side) on (parent2.key = junction.key)
--- ....



--------------------------------------------------------------------------------------------
-----------A  note about COMPOSITE PRIMARY KEYS (https://stackoverflow.com/questions/27607516/sql-join-with-composite-primary-key)
-- SELECT * 
-- FROM Table1
-- INNER JOIN Table2
-- ON Table1.Key1 = Table2.Key1 AND Table1.Key2 = Table2.Key2 AND Table1.Key3 = Table2.Key3
-------------------------------------------------------------------------------------


--- WHERE Filter queries 

-- SELECT *
-- FROM (RANDOM PARENT TBL) 
-- (RANDOM JOIN or LEFT JOIN) (RANDOM CHILD TBL) ON (repsective PARENT KEY) = (repsective CHILD KEY)
-- (RANDOM JOIN or LEFT JOIN) (RANDOM CHILD TBL exclduing selected above) ON (repsective PARENT KEY) = (repsective CHILD KEY)
--...
-- WHERE (Random mix of CAT, CNT, DT variables from both PARENT and CHILD. Let WHERE have a flat strunture with ANDs or ORs and a maximum number of TERMS equals to the sum of all CAT + CNT + DT variables from both parent and child)

