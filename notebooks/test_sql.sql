-- SQLite
SELECT age, workclass, fnlwgt, education, `marital.status`, occupation, relationship, race, sex, `hours.per.week`, `native.country`, capital, income
FROM C1;

-- Aggregate query with a continuous variable being aggregated 
SELECT workclass, COUNT(*), AVG(age)
FROM C1
GROUP BY workclass;

-- Aggregate query without a continuous variable being aggregated 
SELECT workclass, education, COUNT(*)
FROM C1
GROUP BY workclass, education;


-- Aggregate query without a continuous variable being aggregated 
SELECT workclass, education, COUNT(*), AVG(fnlwgt)
FROM C1
GROUP BY workclass, education;

SELECT age, workclass, fnlwgt
FROM C1
GROUP BY workclass, age

-- Cohort 
SELECT *
FROM C1
WHERE (age>60 OR age =20) AND (`marital.status`='Never-married') AND (fnlwgt>50000)


SELECT SUM(fnlwgt)
FROM C1
WHERE (age>60 OR age =20) AND (`marital.status`='Never-married') AND (fnlwgt>50000)

SELECT *
FROM C1
WHERE age>60 OR age =20 AND `marital.status`='Never-married' AND fnlwgt>50000


SELECT SUM(fnlwgt)
FROM C1
WHERE age>60 OR age =20 AND `marital.status`='Never-married' AND fnlwgt>50000




SELECT *
FROM C1
WHERE (education='Some-college') AND (age>60 OR age =20) AND (`marital.status`='Never-married' AND (fnlwgt>50000))


SELECT SUM(fnlwgt), COUNT(*)
FROM C1
WHERE (education='Some-college') AND (age>60 OR age =20) AND (`marital.status`='Never-married' AND (fnlwgt>50000))

n, len(vars), len(vals), len(where_ops), len(logic_ops)
(x='y') AND (x='y' AND x='y'): 1,3,3,3,2, i=1
(x='y') AND (x='y' AND x='y') AND (x='y' AND x='y'): 2, 5, 5, 5, 4, i=2
(x='y') AND (x='y' AND x='y') AND (x='y' AND x='y') AND (x='y' AND x='y'): 3,7,7,7,6,i=3
(x='y') AND (x='y' AND x='y') AND (x='y' AND x='y') AND (x='y' AND x='y') AND (x='y' AND x='y'): 4,9,9,9,8,i=4
var[0],where_ops[0], val[0], logic_ops[0],
var[1],where_ops[1], val[1], logic_ops[1],var[2],where_ops[2], val[2],logic_ops[2], i=1
var[3],where_ops[3], val[3], logic_ops[3],var[4],where_ops[4], val[4],logic_ops[4], i=2
var[4],where_ops[4], val[4], logic_ops[4],var[5],where_ops[5], val[5],logic_ops[5], i=3
var[i],where_ops[i], val[i], logic_ops[i],var[i+1],where_ops[i+1], val[i+1],logic_ops[i+1],
(x='y') AND (x='y' AND x='y') AND (x='y' AND x='y') AND (x='y' AND x='y') AND (x='y' AND x='y') AND (x='y' AND x='y') 


-- Some aggregate queries both conditional and non-conditional
SELECT workclass, COUNT(*), AVG(age)
FROM C1
GROUP BY workclass;

SELECT workclass, education,  COUNT(*), AVG(age)
FROM C1
GROUP BY workclass, education;

SELECT workclass, education,   COUNT(*), AVG(age)
FROM C1
WHERE (age>60 OR age =20) AND (`marital.status`='Never-married') AND (fnlwgt>50000)
GROUP BY workclass, education

-- Note here that we can use the same variable for both grouping (group by) and condition (where)
SELECT workclass, education, `marital.status`,  COUNT(*), AVG(age)
FROM C1
WHERE (age>60 OR age =20) AND (`marital.status`='Never-married') AND (fnlwgt>50000)
GROUP BY workclass, education,`marital.status`



-- A ,major difference between real and syn results
SELECT relationship,race,MIN(`hours.per.week`), COUNT(*) FROM C1  WHERE (sex <> 'Male'  OR  income LIKE '<=50K')   AND (capital = 0  AND  `hours.per.week` > 40)   GROUP BY relationship,race
SELECT relationship,race,MIN(`hours.per.week`), COUNT(*) FROM C1_syn_06  WHERE (sex <> 'Male'  OR  income LIKE '<=50K')   AND (capital = 0  AND  `hours.per.week` > 40)   GROUP BY relationship,race



SELECT occupation from C1 WHERE occupation<>'Sales'