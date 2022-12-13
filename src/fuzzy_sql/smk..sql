SELECT b_sample.Homeless, b_sample.NEOMAT, b_sample.HISPANIC, b_sample.PL_NCHS2, b_sample.FEMALE, b_sample.MEDINCST, b_sample.DNR, b_sample.DIED, b_sample.PL_UR_CA, COUNT(*), AVG(l_sample.PROCTYPE)  FROM b_sample  JOIN l_sample  ON b_sample.PNUM_R=l_sample.PNUM_R

  WHERE   b_sample.DNR <> '0' AND  b_sample.RACE NOT LIKE '1' AND  b_sample.PL_CBSA IN ('2') AND  b_sample.NEOMAT NOT IN ('1', '0') AND  b_sample.Homeless LIKE '0' AND NOT  l_sample.NECODE NOT BETWEEN 0 AND 0 AND  b_sample.PL_UIC20 LIKE '1' AND NOT  b_sample.PL_RUCC2 <> '1' OR  b_sample.DIED = '0' AND  b_sample.HOSPBRTH LIKE '0' AND  l_sample.HCUP_ED < 4 AND NOT  l_sample.Date BETWEEN 3 AND 7 AND  b_sample.PL_NCHS2 IN ('1') AND  l_sample.DRGVER NOT BETWEEN 24 AND 24 AND  b_sample.AGE NOT BETWEEN 69 AND 72 AND  l_sample.TOTCHG <> 50599 AND  b_sample.FEMALE NOT LIKE '1' AND  l_sample.MDC > 6 AND NOT  b_sample.PL_UR_CA NOT LIKE '2' AND NOT  b_sample.HISPANIC LIKE '2' AND  l_sample.ASCHED NOT LIKE '0' AND  b_sample.PL_RUCA4 NOT IN ('1') AND  l_sample.LOS BETWEEN 5 AND 9 AND  l_sample.DX1 NOT IN ('82021', '49392', '66612')  GROUP BY b_sample.Homeless, b_sample.NEOMAT, b_sample.HISPANIC, b_sample.PL_NCHS2, b_sample.FEMALE, b_sample.MEDINCST, b_sample.DNR, b_sample.DIED, b_sample.PL_UR_CA




  SELECT l_sample.DX1, b_sample.PL_NCHS2, b_sample.RACE, b_sample.HOSPBRTH, b_sample.DNR, b_sample.PL_UR_CA, COUNT(*), AVG(b_sample.AGE)  FROM b_sample  
  
  LEFT JOIN l_sample  ON b_sample.PNUM_R=l_sample.PNUM_R  
  
  WHERE   l_sample.DX1 IN ('5990', '41400', '43491') AND  l_sample.PROCTYPE NOT BETWEEN 1 AND 1 AND  b_sample.PL_RUCA4 LIKE '1' OR  l_sample.MDC = 5 AND NOT  l_sample.AWEEKEND = 0 AND  b_sample.FEMALE NOT LIKE '1' AND  l_sample.NECODE < 0 AND  b_sample.AGE = 56 AND NOT  b_sample.DNR LIKE '0' AND  b_sample.RACE IN ('1', '3') AND NOT  l_sample.NDX < 8 AND  l_sample.DRGVER <= 24 AND  b_sample.NEOMAT <> '0' AND  b_sample.HISPANIC = '2' AND  b_sample.HOSPBRTH IN ('0') AND  l_sample.ASCHED NOT IN ('1', '0') AND  b_sample.PL_RUCC2 <> '1' AND  b_sample.MEDINCST = '2' AND NOT  b_sample.Homeless LIKE '0' AND NOT  b_sample.PL_NCHS2 LIKE '1' AND  l_sample.LOS <= 4 AND  b_sample.PL_UR_CA = '2' AND  b_sample.DIED LIKE '0' AND NOT  l_sample.Date = 7.5 AND  l_sample.NPR BETWEEN 1 AND 3 AND  l_sample.TOTCHG = 7537  
  
  GROUP BY l_sample.DX1, b_sample.PL_NCHS2, b_sample.RACE, b_sample.HOSPBRTH, b_sample.DNR, b_sample.PL_UR_CA










  SELECT l_sample.DX1, b_sample.NEOMAT, b_sample.HISPANIC, l_sample.ASCHED, b_sample.PL_NCHS2, b_sample.PL_CBSA, b_sample.FEMALE, b_sample.PL_UIC20, b_sample.MEDINCST, b_sample.RACE, b_sample.DNR, b_sample.DIED, b_sample.PL_UR_CA, b_sample.PL_RUCA4, COUNT(*), AVG(l_sample.MDC)  FROM b_sample  
  
  LEFT JOIN l_sample  ON b_sample.PNUM_R=l_sample.PNUM_R  
  
  GROUP BY l_sample.DX1, b_sample.NEOMAT, b_sample.HISPANIC, l_sample.ASCHED, b_sample.PL_NCHS2, b_sample.PL_CBSA, b_sample.FEMALE, b_sample.PL_UIC20, b_sample.MEDINCST, b_sample.RACE, b_sample.DNR, b_sample.DIED, b_sample.PL_UR_CA, b_sample.PL_RUCA4


  SELECT *  FROM b_sample  
  
  JOIN l_sample  ON b_sample.PNUM_R=l_sample.PNUM_R  
  
  WHERE   l_sample.AWEEKEND NOT BETWEEN 1 AND 1 AND  l_sample.Date < 6 AND NOT  l_sample.ASCHED = '0' AND  b_sample.RACE NOT IN ('1', '3') AND NOT  b_sample.FEMALE IN ('1', '0') AND  l_sample.DX1 NOT LIKE '57400' AND  b_sample.PL_UR_CA <> '1' AND  b_sample.AGE = 59 AND  b_sample.MEDINCST LIKE '2' AND  l_sample.HCUP_ED BETWEEN 0 AND 4 AND  l_sample.DRGVER = 25 AND  b_sample.HISPANIC <> '2' AND  b_sample.PL_UIC20 LIKE '2' AND  b_sample.NEOMAT IN ('0') AND  b_sample.PL_CBSA NOT IN ('2') OR NOT  b_sample.PL_RUCC2 LIKE '1' AND NOT  b_sample.Homeless NOT LIKE '0' AND NOT  l_sample.LOS <= 4 AND  b_sample.PL_NCHS2 <> '1' 