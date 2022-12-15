import sqlite3
import multiprocess as mp

if __name__=='__main__':
    
    simple_sql_sentence="SELECT b_sample.PL_NCHS2, b_sample.HISPANIC, b_sample.HOSPBRTH, b_sample.MEDINCST, b_sample.Homeless, b_sample.DIED, b_sample.NEOMAT, l_sample.DX1, b_sample.DNR, b_sample.PL_UR_CA, b_sample.RACE, b_sample.PL_RUCA4, l_sample.ASCHED, b_sample.PL_UIC20, b_sample.PL_CBSA, b_sample.FEMALE, b_sample.PL_RUCC2, COUNT(*), AVG(l_sample.NECODE)  FROM b_sample  LEFT JOIN l_sample  ON b_sample.PNUM_R=l_sample.PNUM_R  WHERE  b_sample.Homeless = '0'"
    
    complex_sql_sentence="SELECT b_sample.PL_NCHS2, b_sample.HISPANIC, b_sample.HOSPBRTH, b_sample.MEDINCST, b_sample.Homeless, b_sample.DIED, b_sample.NEOMAT, l_sample.DX1, b_sample.DNR, b_sample.PL_UR_CA, b_sample.RACE, b_sample.PL_RUCA4, l_sample.ASCHED, b_sample.PL_UIC20, b_sample.PL_CBSA, b_sample.FEMALE, b_sample.PL_RUCC2, COUNT(*), AVG(l_sample.NECODE)  FROM b_sample  LEFT JOIN l_sample  ON b_sample.PNUM_R=l_sample.PNUM_R  AND   b_sample.Homeless = '0' AND NOT  b_sample.PL_UR_CA LIKE '1' AND  b_sample.MEDINCST <> '3' OR  b_sample.DIED = '0' AND  l_sample.TOTCHG NOT BETWEEN 33220 AND 49640 AND NOT  l_sample.HCUP_ED < 0 AND  l_sample.Date BETWEEN 7 AND 9 AND  b_sample.RACE IN ('3', '1') OR NOT  l_sample.NECODE < 0 OR  l_sample.AWEEKEND = 0 AND  b_sample.FEMALE = '0' AND NOT  l_sample.ASCHED <> '1' OR  b_sample.PL_CBSA <> '2' AND  b_sample.AGE > 27 AND  l_sample.DRGVER = 25 AND NOT  b_sample.HISPANIC <> '2' AND NOT  l_sample.NDX BETWEEN 8 AND 8 AND  l_sample.DX1 <> '3229' AND  l_sample.MDC NOT BETWEEN 4 AND 5 OR  l_sample.NPR NOT BETWEEN 2 AND 3 AND NOT  b_sample.HOSPBRTH IN ('0') AND NOT  b_sample.PL_UIC20 LIKE '1' AND  l_sample.LOS >= 7 AND NOT  l_sample.PROCTYPE <> 1 AND  b_sample.PL_RUCA4 LIKE '2'  GROUP BY b_sample.PL_NCHS2, b_sample.HISPANIC, b_sample.HOSPBRTH, b_sample.MEDINCST, b_sample.Homeless, b_sample.DIED, b_sample.NEOMAT, l_sample.DX1, b_sample.DNR, b_sample.PL_UR_CA, b_sample.RACE, b_sample.PL_RUCA4, l_sample.ASCHED, b_sample.PL_UIC20, b_sample.PL_CBSA, b_sample.FEMALE, b_sample.PL_RUCC2"
    
    def exec_sql4testing(sql_str:str):
        conn=sqlite3.connect('/Users/samer/projects/fuzzy_sql/db/cal.db')
        cur = conn.cursor()
        res=cur.execute(sql_str)
        print(res.fetchone())
        
    
    p = mp.Process(target=exec_sql4testing, name="_test_query_time", args=(complex_sql_sentence,))
    p.start()
    p.join(5)  # wait 5 seconds until process terminates
    if p.is_alive():
        p.terminate()
        p.join()
        print('Cant wait any further! I am skipping this one!')  # MK TEMP
        print("Execution Too Long")   # execution time is too long
    else:
        print("Execution time is OK")   # execution time is ok