CREATE OR REPLACE PACKAGE BODY DWH_EDW.PCG_OSOS_ACTIVE_ENDEX 
IS
/******************************************************************************
   NAME:      PCG_OSOS_ACTIVE_ENDEX 
   PURPOSE:

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        06.08.2017      Murat Öncül       1. Created this package.

******************************************************************************/ 

PROCEDURE PRC_TMP_RDD_DAILYENDEXES AS
BEGIN 
      PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE('ODS_OSOS_ADM', 'TMP_RDD_DAILYENDEXES');
      

--BEGIN 
--      PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE('ODS_OSOS_ADM', 'TMP_RDD_DAILYENDEXES');
--
--       INSERT /*+PARALLEL(16) append nologging */ INTO ODS_OSOS_ADM.TMP_RDD_DAILYENDEXES
--       (    SERNO,
--            OWNERSERNO,
--            DEFINITIONTYPE,
--            SENSORSERNO,
--            ENDEXDATE,
--            ENDEXTYPE,
--            RECORDTYPE,
--            TSUM,
--            T1ENDEX,
--            T2ENDEX,
--            T3ENDEX,
--            T4ENDEX,
--            REACTIVECAPASITIVE,
--            REACTIVEINDUCTIVE,
--            MAXDEMAND,
--            DEMANDDATE,
--            RECORDSTATUS
--       )
--       SELECT /*+PARALLEL(DE,16) PARALLEL(CE,16) */
--            CE.SERNO,
--            CE.OWNERSERNO,
--            CE.DEFINITIONTYPE,
--            CE.SENSORSERNO,
--            CE.ENDEXDATE,
--            CE.ENDEXTYPE,
--            DE.RECORDTYPE,
--            CE.TSUM,
--            CE.T1ENDEX,
--            CE.T2ENDEX,
--            CE.T3ENDEX,
--            CE.T4ENDEX,
--            CE.REACTIVECAPASITIVE,
--            CE.REACTIVEINDUCTIVE,
--            CE.MAXDEMAND,
--            CE.DEMANDDATE,
--            CE.RECORDSTATUS
--       -- SELECT *
--       FROM DWH_ODS.GG_ADM_RDD_DAILYENDEXES DE
--       INNER JOIN DWH_ODS.GG_ADM_CURRENTENDEXES CE ON (DE.CURRENTENDEXSERNO = CE.SERNO)
--       WHERE DE.LPRBULKDATASERNO IS NULL;
--      
--      COMMIT;    
--
--      INSERT /*+PARALLEL(16) append nologging */ INTO ODS_OSOS_ADM.TMP_RDD_DAILYENDEXES
--       (    SERNO,
--            OWNERSERNO,
--            DEFINITIONTYPE,
--            SENSORSERNO,
--            ENDEXDATE,
--            ENDEXTYPE,
--            RECORDTYPE,
--            TSUM,
--            T1ENDEX,
--            T2ENDEX,
--            T3ENDEX,
--            T4ENDEX,
--            REACTIVECAPASITIVE,
--            REACTIVEINDUCTIVE,
--            MAXDEMAND,
--            DEMANDDATE,
--            RECORDSTATUS
--       )
--      SELECT  /*+PARALLEL(D,128) PARALLEL(B,128) */
--      B.SERNO,
--      B.OWNERSERNO,
--      B.DEFINITIONTYPE,
--      B.SENSORSERNO,
--      B.PROFILEDATE,
--      0,
--      1,
--      B.TSUM,
--      NULL,
--      NULL,
--      NULL,
--      NULL,
--      B.REACTIVECAPASITIVE,
--      B.REACTIVEINDUCTIVE,
--      NULL,
--      NULL,
--      B.RECORDSTATUS
--      FROM DWH_ODS.GG_ADM_RDD_DAILYENDEXES D
--      INNER JOIN DWH_ODS.GG_ADM_LPR_BULKDATAS B ON D.LPRBULKDATASERNO=B.SERNO
--      WHERE D.LPRBULKDATASERNO IS NOT NULL;
      
--    COMMIT;
--
--END;


       INSERT /*+PARALLEL(128) append nologging */ INTO ODS_OSOS_ADM.TMP_RDD_DAILYENDEXES
       (    SERNO,
            OWNERSERNO,
            DEFINITIONTYPE,
            SENSORSERNO,
            ENDEXDATE,
            ENDEXTYPE,
            RECORDTYPE,
            TSUM,
            T1ENDEX,
            T2ENDEX,
            T3ENDEX,
            T4ENDEX,
            REACTIVECAPASITIVE,
            REACTIVEINDUCTIVE,
            MAXDEMAND,
            DEMANDDATE,
            RECORDSTATUS
       )
       SELECT /*+PARALLEL(DE,128) PARALLEL(CE,128) */
            CE.SERNO,
            CE.OWNERSERNO,
            CE.DEFINITIONTYPE,
            CE.SENSORSERNO,
            CE.ENDEXDATE,
            CE.ENDEXTYPE,
            DE.RECORDTYPE,
            CE.TSUM,
            CE.T1ENDEX,
            CE.T2ENDEX,
            CE.T3ENDEX,
            CE.T4ENDEX,
            CE.REACTIVECAPASITIVE,
            CE.REACTIVEINDUCTIVE,
            CE.MAXDEMAND,
            CE.DEMANDDATE,
            CE.RECORDSTATUS
       -- SELECT *
       FROM ODS_OSOS_ADM.RDD_DAILYENDEXES DE
         INNER JOIN ODS_OSOS_ADM.RDD_CURRENTENDEXES CE ON (DE.CURRENTENDEXSERNO = CE.SERNO);
      
      COMMIT;    

END;
PROCEDURE PRC_F_MRR_DAILY_ENDEXES AS

BEGIN

      PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE('DWH_EDW', 'F_MRR_DAILY_ENDEXES');
      
      EXECUTE IMMEDIATE '
  
  
      INSERT  /*+ append nologging PARALLEL(64)*/ INTO DWH_EDW.F_MRR_DAILY_ENDEXES
        (SUBSCRIBER_APP_ID, SUBSCRIBER_ID, METER_APP_ID, METER_ID, 
         ENDEX_DATETIME, ENDEX_DATE, MULTIPLIER, TOTAL_ENDEX, BACK_TOTAL_ENDEX, BACK_ENDEX_DATE, 
         T1_ENDEX, T2_ENDEX, T3_ENDEX, T4_ENDEX,BACK_T1_ENDEX,BACK_T2_ENDEX,BACK_T3_ENDEX,BACK_T4_ENDEX, REACTIVE_CAPASITIVE_ENDEX, REACTIVE_INDUCTIVE_ENDEX, 
         MAX_DEMAND, DEMAND_DATE, ENDEX_TYPE, RECORD_TYPE, 
         APPLICATION_ID, CONSUMPTION_VALUE,T1_CONSUMPTION_VALUE,T2_CONSUMPTION_VALUE,T3_CONSUMPTION_VALUE,T4_CONSUMPTION_VALUE, MINUTE_DIFFERENCE, MINUTE_ENDEX_CONSUMPTION) 
  
  
  
      SELECT /*+PARALLEL(128) */ 
         SUBSCRIBER_APP_ID, SUBSCRIBER_ID, METER_APP_ID, METER_ID, 
         ENDEX_DATETIME, ENDEX_DATE, MULTIPLIER, TOTAL_ENDEX, BACK_TOTAL_ENDEX, BACK_ENDEX_DATE, 
         T1_ENDEX, T2_ENDEX, T3_ENDEX, T4_ENDEX,BACK_T1_ENDEX,BACK_T2_ENDEX,BACK_T3_ENDEX,BACK_T4_ENDEX, REACTIVE_CAPASITIVE_ENDEX, REACTIVE_INDUCTIVE_ENDEX, 
         MAX_DEMAND, DEMAND_DATE, ENDEX_TYPE, RECORD_TYPE, 
         APPLICATION_ID,
         (NVL(TOTAL_ENDEX,0) - NVL(BACK_TOTAL_ENDEX,0)) CONSUMPTION_VALUE,
         (NVL(T1_ENDEX,0) - NVL(BACK_T1_ENDEX,0)) T1_CONSUMPTION_VALUE,
         (NVL(T2_ENDEX,0) - NVL(BACK_T2_ENDEX,0)) T2_CONSUMPTION_VALUE,
         (NVL(T3_ENDEX,0) - NVL(BACK_T3_ENDEX,0)) T3_CONSUMPTION_VALUE,
         (NVL(T4_ENDEX,0) - NVL(BACK_T4_ENDEX,0)) T4_CONSUMPTION_VALUE,
         DATEDIFF(''minute'', BACK_ENDEX_DATE, ENDEX_DATETIME) MINUTE_DIFFERENCE,
         (NVL(TOTAL_ENDEX,0) - NVL(BACK_TOTAL_ENDEX,0))/
          DECODE(
            DATEDIFF(''minute'', BACK_ENDEX_DATE,ENDEX_DATETIME)
            ,0
            ,1,
            DATEDIFF(''minute'', BACK_ENDEX_DATE, ENDEX_DATETIME)
           )  AS MINUTE_ENDEX_CONSUMPTION
      FROM
         (

              SELECT /*+PARALLEL(128) */ 
                       SUBSCRIBER_APP_ID,
                       SUBSCRIBER_ID,
                       METER_APP_ID,
                       METER_ID,
                       ENDEX_DATETIME,
                       ENDEX_DATE,
                       MULTIPLIER,
                       TOTAL_ENDEX,
                       NVL(LAG(TOTAL_ENDEX) OVER (PARTITION BY SUBSCRIBER_ID, METER_ID, RECORD_TYPE, ENDEX_TYPE ORDER BY ENDEX_DATETIME),0) BACK_TOTAL_ENDEX,
                       NVL(LAG(ENDEX_DATETIME) OVER (PARTITION BY SUBSCRIBER_ID, METER_ID, RECORD_TYPE, ENDEX_TYPE ORDER BY ENDEX_DATETIME),ENDEX_DATETIME) BACK_ENDEX_DATE,
                       T1_ENDEX,
                       T2_ENDEX,
                       T3_ENDEX,
                       T4_ENDEX,
                       NVL(LAG(T1_ENDEX) OVER (PARTITION BY SUBSCRIBER_ID, METER_ID, RECORD_TYPE, ENDEX_TYPE ORDER BY ENDEX_DATETIME),0) BACK_T1_ENDEX,
                       NVL(LAG(T2_ENDEX) OVER (PARTITION BY SUBSCRIBER_ID, METER_ID, RECORD_TYPE, ENDEX_TYPE ORDER BY ENDEX_DATETIME),0) BACK_T2_ENDEX,
                       NVL(LAG(T3_ENDEX) OVER (PARTITION BY SUBSCRIBER_ID, METER_ID, RECORD_TYPE, ENDEX_TYPE ORDER BY ENDEX_DATETIME),0) BACK_T3_ENDEX,
                       NVL(LAG(T4_ENDEX) OVER (PARTITION BY SUBSCRIBER_ID, METER_ID, RECORD_TYPE, ENDEX_TYPE ORDER BY ENDEX_DATETIME),0) BACK_T4_ENDEX,
                       REACTIVE_CAPASITIVE_ENDEX,
                       REACTIVE_INDUCTIVE_ENDEX,
                       MAX_DEMAND,
                       DEMAND_DATE,
                       ENDEX_TYPE,
                       RECORD_TYPE,
                       APPLICATION_ID
              FROM
              (
                  SELECT /*+PARALLEL(E,128) PARALLEL(D,32) PARALLEL(M,32) */ 
                        E.OWNERSERNO SUBSCRIBER_APP_ID,
                        TO_NUMBER(D.IDENTIFIERVALUE) SUBSCRIBER_ID,
                        E.SENSORSERNO METER_APP_ID,
                        TO_NUMBER(M.IDENTIFIERVALUE) METER_ID,
                        TO_DATE(TO_CHAR(ENDEXDATE), ''YYYYMMDDHH24MISS'') ENDEX_DATETIME,
                        TRUNC(TO_DATE(TO_CHAR(ENDEXDATE), ''YYYYMMDDHH24MISS'')) ENDEX_DATE,
                        M.MULTIPLIER,
                        E.TSUM * M.MULTIPLIER TOTAL_ENDEX,
                        E.T1ENDEX * M.MULTIPLIER T1_ENDEX,
                        E.T2ENDEX * M.MULTIPLIER T2_ENDEX,
                        E.T3ENDEX * M.MULTIPLIER T3_ENDEX,
                        E.T4ENDEX * M.MULTIPLIER T4_ENDEX,
                        E.REACTIVECAPASITIVE REACTIVE_CAPASITIVE_ENDEX,
                        E.REACTIVEINDUCTIVE REACTIVE_INDUCTIVE_ENDEX,
                        E.MAXDEMAND MAX_DEMAND,
                        TO_CHAR(E.DEMANDDATE) DEMAND_DATE,
                        E.ENDEXTYPE ENDEX_TYPE,
                        E.RECORDTYPE RECORD_TYPE,
                        1 APPLICATION_ID
                  --  SELECT *
                  FROM ODS_OSOS_ADM.TMP_RDD_DAILYENDEXES E 
                  INNER JOIN ODS_OSOS_SDM.VAL_DEFINITIONITEMS D ON (E.OWNERSERNO = D.SERNO) AND D.DEFINITIONTYPESERNO IN( 2,9,11)
                  INNER JOIN ODS_OSOS_SDM.VAL_DEFINITIONITEMS M ON (E.SENSORSERNO = M.SERNO) AND M.DEFINITIONTYPESERNO = 4
                  LEFT JOIN DWH_EDW.SUBSCRIBER_MULTIPLIER_HOSTORY M ON 
                      (M.SUBSCRIBER_ID = D.IDENTIFIERVALUE) AND 
                      TRUNC(E.ENDEXDATE / 1000000) BETWEEN CASE WHEN  M.BACK_CREATE_DATE = 999999999999 AND M.CREATE_DATE = 999999999999 THEN 0 ELSE BACK_CREATE_DATE END AND M.CREATE_DATE-1
                  --WHERE OWNERSERNO = 222124
                  WHERE RECORDTYPE = 1
                  -- AND OWNERSERNO = 425998

                  UNION ALL

                  SELECT /*+PARALLEL(128) */
                           SUBSCRIBER_APP_ID,
                           TO_NUMBER(SUBSCRIBER_ID),
                           METER_APP_ID,
                           TO_NUMBER(METER_ID),
                           ENDEX_DATETIME,
                           ENDEX_DATE,
                           MULTIPLIER,
                           TOTAL_ENDEX,
                           T1_ENDEX,
                           T2_ENDEX,
                           T3_ENDEX,
                           T4_ENDEX,
                           0 REACTIVE_CAPASITIVE_ENDEX,
                           0 REACTIVE_INDUCTIVE_ENDEX,
                           0 MAX_DEMAND,
                           ''0'' DEMAND_DATE,
                           0 ENDEX_TYPE,
                           1 RECORD_TYPE,
                           3 APPLICATION_ID
                    -- SELECT COUNT(*) 
                  FROM ODS_OSOS_HAYEN.F_MRR_DAILY_ENDEXES_HAYEN
                  WHERE TRIM(TRANSLATE(SUBSCRIBER_ID,''0123456789'', '' '')) IS NULL 

                 UNION ALL 

                SELECT /*+PARALLEL(128) */ 
                  ABONE_ID,
                  ABONE_NO,
                  SAYAC_ID,
                  SAYAC_NO,
                  OKUMATARIHSAAT,
                  OKUMATARIH,
                  SAYACCARPANI,
                  TNUMERIK,
                  T1NUMERIK,
                  T2NUMERIK,
                  T3NUMERIK,
                  T3NUMERIK,
                 0 REACTIVE_CAPASITIVE_ENDEX,
                  0 REACTIVE_INDUCTIVE_ENDEX,
                  0 MAX_DEMAND,
                  ''0'' DEMAND_DATE,
                  0 ENDEX_TYPE,
                  1 RECORD_TYPE,
                  2 APPLICATION_ID 
                FROM
                (
                    SELECT /*+ PARALLEL (OB1,128) PARALLEL (OB2,128) */ 
                         OB1.ABONE_ID,
                         TO_NUMBER(A.ABONENO) ABONE_NO,
                         OB1.SAYAC_ID,
                         CASE WHEN TRIM(TRANSLATE(OB3.SAYACSERINUMARASI,''0123456789'', '' '')) IS NOT NULL THEN 0 ELSE TO_NUMBER(OB3.SAYACSERINUMARASI) END SAYAC_NO,
                         OB1.OKUMATARIH OKUMATARIHSAAT,
                         TRUNC(OB1.OKUMATARIH) OKUMATARIH,
                         OB1.SAYACCARPANI,
                         OB1.TNUMERIK * OB1.SAYACCARPANI TNUMERIK,
                         OB2.T1NUMERIK * OB1.SAYACCARPANI T1NUMERIK,
                         OB2.T2NUMERIK * OB1.SAYACCARPANI T2NUMERIK,
                         OB2.T3NUMERIK * OB1.SAYACCARPANI T3NUMERIK,
                         OB2.T4NUMERIK * OB1.SAYACCARPANI T4NUMERIK,
                         ROW_NUMBER() OVER (PARTITION BY OB1.ABONE_ID, OB1.SAYAC_ID, TRUNC(OB1.OKUMATARIH) ORDER BY OB1.OKUMATARIH) AS RN
                    -- SELECT *
                    FROM ODS_OSOS_LUNA.OBISOKUMA1 OB1
                    LEFT JOIN ODS_OSOS_LUNA.OBISOKUMA2 OB2 ON (OB1.OBISOKUMAID = OB2.OBISOKUMAID)
                    LEFT JOIN ODS_OSOS_LUNA.OBISOKUMA3 OB3 ON (OB1.OBISOKUMAID = OB3.OBISOKUMAID)
                    INNER JOIN DWH_ODS.LUNA_ABONE A ON (A.ABONEID = OB1.ABONE_ID)
                    WHERE 
                      OB1.OKUMATIPI IN (11) AND 
                      --OB1.ABONE_ID=10143 AND
                      TRIM(TRANSLATE(A.ABONENO,''0123456789'', '' '')) IS NULL
                )
                WHERE RN = 1                  
          ) 
       )

       ';
      
 
--      PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE('DWH_DM', 'K2_ENDEXES_CONSUMPTIONS');
--      
--     
--      EXECUTE IMMEDIATE '
--        INSERT /*+APPEND NOLOGGING */ INTO DWH_DM.K2_ENDEXES_CONSUMPTIONS (AMR_INSTALLATION_ID,
--                                                METER_SERIAL_NUMBER,
--                                                MULTIPLIER,
--                                                READDATE,
--                                                ENDEXDATE,
--                                                TOTALENDEX,
--                                                T1ENDEX,
--                                                T2ENDEX,
--                                                T3ENDEX,
--                                                SAYACETSOKOD,
--                                                SAYACTESISAD)
--        SELECT /*+parallel (E,16)*/
--           E.SUBSCRIBER_ID AMR_INSTALLATION_ID,
--           METER_ID METER_SERIAL_NUMBER,
--           MULTIPLIER,
--           ENDEX_DATETIME READDATE,
--           ENDEX_DATE ENDEXDATE,
--           TOTAL_ENDEX TOTALENDEX,
--           T1_ENDEX T1ENDEX,
--           T2_ENDEX T2ENDEX,
--           T3_ENDEX T3ENDEX,
--           SAYACETSOKOD,
--           SAYACTESISAD
--        --  SELECT E.*
--        FROM DWH_EDW.F_MRR_DAILY_ENDEXES E
--        JOIN DWH_ODS.K2_K3_ON_LISTE L ON (to_number(L.SAYACABONENO) = E.SUBSCRIBER_ID AND IS_NUMERIC(L.SAYACABONENO)=1)
--        WHERE RECORD_TYPE = 1 AND ENDEX_TYPE = 0
--    ';
--    
-- 
--      
--      COMMIT;
--      
--      
--      UPDATE DWH_DM.REPORT_LAST_UPDATE_DATE  SET UPDATE_DATE = SYSDATE 
--      WHERE REPORT_NAME = 'K2 Osos Tüketimi Endeks Bilgileri- Oracle' AND ID =2 ;
--      
--      COMMIT;
--
--        DWH_CONFIG.PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE('DWH_ODS','K2_K3_ON_LISTE');
--        
--        EXECUTE IMMEDIATE 'TRUNCATE TABLE DWH_ODS.K2_K3_ON_LISTE';
--     
--        INSERT /*APPEND NOLOGGING */ INTO  DWH_ODS.K2_K3_ON_LISTE
--        (
--            SAYACETSOKOD, 
--            SAYACABONENO, 
--            SAYACTESISAD, 
--            SAYACOKUYANKURUM, 
--            ST_TIP, 
--            SATICI_ADI, 
--            GONDEREN
--       )
--        SELECT /*+PARALLEL(16) */
--            SAYACETSOKOD, 
--            SAYACABONENO, 
--            SAYACTESISAD, 
--            SAYACOKUYANKURUM, 
--            ST_TIP, 
--            SATICI_ADI, 
--            GONDEREN 
--        FROM EKSIM.K2_K3_ON_LISTE@DBLINKORCLMBS;    
--   
    COMMIT;

END;
--BEGIN
--      PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE('DWH_EDW', 'F_MRR_DAILY_ENDEXES');
--      
--      EXECUTE IMMEDIATE '
--  
--  
--      INSERT  /*+ append nologging PARALLEL(64)*/ INTO DWH_EDW.F_MRR_DAILY_ENDEXES
--        (SUBSCRIBER_APP_ID, SUBSCRIBER_ID, METER_APP_ID, METER_ID, ENDEX_DATETIME, ENDEX_DATE, MULTIPLIER, TOTAL_ENDEX, BACK_TOTAL_ENDEX, BACK_ENDEX_DATE, 
--         T1_ENDEX, T2_ENDEX, T3_ENDEX, T4_ENDEX, REACTIVE_CAPASITIVE_ENDEX, REACTIVE_INDUCTIVE_ENDEX, MAX_DEMAND, DEMAND_DATE, ENDEX_TYPE, RECORD_TYPE, 
--         APPLICATION_ID, CONSUMPTION_VALUE, MINUTE_DIFFERENCE, MINUTE_ENDEX_CONSUMPTION) 
--  
--  
--  
--      SELECT /*+PARALLEL(64) */ 
--         SUBSCRIBER_APP_ID, SUBSCRIBER_ID, METER_APP_ID, METER_ID, 
--         ENDEX_DATETIME, ENDEX_DATE, MULTIPLIER, TOTAL_ENDEX, BACK_TOTAL_ENDEX, BACK_ENDEX_DATE, 
--         T1_ENDEX, T2_ENDEX, T3_ENDEX, T4_ENDEX, REACTIVE_CAPASITIVE_ENDEX, REACTIVE_INDUCTIVE_ENDEX, 
--         MAX_DEMAND, DEMAND_DATE, ENDEX_TYPE, RECORD_TYPE, 
--         APPLICATION_ID,
--         
--         (NVL(TOTAL_ENDEX,0) - NVL(BACK_TOTAL_ENDEX,0)) CONSUMPTION_VALUE,
--         
--         ( BACK_ENDEX_DATE - ENDEX_DATETIME)*1440 MINUTE_DIFFERENCE,
--         
--         (NVL(TOTAL_ENDEX,0) - NVL(BACK_TOTAL_ENDEX,0))/
--          DECODE(
--            ( BACK_ENDEX_DATE - ENDEX_DATETIME)*1440
--            ,0
--            ,1,
--            ( BACK_ENDEX_DATE - ENDEX_DATETIME)*1440
--           )  AS MINUTE_ENDEX_CONSUMPTION
--      FROM
--         (
--
--              SELECT /*+PARALLEL(64) */ 
--                       SUBSCRIBER_APP_ID,
--                       SUBSCRIBER_ID,
--                       METER_APP_ID,
--                       METER_ID,
--                       ENDEX_DATETIME,
--                       ENDEX_DATE,
--                       MULTIPLIER,
--                       TOTAL_ENDEX,
--                       NVL(LAG(TOTAL_ENDEX) OVER (PARTITION BY SUBSCRIBER_ID, METER_ID, RECORD_TYPE, ENDEX_TYPE ORDER BY ENDEX_DATETIME),0) BACK_TOTAL_ENDEX,
--                       NVL(LAG(ENDEX_DATETIME) OVER (PARTITION BY SUBSCRIBER_ID, METER_ID, RECORD_TYPE, ENDEX_TYPE ORDER BY ENDEX_DATETIME),ENDEX_DATETIME) BACK_ENDEX_DATE,
--                       T1_ENDEX,
--                       T2_ENDEX,
--                       T3_ENDEX,
--                       T4_ENDEX,
--                       REACTIVE_CAPASITIVE_ENDEX,
--                       REACTIVE_INDUCTIVE_ENDEX,
--                       MAX_DEMAND,
--                       DEMAND_DATE,
--                       ENDEX_TYPE,
--                       RECORD_TYPE,
--                       APPLICATION_ID
--              FROM
--              (
--                  SELECT /*+PARALLEL(64) */ 
--                        E.OWNERSERNO SUBSCRIBER_APP_ID,
--                        TO_NUMBER(D.IDENTIFIERVALUE) SUBSCRIBER_ID,
--                        E.SENSORSERNO METER_APP_ID,
--                        TO_NUMBER(M.IDENTIFIERVALUE) METER_ID,
--                        TO_DATE(TO_CHAR(ENDEXDATE), ''YYYYMMDDHH24MISS'') ENDEX_DATETIME,
--                        TRUNC(TO_DATE(TO_CHAR(ENDEXDATE), ''YYYYMMDDHH24MISS'')) ENDEX_DATE,
--                        M.MULTIPLIER,
--                        E.TSUM * M.MULTIPLIER TOTAL_ENDEX,
--                        E.T1ENDEX * M.MULTIPLIER T1_ENDEX,
--                        E.T2ENDEX * M.MULTIPLIER T2_ENDEX,
--                        E.T3ENDEX * M.MULTIPLIER T3_ENDEX,
--                        E.T4ENDEX * M.MULTIPLIER T4_ENDEX,
--                        E.REACTIVECAPASITIVE REACTIVE_CAPASITIVE_ENDEX,
--                        E.REACTIVEINDUCTIVE REACTIVE_INDUCTIVE_ENDEX,
--                        E.MAXDEMAND MAX_DEMAND,
--                        TO_CHAR(E.DEMANDDATE) DEMAND_DATE,
--                        E.ENDEXTYPE ENDEX_TYPE,
--                        E.RECORDTYPE RECORD_TYPE,
--                        1 APPLICATION_ID
--                  --  SELECT *
--                  FROM ODS_OSOS_ADM.TMP_RDD_DAILYENDEXES E 
--                  INNER JOIN ODS_OSOS_SDM.VAL_DEFINITIONITEMS D ON (E.OWNERSERNO = D.SERNO) AND D.DEFINITIONTYPESERNO = 2
--                  INNER JOIN ODS_OSOS_SDM.VAL_DEFINITIONITEMS M ON (E.SENSORSERNO = M.SERNO) AND M.DEFINITIONTYPESERNO = 4
--                  LEFT JOIN DWH_EDW.SUBSCRIBER_MULTIPLIER_HOSTORY M ON 
--                      (M.SUBSCRIBER_ID = D.IDENTIFIERVALUE) AND 
--                      TRUNC(E.ENDEXDATE / 1000000) BETWEEN CASE WHEN  M.BACK_CREATE_DATE = 999999999999 AND M.CREATE_DATE = 999999999999 THEN 0 ELSE BACK_CREATE_DATE END AND M.CREATE_DATE-1
--                  --WHERE OWNERSERNO = 222124
--                  WHERE RECORDTYPE = 1
--                  -- AND OWNERSERNO = 425998
--
--                  UNION ALL
--
--                  SELECT /*+PARALLEL(16) */
--                           SUBSCRIBER_APP_ID,
--                           TO_NUMBER(SUBSCRIBER_ID),
--                           METER_APP_ID,
--                           TO_NUMBER(METER_ID),
--                           ENDEX_DATETIME,
--                           ENDEX_DATE,
--                           MULTIPLIER,
--                           TOTAL_ENDEX,
--                           T1_ENDEX,
--                           T2_ENDEX,
--                           T3_ENDEX,
--                           T4_ENDEX,
--                           0 REACTIVE_CAPASITIVE_ENDEX,
--                           0 REACTIVE_INDUCTIVE_ENDEX,
--                           0 MAX_DEMAND,
--                           ''0'' DEMAND_DATE,
--                           0 ENDEX_TYPE,
--                           1 RECORD_TYPE,
--                           3 APPLICATION_ID
--                    -- SELECT COUNT(*) 
--                  FROM ODS_OSOS_HAYEN.F_MRR_DAILY_ENDEXES_HAYEN
--                  WHERE TRIM(TRANSLATE(SUBSCRIBER_ID,''0123456789'', '' '')) IS NULL 
--
--                 UNION ALL 
--
--                SELECT /*+PARALLEL(64) */ 
--                  ABONE_ID,
--                  ABONE_NO,
--                  SAYAC_ID,
--                  SAYAC_NO,
--                  OKUMATARIHSAAT,
--                  OKUMATARIH,
--                  SAYACCARPANI,
--                  TNUMERIK,
--                  T1NUMERIK,
--                  T2NUMERIK,
--                  T3NUMERIK,
--                  T3NUMERIK,
--                 0 REACTIVE_CAPASITIVE_ENDEX,
--                  0 REACTIVE_INDUCTIVE_ENDEX,
--                  0 MAX_DEMAND,
--                  ''0'' DEMAND_DATE,
--                  0 ENDEX_TYPE,
--                  1 RECORD_TYPE,
--                  2 APPLICATION_ID 
--                FROM
--                (
--                    SELECT /*+ PARALLEL (OB1,16) PARALLEL (OB2,16) */ 
--                         OB1.ABONE_ID,
--                         TO_NUMBER(A.ABONENO) ABONE_NO,
--                         OB1.SAYAC_ID,
--                         CASE WHEN TRIM(TRANSLATE(OB3.SAYACSERINUMARASI,''0123456789'', '' '')) IS NOT NULL THEN 0 ELSE TO_NUMBER(OB3.SAYACSERINUMARASI) END SAYAC_NO,
--                         OB1.OKUMATARIH OKUMATARIHSAAT,
--                         TRUNC(OB1.OKUMATARIH) OKUMATARIH,
--                         OB1.SAYACCARPANI,
--                         OB1.TNUMERIK * OB1.SAYACCARPANI TNUMERIK,
--                         OB2.T1NUMERIK * OB1.SAYACCARPANI T1NUMERIK,
--                         OB2.T2NUMERIK * OB1.SAYACCARPANI T2NUMERIK,
--                         OB2.T3NUMERIK * OB1.SAYACCARPANI T3NUMERIK,
--                         OB2.T4NUMERIK * OB1.SAYACCARPANI T4NUMERIK,
--                         ROW_NUMBER() OVER (PARTITION BY OB1.ABONE_ID, OB1.SAYAC_ID, TRUNC(OB1.OKUMATARIH) ORDER BY OB1.OKUMATARIH) AS RN
--                    -- SELECT *
--                    FROM ODS_OSOS_LUNA.OBISOKUMA1 OB1
--                    LEFT JOIN ODS_OSOS_LUNA.OBISOKUMA2 OB2 ON (OB1.OBISOKUMAID = OB2.OBISOKUMAID)
--                    LEFT JOIN ODS_OSOS_LUNA.OBISOKUMA3 OB3 ON (OB1.OBISOKUMAID = OB3.OBISOKUMAID)
--                    INNER JOIN DWH_ODS.LUNA_ABONE A ON (A.ABONEID = OB1.ABONE_ID)
--                    WHERE 
--                      OB1.OKUMATIPI IN (11) AND 
--                      --OB1.ABONE_ID=10143 AND
--                      TRIM(TRANSLATE(A.ABONENO,''0123456789'', '' '')) IS NULL
--                )
--                WHERE RN = 1                  
--          ) 
--       )
--
--       ';
--     
--    COMMIT;
--END;

PROCEDURE PRC_K2_CONSUMPTION AS
BEGIN

        DWH_CONFIG.PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE('DWH_ODS','K2_K3_ON_LISTE');
        
        EXECUTE IMMEDIATE 'TRUNCATE TABLE DWH_ODS.K2_K3_ON_LISTE';
     
        INSERT /*APPEND NOLOGGING */ INTO  DWH_ODS.K2_K3_ON_LISTE
        (
            SAYACETSOKOD, 
            SAYACABONENO, 
            SAYACTESISAD, 
            SAYACOKUYANKURUM, 
            ST_TIP, 
            SATICI_ADI, 
            GONDEREN
       )
        SELECT /*+PARALLEL(16) */
            SAYACETSOKOD, 
            SAYACABONENO, 
            SAYACTESISAD, 
            SAYACOKUYANKURUM, 
            ST_TIP, 
            SATICI_ADI, 
            GONDEREN 
        FROM EKSIM.K2_K3_ON_LISTE@DBLINKORCLMBS;

      PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE('DWH_DM', 'K2_ENDEXES_CONSUMPTIONS');
      
      COMMIT;
      
        INSERT /*+APPEND NOLOGGING */ INTO DWH_DM.K2_ENDEXES_CONSUMPTIONS (AMR_INSTALLATION_ID,
                                                METER_SERIAL_NUMBER,
                                                MULTIPLIER,
                                                READDATE,
                                                ENDEXDATE,
                                                TOTALENDEX,
                                                T1ENDEX,
                                                T2ENDEX,
                                                T3ENDEX,
                                                SAYACETSOKOD,
                                                SAYACTESISAD)
        SELECT /*+parallel (E,128)*/
           E.SUBSCRIBER_ID AMR_INSTALLATION_ID,
           METER_ID METER_SERIAL_NUMBER,
           MULTIPLIER,
           ENDEX_DATETIME READDATE,
           ENDEX_DATE ENDEXDATE,
           TOTAL_ENDEX TOTALENDEX,
           T1_ENDEX T1ENDEX,
           T2_ENDEX T2ENDEX,
           T3_ENDEX T3ENDEX,
           SAYACETSOKOD,
           SAYACTESISAD
        --  SELECT E.*
        FROM DWH_EDW.F_MRR_DAILY_ENDEXES E
        JOIN DWH_ODS.K2_K3_ON_LISTE L ON (to_number(L.SAYACABONENO) = E.SUBSCRIBER_ID AND IS_NUMERIC(L.SAYACABONENO)=1)
        WHERE RECORD_TYPE = 1 AND ENDEX_TYPE = 0;

      COMMIT;
      
      UPDATE DWH_DM.REPORT_LAST_UPDATE_DATE  SET UPDATE_DATE = SYSDATE 
      WHERE REPORT_NAME = 'K2 Osos Tüketimi Endeks Bilgileri- Oracle' AND ID =2 ;
      
      COMMIT;
END;
PROCEDURE PRC_AGRICULTURAL_IRRIGATION AS
BEGIN

DWH_CONFIG.PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE('DM','MRR_AGRICULTURAL_IRRIGATION_D');

INSERT /*+PARALLEL(128) NOLOGGING*/ INTO DM.MRR_AGRICULTURAL_IRRIGATION_D
SELECT /* +PARALLEL(E,128)*/
    M.MBS_PROVINCE,
--    M.AMR_INSTALLATION_ID,
--    E.METER_ID,
      E.ENDEX_DATE,
--    EXTRACT(MONTH FROM E.ENDEX_DATE) AY,
--    EXTRACT(YEAR FROM E.ENDEX_DATE) YIL,
    SUM(CONSUMPTION_VALUE) CONSUMPTION_VALUE,
    COUNT(DISTINCT E.SUBSCRIBER_ID) SUBSCRIBER_COUNT
--    MIN(E.TOTAL_ENDEX) FIRST_ENDEX,
--    MAX(E.TOTAL_ENDEX) LAST_ENDEX,
--    MIN(BACK_TOTAL_ENDEX) BACK_ENDEX_DATE,    
--    MAX(E.TOTAL_ENDEX) - MIN(BACK_TOTAL_ENDEX) ENDEX_FARK,
--    MIN(MULTIPLIER) MIN_MULTIPLIER,
--    MAX(MULTIPLIER) MAX_MULTIPLIER,    
--    MAX(TAKTAR) , 
--    MAX(SOKTAR)
-- SELECT *
FROM DWH_EDW.F_MRR_DAILY_ENDEXES E
INNER JOIN DWH_EDW.AMR_ALL_DETAILS_ARIL M ON TO_CHAR(M.AMR_INSTALLATION_ID)=E.SUBSCRIBER_ID
INNER JOIN
( SELECT 
      TESISATNO, 
      SAYACSERINO, 
      MIN(TO_DATE(TAKILMATARIHI,'YYYYMMDDHH24MISS')) TAKTAR, 
      (CASE WHEN MIN(SOKULMETARIHI) = 0 OR COUNT(*) = 1 THEN TO_DATE(20990307000000,'YYYYMMDDHH24MISS') ELSE TO_DATE(MAX(SOKULMETARIHI),'YYYYMMDDHH24MISS') END ) SOKTAR
    FROM  DWH_ODS.TMP_SAYACTARIHLERI 
    GROUP BY TESISATNO, SAYACSERINO
) S ON M.AMR_INSTALLATION_ID = S.TESISATNO AND METER_ID = SAYACSERINO AND E.BACK_ENDEX_DATE BETWEEN S.TAKTAR AND S.SOKTAR
WHERE 
E.ENDEX_DATE >= TO_DATE('20170101','YYYYMMDD')
--BACK_ENDEX_DATE >= TO_DATE('20161001','YYYYMMDD')
AND  M.SECTOR_INFO = 'Tarýmsal Sulama' 
AND CONSUMPTION_VALUE >0
--  AND (BACK_TOTAL_ENDEX = 0 AND ENDEX_DATETIME = BACK_ENDEX_DATE)
--  AND (     BACK_TOTAL_ENDEX <> 0 OR ENDEX_DATETIME <> BACK_ENDEX_DATE      )
--   GROUP BY M.MBS_PROVINCE, E.ENDEX_DATE
GROUP BY
    M.MBS_PROVINCE,
    E.ENDEX_DATE;
--    M.AMR_INSTALLATION_ID,
--    E.METER_ID,
--    EXTRACT(MONTH FROM E.ENDEX_DATE),
--    EXTRACT(YEAR FROM E.ENDEX_DATE)

DWH_CONFIG.PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE('DM','MRR_AGRICULTURAL_IRRA_COUNT_D');

INSERT /**PARALLEL(128) NOLOGGING*/ INTO DM.MRR_AGRICULTURAL_IRRA_COUNT_D
WITH TOTAL
AS
(
SELECT /* +PARALLEL(E,128)*/
    COUNT(DISTINCT E.SUBSCRIBER_ID) TOTAL_COUNT
FROM DWH_EDW.F_MRR_DAILY_ENDEXES E
    INNER JOIN DWH_EDW.AMR_ALL_DETAILS_ARIL M ON TO_CHAR(M.AMR_INSTALLATION_ID)=E.SUBSCRIBER_ID
    INNER JOIN
    (
    SELECT
      TESISATNO, 
      SAYACSERINO, 
      MIN(TO_DATE(TAKILMATARIHI,'YYYYMMDDHH24MISS')) TAKTAR, 
      (CASE WHEN MIN(SOKULMETARIHI) = 0 OR COUNT(*) = 1 THEN TO_DATE(20990307000000,'YYYYMMDDHH24MISS') ELSE TO_DATE(MAX(SOKULMETARIHI),'YYYYMMDDHH24MISS') END ) SOKTAR
    FROM DWH_ODS.TMP_SAYACTARIHLERI 
    GROUP BY TESISATNO, SAYACSERINO
    ) S ON M.AMR_INSTALLATION_ID = S.TESISATNO AND METER_ID = SAYACSERINO AND E.BACK_ENDEX_DATE BETWEEN S.TAKTAR AND S.SOKTAR
WHERE 
    E.ENDEX_DATE >= TO_DATE('20170101','YYYYMMDD')
    AND M.SECTOR_INFO = 'Tarýmsal Sulama' 
    AND CONSUMPTION_VALUE >0
)
SELECT /* +PARALLEL(E,128)*/
    M.MBS_PROVINCE,
    COUNT(DISTINCT E.SUBSCRIBER_ID) PROVINCE_COUNT,
    (SELECT /* +PARALLEL(E,128)*/ TOTAL_COUNT FROM TOTAL) TOTAL_COUNT
FROM DWH_EDW.F_MRR_DAILY_ENDEXES E
    INNER JOIN DWH_EDW.AMR_ALL_DETAILS_ARIL M ON TO_CHAR(M.AMR_INSTALLATION_ID)=E.SUBSCRIBER_ID
    INNER JOIN
    (
    SELECT
      TESISATNO, 
      SAYACSERINO, 
      MIN(TO_DATE(TAKILMATARIHI,'YYYYMMDDHH24MISS')) TAKTAR, 
      (CASE WHEN MIN(SOKULMETARIHI) = 0 OR COUNT(*) = 1 THEN TO_DATE(20990307000000,'YYYYMMDDHH24MISS') ELSE TO_DATE(MAX(SOKULMETARIHI),'YYYYMMDDHH24MISS') END ) SOKTAR
    FROM DWH_ODS.TMP_SAYACTARIHLERI 
    GROUP BY TESISATNO, SAYACSERINO
    ) S ON M.AMR_INSTALLATION_ID = S.TESISATNO AND METER_ID = SAYACSERINO AND E.BACK_ENDEX_DATE BETWEEN S.TAKTAR AND S.SOKTAR
WHERE 
    E.ENDEX_DATE >= TO_DATE('20170101','YYYYMMDD')
    AND M.SECTOR_INFO = 'Tarýmsal Sulama' 
    AND CONSUMPTION_VALUE >0
GROUP BY
    M.MBS_PROVINCE;
    
COMMIT;

      UPDATE DWH_DM.REPORT_LAST_UPDATE_DATE  SET UPDATE_DATE = SYSDATE 
      WHERE REPORT_NAME = 'TARIMSAL TUKETIM DASHBOARD' AND ID =141;
      
COMMIT;
END;
PROCEDURE PRC_CONSUMPTION_DATA AS
BEGIN
        PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE('DWH_EDW', 'F_MRR_CONSUMPTION_DAILY');
    

            INSERT /*+PARALLEL(128) NOLOGGING*/ INTO DWH_EDW.F_MRR_CONSUMPTION_DAILY
            SELECT /*+PARALLEL(128) NOLOGGING*/ 
                SUBSCRIBER_ID,
                TRUNC(PROFILE_DATE) PROFILE_DATE,
                --MULTIPLIER,
                SUM(RECEIVED_ENERGY_VALUE) RECEIVED_ENERGY_VALUE,
                SUM(DELIVERED_ENERGY_VALUE) DELIVERED_ENERGY_VALUE,
                APPLICATION_ID
            FROM DWH.OSOS_CONSUMPTION_HOURLY
            GROUP BY SUBSCRIBER_ID,TRUNC(PROFILE_DATE),MULTIPLIER,APPLICATION_ID;
        
      
      PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE('DWH_EDW', 'F_MRR_CONSUMPTION_MONTHLY');
      
      INSERT /*+PARALLEL(128) NOLOGGING*/ INTO DWH_EDW.F_MRR_CONSUMPTION_MONTHLY 
      (SUBSCRIBER_ID, PROFILE_DATE, RECEIVED_ENERGY_VALUE, DELIVERED_ENERGY_VALUE)
        SELECT /*+PARALLEL(128) NOLOGGING*/
            SUBSCRIBER_ID,
            TO_CHAR(PROFILE_DATE,'YYYYMM') PROFILE_DATE,
            SUM(RECEIVED_ENERGY_VALUE) RECEIVED_ENERGY_VALUE,
            SUM(DELIVERED_ENERGY_VALUE) DELIVERED_ENERGY_VALUE
        FROM DWH_EDW.F_MRR_CONSUMPTION_DAILY
        GROUP BY SUBSCRIBER_ID,TO_CHAR(PROFILE_DATE,'YYYYMM');
END;

PROCEDURE PRC_AGGRI_CROSSTAB AS
BEGIN

DWH_CONFIG.PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE('DM','MRR_AGRICULTURAL_IRRI_CROSSTAB');

INSERT /**PARALLEL(128) NOLOGGING*/ INTO DM.MRR_AGRICULTURAL_IRRI_CROSSTAB
SELECT 
MBS_PROVINCE, MBS_DISTRICT, AMR_INSTALLATION_ID, METER_ID, CONSUMPTION_VALUE, ENDEX_DATE, TOTAL_ENDEX, BACK_ENDEX_DATE, BACK_TOTAL_ENDEX, T1_ENDEX, T2_ENDEX, T3_ENDEX, T4_ENDEX, ENDEX_FARK, MULTIPLIER,
CASE 
    WHEN USTGRUP IS NOT NULL THEN USTGRUP
ELSE
    'Bireysel'
END TIP,
OSOS_INSTALL_DATE
FROM 
(SELECT /*+PARALLEL(E,128) */
    M.MBS_PROVINCE,
    M.MBS_DISTRICT,
    M.AMR_INSTALLATION_ID,
    E.METER_ID,
    SUM(CONSUMPTION_VALUE) CONSUMPTION_VALUE,
    E.ENDEX_DATE,
    E.TOTAL_ENDEX,
    E.BACK_ENDEX_DATE,
    E.BACK_TOTAL_ENDEX,    
    E.T1_ENDEX,
    E.T2_ENDEX,
    E.T3_ENDEX,
    E.T4_ENDEX,
    (E.TOTAL_ENDEX - BACK_TOTAL_ENDEX) ENDEX_FARK,
    E.MULTIPLIER,
    S.USTGRUP,
    M.OSOS_INSTALL_DATE
FROM DWH_EDW.F_MRR_DAILY_ENDEXES E
INNER JOIN DWH_EDW.AMR_ALL_DETAILS_ARIL M ON TO_CHAR(M.AMR_INSTALLATION_ID)=E.SUBSCRIBER_ID
INNER JOIN
( SELECT /*+PARALLEL(128)*/
      TESISATNO, 
      SAYACSERINO, 
      MIN(TO_DATE(TAKILMATARIHI,'YYYYMMDDHH24MISS')) TAKTAR, 
      (CASE WHEN MIN(SOKULMETARIHI) = 0 OR COUNT(*) = 1 THEN TO_DATE(20990307000000,'YYYYMMDDHH24MISS') ELSE TO_DATE(MAX(SOKULMETARIHI),'YYYYMMDDHH24MISS') END ) SOKTAR
    FROM  DWH_ODS.TMP_SAYACTARIHLERI 
    GROUP BY TESISATNO, SAYACSERINO
) S ON M.AMR_INSTALLATION_ID = S.TESISATNO AND METER_ID = SAYACSERINO AND E.BACK_ENDEX_DATE BETWEEN S.TAKTAR AND S.SOKTAR
LEFT JOIN DWH_ODS.CKS_SULAMA_BIRLIKLERI S ON (S.TESISAT_NO=E.SUBSCRIBER_ID)
WHERE 
E.ENDEX_DATE >= TO_DATE('20170101','YYYYMMDD')
AND  M.SECTOR_INFO = 'Tarýmsal Sulama' 
AND CONSUMPTION_VALUE >0
GROUP BY
    M.MBS_PROVINCE,
    M.MBS_DISTRICT,
    M.AMR_INSTALLATION_ID,
    E.METER_ID,
    E.ENDEX_DATE,
    E.TOTAL_ENDEX,
    E.BACK_ENDEX_DATE,
    E.BACK_TOTAL_ENDEX,    
    E.T1_ENDEX,
    E.T2_ENDEX,
    E.T3_ENDEX,
    E.T4_ENDEX,
    (E.TOTAL_ENDEX - BACK_TOTAL_ENDEX),
    E.MULTIPLIER,
    S.USTGRUP ,
    M.OSOS_INSTALL_DATE
);

COMMIT;

      UPDATE DWH_DM.REPORT_LAST_UPDATE_DATE  SET UPDATE_DATE = SYSDATE 
      WHERE REPORT_NAME = 'TARIMSAL TUKETIM CROSSTAB' AND ID =142;

COMMIT;

END;
PROCEDURE PRC_LIGTHNING_DAILY AS
BEGIN
DWH_CONFIG.PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE('DM','F_MRR_LIGTHING_DAILY');

INSERT /*+PARALLEL(64) NOLOGGING*/ INTO DM.F_MRR_LIGTHING_DAILY
SELECT  /*+ PARALLEL (O,64) */
    S.SUBSCRIBER_ID,
    O.MULTIPLIER,
    O.ENDEX_DATE,
    T.TARIFE_KODU TARIFF_CODE, 
    TRIM(T.TARIFE_KADI) TARIFF_NAME,
    S.PROVINCE AS PROVINCE,
    S.AREA_NAME AS DISTRICT,
    S.ADDRESS AS ADRESS,
    CASE WHEN O.CONSUMPTION_VALUE <> 0 THEN 0 ELSE TRUNC(SYSDATE) - ENDEX_DATE END AS NO_ACCESS_DAY_COUNT,
    T1_CONSUMPTION_VALUE T1,
    T2_CONSUMPTION_VALUE T2,
    T3_CONSUMPTION_VALUE T3,
    S.METER_NUMBER,
    S.LATITUDE,
   S.LONGITUDE
FROM DWH_EDW.F_MRR_DAILY_ENDEXES O
JOIN DWH_EDW.D_SUBSCRIBERS S ON (O.SUBSCRIBER_ID = S.SUBSCRIBER_ID)
JOIN ODS_MBS_DICLE.TARIFELER T ON(T.TARIFE_KODU = S.TARIFF_CODE)
WHERE 
ENDEX_DATE > TRUNC(SYSDATE )- 7
AND ENDEX_DATE <= trunc(SYSDATE)
AND 
T.HAZINE_ODEME_DUR =1;

COMMIT;

      UPDATE DWH_DM.REPORT_LAST_UPDATE_DATE  SET UPDATE_DATE = SYSDATE 
      WHERE REPORT_NAME = 'AYDINLATMA RAPORU' AND ID =161;

COMMIT;


END;
--PROCEDURE PRC_MERGE_CUTOFFS AS
--BEGIN
----        MERGE INTO DWH_EDW.D_MRR_AMR_CUTOFFS E
----        USING 
----        (
----            SELECT /*+PARALLEL(128) */
----                'A' APPLICATION_ID,
----                C.SERNO,
----                D.IDENTIFIERVALUE WIRINGNO,
----                C.OUTAGETYPE,
----                TO_DATE(C.STARTDATE,'YYYYMMDDHH24MISS') AS START_DATE,
----                CASE WHEN C.ENDDATE <> 0 THEN TO_DATE(C.ENDDATE,'YYYYMMDDHH24MISS') END AS END_DATE,
----                'A' RECORDSTATUS
----            FROM ODS_OSOS_ADM.EVT_OUTAGES C
----            LEFT JOIN ODS_OSOS_SDM.VAL_DEFINITIONITEMS D ON(C.OWNERSERNO=D.SERNO)
----        ) F
----        ON (E.SER_NO=F.SERNO AND E.APPLICATION_ID ='A' )
----        WHEN MATCHED THEN
----        UPDATE SET 
----            E.COFF_ENDDATE=F.END_DATE
----        WHEN NOT MATCHED THEN
----        INSERT (APPLICATION_ID,SER_NO,WIRINGNO,CUTOFFTYPE,COFF_STARTDATE,COFF_ENDDATE,RECORDSTATUS)
----        VALUES (F.APPLICATION_ID,F.SERNO,F.WIRINGNO,F.OUTAGETYPE,F.START_DATE,F.END_DATE,F.RECORDSTATUS);
--END;
END PCG_OSOS_ACTIVE_ENDEX;
/