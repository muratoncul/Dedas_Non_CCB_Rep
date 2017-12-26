CREATE OR REPLACE PACKAGE BODY DWH.PKG_OSOS_ACTIVE_ENDEX
IS
-------------------------------------------------------------------------------
--  Created By              : Murat ONCUL
--  Creation Date           : 25.10.2017
--  Last Modification Date  : 25.10.2017
--  Package Name            : PKG_OSOS_ACTIVE_ENDEX
--  Package Version         : 0.0.1
--  Package Description     : 
--
--  [Modification History]
-------------------------------------------------------------------------------

  -----------------------------------
  -- Initialize Log Variables      --
  -----------------------------------
  gv_job_module       constant varchar2(50)  := 'OSOS_ACTIVE_ENDEX';                   -- Job Module Name
  gv_pck              constant varchar2(50)  := 'PKG_OSOS_ACTIVE_ENDEX';               -- PLSQL Package Name
  gv_job_owner        constant varchar2(50)  := 'DWH';                                 -- Owner of the Job
  gv_proc             varchar2(100);                                                   -- Procedure Name

  gv_sql_errm         varchar2(4000);                                                  -- SQL Error Message
  gv_sql_errc         number;                                                          -- SQL Error Code
  gv_dyn_task         long := '';
  
  -- schemas
  gv_dwh_owner               constant varchar2(30) := 'DWH';
  gv_stg_owner               constant varchar2(30) := 'DWH';
  gv_stg_dm_owner            constant varchar2(30) := 'DM';
  gv_ods_owner               constant varchar2(30) := 'DWH_ODS';
  gv_edw_owner               constant varchar2(30) := 'DWH_EDW';
  gv_dwh_dm_owner            constant varchar2(30) := 'DWH_DM';
  gv_osos_adm_owner          constant varchar2(30) := 'ODS_OSOS_ADM';
  gv_osos_sdm_owner          constant varchar2(30) := 'ODS_OSOS_SDM';
  gv_osos_luna_owner         constant varchar2(30) := 'ODS_OSOS_LUNA';
  gv_osos_hayen_owner        constant varchar2(30) := 'ODS_OSOS_HAYEN';
  gv_eksim_owner             constant varchar2(30) := 'EKSIM';
  gv_ods_dicle_owner         constant varchar2(30) := 'ODS_MBS_DICLE';
  ------------------------------------------------------------------------------


    
  PROCEDURE PRC_TMP_RDD_DAILYENDEXES
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat ÖNCÜL
  --  Creation Date  : 03-11-2017
  --  Last Modification Date  : 03-11-2017
  --  Procedure Name : PRC_TMP_RDD_DAILYENDEXES
  --  Description    : Golden Gate üzerinden alýnan datalar birleþtiriliyor.
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'TMP_RDD_DAILYENDEXES';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'GG_ADM_RDD_DAILYENDEXES';
    v_src_table_02  varchar2(30) := 'GG_ADM_CURRENTENDEXES';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TMP_RDD_DAILYENDEXES';

    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
       CREATE TABLE  '||gv_stg_owner||'.'||v_table_name||' 
       PARALLEL 128 NOLOGGING COMPRESS
       AS         
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
       FROM '||gv_ods_owner||'.'||v_src_table_01||' DE
         INNER JOIN '||gv_ods_owner||'.'||v_src_table_02||' CE ON (DE.CURRENTENDEXSERNO = CE.SERNO)
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;

  
  PROCEDURE PRC_ARIL_ACTIVE_ENDEX
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat ÖNCÜL
  --  Creation Date  : 03-11-2017
  --  Last Modification Date  : 03-11-2017
  --  Procedure Name : PRC_ARIL_ACTIVE_ENDEX
  --  Description    : Aril uygulamasýnýn endeksleri alýnýyor.
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'T_ARIL_ACTIVE_ENDEX';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'TMP_RDD_DAILYENDEXES';
    v_src_table_02  varchar2(30) := 'VAL_DEFINITIONITEMS';
    v_src_table_03  varchar2(30) := 'VAL_DEFINITIONITEMS';
    v_src_table_04  varchar2(30) := 'SUBSCRIBER_MULTIPLIER_HOSTORY';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_ARIL_ACTIVE_ENDEX';

    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
      CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
      PARALLEL 128 NOLOGGING COMPRESS
      AS      
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
      FROM '||gv_stg_owner||'.'||v_src_table_01||' E 
      INNER JOIN '||gv_osos_sdm_owner||'.'||v_src_table_02||' D ON (E.OWNERSERNO = D.SERNO) AND D.DEFINITIONTYPESERNO IN( 2,9,11)
      INNER JOIN '||gv_osos_sdm_owner||'.'||v_src_table_03||' M ON (E.SENSORSERNO = M.SERNO) AND M.DEFINITIONTYPESERNO = 4
      LEFT JOIN '||gv_edw_owner||'.'||v_src_table_04||'       M ON (M.SUBSCRIBER_ID = D.IDENTIFIERVALUE) AND (TRUNC(E.ENDEXDATE / 1000000) BETWEEN CASE WHEN  M.BACK_CREATE_DATE = 999999999999 AND M.CREATE_DATE = 999999999999 THEN 0 ELSE BACK_CREATE_DATE END AND M.CREATE_DATE-1)
      WHERE RECORDTYPE = 1
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  
  
  PROCEDURE PRC_HAYEN_ACTIVE_ENDEX
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat ÖNCÜL
  --  Creation Date  : 03-11-2017
  --  Last Modification Date  : 03-11-2017
  --  Procedure Name : PRC_HAYEN_ACTIVE_ENDEX
  --  Description    : Hayen uygulamasýnýn endeksleri alýnýyor.
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'T_HAYEN_ACTIVE_ENDEX';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'F_MRR_DAILY_ENDEXES_HAYEN';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_HAYEN_ACTIVE_ENDEX';

    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
      CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
      PARALLEL 16 NOLOGGING COMPRESS
      AS      
      SELECT /*+PARALLEL(128) */
                SUBSCRIBER_APP_ID,
                TO_NUMBER(SUBSCRIBER_ID) SUBSCRIBER_ID,
                METER_APP_ID,
                TO_NUMBER(METER_ID) METER_ID,
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
      FROM '||gv_osos_hayen_owner||'.'||v_src_table_01||'
      WHERE TRIM(TRANSLATE(SUBSCRIBER_ID,''0123456789'', '' '')) IS NULL 
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  
  PROCEDURE PRC_LUNA_ACTIVE_ENDEX
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat ÖNCÜL
  --  Creation Date  : 03-11-2017
  --  Last Modification Date  : 03-11-2017
  --  Procedure Name : PRC_LUNA_ACTIVE_ENDEX
  --  Description    : Luna uygulamasýnýn endeksleri alýnýyor.
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'T_LUNA_ACTIVE_ENDEX';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'OBISOKUMA1';
    v_src_table_02  varchar2(30) := 'OBISOKUMA2';
    v_src_table_03  varchar2(30) := 'OBISOKUMA3';
    v_src_table_04  varchar2(30) := 'LUNA_ABONE';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_LUNA_ACTIVE_ENDEX';

    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
      CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
      PARALLEL 128 NOLOGGING COMPRESS
      AS      
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
        T4NUMERIK,
        0 REACTIVE_CAPASITIVE_ENDEX,
        0 REACTIVE_INDUCTIVE_ENDEX,
        0 MAX_DEMAND,
        ''0'' DEMAND_DATE,
        0 ENDEX_TYPE,
        1 RECORD_TYPE,
        2 APPLICATION_ID 
      FROM
      (
          SELECT /*+ PARALLEL (OB1,128) PARALLEL (OB2,128) PARALLEL (OB3,128) */ 
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
          FROM '||gv_osos_luna_owner||'.'||v_src_table_01||' OB1
          LEFT JOIN '||gv_osos_luna_owner||'.'||v_src_table_02||' OB2 ON (OB1.OBISOKUMAID = OB2.OBISOKUMAID)
          LEFT JOIN '||gv_osos_luna_owner||'.'||v_src_table_03||' OB3 ON (OB1.OBISOKUMAID = OB3.OBISOKUMAID)
          INNER JOIN '||gv_ods_owner||'.'||v_src_table_04||' A ON (A.ABONEID = OB1.ABONE_ID)
          WHERE 
            OB1.OKUMATIPI IN (11) AND 
            TRIM(TRANSLATE(A.ABONENO,''0123456789'', '' '')) IS NULL
      )
      WHERE RN = 1    
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  
  PROCEDURE PRC_UNION_ACTIVE_ENDEX
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat ÖNCÜL
  --  Creation Date  : 03-11-2017
  --  Last Modification Date  : 03-11-2017
  --  Procedure Name : PRC_UNION_ACTIVE_ENDEX
  --  Description    : Uygulamalara ait olan endeks bilgileri birleþtiriliyor.
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'T_UNION_ACTIVE_ENDEX';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'T_ARIL_ACTIVE_ENDEX';
    v_src_table_02  varchar2(30) := 'T_HAYEN_ACTIVE_ENDEX';
    v_src_table_03  varchar2(30) := 'T_LUNA_ACTIVE_ENDEX';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_UNION_ACTIVE_ENDEX';

    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);
  
    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
      CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
      PARALLEL 128 NOLOGGING COMPRESS
      AS      
      SELECT /*+PARALLEL(128) */ 
        SUBSCRIBER_APP_ID,
        SUBSCRIBER_ID,
        METER_APP_ID,
        METER_ID,
        ENDEX_DATETIME,
        ENDEX_DATE,
        MULTIPLIER,
        TOTAL_ENDEX,
        T1_ENDEX,
        T2_ENDEX,
        T3_ENDEX,
        T4_ENDEX,
        REACTIVE_CAPASITIVE_ENDEX,
        REACTIVE_INDUCTIVE_ENDEX,
        MAX_DEMAND,
        DEMAND_DATE,
        ENDEX_TYPE,
        RECORD_TYPE,
        APPLICATION_ID
      FROM 
      '||gv_stg_owner||'.'||v_src_table_01||'
      UNION ALL
      SELECT /*+PARALLEL(128) */ 
        SUBSCRIBER_APP_ID, 
        SUBSCRIBER_ID, 
        METER_APP_ID, 
        METER_ID, 
        ENDEX_DATETIME, 
        ENDEX_DATE, 
        MULTIPLIER,
        TOTAL_ENDEX, 
        T1_ENDEX, 
        T2_ENDEX, 
        T3_ENDEX, 
        T4_ENDEX, 
        REACTIVE_CAPASITIVE_ENDEX, 
        REACTIVE_INDUCTIVE_ENDEX, 
        MAX_DEMAND,
        DEMAND_DATE, 
        ENDEX_TYPE, 
        RECORD_TYPE, 
        APPLICATION_ID         
      FROM 
      '||gv_stg_owner||'.'||v_src_table_02||'
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
        REACTIVE_CAPASITIVE_ENDEX,
        REACTIVE_INDUCTIVE_ENDEX,
        MAX_DEMAND,
        DEMAND_DATE,
        ENDEX_TYPE,
        RECORD_TYPE,
        APPLICATION_ID       
      FROM 
      '||gv_stg_owner||'.'||v_src_table_03||'
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  
  PROCEDURE PRC_BACK_ENDEX
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat ÖNCÜL
  --  Creation Date  : 03-11-2017
  --  Last Modification Date  : 03-11-2017
  --  Procedure Name : PRC_BACK_ENDEX
  --  Description    : Endekslerin bir önceki deðeleri bulunuyor.
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'T_BACK_ENDEX';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'T_UNION_ACTIVE_ENDEX';
  BEGIN
    gv_proc := 'PRC_BACK_ENDEX';

    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);
      
    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
    PARALLEL 128 NOLOGGING COMPRESS
    AS 
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
      '||gv_stg_owner||'.'||v_src_table_01||'
      ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  
  PROCEDURE PRC_CONSUMPTION_ACTIVE_ENDEX
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat ÖNCÜL
  --  Creation Date  : 03-11-2017
  --  Last Modification Date  : 03-11-2017
  --  Procedure Name : PRC_CONSUMPTION_ACTIVE_ENDEX
  --  Description    : Endeksler üzerinden tüketim hesaplanýyor.
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'T_CONSUMPTION_ACTIVE_ENDEX';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'T_BACK_ENDEX';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_CONSUMPTION_ACTIVE_ENDEX';

    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
      CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
      PARALLEL 128 NOLOGGING COMPRESS
      AS 
      SELECT /*+PARALLEL(128) */ 
        SUBSCRIBER_APP_ID, 
        SUBSCRIBER_ID, 
        METER_APP_ID, 
        METER_ID, 
        ENDEX_DATETIME, 
        ENDEX_DATE, 
        MULTIPLIER, 
        TOTAL_ENDEX, 
        BACK_TOTAL_ENDEX, 
        BACK_ENDEX_DATE, 
        T1_ENDEX, 
        T2_ENDEX, 
        T3_ENDEX, 
        T4_ENDEX,
        BACK_T1_ENDEX,
        BACK_T2_ENDEX,
        BACK_T3_ENDEX,
        BACK_T4_ENDEX, 
        REACTIVE_CAPASITIVE_ENDEX, 
        REACTIVE_INDUCTIVE_ENDEX, 
        MAX_DEMAND, 
        DEMAND_DATE, 
        ENDEX_TYPE, 
        RECORD_TYPE, 
        APPLICATION_ID,
        (NVL(TOTAL_ENDEX,0) - NVL(BACK_TOTAL_ENDEX,0)) CONSUMPTION_VALUE,
        (NVL(T1_ENDEX,0) - NVL(BACK_T1_ENDEX,0)) T1_CONSUMPTION_VALUE,
        (NVL(T2_ENDEX,0) - NVL(BACK_T2_ENDEX,0)) T2_CONSUMPTION_VALUE,
        (NVL(T3_ENDEX,0) - NVL(BACK_T3_ENDEX,0)) T3_CONSUMPTION_VALUE,
        (NVL(T4_ENDEX,0) - NVL(BACK_T4_ENDEX,0)) T4_CONSUMPTION_VALUE,
        DATEDIFF(''minute'', BACK_ENDEX_DATE, ENDEX_DATETIME) MINUTE_DIFFERENCE,
        (NVL(TOTAL_ENDEX,0) - NVL(BACK_TOTAL_ENDEX,0))/DECODE(DATEDIFF(''minute'', BACK_ENDEX_DATE,ENDEX_DATETIME),0,1,DATEDIFF(''minute'', BACK_ENDEX_DATE, ENDEX_DATETIME)) AS MINUTE_ENDEX_CONSUMPTION
      FROM  '||gv_stg_owner||'.'||v_src_table_01||'
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
  
  PROCEDURE PRC_TRUNCATE_PARTITION
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat ÖNCÜL
  --  Creation Date  : 03-11-2017
  --  Last Modification Date  : 03-11-2017
  --  Procedure Name : PRC_TRUNCATE_PARTITION
  --  Description    : Tablo üzerindeki Aril ve Luna partitionlarý siliniyor.
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'F_MRR_DAILY_ENDEXES';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'T_CONSUMPTION_ACTIVE_ENDEX';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TRUNCATE_PARTITION';

    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);
    
    gv_dyn_task := '
      ALTER TABLE '||gv_stg_owner||'.'||v_table_name||' TRUNCATE PARTITION APP_ARIL';

    execute immediate gv_dyn_task;

    COMMIT;

    gv_dyn_task := '
      ALTER TABLE '||gv_stg_owner||'.'||v_table_name||' TRUNCATE PARTITION APP_LUNA';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;

  PROCEDURE PRC_F_MRR_DAILY_ENDEXES
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat ÖNCÜL
  --  Creation Date  : 03-11-2017
  --  Last Modification Date  : 03-11-2017
  --  Procedure Name : PRC_F_MRR_DAILY_ENDEXES
  --  Description    : Endeks tüketimleri ve endeks deðerleri tabloya basýlýyor.
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'F_MRR_DAILY_ENDEXES';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'T_CONSUMPTION_ACTIVE_ENDEX';
    ----------------------------------------------------------------------------
  BEGIN

    gv_proc := 'PRC_F_MRR_DAILY_ENDEXES';
    
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    gv_dyn_task := '

    INSERT --+PARALLEL(128) APPEND NOLOGGING
    INTO '||gv_stg_owner||'.'||v_table_name||'

    SELECT /*+PARALLEL(128) */  
      *
        FROM '||gv_stg_owner||'.'||v_src_table_01||'    
    ';

    execute immediate gv_dyn_task;

    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);

    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  
  PROCEDURE PRC_K2_SUBSCRIBER_LIST
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat ÖNCÜL
  --  Creation Date  : 03-11-2017
  --  Last Modification Date  : 03-11-2017
  --  Procedure Name : PRC_K2_SUBSCRIBER_LIST
  --  Description    : K2 abone listesi alýnýyor.
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'T_K2_K3_ON_LISTE';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'K2_K3_ON_LISTE@DBLINKORCLMBS';
    ----------------------------------------------------------------------------
  BEGIN

    gv_proc := 'PRC_K2_SUBSCRIBER_LIST';
    
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);
      
    plib.drop_table(gv_stg_owner, v_table_name);

    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||'
    PARALLEL 32 NOLOGGING
    AS
    SELECT /*+PARALLEL(16) */
        SAYACETSOKOD, 
        SAYACABONENO, 
        SAYACTESISAD, 
        SAYACOKUYANKURUM, 
        ST_TIP, 
        SATICI_ADI, 
        GONDEREN 
    FROM '||gv_eksim_owner||'.'||v_src_table_01||'  
    ';

    execute immediate gv_dyn_task;

    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);

    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  PROCEDURE PRC_K2_SUBSCRIBER_CONSUMPTION
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat ÖNCÜL
  --  Creation Date  : 03-11-2017
  --  Last Modification Date  : 03-11-2017
  --  Procedure Name : PRC_K2_SUBSCRIBER_CONSUMPTION
  --  Description    : K2 abonelerinin tüketimleri hesaplanýp tabloya basýlýyor.
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'K2_ENDEXES_CONSUMPTIONS';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'T_K2_K3_ON_LISTE';
    v_src_table_02  varchar2(30) := 'F_MRR_DAILY_ENDEXES';
    ----------------------------------------------------------------------------
  BEGIN

    gv_proc := 'PRC_K2_SUBSCRIBER_CONSUMPTION';
    
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);
      
    plib.drop_table(gv_stg_dm_owner, v_table_name);

    gv_dyn_task := '
    CREATE TABLE '||gv_stg_dm_owner||'.'||v_table_name||'
    PARALLEL 128
    NOLOGGING
    AS
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
      FROM '||gv_stg_owner||'.'||v_src_table_02||' E
      JOIN '||gv_dwh_owner||'.'||v_src_table_01||' L ON (to_number(L.SAYACABONENO) = E.SUBSCRIBER_ID AND IS_NUMERIC(L.SAYACABONENO)=1)
      WHERE RECORD_TYPE = 1 AND ENDEX_TYPE = 0
    ';

    execute immediate gv_dyn_task;

    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);

    commit;
    
    gv_dyn_task := '      
      UPDATE DWH_DM.REPORT_LAST_UPDATE_DATE  SET UPDATE_DATE = SYSDATE 
      WHERE REPORT_NAME = ''K2 Osos Tüketimi Endeks Bilgileri- Oracle'' AND ID =161
    ';
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);

    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  PROCEDURE PRC_AGRICULTURAL_IRRIGATION
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat ÖNCÜL
  --  Creation Date  : 03-11-2017
  --  Last Modification Date  : 03-11-2017
  --  Procedure Name : PRC_AGRICULTURAL_IRRIGATION
  --  Description    : Tarýmsal tüketimler hesaplanýyor.
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'MRR_AGRICULTURAL_IRRIGATION_D';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'AMR_ALL_DETAILS_ARIL';
    v_src_table_02  varchar2(30) := 'F_MRR_DAILY_ENDEXES';
    v_src_table_03  varchar2(30) := 'TMP_SAYACTARIHLERI';
    ----------------------------------------------------------------------------
  BEGIN

    gv_proc := 'PRC_AGRICULTURAL_IRRIGATION';
    
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);
      
    plib.drop_table(gv_stg_dm_owner, v_table_name);

    gv_dyn_task := '
    CREATE TABLE '||gv_stg_dm_owner||'.'||v_table_name||'
    PARALLEL 128
    NOLOGGING
    AS
    SELECT /* +PARALLEL(E,128)*/
        M.MBS_PROVINCE,
        E.ENDEX_DATE,
        SUM(CONSUMPTION_VALUE) CONSUMPTION_VALUE,
        COUNT(DISTINCT E.SUBSCRIBER_ID) SUBSCRIBER_COUNT
    FROM '||gv_stg_owner||'.'||v_src_table_02||' E
    INNER JOIN '||gv_edw_owner||'.'||v_src_table_01||' M ON TO_CHAR(M.AMR_INSTALLATION_ID)=E.SUBSCRIBER_ID
    INNER JOIN
    ( SELECT 
          TESISATNO, 
          SAYACSERINO, 
          MIN(TO_DATE(TAKILMATARIHI,''YYYYMMDDHH24MISS'')) TAKTAR, 
          (CASE WHEN MIN(SOKULMETARIHI) = 0 OR COUNT(*) = 1 THEN TO_DATE(20990307000000,''YYYYMMDDHH24MISS'') ELSE TO_DATE(MAX(SOKULMETARIHI),''YYYYMMDDHH24MISS'') END ) SOKTAR
      FROM  '||gv_ods_owner||'.'||v_src_table_03||' 
      GROUP BY TESISATNO, SAYACSERINO
    ) S ON M.AMR_INSTALLATION_ID = S.TESISATNO AND METER_ID = SAYACSERINO AND E.BACK_ENDEX_DATE BETWEEN S.TAKTAR AND S.SOKTAR
    WHERE 
    E.ENDEX_DATE >= TO_DATE(''20170101'',''YYYYMMDD'')
    AND  M.SECTOR_INFO = ''Tarýmsal Sulama''
    AND CONSUMPTION_VALUE >0
    GROUP BY
        M.MBS_PROVINCE,
        E.ENDEX_DATE
    ';

    execute immediate gv_dyn_task;

    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);

    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  PROCEDURE PRC_AGRICULTURAL_IRRIGATION_D
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat ÖNCÜL
  --  Creation Date  : 03-11-2017
  --  Last Modification Date  : 03-11-2017
  --  Procedure Name : PRC_AGRICULTURAL_IRRIGATION_D
  --  Description    : Tarýmsal tüketimi hesaplanan abone sayýlarý belirleniyor.
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'MRR_AGRICULTURAL_IRRA_COUNT_D';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'AMR_ALL_DETAILS_ARIL';
    v_src_table_02  varchar2(30) := 'F_MRR_DAILY_ENDEXES';
    v_src_table_03  varchar2(30) := 'TMP_SAYACTARIHLERI';
    ----------------------------------------------------------------------------
  BEGIN

    gv_proc := 'PRC_AGRICULTURAL_IRRIGATION_D';
    
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);
      
    plib.drop_table(gv_stg_dm_owner, v_table_name);

    gv_dyn_task := '
    CREATE TABLE '||gv_stg_dm_owner||'.'||v_table_name||'
    PARALLEL 128
    NOLOGGING
    AS
    WITH TOTAL AS
    (
    SELECT /* +PARALLEL(E,128)*/
        COUNT(DISTINCT E.SUBSCRIBER_ID) TOTAL_COUNT
    FROM '||gv_stg_owner||'.'||v_src_table_02||' E
        INNER JOIN '||gv_edw_owner||'.'||v_src_table_01||' M ON TO_CHAR(M.AMR_INSTALLATION_ID)=E.SUBSCRIBER_ID
        INNER JOIN
        (
        SELECT
          TESISATNO, 
          SAYACSERINO, 
          MIN(TO_DATE(TAKILMATARIHI,''YYYYMMDDHH24MISS'')) TAKTAR, 
          (CASE WHEN MIN(SOKULMETARIHI) = 0 OR COUNT(*) = 1 THEN TO_DATE(20990307000000,''YYYYMMDDHH24MISS'') ELSE TO_DATE(MAX(SOKULMETARIHI),''YYYYMMDDHH24MISS'') END ) SOKTAR
        FROM '||gv_ods_owner||'.'||v_src_table_03||'
        GROUP BY TESISATNO, SAYACSERINO
        ) S ON M.AMR_INSTALLATION_ID = S.TESISATNO AND METER_ID = SAYACSERINO AND E.BACK_ENDEX_DATE BETWEEN S.TAKTAR AND S.SOKTAR
    WHERE 
        E.ENDEX_DATE >= TO_DATE(''20170101'',''YYYYMMDD'')
        AND M.SECTOR_INFO = ''Tarýmsal Sulama'' 
        AND CONSUMPTION_VALUE >0
    )
    SELECT /* +PARALLEL(E,128)*/
        M.MBS_PROVINCE,
        COUNT(DISTINCT E.SUBSCRIBER_ID) PROVINCE_COUNT,
        (SELECT /* +PARALLEL(E,128)*/ TOTAL_COUNT FROM TOTAL) TOTAL_COUNT
    FROM '||gv_stg_owner||'.'||v_src_table_02||' E
        INNER JOIN '||gv_edw_owner||'.'||v_src_table_01||' M ON TO_CHAR(M.AMR_INSTALLATION_ID)=E.SUBSCRIBER_ID
        INNER JOIN
        (
        SELECT
          TESISATNO, 
          SAYACSERINO, 
          MIN(TO_DATE(TAKILMATARIHI,''YYYYMMDDHH24MISS'')) TAKTAR, 
          (CASE WHEN MIN(SOKULMETARIHI) = 0 OR COUNT(*) = 1 THEN TO_DATE(20990307000000,''YYYYMMDDHH24MISS'') ELSE TO_DATE(MAX(SOKULMETARIHI),''YYYYMMDDHH24MISS'') END ) SOKTAR
        FROM '||gv_ods_owner||'.'||v_src_table_03||'
        GROUP BY TESISATNO, SAYACSERINO
        ) S ON M.AMR_INSTALLATION_ID = S.TESISATNO AND METER_ID = SAYACSERINO AND E.BACK_ENDEX_DATE BETWEEN S.TAKTAR AND S.SOKTAR
    WHERE 
        E.ENDEX_DATE >= TO_DATE(''20170101'',''YYYYMMDD'')
        AND M.SECTOR_INFO = ''Tarýmsal Sulama'' 
        AND CONSUMPTION_VALUE >0
    GROUP BY
        M.MBS_PROVINCE
    ';
    
    execute immediate gv_dyn_task;

    COMMIT;

    gv_dyn_task := '
      UPDATE DWH_DM.REPORT_LAST_UPDATE_DATE  SET UPDATE_DATE = SYSDATE 
      WHERE REPORT_NAME = ''TARIMSAL TUKETIM DASHBOARD'' AND ID =141
    ';

    execute immediate gv_dyn_task;

    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);

    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;

  PROCEDURE PRC_CONSUMPTION_DAILY
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat ÖNCÜL
  --  Creation Date  : 03-11-2017
  --  Last Modification Date  : 03-11-2017
  --  Procedure Name : PRC_CONSUMPTION_DAILY
  --  Description    : YKP den alýnan saatlik tüketim verileri günlük tüketime dönüþtürülüyor.
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'F_MRR_CONSUMPTION_DAILY';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'OSOS_CONSUMPTION_HOURLY';
    ----------------------------------------------------------------------------
  BEGIN

    gv_proc := 'PRC_CONSUMPTION_DAILY';

    -- Initialize Log Variables
    plib.o_log :=
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);
      
    plib.drop_table(gv_stg_owner, v_table_name);

    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' PARALLEL 128 COMPRESS NOLOGGING AS
    SELECT /*+PARALLEL(128) NOLOGGING*/
        SUBSCRIBER_ID,
        TRUNC(PROFILE_DATE) PROFILE_DATE,
        --MULTIPLIER,
        SUM(RECEIVED_ENERGY_VALUE) RECEIVED_ENERGY_VALUE,
        SUM(DELIVERED_ENERGY_VALUE) DELIVERED_ENERGY_VALUE,
        APPLICATION_ID
    FROM '||gv_stg_owner||'.'||v_src_table_01||'
    GROUP BY SUBSCRIBER_ID,TRUNC(PROFILE_DATE),MULTIPLIER,APPLICATION_ID
    ';

    execute immediate gv_dyn_task;

    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);

    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  PROCEDURE PRC_CONSUMPTION_MONTHLY
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat ÖNCÜL
  --  Creation Date  : 03-11-2017
  --  Last Modification Date  : 03-11-2017
  --  Procedure Name : PRC_CONSUMPTION_MONTHLY
  --  Description    : Hesaplanan günlük tüketim verileri aylýk tüketime dönüþtürülüyor.
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'F_MRR_CONSUMPTION_MONTHLY';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'F_MRR_CONSUMPTION_DAILY';
    ----------------------------------------------------------------------------
  BEGIN

    gv_proc := 'PRC_CONSUMPTION_MONTHLY';

    -- Initialize Log Variables
    plib.o_log :=
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);
      
    plib.drop_table(gv_stg_owner, v_table_name);

    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' PARALLEL 128 COMPRESS NOLOGGING AS
    SELECT /*+PARALLEL(128) NOLOGGING*/
        SUBSCRIBER_ID,
        TRUNC(PROFILE_DATE) PROFILE_DATE,
        SUM(RECEIVED_ENERGY_VALUE) RECEIVED_ENERGY_VALUE,
        SUM(DELIVERED_ENERGY_VALUE) DELIVERED_ENERGY_VALUE,
        APPLICATION_ID
    FROM '||gv_stg_owner||'.'||v_src_table_01||'
    GROUP BY SUBSCRIBER_ID,TRUNC(PROFILE_DATE),APPLICATION_ID
    ';

    execute immediate gv_dyn_task;

    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);

    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  PROCEDURE PRC_AGGRI_CROSSTAB
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat ÖNCÜL
  --  Creation Date  : 03-11-2017
  --  Last Modification Date  : 03-11-2017
  --  Procedure Name : PRC_AGGRI_CROSSTAB
  --  Description    : Tarýmsal tüketim verileri crosstab e dönüþtürülüyor.
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'MRR_AGRICULTURAL_IRRI_CROSSTAB';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'F_MRR_DAILY_ENDEXES';
    v_src_table_02  varchar2(30) := 'AMR_ALL_DETAILS_ARIL';
    v_src_table_03  varchar2(30) := 'TMP_SAYACTARIHLERI';
    v_src_table_04  varchar2(30) := 'CKS_SULAMA_BIRLIKLERI';
    ----------------------------------------------------------------------------
  BEGIN

    gv_proc := 'PRC_AGGRI_CROSSTAB';

    -- Initialize Log Variables
    plib.o_log :=
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);
      
    plib.drop_table(gv_stg_dm_owner, v_table_name);

    gv_dyn_task := '
    CREATE TABLE '||gv_stg_dm_owner||'.'||v_table_name||' PARALLEL 128 NOLOGGING COMPRESS AS
    SELECT
    MBS_PROVINCE, MBS_DISTRICT, AMR_INSTALLATION_ID, METER_ID, CONSUMPTION_VALUE, ENDEX_DATE, TOTAL_ENDEX, BACK_ENDEX_DATE, BACK_TOTAL_ENDEX, T1_ENDEX, T2_ENDEX, T3_ENDEX, T4_ENDEX, ENDEX_FARK, MULTIPLIER,
    CASE
        WHEN USTGRUP IS NOT NULL THEN USTGRUP
    ELSE
        ''Bireysel''
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
    FROM '||gv_stg_owner||'.'||v_src_table_01||' E
    INNER JOIN '||gv_edw_owner||'.'||v_src_table_02||' M ON TO_CHAR(M.AMR_INSTALLATION_ID)=E.SUBSCRIBER_ID
    INNER JOIN
    ( SELECT /*+PARALLEL(128)*/
          TESISATNO,
          SAYACSERINO,
          MIN(TO_DATE(TAKILMATARIHI,''YYYYMMDDHH24MISS'')) TAKTAR,
          (CASE WHEN MIN(SOKULMETARIHI) = 0 OR COUNT(*) = 1 THEN TO_DATE(20990307000000,''YYYYMMDDHH24MISS'') ELSE TO_DATE(MAX(SOKULMETARIHI),''YYYYMMDDHH24MISS'') END ) SOKTAR
        FROM  '||gv_ods_owner||'.'||v_src_table_03||'
        GROUP BY TESISATNO, SAYACSERINO
    ) S ON M.AMR_INSTALLATION_ID = S.TESISATNO AND METER_ID = SAYACSERINO AND E.BACK_ENDEX_DATE BETWEEN S.TAKTAR AND S.SOKTAR
    LEFT JOIN '||gv_ods_owner||'.'||v_src_table_04||' S ON (S.TESISATNO=E.SUBSCRIBER_ID)
    WHERE
    E.ENDEX_DATE >= TO_DATE(''20170101'',''YYYYMMDD'')
    AND  M.SECTOR_INFO = ''Tar?msal Sulama''
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
        M.OSOS_INSTALL_DATE)
    ';

    execute immediate gv_dyn_task;

    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);

    commit;

    gv_dyn_task := '      UPDATE DWH_DM.REPORT_LAST_UPDATE_DATE  SET UPDATE_DATE = SYSDATE
                          WHERE REPORT_NAME = ''TARIMSAL TUKETIM CROSSTAB'' AND ID =142';
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);

    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
  PROCEDURE PRC_F_MRR_LIGTHING_DAILY
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat ÖNCÜL
  --  Creation Date  : 03-11-2017
  --  Last Modification Date  : 03-11-2017
  --  Procedure Name : PRC_F_MRR_LIGTHING_DAILY
  --  Description    : Aydýnlatma tüketimleri hesaplanýp tabloya yazýlýyor.
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'F_MRR_LIGTHING_DAILY';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'F_MRR_DAILY_ENDEXES';
    v_src_table_02  varchar2(30) := 'D_SUBSCRIBERS';
    v_src_table_03  varchar2(30) := 'TARIFELER';
    ----------------------------------------------------------------------------
  BEGIN

    gv_proc := 'PRC_F_MRR_LIGTHING_DAILY';

    -- Initialize Log Variables
    plib.o_log :=
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);
      
    plib.drop_table(gv_stg_dm_owner, v_table_name);

    gv_dyn_task := '
    CREATE TABLE '||gv_stg_dm_owner||'.'||v_table_name||' PARALLEL 128 NOLOGGING COMPRESS AS
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
    FROM '||gv_edw_owner||'.'||v_src_table_01||' O
    JOIN '||gv_edw_owner||'.'||v_src_table_02||' S ON (O.SUBSCRIBER_ID = S.SUBSCRIBER_ID)
    JOIN '||gv_ods_dicle_owner||'.'||v_src_table_03||' T ON(T.TARIFE_KODU = S.TARIFF_CODE)
    WHERE 
    ENDEX_DATE > TRUNC(SYSDATE )- 7
    AND ENDEX_DATE <= trunc(SYSDATE)
    AND 
    T.HAZINE_ODEME_DUR =1
    ';

    execute immediate gv_dyn_task;

    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);

    commit;

    gv_dyn_task := '      
      UPDATE DWH_DM.REPORT_LAST_UPDATE_DATE  SET UPDATE_DATE = SYSDATE 
      WHERE REPORT_NAME = ''AYDINLATMA RAPORU'' AND ID =161
    ';
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);

    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;
END;
/