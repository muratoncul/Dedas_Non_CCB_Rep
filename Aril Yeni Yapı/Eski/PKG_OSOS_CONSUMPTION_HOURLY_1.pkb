CREATE OR REPLACE PACKAGE BODY DWH.PKG_OSOS_CONSUMPTION_HOURLY
IS
-------------------------------------------------------------------------------
--  Created By              : Murat �NC�L
--  Creation Date           : 17.10.2017
--  Last Modification Date  : 17.10.2017
--  Package Name            : PKG_OSOS_CONSUMPTION_HOURLY
--  Package Version         : 0.0.1
--  Package Description     : 
--
--  [Modification History]
-------------------------------------------------------------------------------

  -----------------------------------
  -- Initialize Log Variables      --
  -----------------------------------
  gv_job_module       constant varchar2(50)  := 'CONSUMPTION_HOURLY';                  -- Job Module Name
  gv_pck              constant varchar2(50)  := 'PKG_OSOS_CONSUMPTION_HOURLY';         -- PLSQL Package Name
  gv_job_owner        constant varchar2(50)  := 'DWH';                                 -- Owner of the Job
  gv_proc             varchar2(100);                                                   -- Procedure Name

  gv_sql_errm         varchar2(4000);                                                  -- SQL Error Message
  gv_sql_errc         number;                                                          -- SQL Error Code
  gv_dyn_task         long := '';
  
  -- schemas
  gv_dwh_owner               constant varchar2(30) := 'DWH';
  gv_stg_owner               constant varchar2(30) := 'DWH';
  gv_ods_owner               constant varchar2(30) := 'DWH_ODS';
  gv_edw_owner               constant varchar2(30) := 'DWH_EDW';
  gv_osos_adm_owner          constant varchar2(30) := 'ODS_OSOS_ADM';
  gv_osos_sdm_owner          constant varchar2(30) := 'ODS_OSOS_SDM';
  gv_osos_luna_owner         constant varchar2(30) := 'ODS_OSOS_LUNA';
  gv_osos_hayen_owner        constant varchar2(30) := 'ODS_OSOS_HAYEN';
  ------------------------------------------------------------------------------


    
  PROCEDURE PRC_CON_SUBSCRIBER
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat �NC�L
  --  Creation Date  : 03-11-2017
  --  Last Modification Date  : 03-11-2017
  --  Procedure Name : PRC_CON_SUBSCRIBER
  --  Description    : Aril sistemindeki abone t�ketimleri al�n�yor
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'T_CON_SUBSCRIBER';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'GG_CON_SUBSCRIBER';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_CON_SUBSCRIBER';

    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
      CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
      PARALLEL 128 NOLOGGING COMPRESS
      AS      
      SELECT /*+ PARALLEL (128) */
          SERNO,
          OWNERSERNO,
          PROFILEDATE PROFILEDATETIME,
          TO_DATE(TO_CHAR(ROUND(PROFILEDATE/1000000)),''YYYYMMDD'') PROFILEDATE,
          TSUM,
          TSUMOUT,
          MULTIPLIER
      FROM 
        '||gv_ods_owner||'.'||v_src_table_01||' 
      WHERE 
          PROFILESTATUS IN (3,4,6)      
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

  
  PROCEDURE PRC_CON_SUBSTATION
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat �NC�L
  --  Creation Date  : 03-11-2017
  --  Last Modification Date  : 03-11-2017
  --  Procedure Name : PRC_CON_SUBSTATION
  --  Description    : Aril sistemindeki trafo t�ketimleri al�n�yor
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'T_CON_SUBSTATION';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'GG_ADM_CON_SUBSTATION';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_CON_SUBSTATION';

    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
      CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
      PARALLEL 128 NOLOGGING COMPRESS
      AS      
      SELECT /*+ PARALLEL (128) */
          SERNO,
          OWNERSERNO,
          PROFILEDATE PROFILEDATETIME,
          TO_DATE(TO_CHAR(ROUND(PROFILEDATE/1000000)),''YYYYMMDD'') PROFILEDATE,
          TSUM,
          TSUMOUT,
          MULTIPLIER
      FROM 
        '||gv_ods_owner||'.'||v_src_table_01||'
      WHERE 
          PROFILESTATUS IN (3,4,6)
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
  
  
  PROCEDURE PRC_CON_LIGTHING
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat �NC�L
  --  Creation Date  : 03-11-2017
  --  Last Modification Date  : 03-11-2017
  --  Procedure Name : PRC_CON_LIGTHING
  --  Description    : Aril sistemindeki ayd�nlatma t�ketimleri al�n�yor
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'T_CON_LIGHTING';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'GG_ADM_CON_LIGHTING';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_CON_LIGTHING';

    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
      CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
      PARALLEL 16 NOLOGGING COMPRESS
      AS      
      SELECT /*+ PARALLEL (16) */
          SERNO,
          OWNERSERNO,
          PROFILEDATE PROFILEDATETIME,
          TO_DATE(TO_CHAR(ROUND(PROFILEDATE/1000000)),''YYYYMMDD'') PROFILEDATE,
          TSUM,
          TSUMOUT,
          MULTIPLIER
      FROM 
          '||gv_ods_owner||'.'||v_src_table_01||'
      WHERE 
          PROFILESTATUS IN (3,4,6)
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
  
  PROCEDURE PRC_UNION_CONSUMPTION
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat �NC�L
  --  Creation Date  : 03-11-2017
  --  Last Modification Date  : 03-11-2017
  --  Procedure Name : PRC_UNION_CONSUMPTION
  --  Description    : Aril sisteminden al�nan abone, trafo ve ayd�nlatma t�ketimleri birle�tiriliyor
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'T_OSOS_CONSUMPTION';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'T_CON_SUBSCRIBER';
    v_src_table_02  varchar2(30) := 'T_CON_SUBSTATION';
    v_src_table_03  varchar2(30) := 'T_CON_LIGHTING';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_UNION_CONSUMPTION';

    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
      CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
      PARALLEL 128 NOLOGGING COMPRESS
      AS      
      SELECT
        SERNO,
        OWNERSERNO,
        PROFILEDATETIME,
        PROFILEDATE,
        TSUM,
        TSUMOUT,
        MULTIPLIER
      FROM 
        '||gv_stg_owner||'.'||v_src_table_01||'
      UNION ALL
      SELECT
        SERNO,
        OWNERSERNO,
        PROFILEDATETIME,
        PROFILEDATE,
        TSUM,
        TSUMOUT,
        MULTIPLIER
      FROM 
        '||gv_stg_owner||'.'||v_src_table_02||'
      UNION ALL
      SELECT
        SERNO,
        OWNERSERNO,
        PROFILEDATETIME,
        PROFILEDATE,
        TSUM,
        TSUMOUT,
        MULTIPLIER
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
  
  PROCEDURE PRC_TRUNCATE_PARTITION_ARIL
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat �NC�L
  --  Creation Date  : 03-11-2017
  --  Last Modification Date  : 03-11-2017
  --  Procedure Name : PRC_TRUNCATE_PARTITION_ARIL
  --  Description    : T�ketim tablosundaki aril ve luna sistemlerine ait partition lar truncate ediliyor.
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'OSOS_CONSUMPTION_HOURLY_T';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TRUNCATE_PARTITION_ARIL';

    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);
    
    gv_dyn_task := '
      ALTER TABLE '||gv_stg_owner||'.'||v_table_name||' TRUNCATE PARTITION APP_ARIL';

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
  
  PROCEDURE PRC_TRUNCATE_PARTITION_LUNA
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat �NC�L
  --  Creation Date  : 03-11-2017
  --  Last Modification Date  : 03-11-2017
  --  Procedure Name : PRC_TRUNCATE_PARTITION_LUNA
  --  Description    : T�ketim tablosundaki aril ve luna sistemlerine ait partition lar truncate ediliyor.
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'OSOS_CONSUMPTION_HOURLY_T';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_TRUNCATE_PARTITION_LUNA';

    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);
    
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
  
  PROCEDURE PRC_CON_ARIL
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat �NC�L
  --  Creation Date  : 03-11-2017
  --  Last Modification Date  : 03-11-2017
  --  Procedure Name : PRC_CON_ARIL
  --  Description    : Aril sisteminden al�narak birle�tirilen t�ketimler, abone bazl� olarak d�zenleniyor.
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'OSOS_CONSUMPTION_HOURLY_ARIL';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'T_OSOS_CONSUMPTION';
    v_src_table_02  varchar2(30) := 'VAL_DEFINITIONITEMS';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_CON_ARIL';

    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
      CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
      PARALLEL 128 NOLOGGING COMPRESS
      AS      
      SELECT
          /*+ PARALLEL(C,128 ) */
          T.IDENTIFIERVALUE SUBSCRIBER_ID, 
          C.OWNERSERNO OWNER_SERNO, 
          C.PROFILEDATETIME PROFILE_DATETIME,
          C.PROFILEDATE PROFILE_DATE,
          C.TSUM * NVL(C.MULTIPLIER,1) RECEIVED_ENERGY_VALUE,
          C.TSUMOUT * NVL(C.MULTIPLIER,1) DELIVERED_ENERGY_VALUE,
          NVL(C.MULTIPLIER,1) MULTIPLIER,
          1 APPLICATION_ID,
          1 ADD_TYPE
      FROM  '||gv_stg_owner||'.'||v_src_table_01||' C 
      INNER JOIN '||gv_osos_sdm_owner||'.'||v_src_table_02||' T 
          ON (C.OWNERSERNO = T.SERNO)
      WHERE 
          LENGTH(TRIM(TRANSLATE(T.IDENTIFIERVALUE, ''+-0123456789'', '' ''))) IS NULL
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
  
  PROCEDURE PRC_CON_LUNA
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat �NC�L
  --  Creation Date  : 03-11-2017
  --  Last Modification Date  : 03-11-2017
  --  Procedure Name : PRC_CON_LUNA
  --  Description    : Luna sisteminden t�ketimler al�n�yor.
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'OSOS_CONSUMPTION_HOURLY_LUNA';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'OKUMASAATLIK';
    v_src_table_02  varchar2(30) := 'LUNA_ABONE';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_CON_LUNA';

    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
      CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
      PARALLEL 128 NOLOGGING COMPRESS
      AS      
      SELECT /*+PARALLEL(G,128) */ 
          TO_NUMBER(A.ABONENO) SUBSCRIBER_ID,
          0 OWNER_SERNO, 
          TO_NUMBER(''20''||G.PARTITIONID) PROFILE_DATETIME,
          TO_DATE((''20''||G.PARTITIONID),''YYYYMMDDHH24MISS'') PROFILE_DATE,
          G.TUKETIM RECEIVED_ENERGY_VALUE,
          0 DELIVERED_ENERGY_VALUE,
          G.CARPAN MULTIPLIER,
          2 APPLICATION_ID, 
          G.DURUM ADD_TYPE
      FROM '||gv_osos_luna_owner||'.'||v_src_table_01||' G
      INNER JOIN '||gv_ods_owner||'.'||v_src_table_02||' A 
          ON A.ABONEID=G.ABONE_ID AND A.SAYAC_ID=G.SAYAC_ID
      WHERE DWH_CONFIG.IS_NUMERIC(A.ABONENO) = 1 AND LENGTH(G.PARTITIONID)=12
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

  PROCEDURE PRC_CON_HAYEN
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat �NC�L
  --  Creation Date  : 03-11-2017
  --  Last Modification Date  : 03-11-2017
  --  Procedure Name : PRC_CON_HAYEN
  --  Description    : Hayen sisteminden t�ketimler al�n�yor
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'OSOS_CONSUMPTION_HOURLY_HAYEN';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'CONSUMPTION_HOURLY';
    ----------------------------------------------------------------------------
  BEGIN

    gv_proc := 'PRC_CON_HAYEN';
    
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);

    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||'
    PARALLEL 128 NOLOGGING COMPRESS
    AS
    SELECT /*+PARALLEL(128) */  
        WIRINGNO SUBSCRIBER_ID, 
        ID_WIRING OWNER_SERNO, 
        TO_CHAR(DT_PROFILE, ''YYYYMMDDHH24MISS'') PROFILE_DATETIME, 
        TRUNC(DT_PROFILE) PROFILE_DATE, 
        MT_LOAD_PROFILE * NVL(MULTIPLIER,1) RECEIVED_ENERGY_VALUE, 
        MT_NEG_LOAD_PROFILE * NVL(MULTIPLIER,1) DELIVERED_ENERGY_VALUE, 
        NVL(MULTIPLIER,1) MULTIPLIER, 
        3 APPLICATION_ID, 
        1 ADD_TYPE
    FROM '||gv_osos_hayen_owner||'.'||v_src_table_01||' 
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
  
  PROCEDURE PRC_OSOS_CONSUMPTION_HOURLY
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat �NC�L
  --  Creation Date  : 03-11-2017
  --  Last Modification Date  : 03-11-2017
  --  Procedure Name : PRC_OSOS_CONSUMPTION_HOURLY
  --  Description    : T�ketim tablosuna aril ve luna t�ketimleri insert ediliyor.
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'OSOS_CONSUMPTION_HOURLY_T';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'OSOS_CONSUMPTION_HOURLY_ARIL';
    v_src_table_02  varchar2(30) := 'OSOS_CONSUMPTION_HOURLY_LUNA';
    v_src_table_03  varchar2(30) := 'OSOS_CONSUMPTION_HOURLY_HAYEN';
    ----------------------------------------------------------------------------
  BEGIN

    gv_proc := 'PRC_OSOS_CONSUMPTION_HOURLY';
    
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    gv_dyn_task := '
    INSERT --+PARALLEL(128) APPEND NOLOGGING 
    INTO '||gv_stg_owner||'.'||v_table_name||'
    SELECT --+PARALLEL(128) 
        TO_NUMBER(SUBSCRIBER_ID), 
        OWNER_SERNO, 
        PROFILE_DATETIME, 
        PROFILE_DATE, 
        RECEIVED_ENERGY_VALUE, 
        DELIVERED_ENERGY_VALUE, 
        MULTIPLIER, 
        APPLICATION_ID, 
        ADD_TYPE
    FROM '||gv_stg_owner||'.'||v_src_table_01||'  
    UNION ALL  
    SELECT --+PARALLEL(128) 
        TO_NUMBER(SUBSCRIBER_ID), 
        OWNER_SERNO, 
        PROFILE_DATETIME, 
        PROFILE_DATE, 
        RECEIVED_ENERGY_VALUE, 
        DELIVERED_ENERGY_VALUE, 
        MULTIPLIER, 
        APPLICATION_ID, 
        ADD_TYPE
    FROM '||gv_stg_owner||'.'||v_src_table_02||'  
--    UNION ALL  
--    SELECT --+PARALLEL(128) 
--        TO_NUMBER(SUBSCRIBER_ID), 
--        OWNER_SERNO, 
--        PROFILE_DATETIME, 
--        PROFILE_DATE, 
--        RECEIVED_ENERGY_VALUE, 
--        DELIVERED_ENERGY_VALUE, 
--        MULTIPLIER, 
--        APPLICATION_ID, 
--        ADD_TYPE
--    FROM '||gv_stg_owner||'.'||v_src_table_03||'  
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
  PROCEDURE PRC_OSOS_GES_CONSUMPTION
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat �NC�L
  --  Creation Date  : 03-11-2017
  --  Last Modification Date  : 03-11-2017
  --  Procedure Name : PRC_OSOS_CONSUMPTION_HOURLY
  --  Description    : T�ketim tablosuna aril ve luna t�ketimleri insert ediliyor.
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'OSOS_GES_CONSUMPTION';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(70) := 'MASTER_SISTEM_KULLANIM@DBLINKORCLMBS';
    v_src_table_02  varchar2(30) := 'D_SUBSCRIBERS';
    v_src_table_03  varchar2(30) := 'OSOS_CONSUMPTION_HOURLY';
    ----------------------------------------------------------------------------
  BEGIN

    gv_proc := 'PRC_OSOS_GES_CONSUMPTION';
    
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    gv_dyn_task := '
    SELECT /*+PARALLEL (H,128) NOLOGGING*/ 
        M.TESISAT_NO,
        S.NAME,
        H.PROFILE_DATETIME,
        H.RECEIVED_ENERGY_VALUE,
        H.DELIVERED_ENERGY_VALUE 
    FROM '||v_src_table_01||' M 
    INNER JOIN '||gv_edw_owner||'.'||v_src_table_02||' S ON  (M.TESISAT_NO=S.SUBSCRIBER_ID)
    INNER JOIN '||gv_stg_owner||'.'||v_src_table_03||' H ON (M.TESISAT_NO=H.SUBSCRIBER_ID)
    WHERE M.LISANS_DUR=1 AND TO_CHAR(H.PROFILE_DATE,''YYYYMMDD'')>=''20160601''
    ';

    execute immediate gv_dyn_task;

    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);

    commit;

    gv_dyn_task := '
    UPDATE DWH_DM.REPORT_LAST_UPDATE_DATE  SET UPDATE_DATE = SYSDATE 
    WHERE REPORT_NAME = ''GES Veri�/�eki� Miktarlar�'' AND ID =101
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

END;
/