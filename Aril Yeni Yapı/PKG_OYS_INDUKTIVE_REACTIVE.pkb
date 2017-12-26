CREATE OR REPLACE PACKAGE BODY DWH.DWH.PKG_OYS_INDUKTIVE_REACTIVE
IS
-------------------------------------------------------------------------------
--  Created By              : Murat ONCUL
--  Creation Date           : 21.12.2017
--  Last Modification Date  : 21.12.2017
--  Package Name            : PKG_OYS_INDUKTIVE_REACTIVE
--  Package Version         : 0.0.1
--  Package Description     : 
--
--  [Modification History]
-------------------------------------------------------------------------------

  -----------------------------------
  -- Initialize Log Variables      --
  -----------------------------------
  gv_job_module       constant varchar2(50)  := 'OYS_INDUKTIVE_REACTIVE';                   -- Job Module Name
  gv_pck              constant varchar2(50)  := 'PKG_OYS_INDUKTIVE_REACTIVE';               -- PLSQL Package Name
  gv_job_owner        constant varchar2(50)  := 'DWH';                                 -- Owner of the Job
  gv_proc             varchar2(100);                                                   -- Procedure Name

  gv_sql_errm         varchar2(4000);                                                  -- SQL Error Message
  gv_sql_errc         number;                                                          -- SQL Error Code
  gv_dyn_task         long := '';
  
  -- schemas
  gv_dwh_ods_owner           constant varchar2(30) := 'DWH_ODS';
  gv_stg_owner               constant varchar2(30) := 'DWH';
  ------------------------------------------------------------------------------
    
  PROCEDURE PRC_OYS_INDUKTIVE_REACTIVE
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat ONCUL
  --  Creation Date  : 21-12-2017
  --  Last Modification Date  : 21-12-2017
  --  Procedure Name : PRC_OYS_INDUKTIVE_REACTIVE
  --  Description    : Analizör sisteminden gelen induktif ve reaktif değerlerinin takibi
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'INV_REACTIVE_DATA';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'EDW_BASE_DATA';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_OYS_INDUKTIVE_REACTIVE';

    -- Initialize Log Variables
    PL.LOGGER := logtype.init(gv_pck ||'.'||gv_proc);

    PL.DROP_TABLE(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
CREATE TABLE '||gv_stg_owner||'.'||v_table_name||' 
NOLOGGING PARALLEL 128 AS
WITH TAB AS 
            (
                SELECT 
                     CASE WHEN KUNDENNAME LIKE ''%SIIRT%'' THEN ''SIIRT''
                     WHEN KUNDENNAME LIKE ''%URFA%'' THEN ''SANLIURFA''
                     WHEN KUNDENNAME LIKE ''%DIYARBAKIR%'' THEN ''DIYARBAKIR''
                     WHEN KUNDENNAME LIKE ''%BATMAN%'' THEN ''BATMAN''
                     WHEN KUNDENNAME LIKE ''%MARDIN%'' THEN ''MARDIN'' 
                     WHEN KUNDENNAME LIKE ''%SIRNAK%'' THEN ''SIRNAK'' END PROVINCE,
                    TO_DATE(DATUM,''DD.MM.YY'') MEASUREMENT_DATE,
                    0 SAAT,
                     DECODE(NVL(A.KUNDENNUMMER,0),0,SUBSTR(LINIE,1,INSTR(LINIE,''_'')-2) ,A.KUNDENNUMMER) KUNDENNUMMER,
                    substr(LINIE,-7,3) LINIE,
                     LINIE LINIE_NAME,
                    substr(ZPB,-5,4) OLCUM_TIPI,
                    EINDEUTIGELINR OLCUM_NOKTASI_ID,  
                    MVM_00_15 TIME_VAL_15,
                    MVM_00_30 TIME_VAL_30,
                    MVM_00_45 TIME_VAL_45,
                    MVM_00_60 TIME_VAL_60,
                    MC_00_15 TIME_CONTOL_VAL_15,
                    MC_00_30 TIME_CONTOL_VAL_30, 
                    MC_00_45 TIME_CONTOL_VAL_45,
                    MC_00_60 TIME_CONTOL_VAL_60
                FROM   '||gv_dwh_ods_owner||'.'||v_src_table_01||' A
                UNION ALL
                SELECT 
                      CASE WHEN KUNDENNAME LIKE ''%SIIRT%'' THEN ''SIIRT''
                     WHEN KUNDENNAME LIKE ''%URFA%'' THEN ''SANLIURFA''
                     WHEN KUNDENNAME LIKE ''%DIYARBAKIR%'' THEN ''DIYARBAKIR''
                     WHEN KUNDENNAME LIKE ''%BATMAN%'' THEN ''BATMAN''
                     WHEN KUNDENNAME LIKE ''%MARDIN%'' THEN ''MARDIN'' 
                     WHEN KUNDENNAME LIKE ''%SIRNAK%'' THEN ''SIRNAK'' END PROVINCE,
                    TO_DATE(DATUM,''DD.MM.YY'') MEASUREMENT_DATE,
                    1 SAAT,
                    DECODE(NVL(A.KUNDENNUMMER,0),0,SUBSTR(LINIE,1,INSTR(LINIE,''_'')-2) ,A.KUNDENNUMMER) KUNDENNUMMER,
                    substr(LINIE,-7,3) LINIE,
                    LINIE LINIE_NAME,
                    substr(ZPB,-5,4) OLCUM_TIPI,
                    EINDEUTIGELINR OLCUM_NOKTASI_ID,  
                    MVM_01_15,
                    MVM_01_30,
                    MVM_01_45,
                    MVM_01_60,
                    MC_01_15,
                    MC_01_30,
                    MC_01_45,
                    MC_01_60
                FROM   '||gv_dwh_ods_owner||'.'||v_src_table_01||' A
                UNION ALL
                SELECT 
                           CASE WHEN KUNDENNAME LIKE ''%SIIRT%'' THEN ''SIIRT''
                     WHEN KUNDENNAME LIKE ''%URFA%'' THEN ''SANLIURFA''
                     WHEN KUNDENNAME LIKE ''%DIYARBAKIR%'' THEN ''DIYARBAKIR''
                     WHEN KUNDENNAME LIKE ''%BATMAN%'' THEN ''BATMAN''
                     WHEN KUNDENNAME LIKE ''%MARDIN%'' THEN ''MARDIN'' 
                     WHEN KUNDENNAME LIKE ''%SIRNAK%'' THEN ''SIRNAK'' END PROVINCE,
                    TO_DATE(DATUM,''DD.MM.YY'') MEASUREMENT_DATE,
                    2 SAAT,
                    DECODE(NVL(A.KUNDENNUMMER,0),0,SUBSTR(LINIE,1,INSTR(LINIE,''_'')-2) ,A.KUNDENNUMMER) KUNDENNUMMER,
                    substr(LINIE,-7,3) LINIE,
                    LINIE LINIE_NAME,
                    substr(ZPB,-5,4) OLCUM_TIPI,
                    EINDEUTIGELINR OLCUM_NOKTASI_ID,  
                    MVM_02_15,
                    MVM_02_30,
                    MVM_02_45,
                    MVM_02_60,
                    MC_02_15,
                    MC_02_30,
                    MC_02_45,
                    MC_02_60
                FROM   '||gv_dwh_ods_owner||'.'||v_src_table_01||'  A
                UNION ALL
                SELECT 
                           CASE WHEN KUNDENNAME LIKE ''%SIIRT%'' THEN ''SIIRT''
                     WHEN KUNDENNAME LIKE ''%URFA%'' THEN ''SANLIURFA''
                     WHEN KUNDENNAME LIKE ''%DIYARBAKIR%'' THEN ''DIYARBAKIR''
                     WHEN KUNDENNAME LIKE ''%BATMAN%'' THEN ''BATMAN''
                     WHEN KUNDENNAME LIKE ''%MARDIN%'' THEN ''MARDIN'' 
                     WHEN KUNDENNAME LIKE ''%SIRNAK%'' THEN ''SIRNAK'' END PROVINCE,
                    TO_DATE(DATUM,''DD.MM.YY'') MEASUREMENT_DATE,
                    3 SAAT,
                    DECODE(NVL(A.KUNDENNUMMER,0),0,SUBSTR(LINIE,1,INSTR(LINIE,''_'')-2) ,A.KUNDENNUMMER) KUNDENNUMMER,
                    substr(LINIE,-7,3) LINIE,
                    LINIE LINIE_NAME,
                    substr(ZPB,-5,4) OLCUM_TIPI,
                    EINDEUTIGELINR OLCUM_NOKTASI_ID,  
                    MVM_03_15,
                    MVM_03_30,
                    MVM_03_45,
                    MVM_03_60,
                    MC_03_15,
                    MC_03_30,
                    MC_03_45,
                    MC_03_60
                FROM   '||gv_dwh_ods_owner||'.'||v_src_table_01||'  A
                UNION ALL
                SELECT 
                           CASE WHEN KUNDENNAME LIKE ''%SIIRT%'' THEN ''SIIRT''
                     WHEN KUNDENNAME LIKE ''%URFA%'' THEN ''SANLIURFA''
                     WHEN KUNDENNAME LIKE ''%DIYARBAKIR%'' THEN ''DIYARBAKIR''
                     WHEN KUNDENNAME LIKE ''%BATMAN%'' THEN ''BATMAN''
                     WHEN KUNDENNAME LIKE ''%MARDIN%'' THEN ''MARDIN'' 
                     WHEN KUNDENNAME LIKE ''%SIRNAK%'' THEN ''SIRNAK'' END PROVINCE,
                    TO_DATE(DATUM,''DD.MM.YY'') MEASUREMENT_DATE,
                    4 SAAT,
                     DECODE(NVL(A.KUNDENNUMMER,0),0,SUBSTR(LINIE,1,INSTR(LINIE,''_'')-2) ,A.KUNDENNUMMER) KUNDENNUMMER,
                    substr(LINIE,-7,3) LINIE,
                    LINIE LINIE_NAME,
                    substr(ZPB,-5,4) OLCUM_TIPI,
                    EINDEUTIGELINR OLCUM_NOKTASI_ID,  
                    MVM_04_15,
                    MVM_04_30,
                    MVM_04_45,
                    MVM_04_60,
                    MC_04_15,
                    MC_04_30,
                    MC_04_45,
                    MC_04_60
                FROM   '||gv_dwh_ods_owner||'.'||v_src_table_01||'  A
                UNION ALL
                SELECT 
                           CASE WHEN KUNDENNAME LIKE ''%SIIRT%'' THEN ''SIIRT''
                     WHEN KUNDENNAME LIKE ''%URFA%'' THEN ''SANLIURFA''
                     WHEN KUNDENNAME LIKE ''%DIYARBAKIR%'' THEN ''DIYARBAKIR''
                     WHEN KUNDENNAME LIKE ''%BATMAN%'' THEN ''BATMAN''
                     WHEN KUNDENNAME LIKE ''%MARDIN%'' THEN ''MARDIN'' 
                     WHEN KUNDENNAME LIKE ''%SIRNAK%'' THEN ''SIRNAK'' END PROVINCE,
                    TO_DATE(DATUM,''DD.MM.YY'') MEASUREMENT_DATE,
                    5 SAAT,
                     DECODE(NVL(A.KUNDENNUMMER,0),0,SUBSTR(LINIE,1,INSTR(LINIE,''_'')-2) ,A.KUNDENNUMMER) KUNDENNUMMER,
                    substr(LINIE,-7,3) LINIE,
                    LINIE LINIE_NAME,
                    substr(ZPB,-5,4) OLCUM_TIPI,
                    EINDEUTIGELINR OLCUM_NOKTASI_ID,  
                    MVM_05_15,
                    MVM_05_30,
                    MVM_05_45,
                    MVM_05_60,
                    MC_05_15,
                    MC_05_30,
                    MC_05_45,
                    MC_05_60
                FROM   '||gv_dwh_ods_owner||'.'||v_src_table_01||'  A
                UNION ALL
                SELECT 
                           CASE WHEN KUNDENNAME LIKE ''%SIIRT%'' THEN ''SIIRT''
                     WHEN KUNDENNAME LIKE ''%URFA%'' THEN ''SANLIURFA''
                     WHEN KUNDENNAME LIKE ''%DIYARBAKIR%'' THEN ''DIYARBAKIR''
                     WHEN KUNDENNAME LIKE ''%BATMAN%'' THEN ''BATMAN''
                     WHEN KUNDENNAME LIKE ''%MARDIN%'' THEN ''MARDIN'' 
                     WHEN KUNDENNAME LIKE ''%SIRNAK%'' THEN ''SIRNAK'' END PROVINCE,
                    TO_DATE(DATUM,''DD.MM.YY'') MEASUREMENT_DATE,
                    6 SAAT,
                     DECODE(NVL(A.KUNDENNUMMER,0),0,SUBSTR(LINIE,1,INSTR(LINIE,''_'')-2) ,A.KUNDENNUMMER) KUNDENNUMMER,
                    substr(LINIE,-7,3) LINIE,
                    LINIE LINIE_NAME,
                    substr(ZPB,-5,4) OLCUM_TIPI,
                    EINDEUTIGELINR OLCUM_NOKTASI_ID,  
                    MVM_06_15,
                    MVM_06_30,
                    MVM_06_45,
                    MVM_06_60,
                    MC_06_15,
                    MC_06_30,
                    MC_06_45,
                    MC_06_60
                FROM   '||gv_dwh_ods_owner||'.'||v_src_table_01||'  A
                UNION ALL
                SELECT 
                       CASE WHEN KUNDENNAME LIKE ''%SIIRT%'' THEN ''SIIRT''
                     WHEN KUNDENNAME LIKE ''%URFA%'' THEN ''SANLIURFA''
                     WHEN KUNDENNAME LIKE ''%DIYARBAKIR%'' THEN ''DIYARBAKIR''
                     WHEN KUNDENNAME LIKE ''%BATMAN%'' THEN ''BATMAN''
                     WHEN KUNDENNAME LIKE ''%MARDIN%'' THEN ''MARDIN'' 
                     WHEN KUNDENNAME LIKE ''%SIRNAK%'' THEN ''SIRNAK'' END PROVINCE,
                    TO_DATE(DATUM,''DD.MM.YY'') MEASUREMENT_DATE,
                    7 SAAT,
                     DECODE(NVL(A.KUNDENNUMMER,0),0,SUBSTR(LINIE,1,INSTR(LINIE,''_'')-2) ,A.KUNDENNUMMER) KUNDENNUMMER,
                    substr(LINIE,-7,3) LINIE,
                    LINIE LINIE_NAME,
                    substr(ZPB,-5,4) OLCUM_TIPI,
                    EINDEUTIGELINR OLCUM_NOKTASI_ID,  
                    MVM_07_15,
                    MVM_07_30,
                    MVM_07_45,
                    MVM_07_60,
                    MC_07_15,
                    MC_07_30,
                    MC_07_45,
                    MC_07_60
                FROM   '||gv_dwh_ods_owner||'.'||v_src_table_01||'  A
                UNION ALL
                SELECT 
                           CASE WHEN KUNDENNAME LIKE ''%SIIRT%'' THEN ''SIIRT''
                     WHEN KUNDENNAME LIKE ''%URFA%'' THEN ''SANLIURFA''
                     WHEN KUNDENNAME LIKE ''%DIYARBAKIR%'' THEN ''DIYARBAKIR''
                     WHEN KUNDENNAME LIKE ''%BATMAN%'' THEN ''BATMAN''
                     WHEN KUNDENNAME LIKE ''%MARDIN%'' THEN ''MARDIN'' 
                     WHEN KUNDENNAME LIKE ''%SIRNAK%'' THEN ''SIRNAK'' END PROVINCE,
                    TO_DATE(DATUM,''DD.MM.YY'') MEASUREMENT_DATE,
                    8 SAAT,
                     DECODE(NVL(A.KUNDENNUMMER,0),0,SUBSTR(LINIE,1,INSTR(LINIE,''_'')-2) ,A.KUNDENNUMMER) KUNDENNUMMER,
                    substr(LINIE,-7,3) LINIE,
                    LINIE LINIE_NAME,
                    substr(ZPB,-5,4) OLCUM_TIPI,
                    EINDEUTIGELINR OLCUM_NOKTASI_ID,  
                    MVM_08_15,
                    MVM_08_30,
                    MVM_08_45,
                    MVM_08_60,
                    MC_08_15,
                    MC_08_30,
                    MC_08_45,
                    MC_08_60
                FROM   '||gv_dwh_ods_owner||'.'||v_src_table_01||'  A
                UNION ALL
                SELECT 
                       CASE WHEN KUNDENNAME LIKE ''%SIIRT%'' THEN ''SIIRT''
                     WHEN KUNDENNAME LIKE ''%URFA%'' THEN ''SANLIURFA''
                     WHEN KUNDENNAME LIKE ''%DIYARBAKIR%'' THEN ''DIYARBAKIR''
                     WHEN KUNDENNAME LIKE ''%BATMAN%'' THEN ''BATMAN''
                     WHEN KUNDENNAME LIKE ''%MARDIN%'' THEN ''MARDIN'' 
                     WHEN KUNDENNAME LIKE ''%SIRNAK%'' THEN ''SIRNAK'' END PROVINCE,
                    TO_DATE(DATUM,''DD.MM.YY'') MEASUREMENT_DATE,
                    9 SAAT,
                     DECODE(NVL(A.KUNDENNUMMER,0),0,SUBSTR(LINIE,1,INSTR(LINIE,''_'')-2) ,A.KUNDENNUMMER) KUNDENNUMMER,
                    substr(LINIE,-7,3) LINIE,
                    LINIE LINIE_NAME,
                    substr(ZPB,-5,4) OLCUM_TIPI,
                    EINDEUTIGELINR OLCUM_NOKTASI_ID,  
                    MVM_09_15,
                    MVM_09_30,
                    MVM_09_45,
                    MVM_09_60,
                    MC_09_15,
                    MC_09_30,
                    MC_09_45,
                    MC_09_60
                FROM   '||gv_dwh_ods_owner||'.'||v_src_table_01||'  A
                UNION ALL
                SELECT 
                       CASE WHEN KUNDENNAME LIKE ''%SIIRT%'' THEN ''SIIRT''
                     WHEN KUNDENNAME LIKE ''%URFA%'' THEN ''SANLIURFA''
                     WHEN KUNDENNAME LIKE ''%DIYARBAKIR%'' THEN ''DIYARBAKIR''
                     WHEN KUNDENNAME LIKE ''%BATMAN%'' THEN ''BATMAN''
                     WHEN KUNDENNAME LIKE ''%MARDIN%'' THEN ''MARDIN'' 
                     WHEN KUNDENNAME LIKE ''%SIRNAK%'' THEN ''SIRNAK'' END PROVINCE,
                    TO_DATE(DATUM,''DD.MM.YY'') MEASUREMENT_DATE,
                    10 SAAT,
                    DECODE(NVL(A.KUNDENNUMMER,0),0,SUBSTR(LINIE,1,INSTR(LINIE,''_'')-2) ,A.KUNDENNUMMER) KUNDENNUMMER,
                    substr(LINIE,-7,3) LINIE,
                    LINIE LINIE_NAME,
                    substr(ZPB,-5,4) OLCUM_TIPI,
                    EINDEUTIGELINR OLCUM_NOKTASI_ID,  
                    MVM_10_15,
                    MVM_10_30,
                    MVM_10_45,
                    MVM_10_60,
                    MC_10_15,
                    MC_10_30,
                    MC_10_45,
                    MC_10_60
                FROM   '||gv_dwh_ods_owner||'.'||v_src_table_01||'  A
                UNION ALL
                SELECT 
                       CASE WHEN KUNDENNAME LIKE ''%SIIRT%'' THEN ''SIIRT''
                     WHEN KUNDENNAME LIKE ''%URFA%'' THEN ''SANLIURFA''
                     WHEN KUNDENNAME LIKE ''%DIYARBAKIR%'' THEN ''DIYARBAKIR''
                     WHEN KUNDENNAME LIKE ''%BATMAN%'' THEN ''BATMAN''
                     WHEN KUNDENNAME LIKE ''%MARDIN%'' THEN ''MARDIN'' 
                     WHEN KUNDENNAME LIKE ''%SIRNAK%'' THEN ''SIRNAK'' END PROVINCE,
                    TO_DATE(DATUM,''DD.MM.YY'') MEASUREMENT_DATE,
                    11 SAAT,
                     DECODE(NVL(A.KUNDENNUMMER,0),0,SUBSTR(LINIE,1,INSTR(LINIE,''_'')-2) ,A.KUNDENNUMMER) KUNDENNUMMER,
                    substr(LINIE,-7,3) LINIE,
                    LINIE LINIE_NAME,
                    substr(ZPB,-5,4) OLCUM_TIPI,
                    EINDEUTIGELINR OLCUM_NOKTASI_ID,  
                    MVM_11_15,
                    MVM_11_30,
                    MVM_11_45,
                    MVM_11_60,
                    MC_11_15,
                    MC_11_30,
                    MC_11_45,
                    MC_11_60
                FROM   '||gv_dwh_ods_owner||'.'||v_src_table_01||'  A
                UNION ALL
                SELECT 
                           CASE WHEN KUNDENNAME LIKE ''%SIIRT%'' THEN ''SIIRT''
                     WHEN KUNDENNAME LIKE ''%URFA%'' THEN ''SANLIURFA''
                     WHEN KUNDENNAME LIKE ''%DIYARBAKIR%'' THEN ''DIYARBAKIR''
                     WHEN KUNDENNAME LIKE ''%BATMAN%'' THEN ''BATMAN''
                     WHEN KUNDENNAME LIKE ''%MARDIN%'' THEN ''MARDIN'' 
                     WHEN KUNDENNAME LIKE ''%SIRNAK%'' THEN ''SIRNAK'' END PROVINCE,
                    TO_DATE(DATUM,''DD.MM.YY'') MEASUREMENT_DATE,
                    12 SAAT,
                     DECODE(NVL(A.KUNDENNUMMER,0),0,SUBSTR(LINIE,1,INSTR(LINIE,''_'')-2) ,A.KUNDENNUMMER) KUNDENNUMMER,
                    substr(LINIE,-7,3) LINIE,
                    LINIE LINIE_NAME,
                    substr(ZPB,-5,4) OLCUM_TIPI,
                    EINDEUTIGELINR OLCUM_NOKTASI_ID,  
                    MVM_12_15,
                   MVM_12_30,
                    MVM_12_45,
                    MVM_12_60,
                    MC_12_15,
                    MC_12_30,
                    MC_12_45,
                    MC_12_60
                FROM   '||gv_dwh_ods_owner||'.'||v_src_table_01||'  A
                UNION ALL
                SELECT 
                           CASE WHEN KUNDENNAME LIKE ''%SIIRT%'' THEN ''SIIRT''
                     WHEN KUNDENNAME LIKE ''%URFA%'' THEN ''SANLIURFA''
                     WHEN KUNDENNAME LIKE ''%DIYARBAKIR%'' THEN ''DIYARBAKIR''
                     WHEN KUNDENNAME LIKE ''%BATMAN%'' THEN ''BATMAN''
                     WHEN KUNDENNAME LIKE ''%MARDIN%'' THEN ''MARDIN'' 
                     WHEN KUNDENNAME LIKE ''%SIRNAK%'' THEN ''SIRNAK'' END PROVINCE,
                    TO_DATE(DATUM,''DD.MM.YY'') MEASUREMENT_DATE,
                    13 SAAT,
                     DECODE(NVL(A.KUNDENNUMMER,0),0,SUBSTR(LINIE,1,INSTR(LINIE,''_'')-2) ,A.KUNDENNUMMER) KUNDENNUMMER,
                    substr(LINIE,-7,3) LINIE,
                    LINIE LINIE_NAME,
                    substr(ZPB,-5,4) OLCUM_TIPI,
                    EINDEUTIGELINR OLCUM_NOKTASI_ID,  
                    MVM_13_15,
                    MVM_13_30,
                    MVM_13_45,
                    MVM_13_60,
                    MC_13_15,
                    MC_13_30,
                    MC_13_45,
                    MC_13_60
                FROM   '||gv_dwh_ods_owner||'.'||v_src_table_01||'  A
                UNION ALL
                SELECT 
                           CASE WHEN KUNDENNAME LIKE ''%SIIRT%'' THEN ''SIIRT''
                     WHEN KUNDENNAME LIKE ''%URFA%'' THEN ''SANLIURFA''
                     WHEN KUNDENNAME LIKE ''%DIYARBAKIR%'' THEN ''DIYARBAKIR''
                     WHEN KUNDENNAME LIKE ''%BATMAN%'' THEN ''BATMAN''
                     WHEN KUNDENNAME LIKE ''%MARDIN%'' THEN ''MARDIN'' 
                     WHEN KUNDENNAME LIKE ''%SIRNAK%'' THEN ''SIRNAK'' END PROVINCE,
                    TO_DATE(DATUM,''DD.MM.YY'') MEASUREMENT_DATE,
                    14 SAAT,
                     DECODE(NVL(A.KUNDENNUMMER,0),0,SUBSTR(LINIE,1,INSTR(LINIE,''_'')-2) ,A.KUNDENNUMMER) KUNDENNUMMER,
                    substr(LINIE,-7,3) LINIE,
                    LINIE LINIE_NAME,
                    substr(ZPB,-5,4) OLCUM_TIPI,
                    EINDEUTIGELINR OLCUM_NOKTASI_ID,  
                    MVM_14_15,
                    MVM_14_30,
                    MVM_14_45,
                    MVM_14_60,
                    MC_14_15,
                    MC_14_30,
                    MC_14_45,
                    MC_14_60
                FROM   '||gv_dwh_ods_owner||'.'||v_src_table_01||'  A
                UNION ALL
                SELECT 
                           CASE WHEN KUNDENNAME LIKE ''%SIIRT%'' THEN ''SIIRT''
                     WHEN KUNDENNAME LIKE ''%URFA%'' THEN ''SANLIURFA''
                     WHEN KUNDENNAME LIKE ''%DIYARBAKIR%'' THEN ''DIYARBAKIR''
                     WHEN KUNDENNAME LIKE ''%BATMAN%'' THEN ''BATMAN''
                     WHEN KUNDENNAME LIKE ''%MARDIN%'' THEN ''MARDIN'' 
                     WHEN KUNDENNAME LIKE ''%SIRNAK%'' THEN ''SIRNAK'' END PROVINCE,
                    TO_DATE(DATUM,''DD.MM.YY'') MEASUREMENT_DATE,
                    15 SAAT,
                    DECODE(NVL(A.KUNDENNUMMER,0),0,SUBSTR(LINIE,1,INSTR(LINIE,''_'')-2) ,A.KUNDENNUMMER) KUNDENNUMMER,
                    substr(LINIE,-7,3) LINIE,
                    LINIE LINIE_NAME,
                    substr(ZPB,-5,4) OLCUM_TIPI,
                    EINDEUTIGELINR OLCUM_NOKTASI_ID,  
                    MVM_15_15,
                    MVM_15_30,
                    MVM_15_45,
                    MVM_15_60,
                    MC_15_15,
                    MC_15_30,
                    MC_15_45,
                    MC_15_60
                FROM   '||gv_dwh_ods_owner||'.'||v_src_table_01||'  A
                UNION ALL
                SELECT 
                       CASE WHEN KUNDENNAME LIKE ''%SIIRT%'' THEN ''SIIRT''
                     WHEN KUNDENNAME LIKE ''%URFA%'' THEN ''SANLIURFA''
                     WHEN KUNDENNAME LIKE ''%DIYARBAKIR%'' THEN ''DIYARBAKIR''
                     WHEN KUNDENNAME LIKE ''%BATMAN%'' THEN ''BATMAN''
                     WHEN KUNDENNAME LIKE ''%MARDIN%'' THEN ''MARDIN'' 
                     WHEN KUNDENNAME LIKE ''%SIRNAK%'' THEN ''SIRNAK'' END PROVINCE,
                    TO_DATE(DATUM,''DD.MM.YY'') MEASUREMENT_DATE,
                    16 SAAT,
                     DECODE(NVL(A.KUNDENNUMMER,0),0,SUBSTR(LINIE,1,INSTR(LINIE,''_'')-2) ,A.KUNDENNUMMER) KUNDENNUMMER,
                    substr(LINIE,-7,3) LINIE,
                    LINIE LINIE_NAME,
                    substr(ZPB,-5,4) OLCUM_TIPI,
                    EINDEUTIGELINR OLCUM_NOKTASI_ID,  
                    MVM_16_15,
                    MVM_16_30,
                    MVM_16_45,
                    MVM_16_60,
                    MC_16_15,
                    MC_16_30,
                    MC_16_45,
                    MC_16_60
                FROM   '||gv_dwh_ods_owner||'.'||v_src_table_01||'  A
                UNION ALL
                SELECT 
                       CASE WHEN KUNDENNAME LIKE ''%SIIRT%'' THEN ''SIIRT''
                     WHEN KUNDENNAME LIKE ''%URFA%'' THEN ''SANLIURFA''
                     WHEN KUNDENNAME LIKE ''%DIYARBAKIR%'' THEN ''DIYARBAKIR''
                     WHEN KUNDENNAME LIKE ''%BATMAN%'' THEN ''BATMAN''
                     WHEN KUNDENNAME LIKE ''%MARDIN%'' THEN ''MARDIN'' 
                     WHEN KUNDENNAME LIKE ''%SIRNAK%'' THEN ''SIRNAK'' END PROVINCE,
                    TO_DATE(DATUM,''DD.MM.YY'') MEASUREMENT_DATE,
                    17 SAAT,
                     DECODE(NVL(A.KUNDENNUMMER,0),0,SUBSTR(LINIE,1,INSTR(LINIE,''_'')-2) ,A.KUNDENNUMMER) KUNDENNUMMER,
                    substr(LINIE,-7,3) LINIE,
                    LINIE LINIE_NAME,
                    substr(ZPB,-5,4) OLCUM_TIPI,
                    EINDEUTIGELINR OLCUM_NOKTASI_ID,  
                    MVM_17_15,
                    MVM_17_30,
                    MVM_17_45,
                    MVM_17_60,
                    MC_17_15,
                    MC_17_30,
                    MC_17_45,
                    MC_17_60
                FROM   '||gv_dwh_ods_owner||'.'||v_src_table_01||'  A
                UNION ALL
                SELECT 
                       CASE WHEN KUNDENNAME LIKE ''%SIIRT%'' THEN ''SIIRT''
                     WHEN KUNDENNAME LIKE ''%URFA%'' THEN ''SANLIURFA''
                     WHEN KUNDENNAME LIKE ''%DIYARBAKIR%'' THEN ''DIYARBAKIR''
                     WHEN KUNDENNAME LIKE ''%BATMAN%'' THEN ''BATMAN''
                     WHEN KUNDENNAME LIKE ''%MARDIN%'' THEN ''MARDIN'' 
                     WHEN KUNDENNAME LIKE ''%SIRNAK%'' THEN ''SIRNAK'' END PROVINCE,
                    TO_DATE(DATUM,''DD.MM.YY'') MEASUREMENT_DATE,
                    18 SAAT,
                     DECODE(NVL(A.KUNDENNUMMER,0),0,SUBSTR(LINIE,1,INSTR(LINIE,''_'')-2) ,A.KUNDENNUMMER) KUNDENNUMMER,
                    substr(LINIE,-7,3) LINIE,
                    LINIE LINIE_NAME,
                    substr(ZPB,-5,4) OLCUM_TIPI,
                    EINDEUTIGELINR OLCUM_NOKTASI_ID,  
                    MVM_18_15,
                   MVM_18_30,
                    MVM_18_45,
                    MVM_18_60,
                    MC_18_15,
                    MC_18_30,
                    MC_18_45,
                    MC_18_60
                FROM   '||gv_dwh_ods_owner||'.'||v_src_table_01||'  A
                UNION ALL
                SELECT 
                       CASE WHEN KUNDENNAME LIKE ''%SIIRT%'' THEN ''SIIRT''
                     WHEN KUNDENNAME LIKE ''%URFA%'' THEN ''SANLIURFA''
                     WHEN KUNDENNAME LIKE ''%DIYARBAKIR%'' THEN ''DIYARBAKIR''
                     WHEN KUNDENNAME LIKE ''%BATMAN%'' THEN ''BATMAN''
                     WHEN KUNDENNAME LIKE ''%MARDIN%'' THEN ''MARDIN'' 
                     WHEN KUNDENNAME LIKE ''%SIRNAK%'' THEN ''SIRNAK'' END PROVINCE,
                    TO_DATE(DATUM,''DD.MM.YY'') MEASUREMENT_DATE,
                    19 SAAT,
                     DECODE(NVL(A.KUNDENNUMMER,0),0,SUBSTR(LINIE,1,INSTR(LINIE,''_'')-2) ,A.KUNDENNUMMER) KUNDENNUMMER,
                    substr(LINIE,-7,3) LINIE,
                    LINIE LINIE_NAME,
                    substr(ZPB,-5,4) OLCUM_TIPI,
                    EINDEUTIGELINR OLCUM_NOKTASI_ID,  
                    MVM_19_15,
                    MVM_19_30,
                    MVM_19_45,
                    MVM_19_60,
                    MC_19_15,
                    MC_19_30,
                    MC_19_45,
                    MC_19_60
                FROM   '||gv_dwh_ods_owner||'.'||v_src_table_01||'  A
                UNION ALL
                SELECT 
                       CASE WHEN KUNDENNAME LIKE ''%SIIRT%'' THEN ''SIIRT''
                     WHEN KUNDENNAME LIKE ''%URFA%'' THEN ''SANLIURFA''
                     WHEN KUNDENNAME LIKE ''%DIYARBAKIR%'' THEN ''DIYARBAKIR''
                     WHEN KUNDENNAME LIKE ''%BATMAN%'' THEN ''BATMAN''
                     WHEN KUNDENNAME LIKE ''%MARDIN%'' THEN ''MARDIN'' 
                     WHEN KUNDENNAME LIKE ''%SIRNAK%'' THEN ''SIRNAK'' END PROVINCE,
                    TO_DATE(DATUM,''DD.MM.YY'') MEASUREMENT_DATE,
                    20 SAAT,
                     DECODE(NVL(A.KUNDENNUMMER,0),0,SUBSTR(LINIE,1,INSTR(LINIE,''_'')-2) ,A.KUNDENNUMMER) KUNDENNUMMER,
                    substr(LINIE,-7,3) LINIE,
                    LINIE LINIE_NAME,
                    substr(ZPB,-5,4) OLCUM_TIPI,
                    EINDEUTIGELINR OLCUM_NOKTASI_ID,  
                    MVM_20_15,
                    MVM_20_30,
                    MVM_20_45,
                    MVM_20_60,
                    MC_20_15,
                    MC_20_30,
                    MC_20_45,
                    MC_20_60
                FROM   '||gv_dwh_ods_owner||'.'||v_src_table_01||'  A
                UNION ALL
                SELECT 
                       CASE WHEN KUNDENNAME LIKE ''%SIIRT%'' THEN ''SIIRT''
                     WHEN KUNDENNAME LIKE ''%URFA%'' THEN ''SANLIURFA''
                     WHEN KUNDENNAME LIKE ''%DIYARBAKIR%'' THEN ''DIYARBAKIR''
                     WHEN KUNDENNAME LIKE ''%BATMAN%'' THEN ''BATMAN''
                     WHEN KUNDENNAME LIKE ''%MARDIN%'' THEN ''MARDIN'' 
                     WHEN KUNDENNAME LIKE ''%SIRNAK%'' THEN ''SIRNAK'' END PROVINCE,
                    TO_DATE(DATUM,''DD.MM.YY'') MEASUREMENT_DATE,
                    21 SAAT,
                     DECODE(NVL(A.KUNDENNUMMER,0),0,SUBSTR(LINIE,1,INSTR(LINIE,''_'')-2) ,A.KUNDENNUMMER) KUNDENNUMMER,
                    substr(LINIE,-7,3) LINIE,
                    LINIE LINIE_NAME,
                    substr(ZPB,-5,4) OLCUM_TIPI,
                    EINDEUTIGELINR OLCUM_NOKTASI_ID,  
                    MVM_21_15,
                    MVM_21_30,
                   MVM_21_45,
                    MVM_21_60,
                    MC_21_15,
                    MC_21_30,
                    MC_21_45,
                    MC_21_60
                FROM   '||gv_dwh_ods_owner||'.'||v_src_table_01||'  A
                UNION ALL
                SELECT 
                       CASE WHEN KUNDENNAME LIKE ''%SIIRT%'' THEN ''SIIRT''
                     WHEN KUNDENNAME LIKE ''%URFA%'' THEN ''SANLIURFA''
                     WHEN KUNDENNAME LIKE ''%DIYARBAKIR%'' THEN ''DIYARBAKIR''
                     WHEN KUNDENNAME LIKE ''%BATMAN%'' THEN ''BATMAN''
                     WHEN KUNDENNAME LIKE ''%MARDIN%'' THEN ''MARDIN'' 
                     WHEN KUNDENNAME LIKE ''%SIRNAK%'' THEN ''SIRNAK'' END PROVINCE,
                    TO_DATE(DATUM,''DD.MM.YY'') MEASUREMENT_DATE,
                    22 SAAT,
                     DECODE(NVL(A.KUNDENNUMMER,0),0,SUBSTR(LINIE,1,INSTR(LINIE,''_'')-2) ,A.KUNDENNUMMER) KUNDENNUMMER,
                    substr(LINIE,-7,3) LINIE,
                    LINIE LINIE_NAME,
                    substr(ZPB,-5,4) OLCUM_TIPI,
                    EINDEUTIGELINR OLCUM_NOKTASI_ID,  
                    MVM_22_15,
                    MVM_22_30,
                    MVM_22_45,
                    MVM_22_60,
                    MC_22_15,
                    MC_22_30,
                    MC_22_45,
                    MC_22_60
                FROM   '||gv_dwh_ods_owner||'.'||v_src_table_01||'  A
                UNION ALL
                SELECT 
                       CASE WHEN KUNDENNAME LIKE ''%SIIRT%'' THEN ''SIIRT''
                     WHEN KUNDENNAME LIKE ''%URFA%'' THEN ''SANLIURFA''
                     WHEN KUNDENNAME LIKE ''%DIYARBAKIR%'' THEN ''DIYARBAKIR''
                     WHEN KUNDENNAME LIKE ''%BATMAN%'' THEN ''BATMAN''
                     WHEN KUNDENNAME LIKE ''%MARDIN%'' THEN ''MARDIN'' 
                     WHEN KUNDENNAME LIKE ''%SIRNAK%'' THEN ''SIRNAK'' END PROVINCE,
                    TO_DATE(DATUM,''DD.MM.YY'') MEASUREMENT_DATE,
                    23 SAAT,
                    DECODE(NVL(A.KUNDENNUMMER,0),0,SUBSTR(LINIE,1,INSTR(LINIE,''_'')-2) ,A.KUNDENNUMMER) KUNDENNUMMER,
                    substr(LINIE,-7,3) LINIE,
                    LINIE LINIE_NAME,
                    substr(ZPB,-5,4) OLCUM_TIPI,
                    EINDEUTIGELINR OLCUM_NOKTASI_ID,  
                    MVM_23_15,
                    MVM_23_30,
                    MVM_23_45,
                    MVM_23_60,
                    MC_23_15,
                    MC_23_30,
                    MC_23_45,
                    MC_23_60
                FROM   '||gv_dwh_ods_owner||'.'||v_src_table_01||'  A
            ),TAB2 AS 
            (
                SELECT * FROM TAB
            ), TAB3 AS (
            SELECT 
              PROVINCE,
                KUNDENNUMMER ID,
                OLCUM_NOKTASI_ID MEASURING_POINT_ID,
                OLCUM_TIPI PMUM_ID,    
                MEASUREMENT_DATE ,
                SAAT    MEASUREMENT_TIME,
                LINIE MEASUREMENT_TYPE,
                 LINIE_NAME MEASURING_NAME,
                ''M'' TIME_TYPE,
                15  TIME_PERIOD,
                MINUTE MEASURING_MINUTE,
                TO_NUMBER(VALUE) VALUE,
                CONTROL_VALUE
            FROM TAB2
            UNPIVOT ((VALUE,CONTROL_VALUE) FOR MINUTE IN ((TIME_VAL_15,TIME_CONTOL_VAL_15) AS ''15'', (TIME_VAL_30,TIME_CONTOL_VAL_30) AS ''30'', (TIME_VAL_45,TIME_CONTOL_VAL_45) AS ''45'', (TIME_VAL_60,TIME_CONTOL_VAL_60) AS ''60'')))
            SELECT  TAB3.* FROM TAB3
    ';

    execute immediate gv_dyn_task;
    
    PL.LOGGER.SUCCESS(SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      when OTHERS then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        PL.LOGGER.ERROR(SQLERRM,gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;
END;
/