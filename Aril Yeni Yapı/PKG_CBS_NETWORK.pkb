CREATE OR REPLACE PACKAGE BODY DWH.PKG_CBS_NETWORK
IS
-------------------------------------------------------------------------------
--  Created By              : Murat ONCUL
--  Creation Date           : 21.12.2017
--  Last Modification Date  : 21.12.2017
--  Package Name            : PKG_CBS_NETWORK
--  Package Version         : 0.0.1
--  Package Description     : 
--
--  [Modification History]
-------------------------------------------------------------------------------

  -----------------------------------
  -- Initialize Log Variables      --
  -----------------------------------
  gv_job_module       constant varchar2(50)  := 'CBS_NETWORK';                   -- Job Module Name
  gv_pck              constant varchar2(50)  := 'PKG_CBS_NETWORK';               -- PLSQL Package Name
  gv_job_owner        constant varchar2(50)  := 'DWH';                                 -- Owner of the Job
  gv_proc             varchar2(100);                                                   -- Procedure Name

  gv_sql_errm         varchar2(4000);                                                  -- SQL Error Message
  gv_sql_errc         number;                                                          -- SQL Error Code
  gv_dyn_task         long := '';
  
  -- schemas
  gv_dwh_ods_owner           constant varchar2(30) := 'DWH_ODS';
  gv_stg_owner               constant varchar2(30) := 'DWH';
  gv_dm_owner                constant varchar2(30) := 'DM';
  gv_cbs_owner               constant varchar2(30) := 'ODS_CBS_MAESTRO_DEDAS';
  gv_edw_owner               constant varchar2(30) := 'DWH_EDW';
  ------------------------------------------------------------------------------
    
  PROCEDURE PRC_CBS_NETWORK_SUBSCRIBER
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat ONCUL
  --  Creation Date  : 21-12-2017
  --  Last Modification Date  : 21-12-2017
  --  Procedure Name : PRC_CBS_NETWORK_SUBSCRIBER
  --  Description    : CBS sistemindeki abonelerin OSOS durumları
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'NETWORK_RATIO_NONSUBS';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'ADR_ABONE';
    v_src_table_02  varchar2(30) := 'SBK_TRAFOBINATIP';
    v_src_table_03  varchar2(30) := 'AMR_ALL_DETAILS_ARIL';
    v_src_table_04  varchar2(30) := 'ADR_ILCE';
    v_src_table_05  varchar2(30) := 'ADR_MAHALLE';
    v_src_table_06  varchar2(30) := 'D_FEEDER';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_CBS_NETWORK_SUBSCRIBER';

    -- Initialize Log Variables
    PL.LOGGER := logtype.init(gv_pck ||'.'||gv_proc);

    PL.DROP_TABLE(gv_dm_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_dm_owner||'.'||v_table_name||' 
    PARALLEL 128 NOLOGGING AS
      SELECT 
        T.ADR_IL_ID IL_ID, 
        ILCE.ADI ILCE_ID,
        MAH.ADI MAHALLE_ID,
        T.ID TRAFO_ID, 
        T.TESISAT_NO TRAFO_TESISAT_NO, 
        O.AMR_INSTALLATION_ID TRAFO_OSOS,
        T.ADI TRAFO_ADI,
        T.OZELLIK TRAFO_OZELLIK,
        T.ANALIZOR_CIHAZ_ID TRAFO_ANALIZOR_ID,
        F.FEEDER_NAME FIDER_ADI,
        A.TESISAT_NO ABONE_TESISAT_NO,
        O1.AMR_INSTALLATION_ID ABONE_OSOS,
        CASE
            WHEN A.ENERJI_TABLO_KAYIT_ID=T.ID THEN 1
        ELSE
            0
        END ABONE_TRAFO,
        AMR.Z_INSTALLATION_TYPE MONTAJ_TIPI
      FROM  '||gv_cbs_owner||'.'||v_src_table_01||' A
      LEFT JOIN '||gv_cbs_owner||'.'||v_src_table_02||' T ON (A.ENERJI_TABLO_KAYIT_ID=T.ID)
      LEFT JOIN (SELECT DISTINCT AMR_INSTALLATION_ID FROM '||gv_edw_owner||'.'||v_src_table_03||') O ON (LPAD(O.AMR_INSTALLATION_ID,8,0)=LPAD(T.TESISAT_NO,8,0))
      LEFT JOIN (SELECT DISTINCT AMR_INSTALLATION_ID FROM '||gv_edw_owner||'.'||v_src_table_03||') O1 ON (LPAD(O1.AMR_INSTALLATION_ID,8,0)=LPAD(A.TESISAT_NO,8,0))
      LEFT JOIN '||gv_cbs_owner||'.'||v_src_table_04||' ILCE ON (T.ADR_ILCE_ID=ILCE.ID AND T.ADR_IL_ID=ILCE.ADR_IL_ID)
      LEFT JOIN '||gv_cbs_owner||'.'||v_src_table_05||' MAH ON (T.ADR_MAHALLE_ID = MAH.ID AND T.ADR_ILCE_ID = MAH.ADR_ILCE_ID AND T.ADR_IL_ID = MAH.ADR_IL_ID)
      LEFT JOIN (SELECT DISTINCT DEVIDE_ID,FEEDER_NAME FROM '||gv_edw_owner||'.'||v_src_table_06||') F ON F.DEVIDE_ID=T.ANALIZOR_CIHAZ_ID
      LEFT JOIN (SELECT DISTINCT AMR_INSTALLATION_ID,Z_INSTALLATION_TYPE FROM '||gv_edw_owner||'.'||v_src_table_03||') AMR ON (AMR.AMR_INSTALLATION_ID=A.TESISAT_NO)
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