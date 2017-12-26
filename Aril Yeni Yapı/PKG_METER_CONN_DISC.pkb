CREATE OR REPLACE PACKAGE BODY DWH.PKG_METER_CONN_DISC
IS
-------------------------------------------------------------------------------
--  Created By              : Murat ONCUL
--  Creation Date           : 20.12.2017
--  Last Modification Date  : 20.12.2017
--  Package Name            : PKG_METER_CONN_DISC
--  Package Version         : 0.0.1
--  Package Description     : 
--
--  [Modification History]
-------------------------------------------------------------------------------

  -----------------------------------
  -- Initialize Log Variables      --
  -----------------------------------
  gv_job_module       constant varchar2(50)  := 'METER_CONN_DISC';                   -- Job Module Name
  gv_pck              constant varchar2(50)  := 'PKG_METER_CONN_DISC';               -- PLSQL Package Name
  gv_job_owner        constant varchar2(50)  := 'DWH';                                 -- Owner of the Job
  gv_proc             varchar2(100);                                                   -- Procedure Name

  gv_sql_errm         varchar2(4000);                                                  -- SQL Error Message
  gv_sql_errc         number;                                                          -- SQL Error Code
  gv_dyn_task         long := '';
  
  -- schemas
  gv_dwh_ods_owner           constant varchar2(30) := 'DWH_ODS';
  gv_stg_owner               constant varchar2(30) := 'DWH';
  ------------------------------------------------------------------------------


    
  PROCEDURE PRC_LUNA_CONN_DISC
  IS
  ------------------------------------------------------------------------------
  --  Created By     : Murat ONCUL
  --  Creation Date  : 20-12-2017
  --  Last Modification Date  : 20-12-2017
  --  Procedure Name : PRC_LUNA_CONN_DISC
  --  Description    : Luna headend açma - kesme takibi.
  --  [Modification History]
  ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    v_table_name    varchar2(35) := 'LUNA_CONN_DISC';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'LUNA_AKMHAREKET';
    v_src_table_02  varchar2(30) := 'LUNA_ABONE';
    v_src_table_03  varchar2(30) := 'LUNA_SAYAC';
    ----------------------------------------------------------------------------
  BEGIN
    gv_proc := 'PRC_LUNA_CONN_DISC';

    -- Initialize Log Variables
    PL.LOGGER := logtype.init(gv_pck ||'.'||gv_proc);

    PL.DROP_TABLE(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
    CREATE TABLE '||gv_stg_owner||'.'||v_table_name||'  
    NOLOGGING PARALLEL 32 AS
    WITH ACMAKESME AS
    (
    SELECT AKMHAREKETID,
    CASE WHEN AKMISLEMTIPI = 47 THEN ''Kesme İş Emri''
    WHEN  AKMISLEMTIPI = 48 THEN ''Açma İş Emri''
    END ISLEMTIPI,
    KAYITZAMANI ISLEMZAMANI,
    A.ABONENO TESISATNUMARASI,
    S.SAYACNO SAYAÇNUMARASI,
    AKM.SONUC SONUC
    FROM '||gv_dwh_ods_owner||'.'||v_src_table_01||' AKM
    LEFT JOIN '||gv_dwh_ods_owner||'.'||v_src_table_02||' A ON A.ABONEID=AKM.ABONEID
    LEFT JOIN '||gv_dwh_ods_owner||'.'||v_src_table_03||' S ON S.ABONE_ID=A.ABONEID 
    WHERE AKMHAREKETID >362004 /* Kesmeye başladığımız zamanki ID*/
    AND AKMISLEMTIPI IN (47,48)
    )
    SELECT * FROM ACMAKESME
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