CREATE OR REPLACE PACKAGE DWH_EDW.PCG_OSOS_ACTIVE_ENDEX AS
/******************************************************************************
   NAME:      PCG_MBS_ENDEKS_AKTARIM
   PURPOSE:

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        06.08.2017      Murat Öncül       1. Created this package.
*******************************************************************************
*******************************************************************************
PRIVILIDGE



******************************************************************************/
 PROCEDURE PRC_TMP_RDD_DAILYENDEXES ;
 PROCEDURE PRC_F_MRR_DAILY_ENDEXES ;
 PROCEDURE PRC_K2_CONSUMPTION ;
 PROCEDURE PRC_AGRICULTURAL_IRRIGATION;
 PROCEDURE PRC_CONSUMPTION_DATA;
 PROCEDURE PRC_AGGRI_CROSSTAB;
 PROCEDURE PRC_LIGTHNING_DAILY;
-- PROCEDURE PRC_MERGE_CUTOFFS;
 
END PCG_OSOS_ACTIVE_ENDEX;
/