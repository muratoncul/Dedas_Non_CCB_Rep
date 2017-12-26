CREATE OR REPLACE PACKAGE DWH_ODS.PCG_ARIL_REPORTS AS
/******************************************************************************
   NAME:      PCG_MBS_ENDEKS_AKTARIM
   PURPOSE:

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        11.06.2017      Murat Öncül       1. Created this package.
*******************************************************************************
*******************************************************************************
PRIVILIDGE



******************************************************************************/
 PROCEDURE PRC_ARIL_SUBSCRIBERS ;
 PROCEDURE PRC_ARIL_ENDEX;
 PROCEDURE PRC_ARIL_DISTRICT_REPORTS;
 PROCEDURE PRC_BASARSOFT_AKTARIM_BOX;
 
 
END PCG_ARIL_REPORTS;
/