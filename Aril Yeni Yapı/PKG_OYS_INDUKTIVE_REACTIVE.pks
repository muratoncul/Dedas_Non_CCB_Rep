CREATE OR REPLACE PACKAGE DWH.PKG_OYS_INDUKTIVE_REACTIVE AUTHID CURRENT_USER
IS
/************************************************************************************************
   NAME:       PKG_OYS_INDUKTIVE_REACTIVE
   PURPOSE:    Analizörlerden gelen indüktif ve reaktif değerlerinin takibi

   REVISIONS:
    Ver         Date          Authors                                     Description
   ---------  ----------    ----------------------------------           -----------------------
   1.0        21-12-2017    MURAT ONCUL                                   1. Created this package.
**************************************************************************************************

Paket içersindeki prosedurler:
------------------------------
 
*/

  PROCEDURE PRC_OYS_INDUKTIVE_REACTIVE;

END PKG_OYS_INDUKTIVE_REACTIVE;
/