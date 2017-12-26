CREATE OR REPLACE PACKAGE DWH.PKG_METER_CONN_DISC AUTHID CURRENT_USER
IS
/************************************************************************************************
   NAME:       PKG_METER_CONN_DISC
   PURPOSE:    Kesme Açma takibi

   REVISIONS:
    Ver         Date          Authors                                     Description
   ---------  ----------    ----------------------------------           -----------------------
   1.0        20-12-2017    MURAT ONCUL                                   1. Created this package.
**************************************************************************************************

Paket i?ersindeki prosed?rler:
------------------------------
 
*/

  PROCEDURE PRC_LUNA_CONN_DISC;

END PKG_METER_CONN_DISC;
/