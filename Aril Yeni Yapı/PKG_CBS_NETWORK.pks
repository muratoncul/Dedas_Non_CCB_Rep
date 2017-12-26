CREATE OR REPLACE PACKAGE DWH.PKG_CBS_NETWORK AUTHID CURRENT_USER
IS
/************************************************************************************************
   NAME:       PKG_CBS_NETWORK
   PURPOSE:    CBS sisteminde bulunan abonelerin OSOS durumları ve takibi

   REVISIONS:
    Ver         Date          Authors                                     Description
   ---------  ----------    ----------------------------------           -----------------------
   1.0        21-12-2017    MURAT ONCUL                                   1. Created this package.
**************************************************************************************************

Paket içersindeki prosedurler:
------------------------------
 
*/

  PROCEDURE PRC_CBS_NETWORK_SUBSCRIBER;

END PKG_CBS_NETWORK;
/