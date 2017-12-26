CREATE OR REPLACE PACKAGE DWH.PKG_OSOS_CONSUMPTION_HOURLY AUTHID CURRENT_USER
IS
/************************************************************************************************
   NAME:       PKG_OSOS_CONSUMPTION_HOURLY
   PURPOSE:    Saatlik yük profili tüketim verileri

   REVISIONS:
    Ver         Date          Authors                                     Description
   ---------  ----------    ----------------------------------           -----------------------
   1.0        17-10-2017    MURAT ONCUL                                   1. Created this package.
**************************************************************************************************

Paket içersindeki prosedürler:
------------------------------

  PROCEDURE PRC_CON_SUBSCRIBER
    -Aril sistemindeki abone tüketimleri alýnýyor
  PROCEDURE PRC_CON_SUBSTATION
    -Aril sistemindeki trafo tüketimleri alýnýyor
  PROCEDURE PRC_CON_LIGTHING
    -Aril sistemindeki aydýnlatma tüketimleri alýnýyor
  PROCEDURE PRC_UNION_CONSUMPTION
    -Aril sisteminden alýnan abone, trafo ve aydýnlatma tüketimleri birleþtiriliyor
  PROCEDURE PRC_TRUNCATE_PARTITION_ARIL
    -Tüketim tablosundaki aril ve luna sistemlerine ait partition lar truncate ediliyor.
  PROCEDURE PRC_TRUNCATE_PARTITION_LUNA
    -Tüketim tablosundaki aril ve luna sistemlerine ait partition lar truncate ediliyor.
  PROCEDURE PRC_CON_ARIL
    -Aril sisteminden alýnarak birleþtirilen tüketimler, abone bazlý olarak düzenleniyor.
  PROCEDURE PRC_CON_LUNA
    -Luna sisteminden tüketimler alýnýyor.
  PROCEDURE PRC_CON_HAYEN
    -Hayen sisteminden tüketimler alýnýyor
  PROCEDURE PRC_OSOS_CONSUMPTION_HOURLY
    -Tüketim tablosuna aril ve luna tüketimleri insert ediliyor.
*/

  PROCEDURE PRC_CON_SUBSCRIBER;
  PROCEDURE PRC_CON_SUBSTATION;
  PROCEDURE PRC_CON_LIGTHING;
  PROCEDURE PRC_UNION_CONSUMPTION;
  PROCEDURE PRC_CON_ARIL;
  PROCEDURE PRC_CON_LUNA;
  PROCEDURE PRC_CON_HAYEN;
  PROCEDURE PRC_TRUNCATE_PARTITION_ARIL;
  PROCEDURE PRC_TRUNCATE_PARTITION_LUNA;
  PROCEDURE PRC_OSOS_CONSUMPTION_HOURLY;
  PROCEDURE PRC_OSOS_GES_CONSUMPTION;

END PKG_OSOS_CONSUMPTION_HOURLY;
/