CREATE OR REPLACE PACKAGE DWH.PKG_OSOS_CONSUMPTION_HOURLY AUTHID CURRENT_USER
IS
/************************************************************************************************
   NAME:       PKG_OSOS_CONSUMPTION_HOURLY
   PURPOSE:    Saatlik y�k profili t�ketim verileri

   REVISIONS:
    Ver         Date          Authors                                     Description
   ---------  ----------    ----------------------------------           -----------------------
   1.0        17-10-2017    MURAT ONCUL                                   1. Created this package.
**************************************************************************************************

Paket i�ersindeki prosed�rler:
------------------------------

  PROCEDURE PRC_CON_SUBSCRIBER
    -Aril sistemindeki abone t�ketimleri al�n�yor
  PROCEDURE PRC_CON_SUBSTATION
    -Aril sistemindeki trafo t�ketimleri al�n�yor
  PROCEDURE PRC_CON_LIGTHING
    -Aril sistemindeki ayd�nlatma t�ketimleri al�n�yor
  PROCEDURE PRC_UNION_CONSUMPTION
    -Aril sisteminden al�nan abone, trafo ve ayd�nlatma t�ketimleri birle�tiriliyor
  PROCEDURE PRC_TRUNCATE_PARTITION_ARIL
    -T�ketim tablosundaki aril ve luna sistemlerine ait partition lar truncate ediliyor.
  PROCEDURE PRC_TRUNCATE_PARTITION_LUNA
    -T�ketim tablosundaki aril ve luna sistemlerine ait partition lar truncate ediliyor.
  PROCEDURE PRC_CON_ARIL
    -Aril sisteminden al�narak birle�tirilen t�ketimler, abone bazl� olarak d�zenleniyor.
  PROCEDURE PRC_CON_LUNA
    -Luna sisteminden t�ketimler al�n�yor.
  PROCEDURE PRC_CON_HAYEN
    -Hayen sisteminden t�ketimler al�n�yor
  PROCEDURE PRC_OSOS_CONSUMPTION_HOURLY
    -T�ketim tablosuna aril ve luna t�ketimleri insert ediliyor.
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