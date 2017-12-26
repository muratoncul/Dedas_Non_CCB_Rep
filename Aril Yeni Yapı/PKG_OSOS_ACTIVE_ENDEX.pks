CREATE OR REPLACE PACKAGE DWH.PKG_OSOS_ACTIVE_ENDEX AUTHID CURRENT_USER
IS
/************************************************************************************************
   NAME:       PKG_OSOS_ACTIVE_ENDEX
   PURPOSE:    G?nl?k endex ve t?ketim

   REVISIONS:
    Ver         Date          Authors                                     Description
   ---------  ----------    ----------------------------------           -----------------------
   1.0        25-10-2017    MURAT ONCUL                                   1. Created this package.
**************************************************************************************************

Paket i?ersindeki prosed?rler:
------------------------------

  PROCEDURE PRC_TMP_RDD_DAILYENDEXES
    - Golden Gate ?zerinden al?nan datalar birle?tiriliyor.
  PROCEDURE PRC_ARIL_ACTIVE_ENDEX
    - Aril uygulamas?n?n endeksleri al?n?yor.
  PROCEDURE PRC_HAYEN_ACTIVE_ENDEX
    - Hayen uygulamas?n?n endeksleri al?n?yor.
  PROCEDURE PRC_LUNA_ACTIVE_ENDEX
    - Luna uygulamas?n?n endeksleri al?n?yor.
  PROCEDURE PRC_UNION_ACTIVE_ENDEX
    - Uygulamalara ait olan endeks bilgileri birle?tiriliyor..
  PROCEDURE PRC_BACK_ENDEX
    - Endekslerin bir ?nceki de?eleri bulunuyor.
  PROCEDURE PRC_CONSUMPTION_ACTIVE_ENDEX
    - Endeksler ?zerinden t?ketim hesaplan?yor.
  PROCEDURE PRC_TRUNCATE_PARTITION
    - Tablo ?zerindeki Aril ve Luna partitionlar? siliniyor..
  PROCEDURE PRC_F_MRR_DAILY_ENDEXES
    - Endeks t?ketimleri ve endeks de?erleri tabloya bas?l?yor.
  PROCEDURE PRC_K2_SUBSCRIBER_LIST
    - K2 abone listesi al?n?yor.
  PROCEDURE PRC_K2_SUBSCRIBER_CONSUMPTION
    - K2 abonelerinin t?ketimleri hesaplan?p tabloya bas?l?yor.
  PROCEDURE PRC_AGRICULTURAL_IRRIGATION
    - Tar?msal t?ketimler hesaplan?yor.
  PROCEDURE PRC_AGRICULTURAL_IRRIGATION_D
    - Tar?msal t?ketimi hesaplanan abone say?lar? belirleniyor.
  PROCEDURE PRC_CONSUMPTION_DAILY
    - YKP den al?nan saatlik t?ketim verileri g?nl?k t?ketime d?n??t?r?l?yor.
  PROCEDURE PRC_CONSUMPTION_MONTHLY
    - Hesaplanan g?nl?k t?ketim verileri ayl?k t?ketime d?n??t?r?l?yor.
  PROCEDURE PRC_AGGRI_CROSSTAB
    - Tar?msal t?ketim verileri crosstab e d?n??t?r?l?yor.
  PROCEDURE PRC_F_MRR_LIGTHING_DAILY
  , - Ayd?nlatma t?ketimleri hesaplan?p tabloya yaz?l?yor.
*/

  PROCEDURE PRC_TMP_RDD_DAILYENDEXES;
  PROCEDURE PRC_ARIL_ACTIVE_ENDEX;
  PROCEDURE PRC_HAYEN_ACTIVE_ENDEX;
  PROCEDURE PRC_LUNA_ACTIVE_ENDEX;
  PROCEDURE PRC_UNION_ACTIVE_ENDEX;
  PROCEDURE PRC_BACK_ENDEX;
  PROCEDURE PRC_BACK_ENDEX_HAYEN;
  PROCEDURE PRC_CONSUMPTION_ACTIVE_ENDEX;
  PROCEDURE PRC_CONSUMPTION_ACTIVE_ENDEX_H;
  PROCEDURE PRC_F_MRR_DAILY_ENDEXES;
  PROCEDURE PRC_K2_SUBSCRIBER_LIST;
  PROCEDURE PRC_K2_SUBSCRIBER_CONSUMPTION;
  PROCEDURE PRC_AGRICULTURAL_IRRIGATION;
  PROCEDURE PRC_AGRICULTURAL_IRRIGATION_D;
  PROCEDURE PRC_CONSUMPTION_DAILY;
  PROCEDURE PRC_CONSUMPTION_MONTHLY;
  PROCEDURE PRC_AGGRI_CROSSTAB;
  PROCEDURE PRC_F_MRR_LIGTHING_DAILY;

END PKG_OSOS_ACTIVE_ENDEX;
/