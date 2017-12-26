CREATE OR REPLACE PACKAGE DWH.PKG_OSOS_ACTIVE_ENDEX AUTHID CURRENT_USER
IS
/************************************************************************************************
   NAME:       PKG_OSOS_ACTIVE_ENDEX
   PURPOSE:    Günlük endex ve tüketim

   REVISIONS:
    Ver         Date          Authors                                     Description
   ---------  ----------    ----------------------------------           -----------------------
   1.0        25-10-2017    MURAT ONCUL                                   1. Created this package.
**************************************************************************************************

Paket içersindeki prosedürler:
------------------------------

  PROCEDURE PRC_TMP_RDD_DAILYENDEXES
    - Golden Gate üzerinden alýnan datalar birleþtiriliyor.
  PROCEDURE PRC_ARIL_ACTIVE_ENDEX
    - Aril uygulamasýnýn endeksleri alýnýyor.
  PROCEDURE PRC_HAYEN_ACTIVE_ENDEX
    - Hayen uygulamasýnýn endeksleri alýnýyor.
  PROCEDURE PRC_LUNA_ACTIVE_ENDEX
    - Luna uygulamasýnýn endeksleri alýnýyor.
  PROCEDURE PRC_UNION_ACTIVE_ENDEX
    - Uygulamalara ait olan endeks bilgileri birleþtiriliyor..
  PROCEDURE PRC_BACK_ENDEX
    - Endekslerin bir önceki deðeleri bulunuyor.
  PROCEDURE PRC_CONSUMPTION_ACTIVE_ENDEX
    - Endeksler üzerinden tüketim hesaplanýyor.
  PROCEDURE PRC_TRUNCATE_PARTITION
    - Tablo üzerindeki Aril ve Luna partitionlarý siliniyor..
  PROCEDURE PRC_F_MRR_DAILY_ENDEXES
    - Endeks tüketimleri ve endeks deðerleri tabloya basýlýyor.
  PROCEDURE PRC_K2_SUBSCRIBER_LIST
    - K2 abone listesi alýnýyor.
  PROCEDURE PRC_K2_SUBSCRIBER_CONSUMPTION
    - K2 abonelerinin tüketimleri hesaplanýp tabloya basýlýyor.
  PROCEDURE PRC_AGRICULTURAL_IRRIGATION
    - Tarýmsal tüketimler hesaplanýyor.
  PROCEDURE PRC_AGRICULTURAL_IRRIGATION_D
    - Tarýmsal tüketimi hesaplanan abone sayýlarý belirleniyor.
  PROCEDURE PRC_CONSUMPTION_DAILY
    - YKP den alýnan saatlik tüketim verileri günlük tüketime dönüþtürülüyor.
  PROCEDURE PRC_CONSUMPTION_MONTHLY
    - Hesaplanan günlük tüketim verileri aylýk tüketime dönüþtürülüyor.
  PROCEDURE PRC_AGGRI_CROSSTAB
    - Tarýmsal tüketim verileri crosstab e dönüþtürülüyor.
  PROCEDURE PRC_F_MRR_LIGTHING_DAILY
  , - Aydýnlatma tüketimleri hesaplanýp tabloya yazýlýyor.
*/

  PROCEDURE PRC_TMP_RDD_DAILYENDEXES;
  PROCEDURE PRC_ARIL_ACTIVE_ENDEX;
  PROCEDURE PRC_HAYEN_ACTIVE_ENDEX;
  PROCEDURE PRC_LUNA_ACTIVE_ENDEX;
  PROCEDURE PRC_UNION_ACTIVE_ENDEX;
  PROCEDURE PRC_BACK_ENDEX;
  PROCEDURE PRC_CONSUMPTION_ACTIVE_ENDEX;
  PROCEDURE PRC_TRUNCATE_PARTITION;
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