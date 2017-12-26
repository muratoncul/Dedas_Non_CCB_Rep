CREATE OR REPLACE PACKAGE BODY DWH_ODS.PCG_ARIL_REPORTS 
IS
/******************************************************************************
   NAME:      PCG_ARIL_REPORTS
   PURPOSE:

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
    1.0        11.06.2017      Murat Öncül       1. Created this package body.

******************************************************************************/ 

PROCEDURE PRC_ARIL_SUBSCRIBERS AS

BEGIN
PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE('DWH_ODS','TMP_ARIL_VAL_TEXT');

INSERT INTO DWH_ODS.TMP_ARIL_VAL_TEXT
SELECT 
/*+PARALLEL(A,16) */
A.SERNO, A.DEFINITIONSERNO, A.PROPSERNO, A.PROPVALUE, A.LOGENABLED, A.UPDATEDATE, A.UPDATEUSER, A.UPDATETRANSACTIONID, A.RECORDSTATUS
FROM  DWH_ODS.GG_SDM_VAL_TEXT A
WHERE A.PROPSERNO IN 
(
20022,
20013,
20857,
20020,
20029,
20015,
20014,
20027,
20026,
20025,
20023,
20021,
20019,
20017,
20010,
20009,
20008,
20007,
20006,
20005,
20004,
20003,
20012,
20011,
20002,
20001,
20028,
20024,
20018,
20016
);

PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE('DWH_ODS','TMP_ARIL_VAL_NUMBER');

INSERT INTO DWH_ODS.TMP_ARIL_VAL_NUMBER
SELECT 
/*+PARALLEL(B,16) */
B.SERNO, B.DEFINITIONSERNO, B.PROPSERNO, B.PROPVALUE, B.LOGENABLED, B.UPDATEDATE, B.UPDATEUSER, B.UPDATETRANSACTIONID, B.RECORDSTATUS
FROM DWH_ODS.GG_SDM_VAL_NUMBER B
WHERE B.PROPSERNO IN
(
20104,
20103,
20102,
20101,
20132,
20131,
20130,
20129,
20128,
20127,
20126,
20124,
20123,
20122,
20121,
20119,
20118,
20117,
20116,
20115,
20114,
20113,
20112,
20109,
20108,
20107,
20106,
20105
);

PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE('DWH_ODS','TMP_ARIL_VAL_DATE');

INSERT INTO DWH_ODS.TMP_ARIL_VAL_DATE
SELECT 
/*+PARALLEL(C,16) */
C.SERNO, C.DEFINITIONSERNO, C.PROPSERNO, C.PROPVALUE, C.LOGENABLED, C.UPDATEDATE, C.UPDATEUSER, C.UPDATETRANSACTIONID, C.RECORDSTATUS
FROM DWH_ODS.GG_SDM_VAL_DATE C
WHERE C.PROPSERNO IN
(
20862,
20860,
20859,
20224,
20223,
20222,
20221,
20220,
20219,
20218,
20217,
20216,
20215,
20214,
20213,
20212,
20210,
20209,
20208,
20207,
20206,
20205,
20204,
20203,
20202,
20201,
90219,
110228
);

PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE('DWH_ODS','TMP_ARIL_VAL_SELECT');

INSERT INTO DWH_ODS.TMP_ARIL_VAL_SELECT
SELECT 
/*+PARALLEL(D,16) */
D.SERNO, D.DEFINITIONSERNO, D.PROPSERNO, D.PROPVALUE, D.LOGENABLED, D.UPDATEDATE, D.UPDATEUSER, D.UPDATETRANSACTIONID, D.RECORDSTATUS 
FROM DWH_ODS.GG_SDM_VAL_SELECT D
WHERE D.PROPSERNO IN 
(
20120,
20400,
10404,
90403,
20403
);

PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE('DWH_ODS','TMP_ARIL_VAL_DEFINITIONS');

INSERT INTO DWH_ODS.TMP_ARIL_VAL_DEFINITIONS
SELECT 
/*+PARALLEL(E,16) */
E.SERNO,
E.DEFINITIONTYPESERNO,
E.IDENTIFIERVALUE
FROM DWH_ODS.GG_SDM_VAL_DEFINITIONITEMS E
WHERE E.definitiontypeserno IN (2,9,11);

PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE('DWH_EDW','DF_MRR_SUBSCRIBERS');

INSERT INTO  DWH_EDW.DF_MRR_SUBSCRIBERS
SELECT 
/*+PARALLEL(16) */
i.SERNO,
i.DEFINITIONTYPESERNO,
i.IDENTIFIERVALUE,
T20022.PROPVALUE T_Unvani,
T20013.PROPVALUE T_TC_Kimlik_Numarasi,
T20857.PROPVALUE T_Ust_Taným_Numarasi,
T20020.PROPVALUE T_Abone_Durumu,
T20029.PROPVALUE T_Osos_Adresi,
T20015.PROPVALUE T_Son_Endeks_Okumasi,
T20014.PROPVALUE T_Son_Yuk_Profili_Okumasi,
T20027.PROPVALUE T_Beklenen_Tuketim_Araligi,
T20026.PROPVALUE T_BrOncekiEndeksOkumasiCekis,
T20025.PROPVALUE T_AySonuEndeksOkumasiCekis,
T20023.PROPVALUE T_Tedarikci_Adi,
T20021.PROPVALUE T_Abone_Terim,
T20019.PROPVALUE T_Telefon_Numarasi,
T20017.PROPVALUE T_Abone_Numarasi,
N20104.PROPVALUE N_IlceKodu,
N20103.PROPVALUE N_IlKodu,
N20104.PROPVALUE N_IlBolgeKodu,
N20104.PROPVALUE N_FaturaGünü,
T20010.PROPVALUE T_Adres,
T20009.PROPVALUE Tarife_Kodu,
T20008.PROPVALUE T_Uniped_Kodu,
T20007.PROPVALUE T_Beslenme_Tipi,
T20006.PROPVALUE T_Bagli_Oldugu_Isletme_Adi,
T20005.PROPVALUE T_Sayac_Seri_Numarasi,
T20004.PROPVALUE T_Sayac_Marka_Adi,
T20003.PROPVALUE T_Sayac_Marka_Flag_Kodu,
T20012.PROPVALUE T_Dosya_Numarasi,
T20011.PROPVALUE T_Dagitim_Tipi,
T20002.PROPVALUE T_Abone_ERP_Seri_Numarasi,
T20001.PROPVALUE T_MBS_Son_Fatura_Tarihi,
T20028.PROPVALUE T_AboneMobilCihazTekilNumarasi,
T20024.PROPVALUE T_Gerilim_Sinifi,
T20018.PROPVALUE T_Sahsi_Abone_ERP_Seri_No,
T20016.PROPVALUE T_Piyasa_Kat_Numarasi,
V20132.PROPVALUE N_BirÖncekiAylikTüketim,
V20131.PROPVALUE N_BirÖnceAykBosTükSay,
V20130.PROPVALUE N_AylikTüketimDolulukOran,
V20129.PROPVALUE N_AylikBosTüketimSayisi,
V20128.PROPVALUE N_GünlükTüketimDolulukOrani,
V20127.PROPVALUE N_GünlükBosTüketimSayisi,
V20126.PROPVALUE N_ACmaKesmeLimitTipi,
V20124.PROPVALUE N_MaxEndüktifOran,
V20123.PROPVALUE N_MaxKapasitifOran,
V20122.PROPVALUE N_MinEndüktifOran,
V20121.PROPVALUE N_MinKapasitifOrani,
V20119.PROPVALUE N_TuketimDurumu,
V20118.PROPVALUE N_OrtalamaTuketimSaatSayisi,
V20117.PROPVALUE N_GerilimTrafosuCarpani,
V20116.PROPVALUE N_AkimTrafosuCarpani,
V20115.PROPVALUE N_EndeksIlerlemeDurumu,
V20114.PROPVALUE N_Koy_Kodu,
V20113.PROPVALUE N_ERPIsletmeKodu,
V20112.PROPVALUE N_KasabaKodu,
V20109.PROPVALUE N_MBSSenkronizasyonDurumu,
V20108.PROPVALUE N_BagliOlduguIsletmeSeriNo,
V20107.PROPVALUE N_FazAdedi,
V20106.PROPVALUE N_SayacCarpani,
V20105.PROPVALUE N_AktifTahakkukCarpani,
D20862.PROPVALUE D_MBSÝþaretlemeTarihi,
D20860.PROPVALUE D_MBSSonEndeksGönderimTarihi,
D20859.PROPVALUE D_MBSSonEndeksTalepTarihi,
CASE
    WHEN TRIM(D20224.PROPVALUE) IS NOT NULL THEN D20224.PROPVALUE
    WHEN TRIM(D90224.PROPVALUE) IS NOT NULL THEN D90224.PROPVALUE
    WHEN TRIM(D110224.PROPVALUE) IS NOT NULL THEN D110224.PROPVALUE
END
/*D20224.PROPVALUE*/ D_SistemeTanimlanmaTarihi,
D20223.PROPVALUE D_BirÖnceAySonEndksOkuTarCek,
D20222.PROPVALUE D_MBSenkronizasyonTar,
D20221.PROPVALUE D_AySonuOkumaTarihiVer,
D20220.PROPVALUE D_AySonMaxDemndTarVer,
D20219.PROPVALUE D_SonOkumaMaxDemndTarVer,
D20218.PROPVALUE D_SonOkumaTarVeris,
D20217.PROPVALUE D_TuketimDurumuDegisimTarihi,
D20216.PROPVALUE D_GünlükSonOkumaTarihi,
D20215.PROPVALUE D_GünSonYükProfOkuTar,
D20214.PROPVALUE D_GünlSonReadoutOkuTar,
D20213.PROPVALUE D_ReakEndükOranHesapTar,
D20212.PROPVALUE D_ReakKapasitfOraniHesapTar,
D20210.PROPVALUE D_AySonuMaxDemandTarCek,
D20209.PROPVALUE D_SonOkuMaxDemandTarCekis,
D20208.PROPVALUE D_GüncelTarihi,
D20207.PROPVALUE D_BirÖnceEndeksOkuTarCek,
D20206.PROPVALUE D_AySonuOkumaTarihiCekis,
D20205.PROPVALUE D_SonYukProfiliVeriTarihi,
D20204.PROPVALUE D_SonTestFaturaTarihi,
D20203.PROPVALUE D_SonOkumaTarihiCekis,
D20202.PROPVALUE D_SonYükProfiliOkumaTarihi,
D20201.PROPVALUE D_SonFaturaTarihi,
S20120.PROPVALUE S_Abone_Tipi,
S10404.PROPVALUE        S_HaberlesmeTipi,
S90403.PROPVALUE        S_TrafoTipi,
S20403.PROPVALUE        S_OsosDurumu
FROM DWH_ODS.TMP_ARIL_VAL_DEFINITIONS i
LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_TEXT T20022 ON T20022.propserno = 20022 AND T20022.definitionserno = i.serno LEFT JOIN                                                          
DWH_ODS.TMP_ARIL_VAL_TEXT T20013 ON T20013.propserno = 20013 AND T20013.definitionserno = i.serno LEFT JOIN                                                          
DWH_ODS.TMP_ARIL_VAL_TEXT T20857 ON T20857.propserno = 20857 AND T20857.definitionserno = i.serno LEFT JOIN                                                          
DWH_ODS.TMP_ARIL_VAL_TEXT T20020 ON T20020.propserno = 20020 AND T20020.definitionserno = i.serno LEFT JOIN                                                          
DWH_ODS.TMP_ARIL_VAL_TEXT T20029 ON T20029.propserno = 20029 AND T20029.definitionserno = i.serno LEFT JOIN                                                          
DWH_ODS.TMP_ARIL_VAL_TEXT T20015 ON T20015.propserno = 20015 AND T20015.definitionserno = i.serno LEFT JOIN                                                          
DWH_ODS.TMP_ARIL_VAL_TEXT T20014 ON T20014.propserno = 20014 AND T20014.definitionserno = i.serno LEFT JOIN                                                          
DWH_ODS.TMP_ARIL_VAL_TEXT T20027 ON T20027.propserno = 20027 AND T20027.definitionserno = i.serno LEFT JOIN                                                          
DWH_ODS.TMP_ARIL_VAL_TEXT T20026 ON T20026.propserno = 20026 AND T20026.definitionserno = i.serno LEFT JOIN                                                          
DWH_ODS.TMP_ARIL_VAL_TEXT T20025 ON T20025.propserno = 20025 AND T20025.definitionserno = i.serno LEFT JOIN                                                          
DWH_ODS.TMP_ARIL_VAL_TEXT T20023 ON T20023.propserno = 20023 AND T20023.definitionserno = i.serno LEFT JOIN                                                          
DWH_ODS.TMP_ARIL_VAL_TEXT T20021 ON T20021.propserno = 20021 AND T20021.definitionserno = i.serno LEFT JOIN                                                          
DWH_ODS.TMP_ARIL_VAL_TEXT T20019 ON T20019.propserno = 20019 AND T20019.definitionserno = i.serno LEFT JOIN                                                          
DWH_ODS.TMP_ARIL_VAL_TEXT T20017 ON T20017.propserno = 20017 AND T20017.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_NUMBER N20104 ON N20104.propserno = 20104 AND N20104.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_NUMBER N20103 ON N20103.propserno = 20103 AND N20103.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_NUMBER N20102 ON N20102.propserno = 20102 AND N20102.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_NUMBER N20101 ON N20101.propserno = 20101 AND N20101.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_TEXT T20010 ON T20010.propserno = 20010  AND T20010.definitionserno = i.serno LEFT JOIN                                                           
DWH_ODS.TMP_ARIL_VAL_TEXT T20009 ON T20009.propserno = 20009  AND T20009.definitionserno = i.serno LEFT JOIN                                                           
DWH_ODS.TMP_ARIL_VAL_TEXT T20008 ON T20008.propserno = 20008  AND T20008.definitionserno = i.serno LEFT JOIN                                                           
DWH_ODS.TMP_ARIL_VAL_TEXT T20007 ON T20007.propserno = 20007  AND T20007.definitionserno = i.serno LEFT JOIN                                                           
DWH_ODS.TMP_ARIL_VAL_TEXT T20006 ON T20006.propserno = 20006  AND T20006.definitionserno = i.serno LEFT JOIN                                                           
DWH_ODS.TMP_ARIL_VAL_TEXT T20005 ON T20005.propserno = 20005  AND T20005.definitionserno = i.serno LEFT JOIN                                                           
DWH_ODS.TMP_ARIL_VAL_TEXT T20004 ON T20004.propserno = 20004  AND T20004.definitionserno = i.serno LEFT JOIN                                                           
DWH_ODS.TMP_ARIL_VAL_TEXT T20003 ON T20003.propserno = 20003  AND T20003.definitionserno = i.serno LEFT JOIN                                                           
DWH_ODS.TMP_ARIL_VAL_TEXT T20012 ON T20012.propserno = 20012  AND T20012.definitionserno = i.serno LEFT JOIN                                                           
DWH_ODS.TMP_ARIL_VAL_TEXT T20011 ON T20011.propserno = 20011  AND T20011.definitionserno = i.serno LEFT JOIN                                                           
DWH_ODS.TMP_ARIL_VAL_TEXT T20002 ON T20002.propserno = 20002  AND T20002.definitionserno = i.serno LEFT JOIN                                                           
DWH_ODS.TMP_ARIL_VAL_TEXT T20001 ON T20001.propserno = 20001  AND T20001.definitionserno = i.serno LEFT JOIN                                                           
DWH_ODS.TMP_ARIL_VAL_TEXT T20028 ON T20028.propserno = 20028  AND T20028.definitionserno = i.serno LEFT JOIN                                                           
DWH_ODS.TMP_ARIL_VAL_TEXT T20024 ON T20024.propserno = 20024  AND T20024.definitionserno = i.serno LEFT JOIN                                                           
DWH_ODS.TMP_ARIL_VAL_TEXT T20018 ON T20018.propserno = 20018  AND T20018.definitionserno = i.serno LEFT JOIN                                                           
DWH_ODS.TMP_ARIL_VAL_TEXT T20016 ON T20016.propserno = 20016  AND T20016.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_NUMBER V20132  ON V20132.propserno = 20132 AND V20132.definitionserno = i.serno LEFT JOIN                                                             
DWH_ODS.TMP_ARIL_VAL_NUMBER V20131  ON V20131.propserno = 20131 AND V20131.definitionserno = i.serno LEFT JOIN                                                             
DWH_ODS.TMP_ARIL_VAL_NUMBER V20130  ON V20130.propserno = 20130 AND V20130.definitionserno = i.serno LEFT JOIN                                                             
DWH_ODS.TMP_ARIL_VAL_NUMBER V20129  ON V20129.propserno = 20129 AND V20129.definitionserno = i.serno LEFT JOIN                                                             
DWH_ODS.TMP_ARIL_VAL_NUMBER V20128  ON V20128.propserno = 20128 AND V20128.definitionserno = i.serno LEFT JOIN                                                             
DWH_ODS.TMP_ARIL_VAL_NUMBER V20127  ON V20127.propserno = 20127 AND V20127.definitionserno = i.serno LEFT JOIN                                                             
DWH_ODS.TMP_ARIL_VAL_NUMBER V20126  ON V20126.propserno = 20126 AND V20126.definitionserno = i.serno LEFT JOIN                                                             
DWH_ODS.TMP_ARIL_VAL_NUMBER V20124  ON V20124.propserno = 20124 AND V20124.definitionserno = i.serno LEFT JOIN                                                             
DWH_ODS.TMP_ARIL_VAL_NUMBER V20123  ON V20123.propserno = 20123 AND V20123.definitionserno = i.serno LEFT JOIN                                                             
DWH_ODS.TMP_ARIL_VAL_NUMBER V20122  ON V20122.propserno = 20122 AND V20122.definitionserno = i.serno LEFT JOIN                                                             
DWH_ODS.TMP_ARIL_VAL_NUMBER V20121  ON V20121.propserno = 20121 AND V20121.definitionserno = i.serno LEFT JOIN                                                             
DWH_ODS.TMP_ARIL_VAL_NUMBER V20119  ON V20119.propserno = 20119 AND V20119.definitionserno = i.serno LEFT JOIN                                                             
DWH_ODS.TMP_ARIL_VAL_NUMBER V20118  ON V20118.propserno = 20118 AND V20118.definitionserno = i.serno LEFT JOIN                                                             
DWH_ODS.TMP_ARIL_VAL_NUMBER V20117  ON V20117.propserno = 20117 AND V20117.definitionserno = i.serno LEFT JOIN                                                             
DWH_ODS.TMP_ARIL_VAL_NUMBER V20116  ON V20116.propserno = 20116 AND V20116.definitionserno = i.serno LEFT JOIN                                                             
DWH_ODS.TMP_ARIL_VAL_NUMBER V20115  ON V20115.propserno = 20115 AND V20115.definitionserno = i.serno LEFT JOIN                                                             
DWH_ODS.TMP_ARIL_VAL_NUMBER V20114  ON V20114.propserno = 20114 AND V20114.definitionserno = i.serno LEFT JOIN                                                             
DWH_ODS.TMP_ARIL_VAL_NUMBER V20113  ON V20113.propserno = 20113 AND V20113.definitionserno = i.serno LEFT JOIN                                                             
DWH_ODS.TMP_ARIL_VAL_NUMBER V20112  ON V20112.propserno = 20112 AND V20112.definitionserno = i.serno LEFT JOIN                                                             
DWH_ODS.TMP_ARIL_VAL_NUMBER V20109  ON V20109.propserno = 20109 AND V20109.definitionserno = i.serno LEFT JOIN                                                             
DWH_ODS.TMP_ARIL_VAL_NUMBER V20108  ON V20108.propserno = 20108 AND V20108.definitionserno = i.serno LEFT JOIN                                                             
DWH_ODS.TMP_ARIL_VAL_NUMBER V20107  ON V20107.propserno = 20107 AND V20107.definitionserno = i.serno LEFT JOIN                                                             
DWH_ODS.TMP_ARIL_VAL_NUMBER V20106  ON V20106.propserno = 20106 AND V20106.definitionserno = i.serno LEFT JOIN                                                             
DWH_ODS.TMP_ARIL_VAL_NUMBER V20105  ON V20105.propserno = 20105 AND V20105.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_DATE D20862    ON D20862.propserno = 20862  AND D20862.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_DATE D20860    ON D20860.propserno = 20860  AND D20860.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_DATE D20859    ON D20859.propserno = 20859  AND D20859.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_DATE D20224    ON D20224.propserno = 20224  AND D20224.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_DATE D90224    ON D90224.propserno = 90219  AND D90224.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_DATE D110224   ON D110224.propserno = 110228 AND D110224.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_DATE D20223    ON D20223.propserno = 20223  AND D20223.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_DATE D20222    ON D20222.propserno = 20222  AND D20222.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_DATE D20221    ON D20221.propserno = 20221  AND D20221.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_DATE D20220    ON D20220.propserno = 20220  AND D20220.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_DATE D20219    ON D20219.propserno = 20219  AND D20219.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_DATE D20218    ON D20218.propserno = 20218  AND D20218.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_DATE D20217    ON D20217.propserno = 20217  AND D20217.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_DATE D20216    ON D20216.propserno = 20216  AND D20216.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_DATE D20215    ON D20215.propserno = 20215  AND D20215.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_DATE D20214    ON D20214.propserno = 20214  AND D20214.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_DATE D20213    ON D20213.propserno = 20213  AND D20213.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_DATE D20212    ON D20212.propserno = 20212  AND D20212.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_DATE D20210    ON D20210.propserno = 20210  AND D20210.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_DATE D20209    ON D20209.propserno = 20209  AND D20209.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_DATE D20208    ON D20208.propserno = 20208  AND D20208.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_DATE D20207    ON D20207.propserno = 20207  AND D20207.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_DATE D20206    ON D20206.propserno = 20206  AND D20206.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_DATE D20205    ON D20205.propserno = 20205  AND D20205.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_DATE D20204    ON D20204.propserno = 20204  AND D20204.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_DATE D20203    ON D20203.propserno = 20203  AND D20203.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_DATE D20202    ON D20202.propserno = 20202  AND D20202.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_DATE D20201    ON D20201.propserno = 20201  AND D20201.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_SELECT S20120    ON S20120.propserno = 20120 AND S20120.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_SELECT S90403    ON S90403.propserno = 90403 AND S90403.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_SELECT S10404    ON S10404.propserno = 10404 AND S10404.definitionserno = i.serno LEFT JOIN
DWH_ODS.TMP_ARIL_VAL_SELECT S20403    ON S20403.propserno = 20403 AND S20403.definitionserno = i.serno;
END;

PROCEDURE PRC_ARIL_ENDEX AS
BEGIN

PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE('DWH_ODS','ARIL_ENDEX');

INSERT /*+PARALLEL(16) NOLOGGING*/ INTO DWH_ODS.ARIL_ENDEX
with
VAL_DEC1 AS
(
SELECT 
*
FROM DWH_ODS.GG_SDM_VAL_DECIMAL
WHERE PROPSERNO IN
(
20805,
20806,
20807,
20808,
20809,
20810,
20811,
20812,
20209,
20828,
20830,
20829,
20831,
20832,
20834,
20835,
20833,
20219,
110806,
110807,
110808,
110809,
110810,
110811,
110209,
90840,
90841,
90842,
90843,
90844,
90845,
90846,
90847,
90208,
90856,
90858,
90857,
90859,
90860,
90862,
90863,
90861,
90216,
20858,
90822
)
),
VAL_DATE1 AS
(
SELECT 
*
FROM DWH_ODS.GG_SDM_VAL_DATE
WHERE PROPSERNO IN
(
20203,
110203,
90202
)
),
VAL_SELECT1 AS
(
SELECT 
*
FROM DWH_ODS.GG_SDM_VAL_SELECT
WHERE PROPSERNO IN
(
90403
)
),
ABONE AS
(    
select
'ABONE' TIP,
OW.IDENTIFIERVALUE TESISAT_NO,
D20805.propvalue T1,
D20806.propvalue T2,
D20807.propvalue T3,
D20808.propvalue T4,
D20809.propvalue T,
D20810.propvalue RC,
D20811.propvalue RI,
D20812.propvalue MAX_DEM,
D20209.propvalue MAX_DEM_DATE,
D20828.propvalue T_OUT,
D20830.propvalue T3_OUT,
D20829.propvalue T4_OUT,
D20831.propvalue T2_OUT,
D20832.propvalue T1_OUT,
D20834.propvalue RI_OUT,
D20835.propvalue RC_OUT,
D20833.propvalue MAX_DEM_OUT,
D20219.propvalue MAX_DEM_OUT_DATE,
DT20203.propvalue ENDEX_DATE,
D20858.PROPVALUE TRANSFORMER_POWER,
' ' TRANSFORMER_TYPE
from DWH_ODS.GG_SDM_VAL_DEFINITIONITEMS ow 
          LEFT JOIN VAL_DEC1 D20805 ON D20805.propserno = 20805 AND D20805.definitionserno = ow.serno
          LEFT JOIN VAL_DEC1 D20806 ON D20806.propserno = 20806 AND D20806.definitionserno = ow.serno
          LEFT JOIN VAL_DEC1 D20807 ON D20807.propserno = 20807 AND D20807.definitionserno = ow.serno
          LEFT JOIN VAL_DEC1 D20808 ON D20808.propserno = 20808 AND D20808.definitionserno = ow.serno
          LEFT JOIN VAL_DEC1 D20809 ON D20809.propserno = 20809 AND D20809.definitionserno = ow.serno
          LEFT JOIN VAL_DEC1 D20810 ON D20810.propserno = 20810 AND D20810.definitionserno = ow.serno
          LEFT JOIN VAL_DEC1 D20811 ON D20811.propserno = 20811 AND D20811.definitionserno = ow.serno
          LEFT JOIN VAL_DEC1 D20812 ON D20812.propserno = 20812 AND D20812.definitionserno = ow.serno
          LEFT JOIN VAL_DEC1 D20209 ON D20209.propserno = 20209 AND D20209.definitionserno = ow.serno                 
          LEFT JOIN VAL_DEC1 D20828 ON D20828.propserno = 20828 AND D20828.definitionserno = ow.serno                 
          LEFT JOIN VAL_DEC1 D20830 ON D20830.propserno = 20830 AND D20830.definitionserno = ow.serno                 
          LEFT JOIN VAL_DEC1 D20829 ON D20829.propserno = 20829 AND D20829.definitionserno = ow.serno                 
          LEFT JOIN VAL_DEC1 D20831 ON D20831.propserno = 20831 AND D20831.definitionserno = ow.serno                 
          LEFT JOIN VAL_DEC1 D20832 ON D20832.propserno = 20832 AND D20832.definitionserno = ow.serno                 
          LEFT JOIN VAL_DEC1 D20834 ON D20834.propserno = 20834 AND D20834.definitionserno = ow.serno 
          LEFT JOIN VAL_DEC1 D20835 ON D20835.propserno = 20835 AND D20835.definitionserno = ow.serno 
          LEFT JOIN VAL_DEC1 D20833 ON D20833.propserno = 20833 AND D20833.definitionserno = OW.serno  
          LEFT JOIN VAL_DEC1 D20219 ON D20219.propserno = 20219 AND D20219.definitionserno = OW.serno
          LEFT JOIN VAL_DEC1 D20858 ON D20858.propserno = 20858 AND D20858.definitionserno = OW.serno 
          LEFT JOIN VAL_DATE1 DT20203 ON DT20203.propserno = 20203 AND DT20203.definitionserno = ow.serno 
WHERE DEFINITIONTYPESERNO = 2
),
AYDINLATMA AS
(
select
'AYDINLATMA' TIP,
OW.IDENTIFIERVALUE TESISAT_NO,
D110806.propvalue T1,
D110807.propvalue T2,
D110808.propvalue T3,
D110809.propvalue T4,
D110810.propvalue T,
0 RC,
0 RI,
D110811.propvalue MAX_DEM,
D110209.propvalue MAX_DEM_DATE,
0 T_OUT,
0 T3_OUT,
0 T4_OUT,
0 T2_OUT,
0 T1_OUT,
0 RI_OUT,
0 RC_OUT,
0 MAX_DEM_OUT,
0 MAX_DEM_OUT_DATE,
DT110203.propvalue ENDEX_DATE,
0 TRANSFORMER_POWER,
' ' TRANSFORMER_TYPE
from DWH_ODS.GG_SDM_VAL_DEFINITIONITEMS ow 
          LEFT JOIN VAL_DEC1 D110806 ON D110806.propserno = 110806 AND D110806.definitionserno = ow.serno
          LEFT JOIN VAL_DEC1 D110807 ON D110807.propserno = 110807 AND D110807.definitionserno = ow.serno
          LEFT JOIN VAL_DEC1 D110808 ON D110808.propserno = 110808 AND D110808.definitionserno = ow.serno
          LEFT JOIN VAL_DEC1 D110809 ON D110809.propserno = 110809 AND D110809.definitionserno = ow.serno
          LEFT JOIN VAL_DEC1 D110810 ON D110810.propserno = 110810 AND D110810.definitionserno = ow.serno                   
          LEFT JOIN VAL_DEC1 D110811 ON D110811.propserno = 110811 AND D110811.definitionserno = ow.serno
          LEFT JOIN VAL_DEC1 D110209 ON D110209.propserno = 110209 AND D110209.definitionserno = ow.serno          
          LEFT JOIN VAL_DATE1 DT110203 ON DT110203.propserno = 110203 AND DT110203.definitionserno = ow.serno 
          WHERE DEFINITIONTYPESERNO = 11
),
TRAFO AS
(
select
'TRAFO' TIP,
OW.IDENTIFIERVALUE TESISAT_NO,
D90840.propvalue T1,
D90841.propvalue T2,
D90842.propvalue T3,
D90843.propvalue T4,
D90844.propvalue T,
D90845.propvalue RC,
D90846.propvalue RI,
D90847.propvalue MAX_DEM,
D90208.propvalue MAX_DEM_DATE,
D90856.propvalue T_OUT,
D90858.propvalue T3_OUT,
D90857.propvalue T4_OUT,
D90859.propvalue T2_OUT,
D90860.propvalue T1_OUT,
D90862.propvalue RI_OUT,
D90863.propvalue RC_OUT,
D90861.propvalue MAX_DEM_OUT,
D90216.propvalue MAX_DEM_OUT_DATE,
DT90202.propvalue ENDEX_DATE,
D90822.propvalue TRANSFORMER_POWER,
CASE S90403.PROPVALUE
    WHEN 'DM' THEN 'Daðýtým Merkezi'
    WHEN 'DTR' THEN 'Daðýtým Trafosu'
    WHEN 'OTR' THEN 'Özel Trafo'
    WHEN 'TM' THEN 'Trafo Merkezi'
END TRANSFORMER_TYPE
FROM DWH_ODS.GG_SDM_VAL_DEFINITIONITEMS ow 
          LEFT JOIN VAL_DEC1 D90840 ON D90840.propserno = 90840 AND D90840.definitionserno = ow.serno
          LEFT JOIN VAL_DEC1 D90841 ON D90841.propserno = 90841 AND D90841.definitionserno = ow.serno
          LEFT JOIN VAL_DEC1 D90842 ON D90842.propserno = 90842 AND D90842.definitionserno = ow.serno
          LEFT JOIN VAL_DEC1 D90843 ON D90843.propserno = 90843 AND D90843.definitionserno = ow.serno
          LEFT JOIN VAL_DEC1 D90844 ON D90844.propserno = 90844 AND D90844.definitionserno = ow.serno
          LEFT JOIN VAL_DEC1 D90845 ON D90845.propserno = 90845 AND D90845.definitionserno = ow.serno
          LEFT JOIN VAL_DEC1 D90846 ON D90846.propserno = 90846 AND D90846.definitionserno = ow.serno
          LEFT JOIN VAL_DEC1 D90847 ON D90847.propserno = 90847 AND D90847.definitionserno = ow.serno
          LEFT JOIN VAL_DEC1 D90208 ON D90208.propserno = 90208 AND D90208.definitionserno = ow.serno                 
          LEFT JOIN VAL_DEC1 D90856 ON D90856.propserno = 90856 AND D90856.definitionserno = ow.serno                 
          LEFT JOIN VAL_DEC1 D90858 ON D90858.propserno = 90858 AND D90858.definitionserno = ow.serno                 
          LEFT JOIN VAL_DEC1 D90857 ON D90857.propserno = 90857 AND D90857.definitionserno = ow.serno                 
          LEFT JOIN VAL_DEC1 D90859 ON D90859.propserno = 90859 AND D90859.definitionserno = ow.serno                 
          LEFT JOIN VAL_DEC1 D90860 ON D90860.propserno = 90860 AND D90860.definitionserno = ow.serno                 
          LEFT JOIN VAL_DEC1 D90862 ON D90862.propserno = 90862 AND D90862.definitionserno = ow.serno 
          LEFT JOIN VAL_DEC1 D90863 ON D90863.propserno = 90863 AND D90863.definitionserno = OW.serno               
          LEFT JOIN VAL_DEC1 D90861 ON D90861.propserno = 90861 AND D90861.definitionserno = ow.serno                 
          LEFT JOIN VAL_DEC1 D90216 ON D90216.propserno = 90216 AND D90216.definitionserno = ow.serno
          LEFT JOIN VAL_DEC1 D90822 ON D90822.propserno = 90822 AND D90822.definitionserno = OW.serno 
          LEFT JOIN VAL_SELECT1 S90403 ON S90403.propserno = 90403 AND S90403.definitionserno = OW.serno
          LEFT JOIN VAL_DATE1 DT90202 ON DT90202.propserno = 90202 AND DT90202.definitionserno = ow.serno 
          WHERE DEFINITIONTYPESERNO = 9
),
SON AS
(
SELECT * FROM ABONE
UNION ALL
SELECT * FROM AYDINLATMA
UNION ALL
SELECT * FROM TRAFO
)
SELECT * FROM SON;

PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE ('DWH_ODS', 'LUNA_ENDEX');

INSERT /*+PARALLEL(8)*/ INTO DWH_ODS.LUNA_ENDEX
WITH OKUMA AS
(
    SELECT /*+PARALLEL(OB1,16),PARALLEL(OB2,16),PARALLEL(OB3,16),PARALLEL(OB4,16),PARALLEL(OB5,16)) */
            ROW_NUMBER() OVER(PARTITION BY OB1.SAYAC_ID,OB1.ABONE_ID,OB1.OKUMATIPI ORDER BY OB1.OKUMATARIH DESC) SR,
            OB1.OKUMATARIH,OB1.SAYAC_ID,OB1.ABONE_ID,OB1.TNUMERIK,OB1.INDUKTIFENERJINUMERIK,OB1.KAPASITIFENERJINUMERIK,
            OB2.T1NUMERIK,OB2.T2NUMERIK,OB2.T3NUMERIK,OB2.T4NUMERIK,OB3.DEMANDNUMERIK,OB3.DEMANDTARIHI,
            --TO_TIMESTAMP(OB4.SAYACTARIHI ||' '||OB4.SAYACSAATI,'YYYY-MM-DD HH24:MI:SS') ENDEX_DATE
            OB5.TEXPORTNUMERIK,OB5.T1EXPORTNUMERIK,OB5.T2EXPORTNUMERIK,OB5.T3EXPORTNUMERIK,OB5.T4EXPORTNUMERIK,OB5.INDUKTIFENERJIEXPORTNUMERIK
           ,OB5.KAPASITIFENERJIEXPORTNUMERIK,OB5.DEMANDEXPORTNUMERIK,OB1.OKUMATIPI
    FROM ODS_OSOS_LUNA.OBISOKUMA1 OB1
    INNER JOIN ODS_OSOS_LUNA.OBISOKUMA2 OB2 ON (OB1.OBISOKUMAID = OB2.OBISOKUMAID)
    INNER JOIN ODS_OSOS_LUNA.OBISOKUMA3 OB3 ON (OB1.OBISOKUMAID = OB3.OBISOKUMAID)
    LEFT JOIN ODS_OSOS_LUNA.OBISOKUMA4 OB4 ON (OB1.OBISOKUMAID = OB4.OBISOKUMAID)
    LEFT JOIN ODS_OSOS_LUNA.OBISOKUMA5 OB5 ON (OB1.OBISOKUMAID = OB5.OBISOKUMAID)
    WHERE OB1.OKUMATIPI IN (11,22)
)
SELECT * FROM OKUMA WHERE SR=1;

END;

PROCEDURE PRC_ARIL_DISTRICT_REPORTS AS
/* Formatted on 13/06/2017 15:35:20 (QP5 v5.256.13226.35538) */
BEGIN
   PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE ('DWH_ODS', 'ARIL_METER');

   INSERT INTO DWH_ODS.ARIL_METER
      SELECT M.METER_ID,
             M.METER_MASTER_REF_ID METER_MASTER_ID,
             CAST (M.AMR_INSTALLATION_ID AS VARCHAR2 (50))
                AMR_INSTALLATION_ID,
             CASE
                WHEN DWH_CONFIG.IS_NUMERIC (M.AMR_INSTALLATION_ID) = 1
                THEN
                   TO_NUMBER (M.AMR_INSTALLATION_ID)
                ELSE
                   0
             END
                SUBSCRIBER_ID,
             M.MODEM_ID,
             0 SECTOR_ID,
             M.CBS_LAYER_ID,
             M.CBS_LAYER_TYPE_ID,
             M.APPLICATION_WIRING_REF_ID,
             3 APPLICATION_ID,
             M.FLOW_MULTIPLIER,
             M.VOLTAGE_MULTIPLIER,
             M.IS_ACTIVE,
             M.T1_ENDEX_OUT,
             M.T2_ENDEX_OUT,
             M.T3_ENDEX_OUT,
             M.T4_ENDEX_OUT,
             M.TOTAL_ENDEX_OUT,
             M.INDUCTIVE_ENDEX_OUT,
             M.CAPACITIVE_ENDEX_OUT,
             TO_DATE (M.ENDEX_DATE, 'YYYYMMDDHH24MISS') ENDEX_DATE,
             M.T1_ENDEX,
             M.T2_ENDEX,
             M.T3_ENDEX,
             M.T4_ENDEX,
             (  COALESCE (M.T1_ENDEX, 0)
              + COALESCE (M.T2_ENDEX, 0)
              + COALESCE (M.T3_ENDEX, 0)
              + COALESCE (M.T4_ENDEX, 0))
                TOTAL_ENDEX,
             M.INDUCTIVE_ENDEX,
             M.CAPACITIVE_ENDEX,
             M.MAXIMUM_DEMAND,
             CASE
                WHEN LENGTH (M.DEMAND_DATE) = 14
                THEN
                   TO_DATE (M.DEMAND_DATE, 'YYYYMMDDHH24MISS')
             END
                DEMAND_DATE,
             M.LAST_EXTERNAL_STATUS_CHANGE,
             M.LAST_SUCCESS_LPCOMMAND_DATE,
             M.METER_TYPE_DESCRIPTION,
             M.METER_TYPE_BRAND,
             CASE
                WHEN  TRIM(M.METER_TYPE_BRAND) LIKE '%LUN%'     THEN   'LUN' 
                WHEN  TRIM(M.METER_TYPE_BRAND) LIKE '%VIK%'     THEN   'VIK'
                WHEN  TRIM(M.METER_TYPE_BRAND) LIKE '%ELS%'     THEN   'ELS' 
                WHEN  TRIM(M.METER_TYPE_BRAND) LIKE '%MAKEL%'     THEN   'MSY'
                WHEN  TRIM(M.METER_TYPE_BRAND) LIKE '%ELEKTROMED%'     THEN   'ELM'
                WHEN  TRIM(M.METER_TYPE_BRAND) LIKE '%EMH%'     THEN   'EMH'
                WHEN  TRIM(M.METER_TYPE_BRAND) LIKE '%KÖHLER%' OR  TRIM(M.METER_TYPE_BRAND) LIKE '%Köhler%'   THEN   'AEL'
                WHEN  TRIM(M.METER_TYPE_BRAND) LIKE '%LANDIS%'    THEN  'LND' 
             ELSE 
                TRIM(M.METER_TYPE_BRAND)
             END METER_FLAG_CODE,
--             M.METER_MODEL METER_FLAG_CODE,
             ' ' MBS_METER_BRAND,
             M.METER_MODEL_TYPE,
             TO_DATE (M.METER_MANIFACTURE_DATE, 'YYYY')
                METER_MANIFACTURE_DATE,
             1 METER_TYPE_ID,
             M.WIRING_TYPE, /*1 - Abone, 2 - Toplayýcý Sayaç, 3 - Kontrol Sayacý, 0 Diðer*/
             M.Z_INSTALLATION_TYPE INSTALLATION_TYPE,
             M.PROVINCE,
             M.DISTRICT,
             M.VIRTUAL_GROUP_NAMES,
             TO_DATE (M.WIRING_METER_INSTALL_DATE, 'YYYYMMDDHH24MISS')
                WIRING_METER_INSTALL_DATE,
             TO_DATE (M.WIRING_MODEM_INSTALL_DATE, 'YYYYMMDDHH24MISS')
                WIRING_MODEM_INSTALL_DATE,
             TRUNC (TO_DATE (M.OSOS_INSTALL_DATE, 'YYYYMMDDHH24MISS'))
                OSOS_INSTALL_DATE,
             TO_CHAR (TO_DATE (M.OSOS_INSTALL_DATE, 'YYYYMMDDHH24MISS'),
                      'YYYYMM')
                OSOS_INSTALL_PERIOD,
             CASE
                WHEN M.MODEM_ID IS NULL
                THEN
                   TRUNC (TO_DATE (M.OSOS_LEAVE_DATE, 'YYYYMMDDHH24MISS'))
             END
                OSOS_LEAVE_DATE,
             M.WIRING_NOTE_CAPTION,
             M.WIRING_NOTE,
             TRUNC (TO_DATE (M.WIRING_NOTE_DATE, 'YYYYMMDDHH24MISS'))
                WIRING_NOTE_DATE,
             M.WIRING_NOTE_CREATE_USER,
             0 TRANSFORMER_MASTER_ID,
             M.METER_SERIAL_NUMBER,
             M.METER_TYPE_MODEL_NAME,
             M.SUBSCRIBER_NAME,
             M.SECTOR_INFO,
             M.METER_MODEL_TYPE METER_TYPE_NAME,
             M.UEVCBKODU,
             M.DUMMY_STATUS,
             M.VIRTUAL_GROUP_NAMES PHYSICAL_GROUP_TREE_NAMES,
             M.WIRING_TRANSFORMER_TYPE,
             M.TRANSFORMER_POWER,
             M.PARENT_TRANSFORMER_TYPE,
             M.DLMS_STATUS,
             M.MBS_INTEGRATED_STATUS,
             TO_CHAR (TO_DATE (M.METER_MANIFACTURE_DATE, 'YYYY'), 'YYYY')
                METER_PRODUCTION_YEAR,
             M.AMR_VERSION,
             M.METER_MODEL,
             M.Z_INSTALLATION_TYPE,
             M.Z_WIRING_TYPE,
             M.Z_STATUS,
             M.CBS_LAYER_ID BOARD_NUMBER,
             TO_DATE (M.LASTREADDATE, 'YYYYMMDDHH24MISS') LASTREADDATE,
             M.SUBSCRIBERAGRIMENT,
             CASE
                WHEN TO_DATE (M.ENDEX_DATE, 'YYYYMMDDHH24MISS') IS NULL
                THEN
                   3                                           --HÝÇ OKUMA YOK
                WHEN     TO_DATE (M.ENDEX_DATE, 'YYYYMMDDHH24MISS')
                            IS NOT NULL
                     AND TRUNC (
                            NVL (TO_DATE (M.ENDEX_DATE, 'YYYYMMDDHH24MISS'),
                                 TO_DATE (M.ENDEX_DATE, 'YYYYMMDDHH24MISS'))) >=
                            TRUNC (SYSDATE) - 6
                THEN
                   1                                               --OKUMA VAR
                ELSE
                   2                                              --ESKI OKUMA
             END
                READ_STATUS,
             PROVINCE_ID,
             DISTRICT_ID,
             KULLANIM_DURUMU,
             ERISIM_DURUM_ALANI
        FROM ODS_OSOS_SDM.V_D_METER M
       WHERE DWH_CONFIG.IS_NUMERIC (AMR_INSTALLATION_ID) = 1;
--      SELECT M."meter_id" METER_ID,
--             M.METER_MASTER_REF_ID METER_MASTER_ID,
--             CAST (M."amr_installation_id" AS VARCHAR2 (50))
--                AMR_INSTALLATION_ID,
--             CASE
--                WHEN DWH_CONFIG.IS_NUMERIC (M."amr_installation_id") = 1
--                THEN
--                   TO_NUMBER (M."amr_installation_id")
--                ELSE
--                   0
--             END
--                SUBSCRIBER_ID,
--             M."modem_id" MODEM_ID,
--             0 SECTOR_ID,
--             M.CBS_LAYER_ID,
--             M.CBS_LAYER_TYPE_ID,
--             M.APPLICATION_WIRING_REF_ID,
--             3 APPLICATION_ID,
--             M.FLOW_MULTIPLIER,
--             M.VOLTAGE_MULTIPLIER,
--             M.IS_ACTIVE,
--             M.T1_ENDEX_OUT,
--             M.T2_ENDEX_OUT,
--             M.T3_ENDEX_OUT,
--             M.T4_ENDEX_OUT,
--             M.TOTAL_ENDEX_OUT,
--             M.INDUCTIVE_ENDEX_OUT,
--             M.CAPACITIVE_ENDEX_OUT,
--             TO_DATE (M.ENDEX_DATE, 'YYYYMMDDHH24MISS') ENDEX_DATE,
--             M.T1_ENDEX,
--             M.T2_ENDEX,
--             M.T3_ENDEX,
--             M.T4_ENDEX,
--             (  COALESCE (M.T1_ENDEX, 0)
--              + COALESCE (M.T2_ENDEX, 0)
--              + COALESCE (M.T3_ENDEX, 0)
--              + COALESCE (M.T4_ENDEX, 0))
--                TOTAL_ENDEX,
--             M.INDUCTIVE_ENDEX,
--             M.CAPACITIVE_ENDEX,
--             M.MAXIMUM_DEMAND,
--             CASE
--                WHEN LENGTH (M.DEMAND_DATE) = 14
--                THEN
--                   TO_DATE (M.DEMAND_DATE, 'YYYYMMDDHH24MISS')
--             END
--                DEMAND_DATE,
--             M.LAST_EXTERNAL_STATUS_CHANGE,
--             M.LAST_SUCCESS_LPCOMMAND_DATE,
--             M.METER_TYPE_DESCRIPTION,
--             M.METER_TYPE_BRAND,
--             M.METER_MODEL METER_FLAG_CODE,
--             ' ' MBS_METER_BRAND,
--             M.METER_MODEL_TYPE,
--             TO_DATE (M.METER_MANIFACTURE_DATE, 'YYYY')
--                METER_MANIFACTURE_DATE,
--             1 METER_TYPE_ID,
--             M.WIRING_TYPE, /*1 - Abone, 2 - Toplayýcý Sayaç, 3 - Kontrol Sayacý, 0 Diðer*/
--             M.Z_INSTALLATION_TYPE INSTALLATION_TYPE,
--             M.PROVINCE,
--             M.DISTRICT,
--             M.VIRTUAL_GROUP_NAMES,
--             TO_DATE (M.WIRING_METER_INSTALL_DATE, 'YYYYMMDDHH24MISS')
--                WIRING_METER_INSTALL_DATE,
--             TO_DATE (M.WIRING_MODEM_INSTALL_DATE, 'YYYYMMDDHH24MISS')
--                WIRING_MODEM_INSTALL_DATE,
--             TRUNC (TO_DATE (M.OSOS_INSTALL_DATE, 'YYYYMMDDHH24MISS'))
--                OSOS_INSTALL_DATE,
--             TO_CHAR (TO_DATE (M.OSOS_INSTALL_DATE, 'YYYYMMDDHH24MISS'),
--                      'YYYYMM')
--                OSOS_INSTALL_PERIOD,
--             CASE
--                WHEN M."modem_id" IS NULL
--                THEN
--                   TRUNC (TO_DATE (M.OSOS_LEAVE_DATE, 'YYYYMMDDHH24MISS'))
--             END
--                OSOS_LEAVE_DATE,
--             M.WIRING_NOTE_CAPTION,
--             M.WIRING_NOTE,
--             TRUNC (TO_DATE (M.WIRING_NOTE_DATE, 'YYYYMMDDHH24MISS'))
--                WIRING_NOTE_DATE,
--             M.WIRING_NOTE_CREATE_USER,
--             0 TRANSFORMER_MASTER_ID,
--             M.METER_SERIAL_NUMBER,
--             M.METER_TYPE_MODEL_NAME,
--             M."subscriber_name" SUBSCRIBER_NAME,
--             M.SECTOR_INFO,
--             M.METER_MODEL_TYPE METER_TYPE_NAME,
--             M.UEVCBKODU,
--             M.DUMMY_STATUS,
--             M.VIRTUAL_GROUP_NAMES PHYSICAL_GROUP_TREE_NAMES,
--             M.WIRING_TRANSFORMER_TYPE,
--             M.TRANSFORMER_POWER,
--             M.PARENT_TRANSFORMER_TYPE,
--             M.DLMS_STATUS,
--             M.MBS_INTEGRATED_STATUS,
--             TO_CHAR (TO_DATE (M.METER_MANIFACTURE_DATE, 'YYYY'), 'YYYY')
--                METER_PRODUCTION_YEAR,
--             M.AMR_VERSION,
--             M.METER_MODEL,
--             M.Z_INSTALLATION_TYPE,
--             M.Z_WIRING_TYPE,
--             M.Z_STATUS,
--             M.CBS_LAYER_ID BOARD_NUMBER,
--             TO_DATE (M.LASTREADDATE, 'YYYYMMDDHH24MISS') LASTREADDATE,
--             M.SUBSCRIBERAGRIMENT,
--             CASE
--                WHEN TO_DATE (M.ENDEX_DATE, 'YYYYMMDDHH24MISS') IS NULL
--                THEN
--                   3                                           --HÝÇ OKUMA YOK
--                WHEN     TO_DATE (M.ENDEX_DATE, 'YYYYMMDDHH24MISS')
--                            IS NOT NULL
--                     AND TRUNC (
--                            NVL (TO_DATE (M.ENDEX_DATE, 'YYYYMMDDHH24MISS'),
--                                 TO_DATE (M.ENDEX_DATE, 'YYYYMMDDHH24MISS'))) >=
--                            TRUNC (SYSDATE) - 6
--                THEN
--                   1                                               --OKUMA VAR
--                ELSE
--                   2                                              --ESKI OKUMA
--             END
--                READ_STATUS,
--             PROVINCE_ID,
--             DISTRICT_ID,
--             KULLANIM_DURUMU,
--             ERISIM_DURUM_ALANI
--        FROM DWH_ODS.ARIL_ARILDSM_V_D_METER M
--       WHERE DWH_CONFIG.IS_NUMERIC ("amr_installation_id") = 1;

   PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE ('DWH_EDW', 'SUBSCRIBER_ARIL_REPORTS');

   INSERT INTO DWH_EDW.SUBSCRIBER_ARIL_REPORTS
      SELECT TRIM(S.SUBSCRIBER_ID),
             TRIM(S.METER_MODEL) METER_MODEL,
             TRIM(S.METER_NUMBER),
             TRIM(S.METER_MULTIPLIER),
             M.OSOS_DUR,
             COUNT (S.SUBSCRIBER_ID)
                OVER (PARTITION BY S.METER_MODEL, S.METER_NUMBER)
                ROW_COUNT
        FROM DWH_EDW.D_SUBSCRIBERS S
        LEFT JOIN DWH_ODS.MBS_MASTER M ON (S.SUBSCRIBER_ID=M.TESISAT_NO)
       WHERE DWH_CONFIG.IS_NUMERIC (SUBSCRIBER_ID) = 1;

   PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE ('DWH_ODS', 'ARIL_METERS_REPORT');

   INSERT INTO DWH_ODS.ARIL_METERS_REPORT
      SELECT W.METER_ID,
             W.AMR_INSTALLATION_ID,
             W.APPLICATION_WIRING_REF_ID,
             W.SUBSCRIBER_ID,
             W.SUBSCRIBER_NAME,
             W.DUMMY_STATUS,
             W.MODEM_ID,
             W.SECTOR_ID,
             W.CBS_LAYER_ID,
             W.CBS_LAYER_TYPE_ID,
             W.APPLICATION_ID,
             W.FLOW_MULTIPLIER,
             W.VOLTAGE_MULTIPLIER,
             W.IS_ACTIVE,
             W.T1_ENDEX_OUT,
             W.T2_ENDEX_OUT,
             W.T3_ENDEX_OUT,
             W.T4_ENDEX_OUT,
             W.TOTAL_ENDEX_OUT,
             W.INDUCTIVE_ENDEX_OUT,
             W.CAPACITIVE_ENDEX_OUT,
             W.ENDEX_DATE,
             W.T1_ENDEX,
             W.T2_ENDEX,
             W.T3_ENDEX,
             W.T4_ENDEX,
             W.TOTAL_ENDEX,
             W.INDUCTIVE_ENDEX,
             W.CAPACITIVE_ENDEX,
             W.MAXIMUM_DEMAND,
             W.DEMAND_DATE,
             W.LAST_EXTERNAL_STATUS_CHANGE,
             W.LAST_SUCCESS_LPCOMMAND_DATE,
             W.METER_TYPE_DESCRIPTION,
             W.METER_TYPE_BRAND,
             W.METER_FLAG_CODE,
             W.MBS_METER_BRAND,
             W.METER_MODEL_TYPE,
             W.METER_MANIFACTURE_DATE,
             W.METER_TYPE_ID,
             W.WIRING_TYPE,
             W.Z_INSTALLATION_TYPE,
             W.Z_WIRING_TYPE,
             W.Z_STATUS,
             W.INSTALLATION_TYPE,
             W.PROVINCE,
             W.DISTRICT,
             W.VIRTUAL_GROUP_NAMES,
             W.PHYSICAL_GROUP_TREE_NAMES,
             W.WIRING_METER_INSTALL_DATE,
             W.WIRING_MODEM_INSTALL_DATE,
             W.OSOS_INSTALL_DATE,
             W.OSOS_INSTALL_PERIOD,
             W.OSOS_LEAVE_DATE,
             W.WIRING_NOTE_CAPTION,
             W.WIRING_NOTE,
             W.WIRING_NOTE_DATE,
             W.WIRING_NOTE_CREATE_USER,
             W.METER_MASTER_ID,
             W.METER_SERIAL_NUMBER,
             W.METER_TYPE_MODEL_NAME,
             W.METER_TYPE_NAME,
             W.SECTOR_INFO,
             W.METER_MODEL,
             W.UEVCBKODU,
             W.TRANSFORMER_POWER,
             W.WIRING_TYPE TRANSFORMER_TYPE,                      --Trafo_tipi
             W.WIRING_TRANSFORMER_TYPE,
             W.PARENT_TRANSFORMER_TYPE,
             W.DLMS_STATUS,
             W.MBS_INTEGRATED_STATUS,
             W.METER_PRODUCTION_YEAR,
             W.AMR_VERSION,
             W.PROVINCE_ID,
             W.DISTRICT_ID,
             S.METER_NUMBER,
             CAST (NULL AS NUMBER) AS TRANSFORMER_ID,
             CAST (NULL AS NUMBER) AS POLE_ID,
             CAST (NULL AS NUMBER) AS BUILDING_ID,
             W.BOARD_NUMBER,
             CASE
                WHEN     TRIM (W.METER_MODEL) = TRIM (S.METER_MODEL)
                     AND W.METER_SERIAL_NUMBER = S.METER_NUMBER
                     AND (COALESCE (W.FLOW_MULTIPLIER * W.VOLTAGE_MULTIPLIER,
                                    1)) <>
                            (CASE
                                WHEN NVL (S.METER_MULTIPLIER, 0) = 0 THEN 1
                                ELSE NVL (S.METER_MULTIPLIER, 0)
                             END)
                THEN
                   'Tahakkuk Çarpan Farklý'
                WHEN W.METER_SERIAL_NUMBER <> S.METER_NUMBER
                THEN
                   'Sayaç Numarasý Farklý'
                WHEN     TRIM (W.METER_SERIAL_NUMBER) = TRIM (S.METER_NUMBER)
                     AND TRIM (W.METER_FLAG_CODE) <> TRIM (S.METER_MODEL)
                THEN
                   'Sayaç Marka Farklý'
                WHEN W.SUBSCRIBER_ID <> S2.SUBSCRIBER_ID
                THEN
                   'Tesisat Numarasý Farklý'
                WHEN S.SUBSCRIBER_ID IS NULL
                THEN
                   'Mbs Sisteminde Yok'
                WHEN 1 = 1
                THEN
                   'Normal'
             END
                MBS_MATCH_STATUS,
             W.LASTREADDATE,
             W.SUBSCRIBERAGRIMENT,
             W.READ_STATUS,
             W.KULLANIM_DURUMU,
             W.ERISIM_DURUM_ALANI,
             S.OSOS_DUR OSOS_KIND
        FROM DWH_ODS.ARIL_METER W
--             LEFT JOIN DWH_EDW.D_DEFINITIONS CTY
--                ON (CTY.D_CODE = 'PROVINCE' AND CTY.D_ID = W.PROVINCE_ID)
             LEFT JOIN DWH_EDW.SUBSCRIBER_ARIL_REPORTS S
                ON (W.SUBSCRIBER_ID = S.SUBSCRIBER_ID)
             LEFT JOIN DWH_EDW.SUBSCRIBER_ARIL_REPORTS S2
                ON (    TRIM (W.METER_SERIAL_NUMBER) = S2.METER_NUMBER
                    AND TRIM (W.METER_MODEL) = TRIM (S2.METER_MODEL)
                    AND S2.ROW_COUNT = 1)
        WHERE W.IS_ACTIVE=1;

   PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE ('DWH_ODS', 'ARIL_CBS');

   INSERT INTO DWH_ODS.ARIL_CBS
      SELECT /*+PARALLEL(A.16)*/
             COUNT (1) OVER (PARTITION BY A.TESISAT_NO) CNT,
             B.ADR_IL_ID || B.ID BUILDING_ID,
             A.ID ABONE_ID,
             A.TESISAT_NO SUBSCRIBER_ID,
             --A.DIS_KAPI_NO EXTERIOR_DOOR_NUMBER,
             A.IC_KAPI_NO INTERIOR_DOOR_NUMBER,
             A.ENERJI_TABLO_KAYIT_KODU TRANSFORMER_NAME,
             A.DIREK_BOX_NO POLE_BOX_NUMBER,
             A.X_KOORDINAT LATITUDE,
             A.Y_KOORDINAT LONGITUDE,
             -- B.BINA_ADI BUILDING_NAME,
             --B.BINA_TIPI BUILDING_TYPE,
             B.KAPI_NO DOOR_NUMBER,
             B.SITE_ADI SITE_NAME,
             -- B.KAT_SAYISI FLOOR_COUNT,
             --B.DAIRE_SAYISI FLAT_COUNT,
             B.ENERJI_TABLO_KAYIT_KODU TRANSFORMER_NUMBER,
             --B.MAHALLE NEIGHBORHOOD,
             CAST (A.ADR_IL_ID AS VARCHAR (20)) CBS_PROVINCE,
             C.ADI CBS_DISTRICT
        --B.CADDE_SOKAK STREET
        --B.CADDE_SOKAK_KODU STREET_CODE
        --B.YOL_ID WAY_ID
        --B.YOL_OZELLIK WAY_PROPERTY
        FROM ODS_CBS_MAESTRO_DEDAS.ADR_ABONE A
             LEFT JOIN ODS_CBS_MAESTRO_DEDAS.ADR_BINA B
                ON (A.ADR_BINA_ID = B.ID)
             LEFT JOIN ODS_CBS_MAESTRO_DEDAS.ADR_IL I ON (B.ADR_IL_ID = I.ID)
             LEFT JOIN ODS_CBS_MAESTRO_DEDAS.ADR_ILCE C
                ON (B.ADR_ILCE_ID = I.ID);

   PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE ('DWH_ODS', 'ARIL_FEEDER');

   INSERT INTO DWH_ODS.ARIL_FEEDER
      SELECT /*+PARALLEL(TBT.16)*/
             COUNT (1) OVER (PARTITION BY KODU) CNT,
             TBT.ID TRANSFORMER_ID,
             TESISAT_NO SUBSCRIBER_ID,
             TBT.KODU NEW_TRASFORMER_CODE,
             DV.SEGMENT_ID FEEDER_ID
--        FROM DWH_ODS.CBS_DICLESYS_TRAFOBINATIP TBT
          FROM ODS_CBS_MAESTRO_DEDAS.SBK_TRAFOBINATIP TBT
             LEFT JOIN DWH_ODS.INV_GEN_DEVICE DV ON (DV.ID = TBT.ANALIZOR_CIHAZ_ID)
       WHERE TBT.ANALIZOR_CIHAZ_ID IS NOT NULL;

   PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE ('DWH_ODS', 'ARIL_METER_SUBSCRIBER');

   INSERT INTO DWH_ODS.ARIL_METER_SUBSCRIBER
      SELECT M.METER_ID,
             M.AMR_INSTALLATION_ID,
             M.SUBSCRIBER_NAME,
             M.DUMMY_STATUS,
             M.SUBSCRIBER_ID,
             M.MODEM_ID,
             M.SECTOR_ID,
             M.CBS_LAYER_ID,
             M.CBS_LAYER_TYPE_ID,
             M.APPLICATION_WIRING_REF_ID,
             M.APPLICATION_ID,
             M.FLOW_MULTIPLIER,
             M.VOLTAGE_MULTIPLIER,
             M.IS_ACTIVE,
             M.T1_ENDEX_OUT,
             M.T2_ENDEX_OUT,
             M.T3_ENDEX_OUT,
             M.T4_ENDEX_OUT,
             M.TOTAL_ENDEX_OUT,
             M.INDUCTIVE_ENDEX_OUT,
             M.CAPACITIVE_ENDEX_OUT,
             M.ENDEX_DATE,
             M.T1_ENDEX,
             M.T2_ENDEX,
             M.T3_ENDEX,
             M.T4_ENDEX,
             M.TOTAL_ENDEX,
             M.INDUCTIVE_ENDEX,
             M.CAPACITIVE_ENDEX,
             M.OSOS_LEAVE_DATE,
             M.MAXIMUM_DEMAND,
             M.DEMAND_DATE,
             M.LAST_EXTERNAL_STATUS_CHANGE,
             M.LAST_SUCCESS_LPCOMMAND_DATE,
             M.METER_TYPE_DESCRIPTION,
             M.METER_TYPE_BRAND,
             M.METER_FLAG_CODE,
             M.MBS_METER_BRAND,
             M.METER_MODEL_TYPE,
             M.METER_MANIFACTURE_DATE,
             M.METER_TYPE_ID,
             M.WIRING_TYPE,
             M.WIRING_TRANSFORMER_TYPE,
             M.INSTALLATION_TYPE,
             M.PROVINCE,
             M.DISTRICT,
             M.VIRTUAL_GROUP_NAMES,
             M.PHYSICAL_GROUP_TREE_NAMES,
             M.WIRING_METER_INSTALL_DATE,
             M.WIRING_MODEM_INSTALL_DATE,
             M.OSOS_INSTALL_DATE,
             M.OSOS_INSTALL_PERIOD,
             M.WIRING_NOTE_CAPTION,
             M.WIRING_NOTE,
             M.WIRING_NOTE_DATE,
             M.WIRING_NOTE_CREATE_USER,
             M.METER_MASTER_ID,
             M.METER_SERIAL_NUMBER,
             M.METER_TYPE_MODEL_NAME,
             M.METER_TYPE_NAME,
             M.METER_MODEL,
             M.METER_NUMBER,
             M.SECTOR_INFO,
             M.Z_INSTALLATION_TYPE,
             M.Z_WIRING_TYPE,
             M.Z_STATUS,
             M.PROVINCE_ID,
             M.DISTRICT_ID,
             M.TRANSFORMER_TYPE,
             M.UEVCBKODU,
             M.TRANSFORMER_POWER,
             M.DLMS_STATUS,
             M.MBS_INTEGRATED_STATUS,
             M.METER_PRODUCTION_YEAR,
             M.AMR_VERSION,
             C.TRANSFORMER_NAME,
             CF.TRANSFORMER_ID,
             CF.FEEDER_ID,
             ' ' POLE_ID,
             C.BUILDING_ID,
             M.BOARD_NUMBER,
             M.MBS_MATCH_STATUS,
             M.PARENT_TRANSFORMER_TYPE,
             M.LASTREADDATE,
             M.SUBSCRIBERAGRIMENT,
             M.READ_STATUS,
             M.KULLANIM_DURUMU,
             M.ERISIM_DURUM_ALANI,
             M.OSOS_KIND
        FROM DWH_ODS.ARIL_METERS_REPORT M
             LEFT JOIN DWH_ODS.ARIL_CBS C
                ON (    LPAD (C.SUBSCRIBER_ID, 0, 8) =
                           LPAD (M.SUBSCRIBER_ID, 0, 8)
                    AND C.CNT = 1)
             LEFT JOIN DWH_ODS.ARIL_FEEDER CF
                ON (    CF.NEW_TRASFORMER_CODE = C.TRANSFORMER_NAME
                    AND CF.CNT = 1);

   PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE ('DWH_ODS', 'ARIL_MODEM');

   INSERT INTO DWH_ODS.ARIL_MODEM
      SELECT MODEM_ID,
             DEVICE_ID,
             MODEM_BRAND,
             REMOTE_IP,
             GSM_BRAND,
             AMR_MODEL,
             SIGNAL_LENGTH,
             SIGNAL_DATE,
             AMR_VERSION,
             TO_DATE (LAST_ACCESS_DATE, 'YYYYMMDDHH24MISS')
                LAST_ACCESS_DATE,
             ' ' LAST_ACCESS_DAY,
             APPLICATION_MODEM_REF_ID,
             3 APPLICATION_ID,
             LAST_STATUS
        FROM ODS_OSOS_SDM.V_D_MODEM;  
--      SELECT "modem_id" MODEM_ID,
--             "device_id" DEVICE_ID,
--             "modem_brand" MODEM_BRAND,
--             "remote_ip" REMOTE_IP,
--             "gsm_brand" GSM_BRAND,
--             "amr_model" AMR_MODEL,
--             "signal_length" SIGNAL_LENGTH,
--             "signal_date" SIGNAL_DATE,
--             "amr_version" AMR_VERSION,
--             TO_DATE ("last_access_date", 'YYYYMMDDHH24MISS')
--                LAST_ACCESS_DATE,
--             ' ' LAST_ACCESS_DAY,
--             "application_modem_ref_id" APPLICATION_MODEM_REF_ID,
--             3 APPLICATION_ID,
--             "last_status" LAST_STATUS
--        FROM DWH_ODS.ARIL_ARILSDM_V_D_MODEM;

   PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE ('DWH_ODS', 'ARIL_REPORTS_FIRST');

   INSERT INTO DWH_ODS.ARIL_REPORTS_FIRST
SELECT /*+PARALLEL(16)*/
             DISTINCT
             MTR.APPLICATION_ID,                             --1 HAYEN, 2 LUNA
             MTR.AMR_INSTALLATION_ID,                           --OSOS_TESISAT
             --        S.IDENTIFIERVALUE, --MBS_TESISAT_NO
             MTR.IS_ACTIVE,                                    --TESISAT_DURUM
             CASE WHEN MTR.READ_STATUS = 1 THEN 1 ELSE 0 END READ_STATUS, --OKUMA_DUR
             MTR.READ_STATUS READ_STATUS_V2, --OKUMA_DUR 1 OKUMA VAR , 2 ESKI OKUMA, 3 OKUMA YOK
             ' ' PARENT_TRANSFORMER_TYPE,                         --TRAFO TIPI
             MDM.DEVICE_ID,                                         --MODEM NO
             MTR.METER_SERIAL_NUMBER,                          --SAYAÇ SERI NO
             MTR.FLOW_MULTIPLIER,                               --AKIM ÇARPANI
             MTR.VOLTAGE_MULTIPLIER,                         --GERILIM ÇARPANI
             COALESCE (MTR.FLOW_MULTIPLIER * MTR.VOLTAGE_MULTIPLIER, 1)
                ACCRUAL_MULTIPLIER,                         --TAHAKKUK ÇARPANI
             MTR.TOTAL_ENDEX,                                  --TOPLAM ENDEKS
             MTR.INDUCTIVE_ENDEX,                            --INDUKTIF ENDEKS
             MTR.CAPACITIVE_ENDEX,                          --KAPASITIF ENDEKS
             MTR.T1_ENDEX,                                          --T1_ENDEX
             MTR.T2_ENDEX,                                          --T2_ENDEX
             MTR.T3_ENDEX,                                          --T3_ENDEX
             MTR.T4_ENDEX,                                          --T4_ENDEX
             MTR.MAXIMUM_DEMAND,                              --MAXIMUM_DEMAND
             MTR.ENDEX_DATE,                                   --ENDEKS TARIHI
             MDM.LAST_STATUS,                                      --SON DURUM
             MDM.SIGNAL_LENGTH,                                   --SON SINYAL
             MDM.LAST_ACCESS_DATE,                   --MODEM ERÝÞÝM SON TARÝHÝ
             TRUNC (MTR.DEMAND_DATE) DEMAND_DATE,               --DEMAND SAATI
             TO_CHAR (MTR.DEMAND_DATE, 'HH24:MI:SS') DEMAND_TIME, --DEMAND TARIHI
             MDM.AMR_MODEL,                                      --MODEM MODEL
             MTR.LAST_EXTERNAL_STATUS_CHANGE,             --ENTEGRASYON TARIHI
             SUBSTR (D_SistemeTanimlanmaTarihi, 0, 4) OSOS_INSTALL_PERIOD, --OSOS TAKILMA DÖNEMÝ
             D_SistemeTanimlanmaTarihi OSOS_INSTALL_DATE, --OSOS TAKILMA TARÝHÝ
             MTR.OSOS_LEAVE_DATE,                          --OSOS AYRILMA TARÝHÝ
             MTR.METER_TYPE_BRAND,                        
             MTR.METER_TYPE_MODEL_NAME,                         --SAYAÇ MODELÝ
             MTR.METER_TYPE_DESCRIPTION,                          --SAYAÇ TÝPÝ
             MDM.MODEM_BRAND,                                    --MODEM MARKA
             MTR.PROVINCE OSOS_PROVINCE,                                  --IL
             MTR.DISTRICT OSOS_DISTRICT,                                --ILCE
             SC.ADDRESS UEVCBKODU,                           --OSOS ADRES
             MTR.SUBSCRIBER_NAME,                          --OSOS ABONE UNVANI
             SC.NAME NAME,                                      --MBS UNVAN
             SC.PROVINCE MBS_PROVINCE,                                 --MBS IL
             SC.AREA_NAME MBS_DISTRICT,  --MBS ÝLÇE ADI XXXXXXXXXXXXXXXXXXXXXX
             S.T_OSOS_ADRESI,
             SC.CUT_STATUS_NAME,                             --TAKÝP DURUM ADI
             SC.LATITUDE,                                              --CBS X
             SC.LONGITUDE,                                             --CBS Y
             SC.SUBSCRIBER_STATUS_NAME,                      --ABONE DURUM ADI
             SC.SUBSCRIBER_GROUP_NAME,                        --ABONE GRUP ADI
             0 TARIFE_KODU,                      --S.TARIFF_NAME, --TARIFE ADI
             TRIM(SC.CONNECTION_STATUS),                       --MBS BAÐLANTI DURUMU
             SC.OG_STATUS,                                     --MBS OG DURUMU
             SC.OSOS_STATUS MBS_OSOS_STATUS,                                 --MBS OSOS DURUMU
             SC.METER_MULTIPLIER,                              --MBS SAYAÇ ÇARPAN
             --MTR.METER_TYPE_MODEL_NAME MBS_METER_MODEL,--MBS SAYAÇ MODELÝ
             SC.METER_NUMBER,                           --MBS SAYAÇ NO
             SC.INSTALLED_POWER,                            -- MBS KURULU GUCU
             SC.CONTRACT_POWER,                            --MBS SÖZLEÞME GÜCÜ
             CASE
                WHEN S.IDENTIFIERVALUE = MTR.SUBSCRIBER_ID THEN 1
                ELSE 0
             END
                SUBSCRIBER_ID_MATCH,                                 --ESLESME
             CASE
                WHEN TRIM (S.T_SAYAC_SERI_NUMARASI) =
                        TRIM (MTR.METER_SERIAL_NUMBER)
                THEN
                   1
                ELSE
                   0
             END
                METER_NUMBER_MATCH,                            --SAYAC_ESLESME
             ' ' VIRTUAL_GROUP_NAMES,                   --SANAL GRUP BÝLGÝLERÝ
             MTR.DUMMY_STATUS,                                 --TESISAT_TÝPÝ,
             TO_CHAR (MDM.LAST_ACCESS_DATE, 'HH24:MI:SS')
                AMR_LAST_ACCESS_TIME,                 --MODEM SON ERÝÞÝM SAATÝ
             TRUNC (MDM.SIGNAL_DATE) SIGNAL_DATE,    --MODEM SON SÝNYAL TARÝHÝ
             -- TO_CHAR(MDM.SIGNAL_DATE,'HH24:MI:SS') SIGNAL_TIME , --MODEM SON SÝNYAL SAATÝ
             MTR.VIRTUAL_GROUP_NAMES PHYSICAL_GROUP_TREE_NAMES,  -- GRUP AÐACI
             MTR.SECTOR_INFO,                                 --SEKTÖR BÝLGÝSÝ
             MTR.WIRING_TRANSFORMER_TYPE WIRING_TYPE,           --TESISAT TÜRÜ
             MTR.Z_WIRING_TYPE,                               --Z TESISAT TÜRÜ
             MTR.WIRING_NOTE WIRING_NOTE,                       --TESÝSAT NOTU
             MTR.Z_INSTALLATION_TYPE,                          --Z MONTAJ TÝPÝ
             MTR.Z_STATUS,                                            --ZDURUM
             MTR.BOARD_NUMBER, --PANOID XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
             MTR.WIRING_NOTE_DATE WIRING_NOTE_DATE,      --TESÝSAT NOTU TARÝHÝ
             MTR.WIRING_NOTE_CREATE_USER WIRING_NOTE_CREATE_USER, --TESÝSAT NOTU KULLANICISI
             CAST ('Tanýmsýz' AS VARCHAR2 (100)) DEBUG_1,
             CAST ('Tanýmsýz' AS VARCHAR2 (100)) DEBUG_2,
             CAST ('Tanýmsýz' AS VARCHAR2 (100)) DEBUG_3,
             CAST ('Tanýmsýz' AS VARCHAR2 (100)) DEBUG_4,
             CAST ('Tanýmsýz' AS VARCHAR2 (100)) DEBUG_5,
             CAST ('Tanýmsýz' AS VARCHAR2 (100)) DEBUG_6,
             CAST ('Tanýmsýz' AS VARCHAR2 (100)) DEBUG_7,
             CAST ('Tanýmsýz' AS VARCHAR2 (100)) DEBUG_8,
             CAST ('Tanýmsýz' AS VARCHAR2 (100)) DEBUG_9,
             CAST ('Tanýmsýz' AS VARCHAR2 (100)) DEBUG_10,
             MTR.PROVINCE_ID OSOS_PROVINCE_ID,                       --IL KODU
             MTR.DISTRICT_ID OSOS_DISTRICT_ID,                     --ILCE_KODU
             0 TRANSFORMER_POWER,                                 --TRAFO GÜCÜ
             MDM.REMOTE_IP GSM_IP,                                    --GSM_IP
             CASE
                WHEN     LENGTH (MDM.REMOTE_IP) >= 4
                     AND SUBSTR (MDM.REMOTE_IP, 1, 4) = '10.0'
                THEN
                   'Avea'
                WHEN     LENGTH (MDM.REMOTE_IP) >= 4
                     AND SUBSTR (MDM.REMOTE_IP, 1, 4) = '10.5'
                THEN
                   'Turkcell'
                WHEN     LENGTH (MDM.REMOTE_IP) >= 4
                     AND SUBSTR (MDM.REMOTE_IP, 1, 4) = '10.6'
                THEN
                   'Vodafone'
                ELSE
                   'Belli Deðil'
             END
                REMOTE_IP,                                         --REMOTE IP
             MTR.T1_ENDEX_OUT,
             MTR.T2_ENDEX_OUT,
             MTR.T3_ENDEX_OUT,
             MTR.T4_ENDEX_OUT,
             MTR.TOTAL_ENDEX_OUT,
             MTR.INDUCTIVE_ENDEX_OUT,
             MTR.CAPACITIVE_ENDEX_OUT,
             MTR.WIRING_METER_INSTALL_DATE SAYAC_EKLENME_TARÝHÝ,
             MTR.WIRING_MODEM_INSTALL_DATE,     --MODEM TESÝSAT EKLENME TARÝHÝ
             0 DLMS_STATUS,                                      --DLMS DURUMU
             MTR.MBS_INTEGRATED_STATUS,                   --ENTEGRASYON DURUMU
             MTR.LAST_SUCCESS_LPCOMMAND_DATE, -- EN SONKÝ BAÞARILI YÜK PROFÝLÝ TARÝHÝ
             CASE
                WHEN (TRUNC (SYSDATE) - TRUNC (MTR.WIRING_METER_INSTALL_DATE)) <=
                        30
                THEN
                   '0-1 Ay'
                WHEN (TRUNC (SYSDATE) - TRUNC (MTR.WIRING_METER_INSTALL_DATE)) BETWEEN 31
                                                                                   AND 150
                THEN
                   '2-5 Ay'
                WHEN (TRUNC (SYSDATE) - TRUNC (MTR.WIRING_METER_INSTALL_DATE)) >
                        150
                THEN
                   '5 Aydan Buyuk'
                ELSE
                   ''
             END
                WIRING_METER_INSTALL_PERIOD,            --SAYAÇ TAKILMA DÖNEMÝ
             MTR.WIRING_METER_INSTALL_DATE, --LAST_METER_ADD_DATE, --SON SAYAÇ TAKILMA TARIHI
             MTR.METER_PRODUCTION_YEAR,                    --SAYAÇ ÜRETÝM YILI
             MTR.WIRING_NOTE_CAPTION,                    --TESÝSAT NOT BAÞLIÐI
             MDM.AMR_VERSION,                                 --MODEM VERSÝYON
             MTR.MBS_MATCH_STATUS,                        --MBS EÞLEÞME DURUMU
             ' ' Z_WIRING_TYPE_FIRST_PART,        --Z_TESISAT_TURU_ilk_Kirilim
             ' ' Z_WIRING_TYPE_SECOND_PART,    --Z_TESISAT_TURU_ikinci_Kirilim
             SC.LAST_INVOICE_DATE LAST_INVOICE_DATE, --SON TAHAKKUK TARÝHÝ
             TO_CHAR(TO_DATE(T_MBS_SON_FATURA_TARIHI,'DD/MM/YYYY'),'MMYYYY') INVOICE_PERIOD,
--             SUBSTR (T_MBS_SON_FATURA_TARIHI, 0, 6) INVOICE_PERIOD, --TO_CHAR(S.LAST_INVOICE_DATE,'YYYYMM') INVOICE_PERIOD, --TAHAKKUK DÖNEMÝ
              SC.RATION_CARD_ID,                                    --KARNE NO
             ' ' READ_COMPANY_STATUS,
             ' ' MASTER_AMR_INSTALLATION_ID,
             CASE MTR.SUBSCRIBERAGRIMENT
                WHEN 1 THEN 'Aboneli'
                WHEN 0 THEN 'Abonesiz'
             END
                SUBSCRIBERAGRIMENT,
             MTR.CBS_LAYER_TYPE_ID,
             MTR.KULLANIM_DURUMU,
             ' ' ERISIM_DURUM_ALANI,
             S.S_TRAFOTIPI Trafo_Tipi,
             SC.OSOS_STATUS,
             S.S_HABERLESMETIPI,
             S.S_ABONE_TIPI,
             S.S_OSOSDURUMU,
             CASE
                WHEN S.S_ABONE_TIPI = 'AB'
                THEN
                   'Abone'
                WHEN S.S_ABONE_TIPI = 'KT'
                THEN
                   'Kontrol Sayacý'
                WHEN S.S_ABONE_TIPI = 'TN'
                THEN
                   'Toplayýcý Sayaç'
                WHEN S.S_ABONE_TIPI = 'TAN'
                THEN
                   'Tüketim Ayrýþtýrma Sayacý'
                WHEN S.S_ABONE_TIPI = 'OTA'
                THEN
                   'Özel Trafolu Abone'
                WHEN S.S_ABONE_TIPI = 'AS'
                THEN
                   'Abonesiz'
                WHEN S.S_ABONE_TIPI = 'B'
                THEN
                   'Bilinmiyor'
             END
                ABONE_TIPI_TUR,
                SC.CUT_STATUS,
                MDM.APPLICATION_MODEM_REF_ID,
                MTR.OSOS_KIND
        FROM DWH_ODS.ARIL_METERS_REPORT MTR
             LEFT JOIN DWH_ODS.ARIL_MODEM MDM
                ON (    MTR.MODEM_ID = MDM.MODEM_ID
                    AND MTR.APPLICATION_ID = MDM.APPLICATION_ID)
             LEFT JOIN DWH_EDW.DF_MRR_SUBSCRIBERS S
                ON LPAD (MTR.SUBSCRIBER_ID, 8, 0) =
                      LPAD (S.IDENTIFIERVALUE, 8, 0) AND S.S_ABONE_TIPI IS NOT NULL --(CAST(MTR.SUBSCRIBER_ID AS VARCHAR2(50))=S.IDENTIFIERVALUE)
             LEFT JOIN DWH_EDW.D_SUBSCRIBERS SC
                ON (SC.SUBSCRIBER_ID = MTR.SUBSCRIBER_ID)
       WHERE     MDM.DEVICE_ID IS NOT NULL
             AND MTR.DUMMY_STATUS = 'Gerçek'
             AND MTR.IS_ACTIVE IN ('1')
--             AND S.S_ABONE_TIPI IS NOT NULL
             ;

   PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE ('DWH_ODS', 'ARIL_CONTROL_WIRINGS');

   INSERT INTO DWH_ODS.ARIL_CONTROL_WIRINGS
      SELECT /*+PARALLEL(B.16)*/
             CASE
                WHEN (   WIRING_TYPE = 9
                      OR (    WIRING_TYPE = 2
                          AND ABONE_TIPI_TUR IN ('Tüketim Ayrýþtýrma Sayacý',
                                                 'Toplayýcý Sayaç',
                                                 'Kontrol Sayacý')))
                THEN
                   AMR_INSTALLATION_ID
                ELSE
                   NULL
             END
                TESISAT_TUR                               --'Kontrol_Tesisatý'
        FROM DWH_ODS.ARIL_REPORTS_FIRST B                             --21,127
       WHERE CASE
                WHEN (   WIRING_TYPE = 9
                      OR (    WIRING_TYPE = 2
                          AND ABONE_TIPI_TUR IN ('Tüketim Ayrýþtýrma Sayacý',
                                                 'Toplayýcý Sayaç',
                                                 'Kontrol Sayacý')))
                THEN
                   AMR_INSTALLATION_ID
                ELSE
                   NULL
             END
                IS NOT NULL;

   PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE ('DWH_ODS', 'ARIL_NON_SUBSCRIBERS');

   INSERT INTO DWH_ODS.ARIL_NON_SUBSCRIBERS
      SELECT /*+PARALLEL(A.16)*/
             CASE
                WHEN (    AMR_INSTALLATION_ID LIKE '9%'
                      AND LENGTH (AMR_INSTALLATION_ID) = 8)
                THEN
                   AMR_INSTALLATION_ID
                ELSE
                   NULL
             END
                TESISAT_TUR                                         --abonesiz
        FROM DWH_ODS.ARIL_METER_SUBSCRIBER A
       WHERE     NOT EXISTS
                    (SELECT NULL
                       FROM DWH_ODS.ARIL_CONTROL_WIRINGS B
                      WHERE A.AMR_INSTALLATION_ID = B.TESISAT_TUR)
             AND CASE
                    WHEN (    AMR_INSTALLATION_ID LIKE '9%'
                          AND LENGTH (AMR_INSTALLATION_ID) = 8)
                    THEN
                       AMR_INSTALLATION_ID
                    ELSE
                       NULL
                 END
                    IS NOT NULL;                                       --5,922

   PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE ('DWH_ODS', 'ARIL_LIGHTINING_SUBS');

   INSERT INTO DWH_ODS.ARIL_LIGHTINING_SUBS
      SELECT /*+PARALLEL(16)*/
             CASE
                WHEN WIRING_TYPE = 11 THEN AMR_INSTALLATION_ID
                ELSE NULL
             END
                TESISAT_TUR                             -- 'Aydýnlatma' 12,575
        FROM DWH_ODS.ARIL_REPORTS_FIRST
       WHERE CASE
                WHEN WIRING_TYPE = 11 THEN AMR_INSTALLATION_ID
                ELSE NULL
             END
                IS NOT NULL;

   PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE ('DWH_ODS', 'ARIL_REPORTS_SUBS');

   INSERT INTO DWH_ODS.ARIL_REPORTS_SUBS
      SELECT /*+PARALLEL(A.16)*/
             CASE
                WHEN     WIRING_TYPE = 2
                     AND ABONE_TIPI_TUR NOT IN ('Tüketim Ayrýþtýrma Sayacý',
                                                'Toplayýcý Sayaç',
                                                'Kontrol Sayacý')
                THEN
                   AMR_INSTALLATION_ID
                ELSE
                   NULL
             END
                TESISAT_TUR                                          --ABONELÝ
        FROM DWH_ODS.ARIL_REPORTS_FIRST A
       WHERE     NOT EXISTS
                    (SELECT NULL
                       FROM DWH_ODS.ARIL_CONTROL_WIRINGS B
                      WHERE A.AMR_INSTALLATION_ID = B.TESISAT_TUR)
             AND NOT EXISTS
                    (SELECT NULL
                       FROM DWH_ODS.ARIL_NON_SUBSCRIBERS B
                      WHERE A.AMR_INSTALLATION_ID = B.TESISAT_TUR)
             AND NOT EXISTS
                    (SELECT NULL
                       FROM DWH_ODS.ARIL_LIGHTINING_SUBS B
                      WHERE A.AMR_INSTALLATION_ID = B.TESISAT_TUR);  --169,903

   PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE ('DWH_EDW', 'T_AMR_ALL_DETAILS_ARIL');
   
   INSERT INTO DWH_EDW.T_AMR_ALL_DETAILS_ARIL
      SELECT /*+PARALLEL(P.16)*/DISTINCT
P.APPLICATION_ID, P.AMR_INSTALLATION_ID, P.IS_ACTIVE, P.READ_STATUS, P.READ_STATUS_V2, P.PARENT_TRANSFORMER_TYPE, P.DEVICE_ID, P.METER_SERIAL_NUMBER, P.FLOW_MULTIPLIER, P.VOLTAGE_MULTIPLIER, 
   P.ACCRUAL_MULTIPLIER, P.TOTAL_ENDEX, P.INDUCTIVE_ENDEX, P.CAPACITIVE_ENDEX, P.T1_ENDEX, P.T2_ENDEX, P.T3_ENDEX, P.T4_ENDEX, P.MAXIMUM_DEMAND, P.ENDEX_DATE, P.LAST_STATUS, P.SIGNAL_LENGTH, 
   P.LAST_ACCESS_DATE, P.DEMAND_DATE, P.DEMAND_TIME, P.AMR_MODEL, P.LAST_EXTERNAL_STATUS_CHANGE, P.OSOS_INSTALL_PERIOD, P.OSOS_INSTALL_DATE, P.OSOS_LEAVE_DATE, P.METER_TYPE_BRAND, P.METER_TYPE_MODEL_NAME, 
   P.METER_TYPE_DESCRIPTION, P.MODEM_BRAND, P.OSOS_PROVINCE, P.OSOS_DISTRICT, P.UEVCBKODU, P.SUBSCRIBER_NAME, P.NAME, P.MBS_PROVINCE, P.MBS_DISTRICT, P.T_OSOS_ADRESI, P.CUT_STATUS_NAME, P.LATITUDE, P.LONGITUDE, 
   P.SUBSCRIBER_STATUS_NAME, P.SUBSCRIBER_GROUP_NAME, P.TARIFE_KODU, P.CONNECTION_STATUS, P.OG_STATUS, P.MBS_OSOS_STATUS, P.METER_MULTIPLIER, P.METER_NUMBER, P.INSTALLED_POWER, P.CONTRACT_POWER, P.SUBSCRIBER_ID_MATCH, 
   P.METER_NUMBER_MATCH, P.VIRTUAL_GROUP_NAMES, P.DUMMY_STATUS, P.AMR_LAST_ACCESS_TIME, P.SIGNAL_DATE, P.PHYSICAL_GROUP_TREE_NAMES, P.SECTOR_INFO, P.WIRING_TYPE, P.Z_WIRING_TYPE, P.WIRING_NOTE, P.Z_INSTALLATION_TYPE, 
   P.Z_STATUS, P.BOARD_NUMBER, P.WIRING_NOTE_DATE, P.WIRING_NOTE_CREATE_USER, P.DEBUG_1, P.DEBUG_2, P.DEBUG_3, P.DEBUG_4, P.DEBUG_5, P.DEBUG_6, P.DEBUG_7, P.DEBUG_8, P.DEBUG_9, P.DEBUG_10, P.OSOS_PROVINCE_ID, 
   P.OSOS_DISTRICT_ID, P.TRANSFORMER_POWER, P.GSM_IP, P.REMOTE_IP, P.T1_ENDEX_OUT, P.T2_ENDEX_OUT, P.T3_ENDEX_OUT, P.T4_ENDEX_OUT, P.TOTAL_ENDEX_OUT, P.INDUCTIVE_ENDEX_OUT, P.CAPACITIVE_ENDEX_OUT, P."SAYAC_EKLENME_TARÝHÝ", 
   P.WIRING_MODEM_INSTALL_DATE, P.DLMS_STATUS, P.MBS_INTEGRATED_STATUS, P.LAST_SUCCESS_LPCOMMAND_DATE, P.WIRING_METER_INSTALL_PERIOD, P.WIRING_METER_INSTALL_DATE, P.METER_PRODUCTION_YEAR, P.WIRING_NOTE_CAPTION, P.AMR_VERSION, 
   P.MBS_MATCH_STATUS, P.Z_WIRING_TYPE_FIRST_PART, P.Z_WIRING_TYPE_SECOND_PART, P.LAST_INVOICE_DATE, P.INVOICE_PERIOD, P.RATION_CARD_ID, P.READ_COMPANY_STATUS, P.MASTER_AMR_INSTALLATION_ID, P.SUBSCRIBERAGRIMENT, 
   P.CBS_LAYER_TYPE_ID, P.KULLANIM_DURUMU, P.ERISIM_DURUM_ALANI, P.TRAFO_TIPI, P.OSOS_STATUS, P.S_HABERLESMETIPI, P.S_ABONE_TIPI, P.S_OSOSDURUMU, P.ABONE_TIPI_TUR, P.CUT_STATUS,P.APPLICATION_MODEM_REF_ID,
             ERISIM_DURUM_ALANI MODEM_ACCESS_LAST_STATUS,
             CASE
                WHEN KULLANIM_DURUMU IN ('D', 'K') AND P.READ_STATUS = 0
                THEN
                   'OsosKullanýmYok/OkumaYok'
                ELSE
                   'Normal'
             END
                OSOS_READ_NOT,                                --OSOS OKUMA YOK
             CASE
                WHEN     (   NVL(P.CUT_STATUS,0) = 4
                          OR NVL(P.CUT_STATUS,0) = 6)
                     AND P.READ_STATUS = 0
                THEN
                   'MbsEnerjiYok-OkumaYok'
                ELSE
                   'Normal'
             END
                MBS_OSOS_ENERGY_STATUS,               --MBS OSOS ENERJÝ DURUMU
             0 ACTIVE_KWH,                                         --AKTIF_KWH
             CASE
                WHEN P.AMR_INSTALLATION_ID = T2.TESISAT_TUR
                THEN
                   'Aydýnlatma'
                WHEN P.AMR_INSTALLATION_ID = T.TESISAT_TUR
                THEN
                   'KontrolTesisat'
                WHEN P.AMR_INSTALLATION_ID = T1.TESISAT_TUR
                THEN
                   'Abonesiz'
                WHEN P.AMR_INSTALLATION_ID = T3.TESISAT_TUR
                THEN
                   'Aboneli'
                ELSE
                   'Bilinmiyor'
             END
                TESISAT_TIP,
                P.OSOS_KIND
        FROM DWH_ODS.ARIL_REPORTS_FIRST P
             LEFT JOIN DWH_ODS.ARIL_CONTROL_WIRINGS T
                ON P.AMR_INSTALLATION_ID = T.TESISAT_TUR
             LEFT JOIN DWH_ODS.ARIL_NON_SUBSCRIBERS T1
                ON P.AMR_INSTALLATION_ID = T1.TESISAT_TUR
             LEFT JOIN DWH_ODS.ARIL_LIGHTINING_SUBS T2
                ON P.AMR_INSTALLATION_ID = T2.TESISAT_TUR
             LEFT JOIN DWH_ODS.ARIL_REPORTS_SUBS T3
                ON P.AMR_INSTALLATION_ID = T3.TESISAT_TUR;
                
PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE ('DWH', 'TMP_LUNA_METER_INSTALL');

INSERT INTO DWH.TMP_LUNA_METER_INSTALL
    SELECT
        ABONE_ID,SAYAC_ID,MIN(TRUNC(ISLEMTARIHI)) STARTDATE
    FROM DWH_ODS.LUNA_ABONEGECMISI 
    WHERE ISLEMTIPI=2 
    GROUP BY ABONE_ID,SAYAC_ID;
    
PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE ('DWH', 'TMP_LUNA_MODEM_INSTALL');

INSERT INTO DWH.TMP_LUNA_MODEM_INSTALL
    SELECT
        ABONE_ID,MODEM_ID,MIN(TRUNC(ISLEMTARIHI)) STARTDATE
    FROM DWH_ODS.LUNA_ABONEGECMISI 
    WHERE ISLEMTIPI=6 
    GROUP BY ABONE_ID,MODEM_ID;
    
PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE ('DWH', 'TMP_SUBSCRIBERS');

INSERT INTO DWH.TMP_SUBSCRIBERS
SELECT 
    SUBSCRIBER_ID,
    METER_MODEL,
    METER_NUMBER,
    METER_MULTIPLIER
    ,COUNT(SUBSCRIBER_ID) OVER (PARTITION BY METER_MODEL,METER_NUMBER) ROW_COUNT
FROM DWH_EDW.D_SUBSCRIBERS;

PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE ('DWH_EDW', 'AMR_ALL_DETAILS_ARIL');
   
INSERT INTO DWH_EDW.AMR_ALL_DETAILS_ARIL
(
APPLICATION_ID,SUBSCRIBER_ID, AMR_INSTALLATION_ID, IS_ACTIVE, READ_STATUS, READ_STATUS_V2, PARENT_TRANSFORMER_TYPE, DEVICE_ID, METER_SERIAL_NUMBER, FLOW_MULTIPLIER, VOLTAGE_MULTIPLIER, ACCRUAL_MULTIPLIER, TOTAL_ENDEX, INDUCTIVE_ENDEX, 
CAPACITIVE_ENDEX, T1_ENDEX, T2_ENDEX, T3_ENDEX, T4_ENDEX, MAXIMUM_DEMAND, ENDEX_DATE, LAST_STATUS, SIGNAL_LENGTH, LAST_ACCESS_DATE, DEMAND_DATE, DEMAND_TIME, AMR_MODEL, LAST_EXTERNAL_STATUS_CHANGE, OSOS_INSTALL_PERIOD, 
OSOS_INSTALL_DATE, OSOS_LEAVE_DATE, METER_TYPE_BRAND, METER_TYPE_MODEL_NAME, METER_TYPE_DESCRIPTION, MODEM_BRAND, OSOS_PROVINCE, OSOS_DISTRICT, UEVCBKODU, SUBSCRIBER_NAME, NAME, MBS_PROVINCE, MBS_DISTRICT, T_OSOS_ADRESI, 
CUT_STATUS_NAME, LATITUDE, LONGITUDE, SUBSCRIBER_STATUS_NAME, SUBSCRIBER_GROUP_NAME, TARIFE_KODU, CONNECTION_STATUS, OG_STATUS, MBS_OSOS_STATUS, METER_MULTIPLIER, METER_NUMBER, INSTALLED_POWER, CONTRACT_POWER, 
SUBSCRIBER_ID_MATCH, METER_NUMBER_MATCH, VIRTUAL_GROUP_NAMES, DUMMY_STATUS, AMR_LAST_ACCESS_TIME, SIGNAL_DATE, PHYSICAL_GROUP_TREE_NAMES, SECTOR_INFO, WIRING_TYPE, Z_WIRING_TYPE, WIRING_NOTE, Z_INSTALLATION_TYPE, Z_STATUS, 
BOARD_NUMBER, WIRING_NOTE_DATE, WIRING_NOTE_CREATE_USER, DEBUG_1, DEBUG_2, DEBUG_3, DEBUG_4, DEBUG_5, DEBUG_6, DEBUG_7, DEBUG_8, DEBUG_9, DEBUG_10, OSOS_PROVINCE_ID, OSOS_DISTRICT_ID, TRANSFORMER_POWER, GSM_IP, REMOTE_IP, 
T1_ENDEX_OUT, T2_ENDEX_OUT, T3_ENDEX_OUT, T4_ENDEX_OUT, TOTAL_ENDEX_OUT, INDUCTIVE_ENDEX_OUT, CAPACITIVE_ENDEX_OUT, "SAYAC_EKLENME_TARÝHÝ", WIRING_MODEM_INSTALL_DATE, DLMS_STATUS, MBS_INTEGRATED_STATUS, 
LAST_SUCCESS_LPCOMMAND_DATE, WIRING_METER_INSTALL_PERIOD, WIRING_METER_INSTALL_DATE, METER_PRODUCTION_YEAR, WIRING_NOTE_CAPTION, AMR_VERSION, MBS_MATCH_STATUS, Z_WIRING_TYPE_FIRST_PART, Z_WIRING_TYPE_SECOND_PART, 
LAST_INVOICE_DATE, INVOICE_PERIOD, RATION_CARD_ID, READ_COMPANY_STATUS, MASTER_AMR_INSTALLATION_ID, SUBSCRIBERAGRIMENT, CBS_LAYER_TYPE_ID, KULLANIM_DURUMU, ERISIM_DURUM_ALANI, TRAFO_TIPI, OSOS_STATUS, S_HABERLESMETIPI, 
S_ABONE_TIPI, S_OSOSDURUMU, ABONE_TIPI_TUR, MODEM_ACCESS_LAST_STATUS, OSOS_READ_NOT, MBS_OSOS_ENERGY_STATUS, ACTIVE_KWH, TESISAT_TIP, SANALGRUPID,MODEM_NO,OSOS_KIND
)
SELECT 
2 APPLICATION_ID,S.SUBSCRIBER_ID,S.AMR_INSTALLATION_ID,MET.IS_ACTIVE,
CASE 
    WHEN TO_CHAR(EN.OKUMATARIH,'YYYYMMDD')>TO_CHAR(SYSDATE-7, 'YYYYMMDD') THEN 1
ELSE
    0
END READ_STATUS,
CASE 
    WHEN TO_CHAR(EN.OKUMATARIH,'YYYYMMDD') IS NULL  THEN 1  ---Hiç okuma Yok
    WHEN TO_CHAR(EN.OKUMATARIH,'YYYYMMDD') IS NOT NULL AND TO_CHAR(EN.OKUMATARIH,'YYYYMMDD')<TO_CHAR(SYSDATE-7, 'YYYYMMDD') THEN 2 --- Okumasý Kesilen Tesisat
    WHEN TO_CHAR(EN.OKUMATARIH,'YYYYMMDD')>TO_CHAR(SYSDATE-7, 'YYYYMMDD') THEN 3 --okuma var
ELSE
    0
END READ_STATUS_V2,
S.TRANSFORMER_TYPE AS PARENT_TRANSFORMER_TYPE,MOD.MODEM_ID AS DEVICE_ID,MET.METER_SERIAL_NUMBER,MET.FLOW_MULTIPLIER,MET.VOLTAGE_MULTIPLIER,NVL(S.METER_MULTIPLIER,1) AS ACCRUAL_MULTIPLIER,EN.TNUMERIK,EN.INDUKTIFENERJINUMERIK,EN.KAPASITIFENERJINUMERIK,EN.T1NUMERIK,EN.T2NUMERIK,
EN.T3NUMERIK,EN.T4NUMERIK,EN.DEMANDNUMERIK,TO_DATE(TO_CHAR(EN.OKUMATARIH,'DDMMYYYY'),'DD.MM.YYYY') AS ENDEX_DATE,MOD.LAST_STATUS AS MODEM_LAST_STATUS,MOD.SIGNAL_LENGHT AS SIGNAL_LENGTH,TO_DATE(TO_CHAR(TO_DATE(MOD.LAST_ACCESS_DATE,'YYYYMMDDHH24MISS'),'YYYYMMDD'),'YYYY.MM.DD') LAST_ACCESS_DATE,TO_DATE(TO_CHAR(EN.DEMANDTARIHI,'DDMMYYYY'),'DD.MM.YYYY') AS DEMAND_DATE,NULL DEMAND_TIME,MOD.AMR_MODEL,
NULL AS LAST_EXTERNAL_STATUS_CHANGE,TO_CHAR(TO_DATE(S.DEFINITION_DATE_TO_SYSTEM,'YYYYMMDD'),'YYYY') OSOS_INSTALL_PERIOD,TO_DATE(TO_CHAR(TO_DATE(S.DEFINITION_DATE_TO_SYSTEM,'YYYYMMDDHH24MISS'),'YYYYMMDD'),'YYYY.MM.DD') OSOS_INSTALL_DATE,NULL OSOS_LEAVE_DATE,MET.METER_TYPE_BRAND,
MET.METER_TYPE_MODEL_NAME,MET.METER_TYPE_DESCRIPTION,MOD.MODEM_BRAND,IL.ISLETMEADI,ILCE.ISLETMEADI,S.OSOS_ADDRESS AS UEVCBKODU,SB.NAME AS SUBSCRIBER_NAME,SB.NAME,SB.PROVINCE,SB.AREA_NAME,SB.ADDRESS,SB.CUT_STATUS_NAME,SB.LATITUDE,SB.LONGITUDE,
SB.SUBSCRIBER_STATUS_NAME,SB.SUBSCRIBER_GROUP_NAME,SB.TARIFF_CODE,TRIM(SB.CONNECTION_STATUS),SB.OG_STATUS,SB.OSOS_STATUS,SB.METER_MULTIPLIER,SB.METER_NUMBER,SB.INSTALLED_POWER,SB.CONTRACT_POWER,NULL AS SUBSCRIBER_ID_MATCH,NULL AS METER_NUMBER_MATCH,
NULL VIRTUAL_GROUP_NAMES,MET.DUMMY_STATUS ,NULL AMR_LAST_ACCESS_TIME,TO_DATE(TO_CHAR(TO_DATE(MOD.SIGNAL_DATE,'YYYYMMDDHH24MISS'),'YYYYMMDD'),'YYYY.MM.DD') AS SIGNAL_DATE,NULL PHYSICAL_GROUP_TREE_NAMES,S.SECTOR_INFO,MET.Z_WIRING_TYPE AS WIRING_TYPE,
MET.Z_WIRING_TYPE,S.WIRING_NOTE,MET.Z_INSTALLATION_TYPE,MET.Z_STATUS,A.KATMANID AS BOARD_NUMBER, S.WIRING_NOTE_DATE, S.WIRING_NOTE_CREATE_USER,NULL DEBUG_1,NULL DEBUG_2,NULL DEBUG_3,NULL DEBUG_4,NULL DEBUG_5,
NULL DEBUG_6,NULL DEBUG_7,NULL DEBUG_8,NULL DEBUG_9,NULL DEBUG_10,S.PROVINCE_CODE AS OSOS_PROVINCE_CODE,S.DISTRICT_CODE AS OSOS_DISTRICT_CODE,MET.TRANSFORMER_POWER,MOD.REMOTE_IP,MOD.GSM_BRAND,EN.T1EXPORTNUMERIK,EN.T2EXPORTNUMERIK,EN.T3EXPORTNUMERIK,EN.T4EXPORTNUMERIK,
EN.TEXPORTNUMERIK,EN.INDUKTIFENERJIEXPORTNUMERIK,EN.KAPASITIFENERJIEXPORTNUMERIK,LMI.STARTDATE/* SUBSTR(MET.WIRING_METER_INSTALL_DATE,0,8)*/ AS SAYAC_EKLENME_TARIHI,
LMU.STARTDATE /* SUBSTR(MET.WIRING_MODEM_INSTALL_DATE,0,8)*/ AS MODEM_INSTALL_DATE,MET.DLMS_STATUS,S.MBS_SYNC_STATE AS MBS_INTEGRATED_STATUS,NULL/*TO_CHAR(MTR.LAST_SUCCESS_LPCOMMAND_DATE,'YYYYMMDD')  MET.LAST_SUCCESS_LPCOMMAND_DATE*/,LMI.STARTDATE/*TO_CHAR(MTR.WIRING_METER_INSTALL_DATE,'YYYYMM')*/ WIRING_METER_INSTALL_PERIOD,NULL AS METER_INSTALL_DATE,
MET.METER_MANIFACTURE_DATE AS METER_PRODUCTION_YEAR,S.WIRING_NOTE_CAPTION,MOD.AMR_VERSION,
CASE 
    WHEN TRIM(MET.METER_MODEL) = TRIM(SUB.METER_MODEL) AND TO_NUMBER(MET.METER_SERIAL_NUMBER) = TO_NUMBER(SUB.METER_NUMBER) AND (COALESCE(A.AKIMCARPANI*A.GERILIMCARPANI,1)) <> (CASE WHEN NVL(SUB.METER_MULTIPLIER,0) = 0 THEN 1 ELSE NVL(SUB.METER_MULTIPLIER,0) END)
         THEN 'Tahakkuk Çarpan Farklý'
    WHEN TO_NUMBER(MET.METER_SERIAL_NUMBER) <> TO_NUMBER(SUB.METER_NUMBER) THEN  'Sayaç Numarasý Farklý'
    WHEN TO_NUMBER(MET.METER_SERIAL_NUMBER) = TO_NUMBER(SUB.METER_NUMBER) AND TRIM(MET.METER_MODEL) <> TRIM(SUB.METER_MODEL) THEN 'Sayaç Marka Farklý'
    WHEN (CASE WHEN IS_NUMERIC(A.ABONENO) = 1 THEN TO_NUMBER(A.ABONENO) ELSE 0 END) <> SUB2.SUBSCRIBER_ID THEN 'Tesisat Numarasý Farklý'
    WHEN SUB.SUBSCRIBER_ID IS NULL THEN 'Mbs Sisteminde Yok'
    WHEN 1=1 THEN 'Normal'
END SYNC_STATE,
NULL AS Z_WIRING_TYPE_FIRST_PART,NULL AS Z_WIRING_TYPE_SECOND_PART,SB.LAST_INVOICE_DATE,NULL INVOICE_PERIOD,SB.RATION_CARD_ID,NULL READ_COMPANY_STATUS,
A2.ABONENO AS MASTER_AMR_INSTALLATION_ID,S.SUBSCRIBERAGRIMENT,
CASE
    WHEN A.KATMANTIPI = 1 THEN 'ADR_ABONE'
    WHEN A.KATMANTIPI = 2 THEN 'SBK_AGDIREK'
    WHEN A.KATMANTIPI = 3 THEN 'SBK_SDK'
    WHEN A.KATMANTIPI = 4 THEN 'SBK_AYDDIREK'
    WHEN A.KATMANTIPI = 5 THEN 'ADR_BINA'
    WHEN A.KATMANTIPI = 6 THEN 'SBK_ENHDIREK'
    WHEN A.KATMANTIPI = 7 THEN 'SBK_OGMUSDIREK'
    WHEN A.KATMANTIPI = 8 THEN 'SBK_TRAFOBINATIP'
END
 CBS_LAYER_TYPE_ID,
NULL KULLANIM_DURUMU,NULL ERISIM_DURUM_ALANI,S.TRANSFORMER_TYPE,SB.OSOS_STATUS AS MBS_OSOS_STATUS,NULL S_HABERLESME_TIPI,NULL S_ABONE_TIPI,NULL S_OSOSDURUMU,
CASE
    WHEN A.ABONETIPI = 1 THEN 'Abone Tesisatý'
    WHEN A.ABONETIPI = 2 THEN 'Aydýnlatma Tesisatý'
    WHEN A.ABONETIPI = 3 THEN 'Kontrol Tesisatý'
    WHEN A.ABONETIPI = 4 THEN 'Üretim Santrali'
END ABONE_TIPI_TUR,
MOD.LAST_STATUS AS MODEM_ACCESS_LAST_STATUS,NULL AS OSOS_READ_NOT,NULL MBS_OSOS_ENERGY_STATUS,NULL ACTIVE_KWH,
CASE
    WHEN A.ABONETIPI = 1 THEN 'Aboneli'
    WHEN A.ABONETIPI = 2 THEN 'Aydýnlatma'
    WHEN A.ABONETIPI = 3 THEN 'KontrolTesisat'
ELSE
    'Abonesiz'
END TESISAT_TIP,
NULL SANALGRUPID,
MOD.MODEM_NO,
MS.OSOS_DUR AS OSOS_KIND
FROM DWH_EDW.D_MRR_AMR_SUBSCRIBERS S
INNER JOIN DWH_EDW.D_MRR_AMR_METERS MET ON S.AMR_INSTALLATION_ID = MET.AMR_INSTALLATION_ID AND MET.APPLICATION_ID = 2
LEFT JOIN DWH_ODS.LUNA_ABONE A ON S.SERNO = A.ABONEID 
INNER JOIN DWH_EDW.D_MRR_AMR_MODEMS MOD ON S.MODEM_ID = MOD.MODEM_ID AND MOD.APPLICATION_ID = 2
LEFT JOIN DWH_ODS.LUNA_ENDEX EN ON S.SERNO = EN.ABONE_ID AND EN.SAYAC_ID=A.SAYAC_ID AND OKUMATIPI = 11
LEFT JOIN DWH_EDW.D_SUBSCRIBERS SB ON SB.SUBSCRIBER_ID = S.SUBSCRIBER_ID
LEFT JOIN ODS_MBS_DICLE.MASTER MS ON (SB.SUBSCRIBER_ID=MS.TESISAT_NO)
LEFT JOIN DWH_ODS.LUNA_ISLETME ILCE ON (ILCE.ISLETMEID=A.ISLETME_ID AND ILCE.DURUM=1)
LEFT JOIN DWH_ODS.LUNA_ISLETME IL ON (IL.ISLETMEID=ILCE.ISLETME_ID AND IL.DURUM=1)
--LEFT JOIN DWH_EDW.D_METERS MTR ON (S.AMR_INSTALLATION_ID = MTR.AMR_INSTALLATION_ID AND MTR.APPLICATION_ID = 2)
LEFT JOIN DWH_ODS.LUNA_ABONE A2 ON (A2.ABONEUI=S.MASTER_INTEGRATOR_WIRING AND A2.DURUM = 1)
LEFT JOIN DWH.TMP_LUNA_METER_INSTALL LMI ON (LMI.ABONE_ID=A.ABONEID AND LMI.SAYAC_ID=A.SAYAC_ID)
LEFT JOIN DWH.TMP_LUNA_MODEM_INSTALL LMU ON (LMU.ABONE_ID=A.ABONEID AND LMU.MODEM_ID=A.MODEM_ID)
LEFT JOIN DWH.TMP_SUBSCRIBERS SUB ON (SUB.SUBSCRIBER_ID=(CASE WHEN IS_NUMERIC(A.ABONENO) = 1 THEN TO_NUMBER(A.ABONENO) ELSE 0 END))
LEFT JOIN DWH.TMP_SUBSCRIBERS SUB2 ON (TRIM(SUB2.METER_NUMBER)=TO_NUMBER(MET.METER_SERIAL_NUMBER) AND TRIM(MET.METER_MODEL)=TRIM(SUB2.METER_MODEL) AND SUB2.ROW_COUNT=1)
WHERE TRIM(TRANSLATE(S.SUBSCRIBER_ID,'0123456789.', ' ')) IS NULL AND  S.APPLICATION_ID = 2 AND A.DURUM = 1;  --AND S.SUBSCRIBER_ID=6001864

UPDATE DWH_EDW.AMR_ALL_DETAILS_ARIL
SET TESISAT_TIP = 'Yedek Sayaç'
WHERE LPAD(AMR_INSTALLATION_ID,8,0) IN
(
SELECT LPAD(A.ABONENO,8,0) FROM DWH_ODS.LUNA_ABONESANALGRUP AG 
LEFT JOIN DWH_ODS.LUNA_ABONE A ON A.ABONEID = AG.ABONE_ID
WHERE SANALGRUP_ID = 2144 AND A.DURUM = 1 AND AG.DURUM = 1
);

UPDATE DWH_EDW.AMR_ALL_DETAILS_ARIL
SET TESISAT_TIP = 'Abonesiz'
WHERE LPAD(AMR_INSTALLATION_ID,8,0) IN
(
SELECT LPAD(A.ABONENO,8,0) FROM DWH_ODS.LUNA_ABONESANALGRUP AG 
LEFT JOIN DWH_ODS.LUNA_ABONE A ON A.ABONEID = AG.ABONE_ID
WHERE SANALGRUP_ID IN (2065,104) AND A.DURUM = 1 AND AG.DURUM = 1
);

   INSERT /*+PARALLEL(16) NOLOGGING*/ INTO DWH_EDW.AMR_ALL_DETAILS_ARIL        
    SELECT /*+PARALLEL(128)*/
--    A.APPLICATION_ID, A.AMR_INSTALLATION_ID, A.IS_ACTIVE,
--    CASE 
--        WHEN TO_CHAR(TO_DATE(E.ENDEX_DATE,'YYYYMMDDHH24MISS'),'YYYYMMDD')>TO_CHAR(SYSDATE, 'YYYYMMDD')-7 THEN 1
--    ELSE
--        0
--    END READ_STATUS,
--    CASE 
--        WHEN TO_CHAR(TO_DATE(E.ENDEX_DATE,'YYYYMMDDHH24MISS'),'YYYYMMDD')>TO_CHAR(SYSDATE, 'YYYYMMDD')-7 THEN 1
--    ELSE
--        0
--    END READ_STATUS_V2, 
--    A.PARENT_TRANSFORMER_TYPE, A.DEVICE_ID, A.METER_SERIAL_NUMBER, A.FLOW_MULTIPLIER, A.VOLTAGE_MULTIPLIER, 
--    A.ACCRUAL_MULTIPLIER, E.T TOTAL_ENDEX, E.RI INDUCTIVE_ENDEX, E.RC CAPACITIVE_ENDEX, E.T1 T1_ENDEX, E.T2 T2_ENDEX, E.T3 T3_ENDEX, E.T4 T4_ENDEX, E.MAX_DEM MAXIMUM_DEMAND, E.ENDEX_DATE, A.LAST_STATUS, A.SIGNAL_LENGTH, 
--    A.LAST_ACCESS_DATE, A.DEMAND_DATE, A.DEMAND_TIME, A.AMR_MODEL, A.LAST_EXTERNAL_STATUS_CHANGE, A.OSOS_INSTALL_PERIOD, A.OSOS_INSTALL_DATE, A.OSOS_LEAVE_DATE,A.METER_TYPE_BRAND, A.METER_TYPE_MODEL_NAME, 
--    A.METER_TYPE_DESCRIPTION, A.MODEM_BRAND, A.OSOS_PROVINCE, A.OSOS_DISTRICT, A.UEVCBKODU, A.SUBSCRIBER_NAME, A.NAME, A.MBS_PROVINCE, A.MBS_DISTRICT, A.T_OSOS_ADRESI, A.CUT_STATUS_NAME, 
--    A.LATITUDE, A.LONGITUDE, A.SUBSCRIBER_STATUS_NAME, A.SUBSCRIBER_GROUP_NAME, A.TARIFE_KODU, A.CONNECTION_STATUS, A.OG_STATUS, A.MBS_OSOS_STATUS, A.METER_MULTIPLIER, A.METER_NUMBER, 
--    A.INSTALLED_POWER, A.CONTRACT_POWER, A.SUBSCRIBER_ID_MATCH, A.METER_NUMBER_MATCH, A.VIRTUAL_GROUP_NAMES, A.DUMMY_STATUS, A.AMR_LAST_ACCESS_TIME, A.SIGNAL_DATE, A.PHYSICAL_GROUP_TREE_NAMES, 
--    A.SECTOR_INFO, A.WIRING_TYPE, A.Z_WIRING_TYPE, A.WIRING_NOTE, A.Z_INSTALLATION_TYPE, A.Z_STATUS, A.BOARD_NUMBER, A.WIRING_NOTE_DATE, A.WIRING_NOTE_CREATE_USER, 
--    A.DEBUG_1, A.DEBUG_2, A.DEBUG_3, A.DEBUG_4, A.DEBUG_5, A.DEBUG_6, A.DEBUG_7, A.DEBUG_8, A.DEBUG_9, A.DEBUG_10, A.OSOS_PROVINCE_ID, A.OSOS_DISTRICT_ID, E.TRANSFORMER_POWER, A.GSM_IP, A.REMOTE_IP, 
--    E.T1_OUT T1_ENDEX_OUT, E.T2_OUT T2_ENDEX_OUT, E.T3_OUT T3_ENDEX_OUT, E.T4_OUT T4_ENDEX_OUT, E.T_OUT TOTAL_ENDEX_OUT, E.RI_OUT INDUCTIVE_ENDEX_OUT, E.RC_OUT CAPACITIVE_ENDEX_OUT, A."SAYAC_EKLENME_TARÝHÝ", A.WIRING_MODEM_INSTALL_DATE, A.DLMS_STATUS, 
--    A.MBS_INTEGRATED_STATUS, A.LAST_SUCCESS_LPCOMMAND_DATE, A.WIRING_METER_INSTALL_PERIOD, A.WIRING_METER_INSTALL_DATE, A.METER_PRODUCTION_YEAR, A.WIRING_NOTE_CAPTION, A.AMR_VERSION, A.MBS_MATCH_STATUS, 
--    A.Z_WIRING_TYPE_FIRST_PART, A.Z_WIRING_TYPE_SECOND_PART, A.LAST_INVOICE_DATE, A.INVOICE_PERIOD, A.RATION_CARD_ID, A.READ_COMPANY_STATUS, A.MASTER_AMR_INSTALLATION_ID, A.SUBSCRIBERAGRIMENT, A.CBS_LAYER_TYPE_ID, 
--    A.KULLANIM_DURUMU, A.ERISIM_DURUM_ALANI, E.TRANSFORMER_TYPE TRAFO_TIPI, A.OSOS_STATUS, A.S_HABERLESMETIPI, A.S_ABONE_TIPI, A.S_OSOSDURUMU, A.ABONE_TIPI_TUR, A.MODEM_ACCESS_LAST_STATUS, A.OSOS_READ_NOT, A.MBS_OSOS_ENERGY_STATUS, 
--    A.ACTIVE_KWH, A.TESISAT_TIP
    DISTINCT 
    A.APPLICATION_ID,TO_NUMBER(A.AMR_INSTALLATION_ID) SUBSCRIBER_ID, A.AMR_INSTALLATION_ID, A.IS_ACTIVE,
    CASE 
        WHEN TO_CHAR(TO_DATE(E.ENDEX_DATE,'YYYYMMDDHH24MISS'),'YYYYMMDD')>TO_CHAR(SYSDATE-7, 'YYYYMMDD') THEN 1
    ELSE
        0
    END READ_STATUS,
    CASE 
        WHEN E.ENDEX_DATE IS NULL  THEN 1  ---Hiç okuma Yok
        WHEN E.ENDEX_DATE IS NOT NULL AND TO_CHAR(TO_DATE(E.ENDEX_DATE,'YYYYMMDDHH24MISS'),'YYYYMMDD')<TO_CHAR(SYSDATE-7, 'YYYYMMDD') THEN 2 --- Okumasý Kesilen Tesisat
        WHEN TO_CHAR(TO_DATE(E.ENDEX_DATE,'YYYYMMDDHH24MISS'),'YYYYMMDD')>TO_CHAR(SYSDATE-7, 'YYYYMMDD') THEN 3 --okuma var
    ELSE
        0
    END READ_STATUS_V2,
    A.PARENT_TRANSFORMER_TYPE, A.DEVICE_ID, A.METER_SERIAL_NUMBER, A.FLOW_MULTIPLIER, A.VOLTAGE_MULTIPLIER, 
    A.ACCRUAL_MULTIPLIER, E.T TOTAL_ENDEX, E.RI INDUCTIVE_ENDEX, E.RC CAPACITIVE_ENDEX, E.T1 T1_ENDEX, E.T2 T2_ENDEX, E.T3 T3_ENDEX, E.T4 T4_ENDEX, E.MAX_DEM MAXIMUM_DEMAND, 
    TO_DATE(TO_CHAR(TO_DATE(E.ENDEX_DATE,'YYYYMMDDHH24MISS'),'YYYYMMDD'),'YYYY.MM.DD') ENDEX_DATE, A.LAST_STATUS, A.SIGNAL_LENGTH, 
    TO_DATE(TO_CHAR(TO_DATE(MO.LAST_ACCESS_DATE,'YYYYMMDDHH24MISS'),'YYYYMMDD'),'YYYY.MM.DD') LAST_ACCESS_DATE, A.DEMAND_DATE DEMAND_DATE, A.DEMAND_TIME, A.AMR_MODEL, 
    CASE
        WHEN TO_NUMBER(A.LAST_EXTERNAL_STATUS_CHANGE) >0 THEN TO_DATE(TO_CHAR(TO_DATE(A.LAST_EXTERNAL_STATUS_CHANGE,'YYYYMMDDHH24MISS'),'YYYYMMDD'),'YYYY.MM.DD')
    ELSE
        NULL
    END LAST_EXTERNAL_STATUS_CHANGE, 
    A.OSOS_INSTALL_PERIOD, 
    TO_DATE(TO_CHAR(TO_DATE(S.D_SISTEMETANIMLANMATARIHI,'YYYYMMDDHH24MISS'),'YYYYMMDD'),'YYYY.MM.DD') OSOS_INSTALL_DATE, 
    TO_DATE(TO_CHAR(TO_DATE(A.OSOS_LEAVE_DATE,'YYYYMMDDHH24MISS'),'YYYYMMDD'),'YYYY.MM.DD'),A.METER_TYPE_BRAND, A.METER_TYPE_MODEL_NAME, 
    A.METER_TYPE_DESCRIPTION, A.MODEM_BRAND, A.OSOS_PROVINCE, A.OSOS_DISTRICT, A.UEVCBKODU, A.SUBSCRIBER_NAME, A.NAME, A.MBS_PROVINCE, A.MBS_DISTRICT, A.T_OSOS_ADRESI, A.CUT_STATUS_NAME, 
    A.LATITUDE, A.LONGITUDE, A.SUBSCRIBER_STATUS_NAME, A.SUBSCRIBER_GROUP_NAME, A.TARIFE_KODU, TRIM(A.CONNECTION_STATUS), A.OG_STATUS, A.MBS_OSOS_STATUS, A.METER_MULTIPLIER, A.METER_NUMBER, 
    A.INSTALLED_POWER, A.CONTRACT_POWER, A.SUBSCRIBER_ID_MATCH, A.METER_NUMBER_MATCH, A.VIRTUAL_GROUP_NAMES, A.DUMMY_STATUS, A.AMR_LAST_ACCESS_TIME, 
    TO_DATE(TO_CHAR(TO_DATE(A.SIGNAL_DATE,'YYYYMMDDHH24MISS'),'YYYYMMDD'),'YYYY.MM.DD') SIGNAL_DATE, 
    A.PHYSICAL_GROUP_TREE_NAMES, 
    A.SECTOR_INFO, A.WIRING_TYPE, A.Z_WIRING_TYPE, A.WIRING_NOTE, A.Z_INSTALLATION_TYPE, A.Z_STATUS, A.BOARD_NUMBER, A.WIRING_NOTE_DATE, A.WIRING_NOTE_CREATE_USER, 
    A.DEBUG_1, A.DEBUG_2, A.DEBUG_3, A.DEBUG_4, A.DEBUG_5, A.DEBUG_6, A.DEBUG_7, A.DEBUG_8, A.DEBUG_9, A.DEBUG_10, A.OSOS_PROVINCE_ID, A.OSOS_DISTRICT_ID, E.TRANSFORMER_POWER, A.GSM_IP, A.REMOTE_IP, 
    E.T1_OUT T1_ENDEX_OUT, E.T2_OUT T2_ENDEX_OUT, E.T3_OUT T3_ENDEX_OUT, E.T4_OUT T4_ENDEX_OUT, E.T_OUT TOTAL_ENDEX_OUT, E.RI_OUT INDUCTIVE_ENDEX_OUT, E.RC_OUT CAPACITIVE_ENDEX_OUT, TO_DATE(TO_CHAR(A.SAYAC_EKLENME_TARÝHÝ,'DDMMYYYY'),'DD.MM.YYYY') SAYAC_EKLENME_TARÝHÝ, 
    TO_DATE(TO_CHAR(A.WIRING_MODEM_INSTALL_DATE,'DDMMYYYY'),'DD.MM.YYYY') WIRING_MODEM_INSTALL_DATE, 
    A.DLMS_STATUS, 
    A.MBS_INTEGRATED_STATUS, TO_DATE(TO_CHAR(TO_DATE(A.LAST_SUCCESS_LPCOMMAND_DATE,'YYYYMMDDHH24MISS'),'YYYYMMDD'),'YYYY.MM.DD'), A.WIRING_METER_INSTALL_PERIOD, A.WIRING_METER_INSTALL_DATE, A.METER_PRODUCTION_YEAR, A.WIRING_NOTE_CAPTION, A.AMR_VERSION, A.MBS_MATCH_STATUS, 
    A.Z_WIRING_TYPE_FIRST_PART, A.Z_WIRING_TYPE_SECOND_PART, TO_DATE(TO_CHAR(A.LAST_INVOICE_DATE,'DDMMYYYY'),'DD.MM.YYYY') LAST_INVOICE_DATE, A.INVOICE_PERIOD, A.RATION_CARD_ID, A.READ_COMPANY_STATUS, A.MASTER_AMR_INSTALLATION_ID, A.SUBSCRIBERAGRIMENT, A.CBS_LAYER_TYPE_ID, 
    A.KULLANIM_DURUMU, A.ERISIM_DURUM_ALANI, E.TRANSFORMER_TYPE TRAFO_TIPI, A.OSOS_STATUS, A.S_HABERLESMETIPI, A.S_ABONE_TIPI, A.S_OSOSDURUMU, A.ABONE_TIPI_TUR, A.MODEM_ACCESS_LAST_STATUS, A.OSOS_READ_NOT, A.MBS_OSOS_ENERGY_STATUS, 
    A.ACTIVE_KWH, A.TESISAT_TIP,NULL,A.APPLICATION_MODEM_REF_ID,NULL,
    CASE
        WHEN  TRIM(A.METER_TYPE_BRAND) LIKE '%LUN%'     THEN   'LUN' 
        WHEN  TRIM(A.METER_TYPE_BRAND) LIKE '%VIK%'     THEN   'VIK'
        WHEN  TRIM(A.METER_TYPE_BRAND) LIKE '%ELS%'     THEN   'ELS' 
        WHEN  TRIM(A.METER_TYPE_BRAND) LIKE '%MAKEL%'     THEN   'MSY'
        WHEN  TRIM(A.METER_TYPE_BRAND) LIKE '%ELEKTROMED%'     THEN   'ELM'
        WHEN  TRIM(A.METER_TYPE_BRAND) LIKE '%EMH%'     THEN   'EMH'
        WHEN  TRIM(A.METER_TYPE_BRAND) LIKE '%KÖHLER%'     THEN   'AEL'
        WHEN  TRIM(A.METER_TYPE_BRAND) LIKE '%LANDIS%'    THEN  'LND' 
    ELSE 
        TRIM(A.METER_TYPE_BRAND)
    END METER_FLAG,
    M.OSOS_DUR OSOS_KIND
    FROM DWH_EDW.T_AMR_ALL_DETAILS_ARIL A
    LEFT JOIN DWH_ODS.ARIL_ENDEX E ON A.AMR_INSTALLATION_ID=E.TESISAT_NO 
--    AND E.TIP =
--        (CASE 
--            WHEN A.TESISAT_TIP='Aboneli' THEN 'ABONE' 
--            WHEN A.TESISAT_TIP='Aydýnlatma' THEN 'AYDINLATMA' 
--            WHEN A.TESISAT_TIP='KontrolTesisat' THEN 'TRAFO' 
--         ELSE
--            'ABONE'
--         END) 
    INNER JOIN DWH_EDW.D_MRR_AMR_METERS MTR ON A.METER_SERIAL_NUMBER = MTR.METER_SERIAL_NUMBER AND A.APPLICATION_MODEM_REF_ID=MTR.MODEM_REF_ID
    INNER JOIN DWH_EDW.D_MRR_AMR_MODEMS MO ON MTR.MODEM_REF_ID = MO.APPLICATION_MODEM_REF_ID
    LEFT JOIN DWH_EDW.DF_MRR_SUBSCRIBERS S ON A.AMR_INSTALLATION_ID = S.IDENTIFIERVALUE
    LEFT JOIN ODS_MBS_DICLE.MASTER M ON (TO_NUMBER(A.AMR_INSTALLATION_ID) = M.TESISAT_NO)
    WHERE  TRIM(TRANSLATE(A.AMR_INSTALLATION_ID,'0123456789.', ' ')) IS NULL  ; 

--    UPDATE DWH_EDW.AMR_ALL_DETAILS_ARIL
--    SET READ_STATUS =1
--    WHERE TO_CHAR(TO_DATE(ENDEX_DATE,'YYYYMMDDHH24MISS'),'YYYYMMDD')>TO_CHAR(SYSDATE-7, 'YYYYMMDD');
--
--    UPDATE DWH_EDW.AMR_ALL_DETAILS_ARIL
--    SET READ_STATUS_V2 =1
--    WHERE TO_CHAR(TO_DATE(ENDEX_DATE,'YYYYMMDDHH24MISS'),'YYYYMMDD')>TO_CHAR(SYSDATE-7, 'YYYYMMDD');
    
    COMMIT;   
     
      UPDATE DWH_DM.REPORT_LAST_UPDATE_DATE  SET UPDATE_DATE = SYSDATE 
      WHERE REPORT_NAME = 'GUNLUK FAALIYET RAPORLARI' AND ID =143;

    COMMIT;        

END;

PROCEDURE PRC_BASARSOFT_AKTARIM_BOX AS
BEGIN

   PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE ('DWH_ODS', 'TMP_BOX_PANO');
   
    INSERT /*PARALLEL(8) NOLOGGING*/ INTO DWH_ODS.TMP_BOX_PANO
    WITH TABLO AS 
    (
    SELECT
        T.CODE PARENT_CODE,
        T.AMR_INSTALLATION_ID TRASFORMER_AMR,
        S.SUBSCRIBER_ID SUBSCRIBER_AMR,
         S.OSOS_STATUS  
    FROM DWH_EDW.D_TRANSFORMERS T
    LEFT JOIN DWH_EDW.D_MRR_SUBSCRIBERS M ON (M.AMR_INSTALLATION_ID=T.AMR_INSTALLATION_ID)
    LEFT JOIN DWH_EDW.D_SUBSCRIBERS S ON (S.TRANSFORMER_NUMBER=T.CODE)
    LEFT JOIN DWH_EDW.D_MRR_SUBSCRIBERS M2 ON (M2.SUBSCRIBER_ID=S.SUBSCRIBER_ID)
    ), 
    TABLO2 AS (
    SELECT 
                    G.*,
                    C.CALENDAR MEASUREMENT_DATE 
                FROM TABLO G ,DWH_EDW.D_CALENDAR C 
                WHERE C.CALENDAR BETWEEN TO_DATE('20170101','YYYYMMDD') AND TRUNC(SYSDATE)
        )    
    SELECT TABLO2.SUBSCRIBER_AMR,TABLO2.PARENT_CODE,TABLO2.TRASFORMER_AMR,TO_CHAR(TABLO2.MEASUREMENT_DATE,'YYYYMM') MEASUREMENT_DATE,SUM(T_V.TRASFORMER_VALUE) TRASFORMER_VALUE,SUM(S_V_2.SUBSCRIBER_VALUE) SUBSCRIBER_VALUE FROM TABLO2 
     LEFT JOIN (
                SELECT V.SUBSCRIBER_ID,V.PROFILE_DATE,ROUND(SUM(V.RECEIVED_ENERGY_VALUE),3) TRASFORMER_VALUE 
                FROM DWH_EDW.F_MRR_CONSUMPTION_MONTHLY V
                WHERE EXISTS(SELECT NULL FROM TABLO2 T WHERE T.TRASFORMER_AMR=TO_CHAR(V.SUBSCRIBER_ID ))
                                 AND V.PROFILE_DATE >= TO_DATE('20170101','YYYYMMDD')
                           GROUP BY V.SUBSCRIBER_ID,V.PROFILE_DATE
                           ) T_V ON (TABLO2.TRASFORMER_AMR=TO_CHAR(T_V.SUBSCRIBER_ID)  AND TABLO2.MEASUREMENT_DATE=T_V.PROFILE_DATE)                       
        LEFT JOIN (
                           SELECT V.SUBSCRIBER_ID,V.PROFILE_DATE,ROUND(SUM(V.RECEIVED_ENERGY_VALUE),3)  SUBSCRIBER_VALUE 
                           FROM DWH_EDW.F_MRR_CONSUMPTION_MONTHLY V
                           WHERE EXISTS(SELECT NULL FROM TABLO2 T WHERE T.SUBSCRIBER_AMR=TO_CHAR(V.SUBSCRIBER_ID) )
                                 AND V.PROFILE_DATE >= TO_DATE('20170101','YYYYMMDD') 
                           GROUP BY V.SUBSCRIBER_ID,V.PROFILE_DATE
                           ) S_V ON (TABLO2.SUBSCRIBER_AMR=TO_CHAR(S_V.SUBSCRIBER_ID) AND TABLO2.MEASUREMENT_DATE=S_V.PROFILE_DATE   )       
        LEFT JOIN (
                           SELECT F.TESISAT_NO SUBSCRIBER_ID,C.CALENDAR MEASUREMENT_DATE,
                                  ROUND(SUM(( NVL(F.AKTIF_KWH,0) + NVL(F.SATICI_KWH,0))/(TO_DATE(OKUMA_TARIHI,'YYYYMMDD')-TO_DATE(ILK_OKUMA_TARIHI,'YYYYMMDD'))),3) SUBSCRIBER_VALUE
                           FROM DWH_ODS.MBS_FATURA F 
                           JOIN DWH_EDW.D_CALENDAR C ON (C.DAY_ID BETWEEN  ILK_OKUMA_TARIHI AND OKUMA_TARIHI-1)
                           WHERE EXISTS(SELECT NULL FROM TABLO2 T WHERE T.SUBSCRIBER_AMR=F.TESISAT_NO)
                                 AND TO_DATE(F.OKUMA_TARIHI,'YYYYMMDD') >= TO_DATE('20170101','YYYYMMDD') AND FATURA_KODU NOT IN (2,9)
                           GROUP BY F.TESISAT_NO,C.CALENDAR
                           ) S_V_2 ON (TABLO2.SUBSCRIBER_AMR=S_V_2.SUBSCRIBER_ID AND TABLO2.MEASUREMENT_DATE=S_V_2.MEASUREMENT_DATE)
                            WHERE TABLO2.TRASFORMER_AMR IS NOT NULL AND   TABLO2.SUBSCRIBER_AMR  IS NOT NULL
                            GROUP BY TABLO2.SUBSCRIBER_AMR,TABLO2.PARENT_CODE,TABLO2.TRASFORMER_AMR,TO_CHAR(TABLO2.MEASUREMENT_DATE,'YYYYMM') ;

--    PCG_DWH_CONFIGURE.P_TRUNCATE_TABLE ('DWH_EDW', 'OSOS_BASARSOFT_BOX_AKTARIM');                            
                            
--    INSERT /*+PARALLEL(8) NOLOGGING)*/ INTO DWH_EDW.OSOS_BASARSOFT_BOX_AKTARIM
--    SELECT
--    rownum+100832 ID,
--    TO_CHAR(SUBSCRIBER_AMR) SUBSCRIBER_ID, PARENT_CODE TRANSFORMER_CODE, TO_NUMBER(TRASFORMER_AMR) TRANSFORMER_ID, TO_NUMBER(MEASUREMENT_DATE) MEASUREMENT_YEARMONTH, ROUND(TRASFORMER_VALUE) CONSUMPTION_VALUE, ROUND(SUBSCRIBER_VALUE) SUBSCRIBER_VALUE,SYSDATE UPDATE_DATE
--    FROM DWH_ODS.TMP_BOX_PANO
--    WHERE TO_NUMBER(DWH_CONFIG.IS_NUMERIC(TRASFORMER_AMR)) = 1 AND TO_NUMBER(DWH_CONFIG.IS_NUMERIC(MEASUREMENT_DATE)) = 1;
--     
    END;
END PCG_ARIL_REPORTS;
/