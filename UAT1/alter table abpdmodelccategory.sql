--=======================================================
--            ALTER TABLE abpdmodelccategory           --
--=======================================================

ALTER TABLE olapts.abpdmodelccategory
ADD COLUMN IF NOT EXISTS score numeric,
ADD COLUMN IF NOT EXISTS pd numeric;

--=================================================
--CREATE BACKUP OF OLD abpdmodelccategory               --------
--=================================================

--drop table if exists olapts.abpdmodelccategory_backup
create table olapts.abpdmodelccategory_backup as
select * from olapts.abpdmodelccategory;

--=================================================
------CHECK DATA-----------------------------------
--=================================================
select *  from olapts.abpdmodelccategory_backup limit 100;

--=================================================
-------        DROP OLD abpdmodelccategory       ---------------
--=================================================
--****Uncomment row 26 after the backup****--
--DROP TABLE IF EXISTS olapts.abpdmodelccategory;

--=================================================
--              CREATE NEW abpdmodelccategory     --------------
--=================================================
CREATE TABLE IF NOT EXISTS olapts.abpdmodelccategory
(
    factmappdinstanceid_ character varying COLLATE pg_catalog."default",
    pkid_ character varying COLLATE pg_catalog."default",
	score numeric,
	pd numeric,
    accountreceivablesdays numeric,
    accountreceivablesdays2 numeric,
    accountreceivablesdays3 numeric,
    accountspayabledays numeric,
    accountspayabledays2 numeric,
    accountspayabledays3 numeric,
    activityofbusiness integer,
    activityofbusinessval text COLLATE pg_catalog."default",
    afm character varying COLLATE pg_catalog."default",
    buildingcondition integer,
    buildingconditionval text COLLATE pg_catalog."default",
    cashliquidity numeric,
    cashliquidity2 numeric,
    cashliquidity3 numeric,
    cashtocurrentliabilities numeric,
    cashtocurrentliabilities2 numeric,
    cashtocurrentliabilities3 numeric,
    companyname character varying COLLATE pg_catalog."default",
    competitionlevel integer,
    competitionlevelval text COLLATE pg_catalog."default",
    constructioncontracts numeric,
    coopyearsalpha integer,
    costsnxcellfillings numeric,
    currentratio numeric,
    currentratio2 numeric,
    currentratio3 numeric,
    customerprogress integer,
    customerprogressval text COLLATE pg_catalog."default",
    debtcoverageratio numeric,
    debtcoverageratio2 numeric,
    debtcoverageratio3 numeric,
    debttoebitda numeric,
    debttoebitda2 numeric,
    debttoebitda3 numeric,
    debttosales numeric,
    debttosales2 numeric,
    debttosales3 numeric,
    delinqpaidyc1 numeric,
    delinqpaidyc2 numeric,
    delinqpaidyc3 numeric,
    delinqpaidyc4 numeric,
    delinquenciesratio numeric,
    delinquenciesratio2 numeric,
    delinquenciesratio3 numeric,
    deprecimpairment numeric,
    distribution integer,
    dividendspayable numeric,
    ebtda numeric,
    ebtdatodebt numeric,
    ebtdatodebt2 numeric,
    ebtdatodebt3 numeric,
    ebtdatosales numeric,
    ebtdatosales2 numeric,
    ebtdatosales3 numeric,
    exports boolean,
    fcashtocurrentliabilities numeric,
    fdaysinsuppliers numeric,
    fdelinquenciesratio numeric,
    febtdatodebt numeric,
    ffebttosales numeric,
    ffinancialleverage numeric,
    finabilitytopayinterest numeric,
    financialleverage numeric,
    financialleverage2 numeric,
    financialleverage3 numeric,
    financialstatementsareaudited integer,
    financialstatementsareauditedval text COLLATE pg_catalog."default",
    fmanagerialskills numeric,
    fmarketing numeric,
    fownershipdel numeric,
    fpastduestofinancing numeric,
    freeflowstosales numeric,
    freeflowstosales2 numeric,
    freeflowstosales3 numeric,
    fsize numeric,
    ftrendrank numeric,
    fworkingcapitaltosales numeric,
    geographicalcoverage integer,
    geographicalcoverageval text COLLATE pg_catalog."default",
    grossprofitmargin numeric,
    grossprofitmargin2 numeric,
    grossprofitmargin3 numeric,
    inabilitytopayinter integer,
    inabilitytopayinterval text COLLATE pg_catalog."default",
    indicativeratingwithtempdata integer,
    indicativeratingwithtempdataval text COLLATE pg_catalog."default",
    interestcoverage numeric,
    interestcoverage2 numeric,
    interestcoverage3 numeric,
    interestincome numeric,
    inventorydays numeric,
    inventorydays2 numeric,
    inventorydays3 numeric,
    investementsinpropertycp numeric,
    machineryandequipment integer,
    machineryequipmentage integer,
    management integer,
    managerability integer,
    managerabilityval text COLLATE pg_catalog."default",
    marketinglevel integer,
    marketinglevelval text COLLATE pg_catalog."default",
    masterid character varying COLLATE pg_catalog."default",
    mdelinquenciesratio numeric,
    miss integer,
    missval text COLLATE pg_catalog."default",
    mmanagerialskills character varying COLLATE pg_catalog."default",
    mmarketing character varying COLLATE pg_catalog."default",
    mtrendrank character varying COLLATE pg_catalog."default",
    netmargintoreserves numeric,
    netmargintoreserves2 numeric,
    netmargintoreserves3 numeric,
    netmargintototalassets numeric,
    netmargintototalassets2 numeric,
    netmargintototalassets3 numeric,
    numberofproducts integer,
    operatingflows numeric,
    operatingyears integer,
    othercurrentassets numeric,
    othercurrentliabilities numeric,
    otherdebtors numeric,
    ownershipdeliq integer,
    ownershipdeliqval text COLLATE pg_catalog."default",
    pastduestofinancing numeric,
    pastduestofinancing2 numeric,
    pastduestofinancing3 numeric,
    prepaymentsbycustomers numeric,
    prepaymentscp numeric,
    production integer,
    productsandservices integer,
    productsandservicesval text COLLATE pg_catalog."default",
    programmis character varying COLLATE pg_catalog."default",
    programmisval text COLLATE pg_catalog."default",
    quickratio numeric,
    quickratio2 numeric,
    quickratio3 numeric,
    receivables numeric,
    receivables2 numeric,
    receivables3 numeric,
    receivablestosales numeric,
    receivablestosales2 numeric,
    receivablestosales3 numeric,
    sales numeric,
    sales2 numeric,
    sales3 numeric,
    salesgrowth numeric,
    salesgrowth2 numeric,
    salesgrowth3 numeric,
    salestoassets numeric,
    salestoassets2 numeric,
    salestoassets3 numeric,
    sectorprogress integer,
    sectorprogressval text COLLATE pg_catalog."default",
    succesionstatus integer,
    succesionstatusval text COLLATE pg_catalog."default",
    taxes numeric,
    tbscore character varying COLLATE pg_catalog."default",
    tbscoredate date,
    tbscoredateteiresias character varying COLLATE pg_catalog."default",
    technologylevel integer,
    technologylevelval text COLLATE pg_catalog."default",
    token character varying COLLATE pg_catalog."default",
    total bigint,
    totaldelinqyc1 numeric,
    totaldelinqyc2 numeric,
    totaldelinqyc3 numeric,
    totaldelinqyc4 numeric,
    totalfixedassets numeric,
    totalliabilitiestonetworth numeric,
    totalliabilitiestonetworth2 numeric,
    totalliabilitiestonetworth3 numeric,
    transactionid bigint,
    unidividendspayable numeric,
    willinglesstodiscloseinform integer,
    willinglesstodiscloseinformval text COLLATE pg_catalog."default",
    workingcapital numeric,
    workingcapital2 numeric,
    workingcapital3 numeric,
    workingcapitaltosales numeric,
    workingcapitaltosales2 numeric,
    workingcapitaltosales3 numeric,
    organizationchart integer,
    organizationchartval text COLLATE pg_catalog."default",
    wfid_ character varying COLLATE pg_catalog."default",
    taskid_ character varying COLLATE pg_catalog."default",
    versionid_ integer,
    isdeleted_ boolean,
    islatestversion_ boolean,
    baseversionid_ integer,
    contextuserid_ character varying COLLATE pg_catalog."default",
    isvisible_ boolean,
    isvalid_ boolean,
    snapshotid_ integer,
    t_ character varying COLLATE pg_catalog."default",
    createdby_ character varying COLLATE pg_catalog."default",
    createddate_ timestamp without time zone,
    updatedby_ character varying COLLATE pg_catalog."default",
    updateddate_ timestamp without time zone,
    fkid_entity character varying COLLATE pg_catalog."default",
    sourcepopulatedby_ character varying COLLATE pg_catalog."default",
    sourcepopulateddate_ timestamp without time zone,
    populateddate_ timestamp without time zone
);

--=============================================================================
--INSERT INTO olapts.abutp data from olapts.abpdmodelccategory_backup
--=============================================================================
INSERT INTO olapts.abpdmodelccategory(
	factmappdinstanceid_, pkid_,score,pd, accountreceivablesdays, accountreceivablesdays2, accountreceivablesdays3, accountspayabledays, accountspayabledays2, accountspayabledays3, activityofbusiness, activityofbusinessval, afm, buildingcondition, buildingconditionval, cashliquidity, cashliquidity2, cashliquidity3, cashtocurrentliabilities, cashtocurrentliabilities2, cashtocurrentliabilities3, companyname, competitionlevel, competitionlevelval, constructioncontracts, coopyearsalpha, costsnxcellfillings, currentratio, currentratio2, currentratio3, customerprogress, customerprogressval, debtcoverageratio, debtcoverageratio2, debtcoverageratio3, debttoebitda, debttoebitda2, debttoebitda3, debttosales, debttosales2, debttosales3, delinqpaidyc1, delinqpaidyc2, delinqpaidyc3, delinqpaidyc4, delinquenciesratio, delinquenciesratio2, delinquenciesratio3, deprecimpairment, distribution, dividendspayable, ebtda, ebtdatodebt, ebtdatodebt2, ebtdatodebt3, ebtdatosales, ebtdatosales2, ebtdatosales3, exports, fcashtocurrentliabilities, fdaysinsuppliers, fdelinquenciesratio, febtdatodebt, ffebttosales, ffinancialleverage, finabilitytopayinterest, financialleverage, financialleverage2, financialleverage3, financialstatementsareaudited, financialstatementsareauditedval, fmanagerialskills, fmarketing, fownershipdel, fpastduestofinancing, freeflowstosales, freeflowstosales2, freeflowstosales3, fsize, ftrendrank, fworkingcapitaltosales, geographicalcoverage, geographicalcoverageval, grossprofitmargin, grossprofitmargin2, grossprofitmargin3, inabilitytopayinter, inabilitytopayinterval, indicativeratingwithtempdata, indicativeratingwithtempdataval, interestcoverage, interestcoverage2, interestcoverage3, interestincome, inventorydays, inventorydays2, inventorydays3, investementsinpropertycp, machineryandequipment, machineryequipmentage, management, managerability, managerabilityval, marketinglevel, marketinglevelval, masterid, mdelinquenciesratio, miss, missval, mmanagerialskills, mmarketing, mtrendrank, netmargintoreserves, netmargintoreserves2, netmargintoreserves3, netmargintototalassets, netmargintototalassets2, netmargintototalassets3, numberofproducts, operatingflows, operatingyears, othercurrentassets, othercurrentliabilities, otherdebtors, ownershipdeliq, ownershipdeliqval, pastduestofinancing, pastduestofinancing2, pastduestofinancing3, prepaymentsbycustomers, prepaymentscp, production, productsandservices, productsandservicesval, programmis, programmisval, quickratio, quickratio2, quickratio3, receivables, receivables2, receivables3, receivablestosales, receivablestosales2, receivablestosales3, sales, sales2, sales3, salesgrowth, salesgrowth2, salesgrowth3, salestoassets, salestoassets2, salestoassets3, sectorprogress, sectorprogressval, succesionstatus, succesionstatusval, taxes, tbscore, tbscoredate, tbscoredateteiresias, technologylevel, technologylevelval, token, total, totaldelinqyc1, totaldelinqyc2, totaldelinqyc3, totaldelinqyc4, totalfixedassets, totalliabilitiestonetworth, totalliabilitiestonetworth2, totalliabilitiestonetworth3, transactionid, unidividendspayable, willinglesstodiscloseinform, willinglesstodiscloseinformval, workingcapital, workingcapital2, workingcapital3, workingcapitaltosales, workingcapitaltosales2, workingcapitaltosales3, organizationchart, organizationchartval, wfid_, taskid_, versionid_, isdeleted_, islatestversion_, baseversionid_, contextuserid_, isvisible_, isvalid_, snapshotid_, t_, createdby_, createddate_, updatedby_, updateddate_, fkid_entity, sourcepopulatedby_, sourcepopulateddate_, populateddate_)
select
	factmappdinstanceid_, pkid_,score,pd, accountreceivablesdays, accountreceivablesdays2, accountreceivablesdays3, accountspayabledays, accountspayabledays2, accountspayabledays3, activityofbusiness, activityofbusinessval, afm, buildingcondition, buildingconditionval, cashliquidity, cashliquidity2, cashliquidity3, cashtocurrentliabilities, cashtocurrentliabilities2, cashtocurrentliabilities3, companyname, competitionlevel, competitionlevelval, constructioncontracts, coopyearsalpha, costsnxcellfillings, currentratio, currentratio2, currentratio3, customerprogress, customerprogressval, debtcoverageratio, debtcoverageratio2, debtcoverageratio3, debttoebitda, debttoebitda2, debttoebitda3, debttosales, debttosales2, debttosales3, delinqpaidyc1, delinqpaidyc2, delinqpaidyc3, delinqpaidyc4, delinquenciesratio, delinquenciesratio2, delinquenciesratio3, deprecimpairment, distribution, dividendspayable, ebtda, ebtdatodebt, ebtdatodebt2, ebtdatodebt3, ebtdatosales, ebtdatosales2, ebtdatosales3, exports, fcashtocurrentliabilities, fdaysinsuppliers, fdelinquenciesratio, febtdatodebt, ffebttosales, ffinancialleverage, finabilitytopayinterest, financialleverage, financialleverage2, financialleverage3, financialstatementsareaudited, financialstatementsareauditedval, fmanagerialskills, fmarketing, fownershipdel, fpastduestofinancing, freeflowstosales, freeflowstosales2, freeflowstosales3, fsize, ftrendrank, fworkingcapitaltosales, geographicalcoverage, geographicalcoverageval, grossprofitmargin, grossprofitmargin2, grossprofitmargin3, inabilitytopayinter, inabilitytopayinterval, indicativeratingwithtempdata, indicativeratingwithtempdataval, interestcoverage, interestcoverage2, interestcoverage3, interestincome, inventorydays, inventorydays2, inventorydays3, investementsinpropertycp, machineryandequipment, machineryequipmentage, management, managerability, managerabilityval, marketinglevel, marketinglevelval, masterid, mdelinquenciesratio, miss, missval, mmanagerialskills, mmarketing, mtrendrank, netmargintoreserves, netmargintoreserves2, netmargintoreserves3, netmargintototalassets, netmargintototalassets2, netmargintototalassets3, numberofproducts, operatingflows, operatingyears, othercurrentassets, othercurrentliabilities, otherdebtors, ownershipdeliq, ownershipdeliqval, pastduestofinancing, pastduestofinancing2, pastduestofinancing3, prepaymentsbycustomers, prepaymentscp, production, productsandservices, productsandservicesval, programmis, programmisval, quickratio, quickratio2, quickratio3, receivables, receivables2, receivables3, receivablestosales, receivablestosales2, receivablestosales3, sales, sales2, sales3, salesgrowth, salesgrowth2, salesgrowth3, salestoassets, salestoassets2, salestoassets3, sectorprogress, sectorprogressval, succesionstatus, succesionstatusval, taxes, tbscore, tbscoredate, tbscoredateteiresias, technologylevel, technologylevelval, token, total, totaldelinqyc1, totaldelinqyc2, totaldelinqyc3, totaldelinqyc4, totalfixedassets, totalliabilitiestonetworth, totalliabilitiestonetworth2, totalliabilitiestonetworth3, transactionid, unidividendspayable, willinglesstodiscloseinform, willinglesstodiscloseinformval, workingcapital, workingcapital2, workingcapital3, workingcapitaltosales, workingcapitaltosales2, workingcapitaltosales3, organizationchart, organizationchartval, wfid_, taskid_, versionid_, isdeleted_, islatestversion_, baseversionid_, contextuserid_, isvisible_, isvalid_, snapshotid_, t_, createdby_, createddate_, updatedby_, updateddate_, fkid_entity, sourcepopulatedby_, sourcepopulateddate_, populateddate_
from olapts.abpdmodelccategory_backup;

	
--=================================================
--CHECK DATA
--=================================================
--select * from olapts.abpdmodelccategory

--=================================================
--DROP BACKUP TABLE
--=================================================
--DROP TABLE olapts.abpdmodelccategory_backup

--=================================================
--Run Ralph Script
--=================================================