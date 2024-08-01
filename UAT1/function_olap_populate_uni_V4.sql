-----------------------------------------------------------------------------------------------------------------------------------------------------------
--                                                   Run script in OLAP database 
-----------------------------------------------------------------------------------------------------------------------------------------------------------

-- FUNCTION: olapts.populate_olap_uni()

-- DROP FUNCTION IF EXISTS olapts.populate_olap_uni();

CREATE OR REPLACE FUNCTION olapts.populate_olap_uni(
	)
    RETURNS boolean
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare
 pl_status boolean:=FALSE;
 max_refreshhistory TIMESTAMP;
 varprevsuccessdate TIMESTAMP ;
begin
max_refreshhistory =(SELECT COALESCE(max(asofdate), NOW()::timestamp) from olapts.refreshhistory);	
--------------
-- run with postgres account in olap database
-- GRANT USAGE ON FOREIGN SERVER matenantserver to olap;
--Run once
--ALTER SYSTEM SET checkpoint_completion_target = '0.9';

-- drop table olapts.refreshhistory;
-- run with olap account in olap database
-- IMPORT FOREIGN SCHEMA tenant LIMIT TO (utp) FROM SERVER matenantserver INTO madata;
-- IMPORT FOREIGN SCHEMA tenant LIMIT TO (leverageindication) FROM SERVER matenantserver INTO madata;
-- IMPORT FOREIGN SCHEMA tenant LIMIT TO (teiresiasdata) FROM SERVER matenantserver INTO madata;
-- IMPORT FOREIGN SCHEMA tenant LIMIT TO (espolicy) FROM SERVER matenantserver INTO madata;
-- IMPORT FOREIGN SCHEMA tenant LIMIT TO (ebadefinition) FROM SERVER matenantserver INTO madata;
-- IMPORT FOREIGN SCHEMA tenant LIMIT TO (correctiveactions) FROM SERVER matenantserver INTO madata;
-- IMPORT FOREIGN SCHEMA tenant LIMIT TO (correctiveactionsmaster) FROM SERVER matenantserver INTO madata;
-- IMPORT FOREIGN SCHEMA tenant LIMIT TO (specialdelta) FROM SERVER matenantserver INTO madata;
-- IMPORT FOREIGN SCHEMA tenant LIMIT TO (esgassessment) FROM SERVER matenantserver INTO madata;
-- IMPORT FOREIGN SCHEMA tenant LIMIT TO (esgquestion) FROM SERVER matenantserver INTO madata;
-- IMPORT FOREIGN SCHEMA tenant LIMIT TO (esgoverallassessment) FROM SERVER matenantserver INTO madata;

--Call function imports_and_fetch_size suggested by DSP
raise notice '% - Step imports_and_fetch_size - part a start', clock_timestamp(); 
PERFORM olapts.imports_and_fetch_size();
raise notice '% - Step imports_and_fetch_size - part a end', clock_timestamp(); 

-- Increase work memory and effective cache for complex operations/sorts
set work_mem = '10 GB';
set maintenance_work_mem = '10 GB';
set effective_cache_size = '48 GB';
set effective_io_concurrency = 300;

--suggest planner to disregard sequential scanning and enable partitionwise aggregates
set random_page_cost = 1.5;
set enable_partitionwise_join = on;
set enable_partitionwise_aggregate = on;
set force_parallel_mode = on;
set parallel_setup_cost = 10;
set parallel_tuple_cost = 0.001;

-- Increase workers to reach multiparallel processing
--set max_worker_processes = 32;
set max_parallel_workers_per_gather = 16;
set max_parallel_maintenance_workers = 10;
set max_parallel_workers = 32;
set parallel_leader_participation = on;
set default_statistics_target = 500;

--JIT
set jit = on;
set jit_above_cost = 100000;
set jit_optimize_above_cost = 500000;
set jit_inline_above_cost = 500000;

SET TIMEZONE = 'UTC';
SET SESSION myvariables.popdate = NOW;

--DO $$
--DECLARE
--varprevsuccessdate TIMESTAMP ;
--BEGIN

CREATE TABLE IF NOT EXISTS olapts.refreshhistory (
tablename VARCHAR,
asofdate TIMESTAMP WITHOUT TIME ZONE DEFAULT (now() at time zone 'utc'),
prevsuccessdate TIMESTAMP WITHOUT TIME ZONE DEFAULT (now() at time zone 'utc')
);
	   
--END $$;

--DO $$
--DECLARE
--varprevsuccessdate TIMESTAMP ;
--
--BEGIN
--
---- Model B Category ----

-- If table exists in refresh history --
IF EXISTS (select from olapts.refreshhistory where tablename = 'ABPDMODELBCATEGORY') THEN
raise notice '% - Step abpdmodelbcategory - part a start', clock_timestamp();
insert into olapts.abpdmodelbcategory
	SELECT 
		 mapinstance.id_ AS factmapinstanceid_,
		 mapinstance.pkid_::varchar as pkid,	
		(mapinstance.jsondoc_ ->> 'Pd'::text)::numeric AS pd,
		(mapinstance.jsondoc_->>'Job')::int4 job,
		(l0.jsondoc_->> 'Value'::text) AS jobval,
		(mapinstance.jsondoc_ ->> 'Debt'::text)::numeric AS debt,
        (mapinstance.jsondoc_->>'Grade') AS grade,		
		(l1.jsondoc_->> 'Grade'::text) AS gradeval,
		(mapinstance.jsondoc_ ->> 'Score'::text)::numeric AS score,
		(mapinstance.jsondoc_ ->> 'Year1'::text)::integer AS year1,
		(mapinstance.jsondoc_ ->> 'Year2'::text)::integer AS year2,
		(mapinstance.jsondoc_ ->> 'Year3'::text)::integer AS year3,
		(mapinstance.jsondoc_ ->> 'Sales1'::text)::numeric AS sales1,
		(mapinstance.jsondoc_ ->> 'Sales2'::text)::numeric AS sales2,
		(mapinstance.jsondoc_ ->> 'Sales3'::text)::numeric AS sales3,
		(mapinstance.jsondoc_ ->> 'Balance'::text)::numeric AS balance,
		(mapinstance.jsondoc_ ->> 'Exports'::text)::boolean AS exports,
		(mapinstance.jsondoc_ ->> 'ModelId'::text)::character varying AS modelid,
		(mapinstance.jsondoc_ ->> 'Months1'::text)::numeric AS months1,
		(mapinstance.jsondoc_ ->> 'Months2'::text)::numeric AS months2,
		(mapinstance.jsondoc_ ->> 'Months3'::text)::numeric AS months3,
		(mapinstance.jsondoc_ ->> 'BankDept'::text)::numeric AS bankdept,
		(mapinstance.jsondoc_ ->> 'EntityId'::text)::integer AS entityid,
		(mapinstance.jsondoc_ ->> 'Sales112'::text)::numeric AS sales112,
		(mapinstance.jsondoc_ ->> 'Sales212'::text)::numeric AS sales212,
		(mapinstance.jsondoc_ ->> 'BankDebt2'::text)::numeric AS bankdebt2,
		(mapinstance.jsondoc_ ->> 'ProfitLoss1'::text)::numeric AS profitloss1,
		(mapinstance.jsondoc_ ->> 'ProfitLoss2'::text)::numeric AS profitloss2,
		(mapinstance.jsondoc_ ->> 'ProfitLoss3'::text)::numeric AS profitloss3,
		(mapinstance.jsondoc_ ->> 'SalesGrowth'::text)::numeric AS salesgrowth,
		(mapinstance.jsondoc_ ->> 'MsalesGrowth'::text)::numeric AS msalesgrowth,
		(mapinstance.jsondoc_ ->> 'AmountPastDue'::text)::numeric AS amountpastdue,	
        (mapinstance.jsondoc_->>'CreditHistory')::int4 credithistory,	
		(l2.jsondoc_->> 'Value'::text) AS credithistoryval,
		(mapinstance.jsondoc_ ->> 'DelinqDateTo1'::text)::timestamp without time zone AS delinqdateto1,
		(mapinstance.jsondoc_ ->> 'DelinqDateTo2'::text)::timestamp without time zone AS delinqdateto2,
		(mapinstance.jsondoc_ ->> 'DelinqDateTo3'::text)::timestamp without time zone AS delinqdateto3,
		(mapinstance.jsondoc_ ->> 'RecentDamages'::text)::boolean AS recentdamages,
		(mapinstance.jsondoc_ ->> 'BalanceToSales'::text)::numeric AS balancetosales,
        (mapinstance.jsondoc_->>'ManagerAbility')::int4 managerability,
		(l3.jsondoc_ ->> 'Value'::text) AS managerabilityval, 
		(mapinstance.jsondoc_ ->> 'OperatingYears'::text)::integer AS operatingyears,
		(mapinstance.jsondoc_ ->> 'TeiresiasDate1'::text)::character varying AS teiresiasdate1,
		(mapinstance.jsondoc_ ->> 'TeiresiasDate2'::text)::character varying AS teiresiasdate2,
		(mapinstance.jsondoc_ ->> 'TeiresiasDate3'::text)::character varying AS teiresiasdate3,
		(mapinstance.jsondoc_ ->> 'DelinqDateFrom1'::text)::timestamp without time zone AS delinqdatefrom1,
		(mapinstance.jsondoc_ ->> 'DelinqDateFrom2'::text)::timestamp without time zone AS delinqdatefrom2,
		(mapinstance.jsondoc_ ->> 'DelinqDateFrom3'::text)::timestamp without time zone AS delinqdatefrom3,
		(mapinstance.jsondoc_ ->> 'EntityVersionId'::text)::integer AS entityversionid,
		(mapinstance.jsondoc_ ->> 'MbalanceToSales'::text)::numeric AS mbalancetosales,
		(mapinstance.jsondoc_ ->> 'OtherRiskPoints'::text)::boolean AS otherriskpoints,	
		(mapinstance.jsondoc_->>'SuccessorStatus')::int4 successorstatus,
		(l4.jsondoc_ ->> 'Value'::text) AS successorstatusval,  
        (mapinstance.jsondoc_->>'CompetitionLevel')::int4 competitionlevel,
		(l5.jsondoc_ ->> 'Value'::text) AS competitionlevelval, 
		(mapinstance.jsondoc_ ->> 'CooperationYears'::text)::integer AS cooperationyears,
		(mapinstance.jsondoc_ ->> 'TotalDelinqYear1'::text)::numeric AS totaldelinqyear1,
		(mapinstance.jsondoc_ ->> 'TotalDelinqYear2'::text)::numeric AS totaldelinqyear2,
		(mapinstance.jsondoc_ ->> 'TotalDelinqYear3'::text)::numeric AS totaldelinqyear3,
		(mapinstance.jsondoc_ ->> 'InterimVatStmmnt1'::text)::numeric AS interimvatstmmnt1,
		(mapinstance.jsondoc_ ->> 'InterimVatStmmnt2'::text)::numeric AS interimvatstmmnt2,
		(mapinstance.jsondoc_ ->> 'NumberOfEmployees'::text)::integer AS numberofemployees,
        (mapinstance.jsondoc_->>'BusinessOfActivity')::int4 businessofactivity,	
		(l6.jsondoc_ ->> 'Value'::text) AS businessofactivityval, 
		(mapinstance.jsondoc_ ->> 'DelinquenciesRatio'::text)::numeric AS delinquenciesratio,
		(mapinstance.jsondoc_ ->> 'MpastDuesToBalance'::text)::numeric AS mpastduestobalance,	
		(mapinstance.jsondoc_->>'InabilityToPayInter')::int4 inabilitytopayinter,
		(l7.jsondoc_ ->> 'Value'::text) AS inabilitytopayinterval, 
		(mapinstance.jsondoc_ ->> 'InterimVatStatement'::text)::timestamp without time zone AS interimvatstatement,
		(mapinstance.jsondoc_ ->> 'MdelinquenciesRatio'::text)::numeric AS mdelinquenciesratio,
		(mapinstance.jsondoc_ ->> 'InvestmentInProgress'::text)::boolean AS investmentinprogress,	
		(mapinstance.jsondoc_->>'CustomerConcentration')::int4 customerconcentration,
		(l8.jsondoc_ ->> 'Value'::text) AS customerconcentrationval, 
		(mapinstance.jsondoc_ ->> 'DateInterimVatStmmnt1'::text)::timestamp without time zone AS dateinterimvatstmmnt1,
		(mapinstance.jsondoc_ ->> 'DateInterimVatStmmnt2'::text)::timestamp without time zone AS dateinterimvatstmmnt2,
		(mapinstance.jsondoc_ ->> 'InabilityToPayInterest'::text)::numeric AS inabilitytopayinterest, 	
		(mapinstance.jsondoc_ ->> 'MinabilityToPayInterest'::text)::numeric AS minabilitytopayinterest, 	
		(mapinstance.jsondoc_->>'PersonalPropertyLending')::int4 personalpropertylending,
		(l9.jsondoc_ ->> 'Value'::text) AS personalpropertylendingval, 
		(mapinstance.jsondoc_ ->> 'SeriousLiquitidyProblems'::text)::boolean AS seriousliquitidyproblems,
		(mapinstance.jsondoc_ ->> 'ShareholdersDeliquencies'::text)::boolean AS shareholdersdeliquencies,
		(mapinstance.jsondoc_ ->> 'WillingnessToDisclseInfo'::text)::boolean AS willingnesstodisclseinfo,
		(mapinstance.jsondoc_ ->> 'ProblemWithTaxesAwareness'::text)::boolean AS problemwithtaxesawareness,	
		(mapinstance.jsondoc_->>'ShareHoldersCreditHistory')::int4 shareholderscredithistory,
		(l10.jsondoc_ ->> 'Value'::text) AS shareholderscredithistoryval, 	
		(mapinstance.jsondoc_->>'IndicativeRatingWithTempData')::int4 indicativeratingwithtempdata,
		(l11.jsondoc_ ->> 'Value'::text) AS indicativeratingwithtempdataval, 
		(mapinstance.jsondoc_ ->> 'IrregularUsageCurrentAccounts'::text)::boolean AS irregularusagecurrentaccounts,
		(mapinstance.jsondoc_ ->> 'ProblemWithInsuranseAwareness'::text)::boolean AS problemwithinsuranseawareness,	
		(mapinstance.jsondoc_->>'SuitabilityOfBuildingsAndEquip')::int4 suitabilityofbuildingsandequip,
		(l12.jsondoc_ ->> 'Value'::text) AS suitabilityofbuildingsandequipval, 
		(mapinstance.jsondoc_->>'PastDuesToBalance')::numeric as pastduestobalance,
		(mapinstance.jsondoc_->>'FmanagerSkills')::numeric as fmanagerskills,
		(mapinstance.jsondoc_->>'FcompetitionLevel')::numeric as fcompetitionlevel,
		(mapinstance.jsondoc_->>'FbusinessCreditHistory')::numeric as fbusinesscredithistory,
		mapinstance.wfid_::varchar,
		mapinstance.taskid_::varchar,
		mapinstance.versionid_::int4,
		mapinstance.isdeleted_::boolean,
		mapinstance.islatestversion_::boolean,
		mapinstance.baseversionid_::int4,
		mapinstance.contextuserid_::varchar,
		mapinstance.isvisible_::boolean,
		mapinstance.isvalid_::boolean,
		mapinstance.snapshotid_::int4,
		mapinstance.t_::varchar,
		mapinstance.createdby_::varchar,
		mapinstance.createddate_::timestamp,
		mapinstance.updatedby_::varchar,
		mapinstance.updateddate_::timestamp,
		mapinstance.fkid_entity,
		CASE WHEN mapinstance.updateddate_ > mapinstance.createddate_ THEN mapinstance.updatedby_ ELSE mapinstance.createdby_ END AS sourcepopulatedby_,
		GREATEST(mapinstance.createddate_, mapinstance.updateddate_) AS sourcepopulateddate_
		,current_setting('myvariables.popdate')::timestamp as populateddate_
		FROM madata.mapinstance	
		LEFT JOIN madata.custom_lookup as l0 ON l0.t_ = 'JobsBcategory' and l0.jsondoc_->>'Key' = mapinstance.jsondoc_->>'Job'		
		LEFT JOIN madata.custom_lookup as l1 ON l1.t_='MapPdModelScale' and l1.jsondoc_->>'Id' = mapinstance.jsondoc_->>'Grade'		
		LEFT JOIN madata.custom_lookup as l2 ON l2.t_ = 'BusinessCreditHistory' and l2.jsondoc_->>'Key' = mapinstance.jsondoc_->>'CreditHistory'		
		LEFT JOIN madata.custom_lookup as l3 ON l3.t_ = 'ManagerSkills' and l3.jsondoc_->>'Key' = mapinstance.jsondoc_->>'ManagerAbility'		
		LEFT JOIN madata.custom_lookup as l4 ON l4.t_ = 'SuccessorStatus' and l4.jsondoc_->>'Id' = mapinstance.jsondoc_->>'SuccessorStatus'		
		LEFT JOIN madata.custom_lookup as l5 ON l5.t_ = 'Competition' and l5.jsondoc_->>'Key' = mapinstance.jsondoc_->>'CompetitionLevel'		
		LEFT JOIN madata.custom_lookup as l6 ON l6.t_ = 'BusinessActivity' and l6.jsondoc_->>'Key' = mapinstance.jsondoc_->>'BusinessOfActivity'		
		LEFT JOIN madata.custom_lookup as l7 ON l7.t_ = 'TrueFalse' and l7.jsondoc_->>'Key' = mapinstance.jsondoc_->>'InabilityToPayInter'		
		LEFT JOIN madata.custom_lookup as l8 ON l8.t_ = 'CustomerConcentration' and l8.jsondoc_->>'Id' = mapinstance.jsondoc_->>'CustomerConcentration'
		LEFT JOIN madata.custom_lookup as l9 ON l9.t_ = 'PersonalPropertyRelatedToLending' and l9.jsondoc_->>'Key' = mapinstance.jsondoc_->>'PersonalPropertyLending'		
		LEFT JOIN madata.custom_lookup as l10 ON l10.t_ = 'ShareholdersCreditHistory' and l10.jsondoc_->>'Key' = mapinstance.jsondoc_->>'ShareHoldersCreditHistory'		
		LEFT JOIN madata.custom_lookup as l11 ON l11.t_ = 'TrueFalse' and l11.jsondoc_->>'Key' = mapinstance.jsondoc_->>'IndicativeRatingWithTempData'		
		LEFT JOIN madata.custom_lookup as l12 ON l12.t_ = 'SuitabilityOfBuildingsAndEquipment' and l12.jsondoc_->>'Id' = mapinstance.jsondoc_->>'SuitabilityOfBuildingsAndEquip'		
		WHERE 
			GREATEST(mapinstance.updateddate_, mapinstance.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABPDMODELBCATEGORY')
			AND GREATEST(mapinstance.updateddate_, mapinstance.createddate_)::timestamp <= max_refreshhistory 
			AND mapinstance.t_ = 'PdModelBcategory';
	raise notice '% - Step abpdmodelbcategory - part a end', clock_timestamp();
	
-- If table doesn't exist in refresh history --
ELSE
raise notice '% Step abpdmodelbcategory - part b start', clock_timestamp();
	DROP TABLE IF EXISTS olapts.abpdmodelbcategory;
	CREATE TABLE olapts.abpdmodelbcategory AS
		SELECT 
		 mapinstance.id_ AS factmapinstanceid_,
		 mapinstance.pkid_::varchar as pkid_,	
		(mapinstance.jsondoc_ ->> 'Pd'::text)::numeric AS pd,
		(mapinstance.jsondoc_->>'Job')::int4 job,
		(l0.jsondoc_->> 'Value'::text) AS jobval,
		(mapinstance.jsondoc_ ->> 'Debt'::text)::numeric AS debt,	
        (mapinstance.jsondoc_->>'Grade') AS grade,		
		(l1.jsondoc_->> 'Grade'::text) AS gradeval,
		(mapinstance.jsondoc_ ->> 'Score'::text)::numeric AS score,
		(mapinstance.jsondoc_ ->> 'Year1'::text)::integer AS year1,
		(mapinstance.jsondoc_ ->> 'Year2'::text)::integer AS year2,
		(mapinstance.jsondoc_ ->> 'Year3'::text)::integer AS year3,
		(mapinstance.jsondoc_ ->> 'Sales1'::text)::numeric AS sales1,
		(mapinstance.jsondoc_ ->> 'Sales2'::text)::numeric AS sales2,
		(mapinstance.jsondoc_ ->> 'Sales3'::text)::numeric AS sales3,
		(mapinstance.jsondoc_ ->> 'Balance'::text)::numeric AS balance,
		(mapinstance.jsondoc_ ->> 'Exports'::text)::boolean AS exports,
		(mapinstance.jsondoc_ ->> 'ModelId'::text)::character varying AS modelid,
		(mapinstance.jsondoc_ ->> 'Months1'::text)::numeric AS months1,
		(mapinstance.jsondoc_ ->> 'Months2'::text)::numeric AS months2,
		(mapinstance.jsondoc_ ->> 'Months3'::text)::numeric AS months3,
		(mapinstance.jsondoc_ ->> 'BankDept'::text)::numeric AS bankdept,
		(mapinstance.jsondoc_ ->> 'EntityId'::text)::integer AS entityid,
		(mapinstance.jsondoc_ ->> 'Sales112'::text)::numeric AS sales112,
		(mapinstance.jsondoc_ ->> 'Sales212'::text)::numeric AS sales212,
		(mapinstance.jsondoc_ ->> 'BankDebt2'::text)::numeric AS bankdebt2,
		(mapinstance.jsondoc_ ->> 'ProfitLoss1'::text)::numeric AS profitloss1,
		(mapinstance.jsondoc_ ->> 'ProfitLoss2'::text)::numeric AS profitloss2,
		(mapinstance.jsondoc_ ->> 'ProfitLoss3'::text)::numeric AS profitloss3,
		(mapinstance.jsondoc_ ->> 'SalesGrowth'::text)::numeric AS salesgrowth,
		(mapinstance.jsondoc_ ->> 'MsalesGrowth'::text)::numeric AS msalesgrowth,
		(mapinstance.jsondoc_ ->> 'AmountPastDue'::text)::numeric AS amountpastdue,	
        (mapinstance.jsondoc_->>'CreditHistory')::int4 credithistory,	
		(l2.jsondoc_->> 'Value'::text) AS credithistoryval,
		(mapinstance.jsondoc_ ->> 'DelinqDateTo1'::text)::timestamp without time zone AS delinqdateto1,
		(mapinstance.jsondoc_ ->> 'DelinqDateTo2'::text)::timestamp without time zone AS delinqdateto2,
		(mapinstance.jsondoc_ ->> 'DelinqDateTo3'::text)::timestamp without time zone AS delinqdateto3,
		(mapinstance.jsondoc_ ->> 'RecentDamages'::text)::boolean AS recentdamages,
		(mapinstance.jsondoc_ ->> 'BalanceToSales'::text)::numeric AS balancetosales,
        (mapinstance.jsondoc_->>'ManagerAbility')::int4 managerability,
		(l3.jsondoc_ ->> 'Value'::text) AS managerabilityval, 
		(mapinstance.jsondoc_ ->> 'OperatingYears'::text)::integer AS operatingyears,
		(mapinstance.jsondoc_ ->> 'TeiresiasDate1'::text)::character varying AS teiresiasdate1,
		(mapinstance.jsondoc_ ->> 'TeiresiasDate2'::text)::character varying AS teiresiasdate2,
		(mapinstance.jsondoc_ ->> 'TeiresiasDate3'::text)::character varying AS teiresiasdate3,
		(mapinstance.jsondoc_ ->> 'DelinqDateFrom1'::text)::timestamp without time zone AS delinqdatefrom1,
		(mapinstance.jsondoc_ ->> 'DelinqDateFrom2'::text)::timestamp without time zone AS delinqdatefrom2,
		(mapinstance.jsondoc_ ->> 'DelinqDateFrom3'::text)::timestamp without time zone AS delinqdatefrom3,
		(mapinstance.jsondoc_ ->> 'EntityVersionId'::text)::integer AS entityversionid,
		(mapinstance.jsondoc_ ->> 'MbalanceToSales'::text)::numeric AS mbalancetosales,
		(mapinstance.jsondoc_ ->> 'OtherRiskPoints'::text)::boolean AS otherriskpoints,	
		(mapinstance.jsondoc_->>'SuccessorStatus')::int4 successorstatus,
		(l4.jsondoc_ ->> 'Value'::text) AS successorstatusval,  
        (mapinstance.jsondoc_->>'CompetitionLevel')::int4 competitionlevel,
		(l5.jsondoc_ ->> 'Value'::text) AS competitionlevelval, 
		(mapinstance.jsondoc_ ->> 'CooperationYears'::text)::integer AS cooperationyears,
		(mapinstance.jsondoc_ ->> 'TotalDelinqYear1'::text)::numeric AS totaldelinqyear1,
		(mapinstance.jsondoc_ ->> 'TotalDelinqYear2'::text)::numeric AS totaldelinqyear2,
		(mapinstance.jsondoc_ ->> 'TotalDelinqYear3'::text)::numeric AS totaldelinqyear3,
		(mapinstance.jsondoc_ ->> 'InterimVatStmmnt1'::text)::numeric AS interimvatstmmnt1,
		(mapinstance.jsondoc_ ->> 'InterimVatStmmnt2'::text)::numeric AS interimvatstmmnt2,
		(mapinstance.jsondoc_ ->> 'NumberOfEmployees'::text)::integer AS numberofemployees,
        (mapinstance.jsondoc_->>'BusinessOfActivity')::int4 businessofactivity,	
		(l6.jsondoc_ ->> 'Value'::text) AS businessofactivityval, 
		(mapinstance.jsondoc_ ->> 'DelinquenciesRatio'::text)::numeric AS delinquenciesratio,
		(mapinstance.jsondoc_ ->> 'MpastDuesToBalance'::text)::numeric AS mpastduestobalance,	
		(mapinstance.jsondoc_->>'InabilityToPayInter')::int4 inabilitytopayinter,
		(l7.jsondoc_ ->> 'Value'::text) AS inabilitytopayinterval, 
		(mapinstance.jsondoc_ ->> 'InterimVatStatement'::text)::timestamp without time zone AS interimvatstatement,
		(mapinstance.jsondoc_ ->> 'MdelinquenciesRatio'::text)::numeric AS mdelinquenciesratio,
		(mapinstance.jsondoc_ ->> 'InvestmentInProgress'::text)::boolean AS investmentinprogress,	
		(mapinstance.jsondoc_->>'CustomerConcentration')::int4 customerconcentration,
		(l8.jsondoc_ ->> 'Value'::text) AS customerconcentrationval, 
		(mapinstance.jsondoc_ ->> 'DateInterimVatStmmnt1'::text)::timestamp without time zone AS dateinterimvatstmmnt1,
		(mapinstance.jsondoc_ ->> 'DateInterimVatStmmnt2'::text)::timestamp without time zone AS dateinterimvatstmmnt2,
		(mapinstance.jsondoc_ ->> 'InabilityToPayInterest'::text)::numeric AS inabilitytopayinterest, 	
		(mapinstance.jsondoc_ ->> 'MinabilityToPayInterest'::text)::numeric AS minabilitytopayinterest, 	
		(mapinstance.jsondoc_->>'PersonalPropertyLending')::int4 personalpropertylending,
		(l9.jsondoc_ ->> 'Value'::text) AS personalpropertylendingval, 
		(mapinstance.jsondoc_ ->> 'SeriousLiquitidyProblems'::text)::boolean AS seriousliquitidyproblems,
		(mapinstance.jsondoc_ ->> 'ShareholdersDeliquencies'::text)::boolean AS shareholdersdeliquencies,
		(mapinstance.jsondoc_ ->> 'WillingnessToDisclseInfo'::text)::boolean AS willingnesstodisclseinfo,
		(mapinstance.jsondoc_ ->> 'ProblemWithTaxesAwareness'::text)::boolean AS problemwithtaxesawareness,	
		(mapinstance.jsondoc_->>'ShareHoldersCreditHistory')::int4 shareholderscredithistory,
		(l10.jsondoc_ ->> 'Value'::text) AS shareholderscredithistoryval, 	
		(mapinstance.jsondoc_->>'IndicativeRatingWithTempData')::int4 indicativeratingwithtempdata,
		(l11.jsondoc_ ->> 'Value'::text) AS indicativeratingwithtempdataval, 
		(mapinstance.jsondoc_ ->> 'IrregularUsageCurrentAccounts'::text)::boolean AS irregularusagecurrentaccounts,
		(mapinstance.jsondoc_ ->> 'ProblemWithInsuranseAwareness'::text)::boolean AS problemwithinsuranseawareness,	
		(mapinstance.jsondoc_->>'SuitabilityOfBuildingsAndEquip')::int4 suitabilityofbuildingsandequip,
		(l12.jsondoc_ ->> 'Value'::text) AS suitabilityofbuildingsandequipval, 
		(mapinstance.jsondoc_->>'PastDuesToBalance')::numeric as pastduestobalance,
		(mapinstance.jsondoc_->>'FmanagerSkills')::numeric as fmanagerskills,
		(mapinstance.jsondoc_->>'FcompetitionLevel')::numeric as fcompetitionlevel,
		(mapinstance.jsondoc_->>'FbusinessCreditHistory')::numeric as fbusinesscredithistory,
		mapinstance.wfid_::varchar,
		mapinstance.taskid_::varchar,
		mapinstance.versionid_::int4,
		mapinstance.isdeleted_::boolean,
		mapinstance.islatestversion_::boolean,
		mapinstance.baseversionid_::int4,
		mapinstance.contextuserid_::varchar,
		mapinstance.isvisible_::boolean,
		mapinstance.isvalid_::boolean,
		mapinstance.snapshotid_::int4,
		mapinstance.t_::varchar,
		mapinstance.createdby_::varchar,
		mapinstance.createddate_::timestamp,
		mapinstance.updatedby_::varchar,
		mapinstance.updateddate_::timestamp,
		mapinstance.fkid_entity,
		CASE WHEN mapinstance.updateddate_ > mapinstance.createddate_ THEN mapinstance.updatedby_ ELSE mapinstance.createdby_ END AS sourcepopulatedby_,
		GREATEST(mapinstance.createddate_, mapinstance.updateddate_) AS sourcepopulateddate_
		,current_setting('myvariables.popdate')::timestamp as populateddate_
		FROM madata.mapinstance	
		LEFT JOIN madata.custom_lookup as l0 ON l0.t_ = 'JobsBcategory' and l0.jsondoc_->>'Key' = mapinstance.jsondoc_->>'Job'		
		LEFT JOIN madata.custom_lookup as l1 ON l1.t_='MapPdModelScale' and l1.jsondoc_->>'Id' = mapinstance.jsondoc_->>'Grade'		
		LEFT JOIN madata.custom_lookup as l2 ON l2.t_ = 'BusinessCreditHistory' and l2.jsondoc_->>'Key' = mapinstance.jsondoc_->>'CreditHistory'		
		LEFT JOIN madata.custom_lookup as l3 ON l3.t_ = 'ManagerSkills' and l3.jsondoc_->>'Key' = mapinstance.jsondoc_->>'ManagerAbility'		
		LEFT JOIN madata.custom_lookup as l4 ON l4.t_ = 'SuccessorStatus' and l4.jsondoc_->>'Id' = mapinstance.jsondoc_->>'SuccessorStatus'		
		LEFT JOIN madata.custom_lookup as l5 ON l5.t_ = 'Competition' and l5.jsondoc_->>'Key' = mapinstance.jsondoc_->>'CompetitionLevel'		
		LEFT JOIN madata.custom_lookup as l6 ON l6.t_ = 'BusinessActivity' and l6.jsondoc_->>'Key' = mapinstance.jsondoc_->>'BusinessOfActivity'		
		LEFT JOIN madata.custom_lookup as l7 ON l7.t_ = 'TrueFalse' and l7.jsondoc_->>'Key' = mapinstance.jsondoc_->>'InabilityToPayInter'		
		LEFT JOIN madata.custom_lookup as l8 ON l8.t_ = 'CustomerConcentration' and l8.jsondoc_->>'Id' = mapinstance.jsondoc_->>'CustomerConcentration'
		LEFT JOIN madata.custom_lookup as l9 ON l9.t_ = 'PersonalPropertyRelatedToLending' and l9.jsondoc_->>'Key' = mapinstance.jsondoc_->>'PersonalPropertyLending'		
		LEFT JOIN madata.custom_lookup as l10 ON l10.t_ = 'ShareholdersCreditHistory' and l10.jsondoc_->>'Key' = mapinstance.jsondoc_->>'ShareHoldersCreditHistory'		
		LEFT JOIN madata.custom_lookup as l11 ON l11.t_ = 'TrueFalse' and l11.jsondoc_->>'Key' = mapinstance.jsondoc_->>'IndicativeRatingWithTempData'		
		LEFT JOIN madata.custom_lookup as l12 ON l12.t_ = 'SuitabilityOfBuildingsAndEquipment' and l12.jsondoc_->>'Id' = mapinstance.jsondoc_->>'SuitabilityOfBuildingsAndEquip'		
		WHERE  
			GREATEST(mapinstance.updateddate_, mapinstance.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABPDMODELBCATEGORY')
			AND GREATEST(mapinstance.updateddate_, mapinstance.createddate_)::timestamp <= max_refreshhistory 
			AND mapinstance.t_ = 'PdModelBcategory'
		;
raise notice '% - Step abpdmodelbcategory - part b end', clock_timestamp();

raise notice '% - Step abpdmodelbcategory_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abpdmodelbcategory_idx;
DROP INDEX if exists olapts.abpdmodelbcategory_idx2;
CREATE INDEX IF NOT EXISTS abpdmodelbcategory_idx ON olapts.abpdmodelbcategory (factmapinstanceid_);
CREATE INDEX IF NOT EXISTS abpdmodelbcategory_idx2 ON olapts.abpdmodelbcategory (pkid_,versionid_);
	
raise notice '% - Step abpdmodelbcategory_idx - part a end', clock_timestamp(); 

END IF;

-- Create or update flag table -- 
raise notice '% step abpdmodelbcategory - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abpdmodelbcategoryflag;
CREATE TABLE IF NOT EXISTS olapts.abpdmodelbcategoryflag AS
	select
	id_,
	pkid_,
	wfid_ wfid_,
	taskid_ taskid_, 
	versionid_ versionid_,
	isdeleted_ isdeleted_,
	islatestversion_ islatestversion_,
	baseversionid_ baseversionid_,
	contextuserid_ contextuserid_,
	isvisible_ isvisible_,
	isvalid_ isvalid_,
	snapshotid_ snapshotid_,
	t_ t_,
	createdby_ createdby_,
	createddate_ createddate_,
	updatedby_ updatedby_,
	updateddate_ updateddate_,
	(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
	GREATEST(updateddate_, createddate_) as sourcepopulateddate_
	,current_setting('myvariables.popdate')::timestamp as populateddate_
	FROM madata.mapinstance
	where 
	GREATEST(mapinstance.updateddate_, mapinstance.createddate_)::timestamp <= max_refreshhistory 
	and mapinstance.t_ = 'PdModelBcategory';

raise notice '% - Step abpdmodelbcategoryflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abpdmodelbcategoryflag_idx;
DROP INDEX if exists olapts.abpdmodelbcategoryflag_idx2;
CREATE INDEX IF NOT EXISTS abpdmodelbcategoryflag_idx ON olapts.abpdmodelbcategoryflag (id_);
CREATE INDEX IF NOT EXISTS abpdmodelbcategoryflag_idx2 ON olapts.abpdmodelbcategoryflag (pkid_,versionid_);

ANALYZE olapts.abpdmodelbcategoryflag ;

-- Update refresh history -- 

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABPDMODELBCATEGORY';
delete from olapts.refreshhistory where tablename = 'ABPDMODELBCATEGORY';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABPDMODELBCATEGORY' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABPDMODELBCATEGORYFLAG';
delete from olapts.refreshhistory where tablename = 'ABPDMODELBCATEGORYFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABPDMODELBCATEGORYFLAG' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abpdmodelbcategory - part c end', clock_timestamp();

--END $$;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---correctiveactionsmaster
--DO $$
--DECLARE
--varprevsuccessdate TIMESTAMP ;
--BEGIN

IF EXISTS (select from olapts.refreshhistory where tablename = 'ABCORRECTIVEACTIONSMASTER') THEN
raise notice '% - Step abcorrectiveactionsmaster - part a start', clock_timestamp();
insert into olapts.abcorrectiveactionsmaster
	SELECT
		correctiveactionsmaster.id_ AS id_,
		correctiveactionsmaster.pkid_::varchar as pkid_,
		(correctiveactionsmaster.jsondoc_ ->> 'CorrectiveActionsMasterId') AS CorrectiveActionsMasterId,
		(correctiveactionsmaster.jsondoc_ ->> 'CorrectiveActionsMasterSaved')::integer AS CorrectiveActionsMasterSaved,
		(correctiveactionsmaster.jsondoc_ ->> 'Date')::date AS Date,
		(correctiveactionsmaster.jsondoc_ ->> 'EntityId')::numeric AS EntityId,
		(correctiveactionsmaster.jsondoc_ ->> 'EntityVersionId')::numeric AS EntityVersionId,
		(correctiveactionsmaster.jsondoc_ ->> 'EsPolicyCategorization') AS EsPolicyCategorization,
		(correctiveactionsmaster.jsondoc_ ->> 'EsPolicyId')::numeric AS EsPolicyId,
		(correctiveactionsmaster.jsondoc_ ->> 'EsPolicyVersionId')::numeric AS EsPolicyVersionId,
		(correctiveactionsmaster.jsondoc_ ->> 'User') AS User,
		correctiveactionsmaster.wfid_::varchar,
		correctiveactionsmaster.taskid_::varchar,
		correctiveactionsmaster.versionid_::int4,
		correctiveactionsmaster.isdeleted_::boolean,
		correctiveactionsmaster.islatestversion_::boolean,
		correctiveactionsmaster.baseversionid_::int4,
		correctiveactionsmaster.contextuserid_::varchar,
		correctiveactionsmaster.isvisible_::boolean,
		correctiveactionsmaster.isvalid_::boolean,
		correctiveactionsmaster.snapshotid_::int4,
		correctiveactionsmaster.t_::varchar,
		correctiveactionsmaster.createdby_::varchar,
		correctiveactionsmaster.createddate_::timestamp,
		correctiveactionsmaster.updatedby_::varchar,
		correctiveactionsmaster.updateddate_::timestamp,
		correctiveactionsmaster.fkid_entity,
		CASE WHEN correctiveactionsmaster.updateddate_ > correctiveactionsmaster.createddate_ THEN correctiveactionsmaster.updatedby_ ELSE correctiveactionsmaster.createdby_ END AS sourcepopulatedby_,
		GREATEST(correctiveactionsmaster.createddate_, correctiveactionsmaster.updateddate_) AS sourcepopulateddate_
		,current_setting('myvariables.popdate')::timestamp as populateddate_
	FROM
		madata.correctiveactionsmaster
	WHERE	
		GREATEST(correctiveactionsmaster.updateddate_, correctiveactionsmaster.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABCORRECTIVEACTIONSMASTER')
		AND GREATEST(correctiveactionsmaster.updateddate_, correctiveactionsmaster.createddate_)::timestamp <= max_refreshhistory 
		AND correctiveactionsmaster.t_ = 'CorrectiveActionsMaster'
	;
raise notice '% - Step abcorrectiveactionsmaster - part a end', clock_timestamp();
--------------------------------------------

ELSE
	raise notice '% Step abcorrectiveactionsmaster - part b start', clock_timestamp();
	DROP TABLE IF EXISTS olapts.abcorrectiveactionsmaster;
	CREATE TABLE olapts.abcorrectiveactionsmaster AS
	SELECT
		correctiveactionsmaster.id_ AS id_,
		correctiveactionsmaster.pkid_::varchar as pkid_,
		(correctiveactionsmaster.jsondoc_ ->> 'CorrectiveActionsMasterId') AS CorrectiveActionsMasterId,
		(correctiveactionsmaster.jsondoc_ ->> 'CorrectiveActionsMasterSaved')::integer AS CorrectiveActionsMasterSaved,
		(correctiveactionsmaster.jsondoc_ ->> 'Date')::date AS Date,
		(correctiveactionsmaster.jsondoc_ ->> 'EntityId')::numeric AS EntityId,
		(correctiveactionsmaster.jsondoc_ ->> 'EntityVersionId')::numeric AS EntityVersionId,
		(correctiveactionsmaster.jsondoc_ ->> 'EsPolicyCategorization') AS EsPolicyCategorization,
		(correctiveactionsmaster.jsondoc_ ->> 'EsPolicyId')::numeric AS EsPolicyId,
		(correctiveactionsmaster.jsondoc_ ->> 'EsPolicyVersionId')::numeric AS EsPolicyVersionId,
		(correctiveactionsmaster.jsondoc_ ->> 'User') AS User,
		correctiveactionsmaster.wfid_::varchar,
		correctiveactionsmaster.taskid_::varchar,
		correctiveactionsmaster.versionid_::int4,
		correctiveactionsmaster.isdeleted_::boolean,
		correctiveactionsmaster.islatestversion_::boolean,
		correctiveactionsmaster.baseversionid_::int4,
		correctiveactionsmaster.contextuserid_::varchar,
		correctiveactionsmaster.isvisible_::boolean,
		correctiveactionsmaster.isvalid_::boolean,
		correctiveactionsmaster.snapshotid_::int4,
		correctiveactionsmaster.t_::varchar,
		correctiveactionsmaster.createdby_::varchar,
		correctiveactionsmaster.createddate_::timestamp,
		correctiveactionsmaster.updatedby_::varchar,
		correctiveactionsmaster.updateddate_::timestamp,
		correctiveactionsmaster.fkid_entity,
		CASE WHEN correctiveactionsmaster.updateddate_ > correctiveactionsmaster.createddate_ THEN correctiveactionsmaster.updatedby_ ELSE correctiveactionsmaster.createdby_ END AS sourcepopulatedby_,
		GREATEST(correctiveactionsmaster.createddate_, correctiveactionsmaster.updateddate_) AS sourcepopulateddate_
		,current_setting('myvariables.popdate')::timestamp as populateddate_
	FROM
		madata.correctiveactionsmaster
	WHERE	
		GREATEST(correctiveactionsmaster.updateddate_, correctiveactionsmaster.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABCORRECTIVEACTIONSMASTER')
		AND GREATEST(correctiveactionsmaster.updateddate_, correctiveactionsmaster.createddate_)::timestamp <= max_refreshhistory 
		AND correctiveactionsmaster.t_ = 'CorrectiveActionsMaster';
	
raise notice '% - Step abcorrectiveactionsmaster - part b end', clock_timestamp();

--abcorrectiveactionsmaster
raise notice '% - Step abcorrectiveactionsmaster_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abcorrectiveactionsmaster_idx;
DROP INDEX if exists olapts.abcorrectiveactionsmaster_idx2;
CREATE INDEX IF NOT EXISTS abcorrectiveactionsmaster_idx ON olapts.abcorrectiveactionsmaster (id_);
CREATE INDEX IF NOT EXISTS abcorrectiveactionsmaster_idx2 ON olapts.abcorrectiveactionsmaster (pkid_,versionid_);

	
raise notice '% - Step abcorrectiveactionsmaster_idx - part a end', clock_timestamp(); 
END IF;	
	
------------------------------------------------------------------------
raise notice '% step abcorrectiveactionsmaster - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abcorrectiveactionsmasterflag;
CREATE TABLE IF NOT EXISTS olapts.abcorrectiveactionsmasterflag AS

select
id_,
pkid_,
wfid_ wfid_,
taskid_ taskid_, 
versionid_ versionid_,
isdeleted_ isdeleted_,
islatestversion_ islatestversion_,
baseversionid_ baseversionid_,
contextuserid_ contextuserid_,
isvisible_ isvisible_,
isvalid_ isvalid_,
snapshotid_ snapshotid_,
t_ t_,
createdby_ createdby_,
createddate_ createddate_,
updatedby_ updatedby_,
updateddate_ updateddate_,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_, createddate_) as sourcepopulateddate_
,current_setting('myvariables.popdate')::timestamp as populateddate_
FROM madata.correctiveactionsmaster
where GREATEST(correctiveactionsmaster.updateddate_, correctiveactionsmaster.createddate_)::timestamp <= max_refreshhistory 
and correctiveactionsmaster.t_ = 'CorrectiveActionsMaster'
;

raise notice '% - Step abcorrectiveactionsmasterflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abcorrectiveactionsmasterflag_idx;
DROP INDEX if exists olapts.abcorrectiveactionsmasterflag_idx2;
CREATE INDEX IF NOT EXISTS abcorrectiveactionsmasterflag_idx ON olapts.abcorrectiveactionsmasterflag (id_);
CREATE INDEX IF NOT EXISTS abcorrectiveactionsmasterflag_idx2 ON olapts.abcorrectiveactionsmasterflag (pkid_,versionid_);
ANALYZE olapts.abcorrectiveactionsmasterflag ;

raise notice '% - Step abcorrectiveactionsmasterflag_idx - part a end', clock_timestamp(); 

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABCORRECTIVEACTIONSMASTER';
delete from olapts.refreshhistory where tablename = 'ABCORRECTIVEACTIONSMASTER';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABCORRECTIVEACTIONSMASTER' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABCORRECTIVEACTIONSMASTERFLAG';
delete from olapts.refreshhistory where tablename = 'ABCORRECTIVEACTIONSMASTERFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABCORRECTIVEACTIONSMASTERFLAG' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abcorrectiveactionsmaster - part c end', clock_timestamp();

--END $$;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--DO $$
--DECLARE
--varprevsuccessdate TIMESTAMP ;
--BEGIN

--CorrectiveActionsDetail
IF EXISTS (select from olapts.refreshhistory where tablename = 'ABCORRECTIVEACTIONS') THEN
raise notice '% - Step abcorrectiveactions - part a start', clock_timestamp();
insert into olapts.abcorrectiveactions
	SELECT
		correctiveactions.id_ AS id_,
		correctiveactions.pkid_::varchar as pkid_,
		(correctiveactions.jsondoc_ ->> 'CorrectiveActionsId')::numeric AS CorrectiveActionsId,
		(correctiveactions.jsondoc_ ->> 'Comment') AS Comment,
		(correctiveactions.jsondoc_ ->> 'CorrectiveActionsMasterId') AS CorrectiveActionsMasterId,
		(correctiveactions.jsondoc_ ->> 'CorrectiveActionsSaved')::integer AS CorrectiveActionsSaved,
		(correctiveactions.jsondoc_ ->> 'CorrectiveSettlement')::int4 CorrectiveSettlement,
		(l1.jsondoc_ ->> 'Value') AS CorrectiveSettlementval,
		(correctiveactions.jsondoc_ ->> 'FindingCharacterization')::int4 FindingCharacterization,
		(l2.jsondoc_ ->> 'Value') AS FindingCharacterizationval,
		(correctiveactions.jsondoc_ ->> 'FindingsDescriptions')::int4 FindingsDescriptions, --add 20/5
		(l3.jsondoc_ ->> 'Value') AS FindingsDescriptionsval,
		(correctiveactions.jsondoc_ ->> 'SettlementDate')::timestamp AS SettlementDate,	
		correctiveactions.wfid_::varchar,
		correctiveactions.taskid_::varchar,
		correctiveactions.versionid_::int4,
		correctiveactions.isdeleted_::boolean,
		correctiveactions.islatestversion_::boolean,
		correctiveactions.baseversionid_::int4,
		correctiveactions.contextuserid_::varchar,
		correctiveactions.isvisible_::boolean,
		correctiveactions.isvalid_::boolean,
		correctiveactions.snapshotid_::int4,
		correctiveactions.t_::varchar,
		correctiveactions.createdby_::varchar,
		correctiveactions.createddate_::timestamp,
		correctiveactions.updatedby_::varchar,
		correctiveactions.updateddate_::timestamp,
		--correctiveactions.fkid_entity,
		CASE WHEN correctiveactions.updateddate_ > correctiveactions.createddate_ THEN correctiveactions.updatedby_ ELSE correctiveactions.createdby_ END AS sourcepopulatedby_,
		GREATEST(correctiveactions.createddate_, correctiveactions.updateddate_) AS sourcepopulateddate_
		,current_setting('myvariables.popdate')::timestamp as populateddate_
	FROM madata.correctiveactions
		left join madata.custom_lookup l1 on l1.t_ = 'TrueFalse'  and l1.jsondoc_->>'Key'  = correctiveactions.jsondoc_ ->> 'CorrectiveSettlement'
		left join madata.custom_lookup l2 on l2.t_ = 'EsFindingsCharacterization'  and l2.jsondoc_->>'Key'  = correctiveactions.jsondoc_ ->> 'FindingCharacterization'
		left join madata.custom_lookup l3 on l3.t_ = 'FindingsDescription'  and l3.jsondoc_->>'Key'  = correctiveactions.jsondoc_ ->> 'FindingsDescriptions'		
	WHERE	
		GREATEST(correctiveactions.updateddate_, correctiveactions.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABCORRECTIVEACTIONS')
		AND GREATEST(correctiveactions.updateddate_, correctiveactions.createddate_)::timestamp <= max_refreshhistory
		AND correctiveactions.t_ = 'CorrectiveActions'
	;
raise notice '% - Step abcorrectiveactions - part a end', clock_timestamp();
--------------------------------------------
ELSE
raise notice '% Step abcorrectiveactions - part b start', clock_timestamp();
	DROP TABLE IF EXISTS olapts.abcorrectiveactions;
	CREATE TABLE olapts.abcorrectiveactions AS
	SELECT
		correctiveactions.id_ AS id_,
		correctiveactions.pkid_::varchar as pkid_,
		(correctiveactions.jsondoc_ ->> 'CorrectiveActionsId')::numeric AS CorrectiveActionsId,
		(correctiveactions.jsondoc_ ->> 'Comment') AS Comment,
		(correctiveactions.jsondoc_ ->> 'CorrectiveActionsMasterId') AS CorrectiveActionsMasterId,
		(correctiveactions.jsondoc_ ->> 'CorrectiveActionsSaved')::integer AS CorrectiveActionsSaved,	
		(correctiveactions.jsondoc_ ->> 'CorrectiveSettlement')::int4 CorrectiveSettlement,
		(l1.jsondoc_ ->> 'Value') AS CorrectiveSettlementval,
		(correctiveactions.jsondoc_ ->> 'FindingCharacterization')::int4 FindingCharacterization,
		(l2.jsondoc_ ->> 'Value') AS FindingCharacterizationval,
		(correctiveactions.jsondoc_ ->> 'FindingsDescriptions')::int4 FindingsDescriptions, --add 20/5
		(l3.jsondoc_ ->> 'Value') AS FindingsDescriptionsval,
		(correctiveactions.jsondoc_ ->> 'SettlementDate')::timestamp AS SettlementDate,	
		correctiveactions.wfid_::varchar,
		correctiveactions.taskid_::varchar,
		correctiveactions.versionid_::int4,
		correctiveactions.isdeleted_::boolean,
		correctiveactions.islatestversion_::boolean,
		correctiveactions.baseversionid_::int4,
		correctiveactions.contextuserid_::varchar,
		correctiveactions.isvisible_::boolean,
		correctiveactions.isvalid_::boolean,
		correctiveactions.snapshotid_::int4,
		correctiveactions.t_::varchar,
		correctiveactions.createdby_::varchar,
		correctiveactions.createddate_::timestamp,
		correctiveactions.updatedby_::varchar,
		correctiveactions.updateddate_::timestamp,
		--correctiveactions.fkid_entity,
		CASE WHEN correctiveactions.updateddate_ > correctiveactions.createddate_ THEN correctiveactions.updatedby_ ELSE correctiveactions.createdby_ END AS sourcepopulatedby_,
		GREATEST(correctiveactions.createddate_, correctiveactions.updateddate_) AS sourcepopulateddate_
		,current_setting('myvariables.popdate')::timestamp as populateddate_
	FROM
		madata.correctiveactions
		left join madata.custom_lookup l1 on l1.t_ = 'TrueFalse'  and l1.jsondoc_->>'Key'  = correctiveactions.jsondoc_ ->> 'CorrectiveSettlement'
		left join madata.custom_lookup l2 on l2.t_ = 'EsFindingsCharacterization'  and l2.jsondoc_->>'Key'  = correctiveactions.jsondoc_ ->> 'FindingCharacterization'
		left join madata.custom_lookup l3 on l3.t_ = 'FindingsDescription'  and l3.jsondoc_->>'Key'  = correctiveactions.jsondoc_ ->> 'FindingsDescriptions'	
	WHERE	
		GREATEST(correctiveactions.updateddate_, correctiveactions.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABCORRECTIVEACTIONS')
		AND GREATEST(correctiveactions.updateddate_, correctiveactions.createddate_)::timestamp <= max_refreshhistory 
		AND correctiveactions.t_ = 'CorrectiveActions';
raise notice '% - Step abcorrectiveactions - part b end', clock_timestamp();

--abcorrectiveactions
raise notice '% - Step abcorrectiveactions_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abcorrectiveactions_idx;
DROP INDEX if exists olapts.abcorrectiveactions_idx2;
CREATE INDEX IF NOT EXISTS abcorrectiveactions_idx ON olapts.abcorrectiveactions (id_);
CREATE INDEX IF NOT EXISTS abcorrectiveactions_idx2 ON olapts.abcorrectiveactions (pkid_,versionid_);
	
raise notice '% - Step abcorrectiveactions_idx - part a end', clock_timestamp(); 
END IF;
	
	------------------------------------------------------------------------
raise notice '% step abcorrectiveactions - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abcorrectiveactionsflag;
CREATE TABLE IF NOT EXISTS olapts.abcorrectiveactionsflag AS

select
id_,
pkid_,
wfid_ wfid_,
taskid_ taskid_, 
versionid_ versionid_,
isdeleted_ isdeleted_,
islatestversion_ islatestversion_,
baseversionid_ baseversionid_,
contextuserid_ contextuserid_,
isvisible_ isvisible_,
isvalid_ isvalid_,
snapshotid_ snapshotid_,
t_ t_,
createdby_ createdby_,
createddate_ createddate_,
updatedby_ updatedby_,
updateddate_ updateddate_,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_, createddate_) as sourcepopulateddate_
,current_setting('myvariables.popdate')::timestamp as populateddate_
FROM madata.correctiveactions
where 
GREATEST(correctiveactions.updateddate_, correctiveactions.createddate_)::timestamp <= max_refreshhistory 
and correctiveactions.t_ = 'CorrectiveActions';

raise notice '% - Step abcorrectiveactionsflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abcorrectiveactionsflag_idx;
DROP INDEX if exists olapts.abcorrectiveactionsflag_idx2;
CREATE INDEX IF NOT EXISTS abcorrectiveactionsflag_idx ON olapts.abcorrectiveactionsflag (id_);
CREATE INDEX IF NOT EXISTS abcorrectiveactionsflag_idx2 ON olapts.abcorrectiveactionsflag (pkid_,versionid_);
ANALYZE olapts.abcorrectiveactionsflag ;

raise notice '% - Step abcorrectiveactionsflag_idx - part a end', clock_timestamp(); 

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABCORRECTIVEACTIONS';
delete from olapts.refreshhistory where tablename = 'ABCORRECTIVEACTIONS';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABCORRECTIVEACTIONS' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABCORRECTIVEACTIONSFLAG';
delete from olapts.refreshhistory where tablename = 'ABCORRECTIVEACTIONSFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABCORRECTIVEACTIONSFLAG' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abcorrectiveactions - part c end', clock_timestamp();

-------------------REF Data--------------------------

--EsFindingsCharacterization
raise notice '% - Step abesfindingscharacterization - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abesfindingscharacterization;
CREATE TABLE olapts.abesfindingscharacterization AS
select l.jsondoc_->>'Key' esfindingscharacterizationkey_,
l.jsondoc_->>'Value' esfindingscharacterizationvalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'EsFindingsCharacterization';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABESFINDINGSCHARACTERIZATION';
delete from olapts.refreshhistory where tablename = 'ABESFINDINGSCHARACTERIZATION';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABESFINDINGSCHARACTERIZATION' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abesfindingscharacterization - part a end', clock_timestamp();

--FindingsDescriptions
raise notice '% - Step abfindingsdescriptions - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abfindingsdescriptions;
CREATE TABLE olapts.abfindingsdescriptions AS
select l.jsondoc_->>'Key' findingsdescriptionskey_,
l.jsondoc_->>'Value' findingsdescriptionsvalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'FindingsDescription';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABFINDINGSDESCRIPTIONS';
delete from olapts.refreshhistory where tablename = 'ABFINDINGSDESCRIPTIONS';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABFINDINGSDESCRIPTIONS' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abfindingsdescriptions - part a end', clock_timestamp();

--END $$;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--ebadefinition

--DO $$
--DECLARE
--varprevsuccessdate TIMESTAMP ;
--BEGIN

IF EXISTS (select from olapts.refreshhistory where tablename = 'ABEBADEFINITION') THEN
raise notice '% - Step abebadefiniton - part a start', clock_timestamp();
insert into olapts.abebadefinition
	SELECT
		ebadefinition.id_ AS id_,
		ebadefinition.pkid_::varchar as pkid_,
		(ebadefinition.jsondoc_ ->> 'EbaId')::numeric AS EbaId,
		(ebadefinition.jsondoc_ ->> 'Active')::boolean AS Active,
		(ebadefinition.jsondoc_ ->> 'Comment')::int4 AS Comment,
		(l3.jsondoc_ ->> 'Value') AS Commentval,	
		(ebadefinition.jsondoc_ ->> 'CooperativeDebtor')::boolean AS CooperativeDebtor,
		(ebadefinition.jsondoc_ ->> 'CooperativeDebtorEdit')::boolean AS CooperativeDebtorEdit,
		(ebadefinition.jsondoc_ ->> 'CreationDate')::timestamp AS CreationDate,
		(ebadefinition.jsondoc_ ->> 'DeteriorationOfCreditRisk') DeteriorationOfCreditRisk, 
		(l4.jsondoc_ ->> 'Value') AS DeteriorationOfCreditRiskval,
		(ebadefinition.jsondoc_ ->> 'EbaSaved')::integer AS EbaSaved,
		(ebadefinition.jsondoc_ ->> 'EntityId')::numeric AS EntityId,
		(ebadefinition.jsondoc_ ->> 'EntityVersionId')::numeric AS EntityVersionId,
		(ebadefinition.jsondoc_ ->> 'FinancialDifficulty')::boolean AS FinancialDifficulty,
		(ebadefinition.jsondoc_ ->> 'FinancialDifficultyEdit')::boolean AS FinancialDifficultyEdit,
		(ebadefinition.jsondoc_ ->> 'IsNpl') IsNpl,
		(l7.jsondoc_ ->> 'Value') AS IsNplval,		
		(ebadefinition.jsondoc_ ->> 'Range')::integer AS Range,	
		(ebadefinition.jsondoc_ ->> 'RatingId') AS RatingId,
		(ebadefinition.jsondoc_ ->> 'RatingVersionId')::integer AS RatingVersionId,
		(ebadefinition.jsondoc_ ->> 'Reason')::int4 Reason,
		(l8.jsondoc_ ->> 'Value') AS Reasonval,
		(ebadefinition.jsondoc_ ->> 'SubstantialDelay') SubstantialDelay,
		(l9.jsondoc_ ->> 'Value') AS SubstantialDelayval,
		(ebadefinition.jsondoc_ ->> 'User') AS User,
		(ebadefinition.jsondoc_ ->> 'UtpFlag') UtpFlag,
		(l10.jsondoc_ ->> 'Value') AS UtpFlagval,
		(ebadefinition.jsondoc_ ->> 'VdInitialValue')::int4 VdInitialValue,
		(l12.jsondoc_ ->> 'Value') AS VdInitialValueval,
		(ebadefinition.jsondoc_ ->> 'ViableDebtor')::boolean AS ViableDebtor,
		(ebadefinition.jsondoc_ ->> 'ViableDebtorEdit')::boolean AS ViableDebtorEdit,
		ebadefinition.wfid_::varchar,
		ebadefinition.taskid_::varchar,
		ebadefinition.versionid_::int4,
		ebadefinition.isdeleted_::boolean,
		ebadefinition.islatestversion_::boolean,
		ebadefinition.baseversionid_::int4,
		ebadefinition.contextuserid_::varchar,
		ebadefinition.isvisible_::boolean,
		ebadefinition.isvalid_::boolean,
		ebadefinition.snapshotid_::int4,
		ebadefinition.t_::varchar,
		ebadefinition.createdby_::varchar,
		ebadefinition.createddate_::timestamp,
		ebadefinition.updatedby_::varchar,
		ebadefinition.updateddate_::timestamp,
		ebadefinition.fkid_entity,
		CASE WHEN ebadefinition.updateddate_ > ebadefinition.createddate_ THEN ebadefinition.updatedby_ ELSE ebadefinition.createdby_ END AS sourcepopulatedby_,
		GREATEST(ebadefinition.createddate_, ebadefinition.updateddate_) AS sourcepopulateddate_
		,current_setting('myvariables.popdate')::timestamp as populateddate_
	FROM madata.ebadefinition
		left join madata.custom_lookup l3 on l3.t_ = 'EbaComment'  and l3.jsondoc_->>'Key'  = ebadefinition.jsondoc_ ->> 'Comment'
		left join madata.custom_lookup l4 on l4.t_ = 'YesNoRm'  and l4.jsondoc_->>'Key'  = ebadefinition.jsondoc_ ->> 'DeteriorationOfCreditRisk'
		left join madata.custom_lookup l7 on l7.t_ = 'YesNo'  and l7.jsondoc_->>'Key'  = ebadefinition.jsondoc_ ->> 'IsNpl'
		left join madata.custom_lookup l8 on l8.t_ = 'EbaTrigger'  and l8.jsondoc_->>'Key'  = ebadefinition.jsondoc_ ->> 'Reason'
		left join madata.custom_lookup l9 on l9.t_ = 'YesNoRm'  and l9.jsondoc_->>'Key'  = ebadefinition.jsondoc_ ->> 'SubstantialDelay'
		left join madata.custom_lookup l10 on l10.t_ = 'YesNo'  and l10.jsondoc_->>'Key'  = ebadefinition.jsondoc_ ->> 'UtpFlag'
		left join madata.custom_lookup l12 on l12.t_ = 'EbaYesNo'  and l12.jsondoc_->>'Key'  = ebadefinition.jsondoc_ ->> 'VdInitialValue'
	WHERE	
		GREATEST(ebadefinition.updateddate_, ebadefinition.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABEBADEFINITION')
		AND GREATEST(ebadefinition.updateddate_, ebadefinition.createddate_)::timestamp <= max_refreshhistory 
		AND ebadefinition.t_ = 'EbaDefinition';
raise notice '% - Step abebadefinition - part a end', clock_timestamp();
--------------------------------------------

ELSE
raise notice '% Step abebadefinition - part b start', clock_timestamp();
	DROP TABLE IF EXISTS olapts.abebadefinition;
	CREATE TABLE olapts.abebadefinition AS
	SELECT
		ebadefinition.id_ AS id_,
		ebadefinition.pkid_::varchar as pkid_,
		(ebadefinition.jsondoc_ ->> 'EbaId')::numeric AS EbaId,
		(ebadefinition.jsondoc_ ->> 'Active')::boolean AS Active,
		(ebadefinition.jsondoc_ ->> 'Comment')::int4 AS Comment,
		(l3.jsondoc_ ->> 'Value') AS Commentval,	
		(ebadefinition.jsondoc_ ->> 'CooperativeDebtor')::boolean AS CooperativeDebtor,
		(ebadefinition.jsondoc_ ->> 'CooperativeDebtorEdit')::boolean AS CooperativeDebtorEdit,
		(ebadefinition.jsondoc_ ->> 'CreationDate')::timestamp AS CreationDate,
		(ebadefinition.jsondoc_ ->> 'DeteriorationOfCreditRisk') DeteriorationOfCreditRisk, 
		(l4.jsondoc_ ->> 'Value') AS DeteriorationOfCreditRiskval,
		(ebadefinition.jsondoc_ ->> 'EbaSaved')::integer AS EbaSaved,
		(ebadefinition.jsondoc_ ->> 'EntityId')::numeric AS EntityId,
		(ebadefinition.jsondoc_ ->> 'EntityVersionId')::numeric AS EntityVersionId,
		(ebadefinition.jsondoc_ ->> 'FinancialDifficulty')::boolean AS FinancialDifficulty,
		(ebadefinition.jsondoc_ ->> 'FinancialDifficultyEdit')::boolean AS FinancialDifficultyEdit,
		(ebadefinition.jsondoc_ ->> 'IsNpl') IsNpl,
		(l7.jsondoc_ ->> 'Value') AS IsNplval,		
		(ebadefinition.jsondoc_ ->> 'Range')::integer AS Range,	
		(ebadefinition.jsondoc_ ->> 'RatingId') AS RatingId,
		(ebadefinition.jsondoc_ ->> 'RatingVersionId')::integer AS RatingVersionId,
		(ebadefinition.jsondoc_ ->> 'Reason')::int4 Reason,
		(l8.jsondoc_ ->> 'Value') AS Reasonval,
		(ebadefinition.jsondoc_ ->> 'SubstantialDelay') SubstantialDelay,
		(l9.jsondoc_ ->> 'Value') AS SubstantialDelayval,
		(ebadefinition.jsondoc_ ->> 'User') AS User,
		(ebadefinition.jsondoc_ ->> 'UtpFlag') UtpFlag,
		(l10.jsondoc_ ->> 'Value') AS UtpFlagval,
		(ebadefinition.jsondoc_ ->> 'VdInitialValue')::int4 VdInitialValue,
		(l12.jsondoc_ ->> 'Value') AS VdInitialValueval,
		(ebadefinition.jsondoc_ ->> 'ViableDebtor')::boolean AS ViableDebtor,
		(ebadefinition.jsondoc_ ->> 'ViableDebtorEdit')::boolean AS ViableDebtorEdit,
		ebadefinition.wfid_::varchar,
		ebadefinition.taskid_::varchar,
		ebadefinition.versionid_::int4,
		ebadefinition.isdeleted_::boolean,
		ebadefinition.islatestversion_::boolean,
		ebadefinition.baseversionid_::int4,
		ebadefinition.contextuserid_::varchar,
		ebadefinition.isvisible_::boolean,
		ebadefinition.isvalid_::boolean,
		ebadefinition.snapshotid_::int4,
		ebadefinition.t_::varchar,
		ebadefinition.createdby_::varchar,
		ebadefinition.createddate_::timestamp,
		ebadefinition.updatedby_::varchar,
		ebadefinition.updateddate_::timestamp,
		ebadefinition.fkid_entity,
		CASE WHEN ebadefinition.updateddate_ > ebadefinition.createddate_ THEN ebadefinition.updatedby_ ELSE ebadefinition.createdby_ END AS sourcepopulatedby_,
		GREATEST(ebadefinition.createddate_, ebadefinition.updateddate_) AS sourcepopulateddate_
		,current_setting('myvariables.popdate')::timestamp as populateddate_
	FROM madata.ebadefinition
		left join madata.custom_lookup l3 on l3.t_ = 'EbaComment'  and l3.jsondoc_->>'Key'  = ebadefinition.jsondoc_ ->> 'Comment'
		left join madata.custom_lookup l4 on l4.t_ = 'YesNoRm'  and l4.jsondoc_->>'Key'  = ebadefinition.jsondoc_ ->> 'DeteriorationOfCreditRisk'
		left join madata.custom_lookup l7 on l7.t_ = 'YesNo'  and l7.jsondoc_->>'Key'  = ebadefinition.jsondoc_ ->> 'IsNpl'
		left join madata.custom_lookup l8 on l8.t_ = 'EbaTrigger'  and l8.jsondoc_->>'Key'  = ebadefinition.jsondoc_ ->> 'Reason'
		left join madata.custom_lookup l9 on l9.t_ = 'YesNoRm'  and l9.jsondoc_->>'Key'  = ebadefinition.jsondoc_ ->> 'SubstantialDelay'
		left join madata.custom_lookup l10 on l10.t_ = 'YesNo'  and l10.jsondoc_->>'Key'  = ebadefinition.jsondoc_ ->> 'UtpFlag'
		left join madata.custom_lookup l12 on l12.t_ = 'EbaYesNo'  and l12.jsondoc_->>'Key'  = ebadefinition.jsondoc_ ->> 'VdInitialValue'
	WHERE	
		GREATEST(ebadefinition.updateddate_, ebadefinition.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABEBADEFINITION')
		AND GREATEST(ebadefinition.updateddate_, ebadefinition.createddate_)::timestamp <= max_refreshhistory 
		AND ebadefinition.t_ = 'EbaDefinition'
	;
	
raise notice '% - Step abebadefiniton - part b end', clock_timestamp();

--abebadefinition
raise notice '% - Step abebadefiniton_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abebadefinition_idx;
DROP INDEX if exists olapts.abebadefinition_idx2;
CREATE INDEX IF NOT EXISTS abebadefiniton_idx ON olapts.abebadefinition (id_);
CREATE INDEX IF NOT EXISTS abebadefiniton_idx2 ON olapts.abebadefinition (pkid_,versionid_);
	
raise notice '% - Step abebadefinition_idx - part a end', clock_timestamp(); 
END IF;

------------------------------------------------------------------------
raise notice '% step abebadefiniton - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abebadefinitionflag;
CREATE TABLE IF NOT EXISTS olapts.abebadefinitionflag AS

select
id_,
pkid_,
wfid_ wfid_,
taskid_ taskid_, 
versionid_ versionid_,
isdeleted_ isdeleted_,
islatestversion_ islatestversion_,
baseversionid_ baseversionid_,
contextuserid_ contextuserid_,
isvisible_ isvisible_,
isvalid_ isvalid_,
snapshotid_ snapshotid_,
t_ t_,
createdby_ createdby_,
createddate_ createddate_,
updatedby_ updatedby_,
updateddate_ updateddate_,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_, createddate_) as sourcepopulateddate_
,current_setting('myvariables.popdate')::timestamp as populateddate_
FROM madata.ebadefinition
where 
GREATEST(ebadefinition.updateddate_, ebadefinition.createddate_)::timestamp <= max_refreshhistory 
and ebadefinition.t_ = 'EbaDefinition';

raise notice '% - Step abebadefinitonflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abebadefinitionflag_idx;
DROP INDEX if exists olapts.abebadefinitonflag_idx2;
CREATE INDEX IF NOT EXISTS abebadefinitionflag_idx ON olapts.abebadefinitionflag (id_);
CREATE INDEX IF NOT EXISTS abebadefinitionflag_idx2 ON olapts.abebadefinitionflag (pkid_,versionid_);
ANALYZE olapts.abebadefinitionflag ;

raise notice '% - Step abebadefinitionflag_idx - part a end', clock_timestamp(); 

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABEBADEFINITION';
delete from olapts.refreshhistory where tablename = 'ABEBADEFINITION';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABEBADEFINITION' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABEBADEFINITIONFLAG';
delete from olapts.refreshhistory where tablename = 'ABEBADEFINITIONFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABEBADEFINITIONFLAG' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abebadefiniton - part c end', clock_timestamp();

-------------------REF Data--------------------------
--EbaYesNo
raise notice '% - Step abebayesno - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abebayesno;
CREATE TABLE olapts.abebayesno AS
select l.jsondoc_->>'Key' ebayesnokey_,
l.jsondoc_->>'Value' ebayesnovalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_
,current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'EbaYesNo';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABEBAYESNO';
delete from olapts.refreshhistory where tablename = 'ABEBAYESNO';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABEBAYESNO' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abebayesno - part a end', clock_timestamp();

--EbaComment
raise notice '% - Step abebacomment - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abebacomment;
CREATE TABLE olapts.abebacomment AS
select l.jsondoc_->>'Key' ebacommentkey_,
l.jsondoc_->>'Value' ebacommentvalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_
,current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'EbaComment';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABEBACOMMENT';
delete from olapts.refreshhistory where tablename = 'ABEBACOMMENT';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABEBACOMMENT' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abebacomment - part a end', clock_timestamp();

--YesNoRM
raise notice '% - Step abyesnorm - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abyesnorm;
CREATE TABLE olapts.abyesnorm AS
select l.jsondoc_->>'Key' yesnormkey_,
l.jsondoc_->>'Value' yesnormvalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_
,current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'YesNoRm';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABYESNORM';
delete from olapts.refreshhistory where tablename = 'ABYESNORM';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABYESNORM' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abyesnorm - part a end', clock_timestamp();

--YesNo
raise notice '% - Step abyesno - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abyesno;
CREATE TABLE olapts.abyesno AS
select l.jsondoc_->>'Key' yesnokey_,
l.jsondoc_->>'Value' yesnovalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_
,current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'YesNo';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABYESNO';
delete from olapts.refreshhistory where tablename = 'ABYESNO';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABYESNO' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abyesno - part a end', clock_timestamp();

--EbaTrigger
raise notice '% - Step abebatrigger - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abebatrigger;
CREATE TABLE olapts.abebatrigger AS
select l.jsondoc_->>'Key' ebatriggerkey_,
l.jsondoc_->>'Value' ebatriggervalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_
,current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'EbaTrigger';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABEBATRIGGER';
delete from olapts.refreshhistory where tablename = 'ABEBATRIGGER';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABEBATRIGGER' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abebatrigger - part a end', clock_timestamp();

--END $$;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--espolicy
--DO $$
--DECLARE
--varprevsuccessdate TIMESTAMP ;
--BEGIN

IF EXISTS (select from olapts.refreshhistory where tablename = 'ABESPOLICY') THEN
raise notice '% - Step abespolicy - part a start', clock_timestamp();
insert into olapts.abespolicy
	SELECT
	espolicy.id_ AS id_,
	espolicy.pkid_::varchar as pkid_,
	(espolicy.jsondoc_ ->> 'EsPolicyId')::numeric AS EsPolicyId,
	(espolicy.jsondoc_ ->> 'ActionPlan')::int4 ActionPlan,
	(l1.jsondoc_ ->> 'Value') AS ActionPlanval,
	(espolicy.jsondoc_ ->> 'ActionPlanAssessDate')::timestamp AS ActionPlanAssessDate,
	(espolicy.jsondoc_ ->> 'ActionPlanAssessUser') AS ActionPlanAssessUser,
	(espolicy.jsondoc_ ->> 'ActionPlanEnddate')::timestamp AS ActionPlanEnddate,
	(espolicy.jsondoc_ ->> 'ActionPlanUpdate')::boolean AS ActionPlanUpdate,
	(espolicy.jsondoc_ ->> 'ActionPlanUpdateDate')::timestamp AS ActionPlanUpdateDate,
	(espolicy.jsondoc_ ->> 'ActionPlanUpdateUser') AS ActionPlanUpdateUser,
	(espolicy.jsondoc_ ->> 'Active') AS Active,
	(espolicy.jsondoc_ ->> 'AssessmentCreatedDate')::date AS AssessmentCreatedDate,
	(espolicy.jsondoc_ ->> 'AssessmentUserCreated') AS AssessmentUserCreated,
	(espolicy.jsondoc_ ->> 'Authorization')::int4 AS Authorization,
	(l2.jsondoc_ ->> 'Value') AS Authorizationval,
	(espolicy.jsondoc_ ->> 'AuthorizeChecker')::boolean AS AuthorizeChecker,
	(espolicy.jsondoc_ ->> 'AuthorizeShow')::boolean AS AuthorizeShow,
	(espolicy.jsondoc_ ->> 'BusinessSector')::int4 BusinessSector,
	(l3.jsondoc_ ->> 'Value') AS BusinessSectorval,
	(espolicy.jsondoc_ ->> 'BusinessSectorLarge')::int4 BusinessSectorLarge,
	(l4.jsondoc_ ->> 'Value') AS BusinessSectorLargeval,
	(espolicy.jsondoc_ ->> 'CalculateGeneralRisk') AS CalculateGeneralRisk,
	(espolicy.jsondoc_ ->> 'Choice')::int4 Choice,
	(l5.jsondoc_ ->> 'Value') AS Choiceval,
	(espolicy.jsondoc_ ->> 'CollateralOfferLarge')::int4 CollateralOfferLarge,
	(l6.jsondoc_ ->> 'Value') AS CollateralOfferLargeval,
	(espolicy.jsondoc_ ->> 'CreatedDate')::timestamp AS timestamp,
	(espolicy.jsondoc_ ->> 'CreatedUser') AS CreatedUser,
	(espolicy.jsondoc_ ->> 'EnSoAssessmentEndDate')::timestamp AS EnSoAssessmentEndDate,
	(espolicy.jsondoc_ ->> 'EnSoOverrideCategorization')::int4 EnSoOverrideCategorization,
	(l7.jsondoc_ ->> 'Value') AS EnSoOverrideCategorizationval,
	(espolicy.jsondoc_ ->> 'EnsoCreditCommittee') EnsoCreditCommittee,
	(l8.jsondoc_ ->> 'Value') AS EnsoCreditCommitteeval,
	(espolicy.jsondoc_ ->> 'EntityId')::numeric AS EntityId,
	(espolicy.jsondoc_ ->> 'EntityVersionId')::numeric AS EntityVersionId,
	(espolicy.jsondoc_ ->> 'EsCalculateRiskSme') AS EsCalculateRiskSme,
	(espolicy.jsondoc_ ->> 'EscalculationRiskLarge') AS EscalculationRiskLarge,
	(espolicy.jsondoc_ ->> 'ExpirationDate')::date AS ExpirationDate,
	(espolicy.jsondoc_ ->> 'FlagUpdatedDate')::timestamp AS FlagUpdatedDate,
	(espolicy.jsondoc_ ->> 'FlagUpdatedUser') AS FlagUpdatedUser,
	(espolicy.jsondoc_ ->> 'Originated') AS Originated,
	(espolicy.jsondoc_ ->> 'OverrideReason') AS OverrideReason,
	(espolicy.jsondoc_ ->> 'PurposeOfLoan')::int4 PurposeOfLoan,
	(l9.jsondoc_ ->> 'Value') AS PurposeOfLoanval,
	(espolicy.jsondoc_ ->> 'PurposeOfLoanLarge')::int4 PurposeOfLoanLarge,
	(l10.jsondoc_ ->> 'Value') AS PurposeOfLoanLargeval,
	(espolicy.jsondoc_ ->> 'RiskCategorization')::int4 RiskCategorization,
	(l11.jsondoc_ ->> 'Value') AS RiskCategorizationval,
	(espolicy.jsondoc_ ->> 'SizeOfLoan')::int4 SizeOfLoan,
	(l13.jsondoc_ ->> 'Value') AS SizeOfLoanval,
	(espolicy.jsondoc_ ->> 'SizeOfLoanLarge')::int4 SizeOfLoanLarge,
	(l14.jsondoc_ ->> 'Value') AS SizeOfLoanLargeval,
	(espolicy.jsondoc_ ->> 'TermOfLoan')::int4 TermOfLoan,
	(l15.jsondoc_ ->> 'Value') AS TermOfLoanval,
	(espolicy.jsondoc_ ->> 'TermOfLoanLarge')::int4 TermOfLoanLarge,
	(l16.jsondoc_ ->> 'Value') AS TermOfLoanLargeval,
	espolicy.wfid_::varchar,
	espolicy.taskid_::varchar,
	espolicy.versionid_::int4,
	espolicy.isdeleted_::boolean,
	espolicy.islatestversion_::boolean,
	espolicy.baseversionid_::int4,
	espolicy.contextuserid_::varchar,
	espolicy.isvisible_::boolean,
	espolicy.isvalid_::boolean,
	espolicy.snapshotid_::int4,
	espolicy.t_::varchar,
	espolicy.createdby_::varchar,
	espolicy.createddate_::timestamp,
	espolicy.updatedby_::varchar,
	espolicy.updateddate_::timestamp,
	espolicy.fkid_entity,
	CASE WHEN espolicy.updateddate_ > espolicy.createddate_ THEN espolicy.updatedby_ ELSE espolicy.createdby_ END AS sourcepopulatedby_,
	GREATEST(espolicy.createddate_, espolicy.updateddate_) AS sourcepopulateddate_
	,current_setting('myvariables.popdate')::timestamp as populateddate_
	FROM madata.espolicy
	left join madata.custom_lookup l1 on l1.t_ = 'ActionPlan'  and l1.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'ActionPlan'
	left join madata.custom_lookup l2 on l2.t_ = 'TrueFalse'  and l2.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'Authorization'
	left join madata.custom_lookup l3 on l3.t_ = 'MediumHighSme'  and l3.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'BusinessSector'
	left join madata.custom_lookup l4 on l4.t_ = 'EsLarge'  and l4.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'BusinessSectorLarge'
	left join madata.custom_lookup l5 on l5.t_ = 'ChooseView'  and l5.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'Choice'
	left join madata.custom_lookup l6 on l6.t_ = 'EsLarge'  and l6.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'CollateralOfferLarge'
	left join madata.custom_lookup l7 on l7.t_ = 'Riskcategorization'  and l7.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'EnSoOverrideCategorization'
	left join madata.custom_lookup l8 on l8.t_ = 'EnSoCreditCommittee'  and l8.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'EnsoCreditCommittee'
	left join madata.custom_lookup l9 on l9.t_ = 'EnSoParam'  and l9.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'PurposeOfLoan'
	left join madata.custom_lookup l10 on l10.t_ = 'EsLarge'  and l10.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'PurposeOfLoanLarge'
	left join madata.custom_lookup l11 on l11.t_ = 'Riskcategorization'  and l11.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'RiskCategorization'
	--left join madata.custom_lookup l12 on l12.t_ = 'Riskcategorization'  and l12.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'RiskCategorizationLarge'
	left join madata.custom_lookup l13 on l13.t_ = 'EnSoParam'  and l13.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'SizeOfLoan'
	left join madata.custom_lookup l14 on l14.t_ = 'EsLarge'  and l14.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'SizeOfLoanLarge'
	left join madata.custom_lookup l15 on l15.t_ = 'EnSoParam'  and l15.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'TermOfLoan'
	left join madata.custom_lookup l16 on l16.t_ = 'EsLarge'  and l16.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'TermOfLoanLarge'		
	WHERE GREATEST(espolicy.updateddate_, espolicy.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) 	from olapts.refreshhistory where tablename = 'ABESPOLICY')
	AND GREATEST(espolicy.updateddate_, espolicy.createddate_)::timestamp <= max_refreshhistory 
	AND espolicy.t_ = 'EsPolicy';
	raise notice '% - Step abespolicy - part a end', clock_timestamp();
--------------------------------------------
ELSE
	raise notice '% Step abespolicy - part b start', clock_timestamp();
	DROP TABLE IF EXISTS olapts.abespolicy;
	CREATE TABLE olapts.abespolicy AS
	SELECT
	espolicy.id_ AS id_,
	espolicy.pkid_::varchar as pkid_,
	(espolicy.jsondoc_ ->> 'EsPolicyId')::numeric AS EsPolicyId,
	(espolicy.jsondoc_ ->> 'ActionPlan')::int4 ActionPlan,
	(l1.jsondoc_ ->> 'Value') AS ActionPlanval,
	(espolicy.jsondoc_ ->> 'ActionPlanAssessDate')::timestamp AS ActionPlanAssessDate,
	(espolicy.jsondoc_ ->> 'ActionPlanAssessUser') AS ActionPlanAssessUser,
	(espolicy.jsondoc_ ->> 'ActionPlanEnddate')::timestamp AS ActionPlanEnddate,
	(espolicy.jsondoc_ ->> 'ActionPlanUpdate')::boolean AS ActionPlanUpdate,
	(espolicy.jsondoc_ ->> 'ActionPlanUpdateDate')::timestamp AS ActionPlanUpdateDate,
	(espolicy.jsondoc_ ->> 'ActionPlanUpdateUser') AS ActionPlanUpdateUser,
	(espolicy.jsondoc_ ->> 'Active') AS Active,
	(espolicy.jsondoc_ ->> 'AssessmentCreatedDate')::date AS AssessmentCreatedDate,
	(espolicy.jsondoc_ ->> 'AssessmentUserCreated') AS AssessmentUserCreated,
	(espolicy.jsondoc_ ->> 'Authorization')::int4 AS Authorization,
	(l2.jsondoc_ ->> 'Value') AS Authorizationval,
	(espolicy.jsondoc_ ->> 'AuthorizeChecker')::boolean AS AuthorizeChecker,
	(espolicy.jsondoc_ ->> 'AuthorizeShow')::boolean AS AuthorizeShow,
	(espolicy.jsondoc_ ->> 'BusinessSector')::int4 BusinessSector,
	(l3.jsondoc_ ->> 'Value') AS BusinessSectorval,
	(espolicy.jsondoc_ ->> 'BusinessSectorLarge')::int4 BusinessSectorLarge,
	(l4.jsondoc_ ->> 'Value') AS BusinessSectorLargeval,
	(espolicy.jsondoc_ ->> 'CalculateGeneralRisk') AS CalculateGeneralRisk,
	(espolicy.jsondoc_ ->> 'Choice')::int4 Choice,
	(l5.jsondoc_ ->> 'Value') AS Choiceval,
	(espolicy.jsondoc_ ->> 'CollateralOfferLarge')::int4 CollateralOfferLarge,
	(l6.jsondoc_ ->> 'Value') AS CollateralOfferLargeval,
	(espolicy.jsondoc_ ->> 'CreatedDate')::timestamp AS timestamp,
	(espolicy.jsondoc_ ->> 'CreatedUser') AS CreatedUser,
	(espolicy.jsondoc_ ->> 'EnSoAssessmentEndDate')::timestamp AS EnSoAssessmentEndDate,
	(espolicy.jsondoc_ ->> 'EnSoOverrideCategorization')::int4 EnSoOverrideCategorization,
	(l7.jsondoc_ ->> 'Value') AS EnSoOverrideCategorizationval,
	(espolicy.jsondoc_ ->> 'EnsoCreditCommittee') EnsoCreditCommittee,
	(l8.jsondoc_ ->> 'Value') AS EnsoCreditCommitteeval,
	(espolicy.jsondoc_ ->> 'EntityId')::numeric AS EntityId,
	(espolicy.jsondoc_ ->> 'EntityVersionId')::numeric AS EntityVersionId,
	(espolicy.jsondoc_ ->> 'EsCalculateRiskSme') AS EsCalculateRiskSme,
	(espolicy.jsondoc_ ->> 'EscalculationRiskLarge') AS EscalculationRiskLarge,
	(espolicy.jsondoc_ ->> 'ExpirationDate')::date AS ExpirationDate,
	(espolicy.jsondoc_ ->> 'FlagUpdatedDate')::timestamp AS FlagUpdatedDate,
	(espolicy.jsondoc_ ->> 'FlagUpdatedUser') AS FlagUpdatedUser,
	(espolicy.jsondoc_ ->> 'Originated') AS Originated,
	(espolicy.jsondoc_ ->> 'OverrideReason') AS OverrideReason,
	(espolicy.jsondoc_ ->> 'PurposeOfLoan')::int4 PurposeOfLoan,
	(l9.jsondoc_ ->> 'Value') AS PurposeOfLoanval,
	(espolicy.jsondoc_ ->> 'PurposeOfLoanLarge')::int4 PurposeOfLoanLarge,
	(l10.jsondoc_ ->> 'Value') AS PurposeOfLoanLargeval,
	(espolicy.jsondoc_ ->> 'RiskCategorization')::int4 RiskCategorization,
	(l11.jsondoc_ ->> 'Value') AS RiskCategorizationval,
	(espolicy.jsondoc_ ->> 'SizeOfLoan')::int4 SizeOfLoan,
	(l13.jsondoc_ ->> 'Value') AS SizeOfLoanval,
	(espolicy.jsondoc_ ->> 'SizeOfLoanLarge')::int4 SizeOfLoanLarge,
	(l14.jsondoc_ ->> 'Value') AS SizeOfLoanLargeval,
	(espolicy.jsondoc_ ->> 'TermOfLoan')::int4 TermOfLoan,
	(l15.jsondoc_ ->> 'Value') AS TermOfLoanval,
	(espolicy.jsondoc_ ->> 'TermOfLoanLarge')::int4 TermOfLoanLarge,
	(l16.jsondoc_ ->> 'Value') AS TermOfLoanLargeval,
	espolicy.wfid_::varchar,
	espolicy.taskid_::varchar,
	espolicy.versionid_::int4,
	espolicy.isdeleted_::boolean,
	espolicy.islatestversion_::boolean,
	espolicy.baseversionid_::int4,
	espolicy.contextuserid_::varchar,
	espolicy.isvisible_::boolean,
	espolicy.isvalid_::boolean,
	espolicy.snapshotid_::int4,
	espolicy.t_::varchar,
	espolicy.createdby_::varchar,
	espolicy.createddate_::timestamp,
	espolicy.updatedby_::varchar,
	espolicy.updateddate_::timestamp,
	espolicy.fkid_entity,
	CASE WHEN espolicy.updateddate_ > espolicy.createddate_ THEN espolicy.updatedby_ ELSE espolicy.createdby_ END AS sourcepopulatedby_,
	GREATEST(espolicy.createddate_, espolicy.updateddate_) AS sourcepopulateddate_
	,current_setting('myvariables.popdate')::timestamp as populateddate_
	FROM madata.espolicy
	left join madata.custom_lookup l1 on l1.t_ = 'ActionPlan'  and l1.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'ActionPlan'
	left join madata.custom_lookup l2 on l2.t_ = 'TrueFalse'  and l2.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'Authorization'
	left join madata.custom_lookup l3 on l3.t_ = 'MediumHighSme'  and l3.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'BusinessSector'
	left join madata.custom_lookup l4 on l4.t_ = 'EsLarge'  and l4.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'BusinessSectorLarge'
	left join madata.custom_lookup l5 on l5.t_ = 'ChooseView'  and l5.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'Choice'
	left join madata.custom_lookup l6 on l6.t_ = 'EsLarge'  and l6.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'CollateralOfferLarge'
	left join madata.custom_lookup l7 on l7.t_ = 'Riskcategorization'  and l7.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'EnSoOverrideCategorization'
	left join madata.custom_lookup l8 on l8.t_ = 'EnSoCreditCommittee'  and l8.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'EnsoCreditCommittee'
	left join madata.custom_lookup l9 on l9.t_ = 'EnSoParam'  and l9.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'PurposeOfLoan'
	left join madata.custom_lookup l10 on l10.t_ = 'EsLarge'  and l10.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'PurposeOfLoanLarge'
	left join madata.custom_lookup l11 on l11.t_ = 'Riskcategorization'  and l11.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'RiskCategorization'
	--left join madata.custom_lookup l12 on l12.t_ = 'Riskcategorization'  and l12.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'RiskCategorizationLarge'
	left join madata.custom_lookup l13 on l13.t_ = 'EnSoParam'  and l13.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'SizeOfLoan'
	left join madata.custom_lookup l14 on l14.t_ = 'EsLarge'  and l14.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'SizeOfLoanLarge'
	left join madata.custom_lookup l15 on l15.t_ = 'EnSoParam'  and l15.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'TermOfLoan'
	left join madata.custom_lookup l16 on l16.t_ = 'EsLarge'  and l16.jsondoc_->>'Key'  = espolicy.jsondoc_ ->> 'TermOfLoanLarge'		
	WHERE GREATEST(espolicy.updateddate_, espolicy.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) 	from olapts.refreshhistory where tablename = 'ABESPOLICY')
	AND GREATEST(espolicy.updateddate_, espolicy.createddate_)::timestamp <= max_refreshhistory 
	AND espolicy.t_ = 'EsPolicy';
	
raise notice '% - Step abespolicy - part b end', clock_timestamp();

--abespolicy
raise notice '% - Step abespolicy_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abespolicy_idx;
DROP INDEX if exists olapts.abespolicy_idx2;
CREATE INDEX IF NOT EXISTS abespolicy_idx ON olapts.abespolicy (id_);
CREATE INDEX IF NOT EXISTS abespolicy_idx2 ON olapts.abespolicy (pkid_,versionid_);
	
raise notice '% - Step abespolicy_idx - part a end', clock_timestamp(); 
END IF;
------------------------------------------------------------------------
raise notice '% step abespolicy - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abespolicyflag;
CREATE TABLE IF NOT EXISTS olapts.abespolicyflag AS

select
id_,
pkid_,
wfid_ wfid_,
taskid_ taskid_, 
versionid_ versionid_,
isdeleted_ isdeleted_,
islatestversion_ islatestversion_,
baseversionid_ baseversionid_,
contextuserid_ contextuserid_,
isvisible_ isvisible_,
isvalid_ isvalid_,
snapshotid_ snapshotid_,
t_ t_,
createdby_ createdby_,
createddate_ createddate_,
updatedby_ updatedby_,
updateddate_ updateddate_,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_, createddate_) as sourcepopulateddate_
,current_setting('myvariables.popdate')::timestamp as populateddate_
FROM madata.espolicy
where 
 GREATEST(espolicy.updateddate_, espolicy.createddate_)::timestamp <= max_refreshhistory 
and espolicy.t_ = 'EsPolicy'
;

raise notice '% - Step abespolicyflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abespolicyflag_idx;
DROP INDEX if exists olapts.abespolicyflag_idx2;
CREATE INDEX IF NOT EXISTS abespolicyflag_idx ON olapts.abespolicyflag (id_);
CREATE INDEX IF NOT EXISTS abespolicyflag_idx2 ON olapts.abespolicyflag (pkid_,versionid_);
ANALYZE olapts.abespolicyflag ;

raise notice '% - Step abespolicyflag_idx - part a end', clock_timestamp(); 

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABESPOLICY';
delete from olapts.refreshhistory where tablename = 'ABESPOLICY';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABESPOLICY' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABESPOLICYFLAG';
delete from olapts.refreshhistory where tablename = 'ABESPOLICYFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABESPOLICYFLAG' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abespolicy - part c end', clock_timestamp();

------------------------------------------

-------------------REF Data--------------------------

--ActionPlan
raise notice '% - Step abactionplan - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abactionplan;
CREATE TABLE olapts.abactionplan AS
select l.jsondoc_->>'Key' actionplankey_,
l.jsondoc_->>'Value' actionplanvalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'ActionPlan';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABACTIONPLAN';
delete from olapts.refreshhistory where tablename = 'ABACTIONPLAN';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABACTIONPLAN' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abactionplan - part a end', clock_timestamp();

--TrueFalse exists in utp migration script

--MediumHighSme
raise notice '% - Step abmediumhighsme - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abmediumhighsme;
CREATE TABLE olapts.abmediumhighsme AS
select l.jsondoc_->>'Key' mediumhighsmekey_,
l.jsondoc_->>'Value' mediumhighsmevalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'MediumHighSme';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABMEDIUMHIGHSME';
delete from olapts.refreshhistory where tablename = 'ABMEDIUMHIGHSME';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABMEDIUMHIGHSME' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abmediumhighsme - part a end', clock_timestamp();

--EsLarge
raise notice '% - Step abeslarge - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abeslarge;
CREATE TABLE olapts.abeslarge AS
select l.jsondoc_->>'Key' eslargekey_,
l.jsondoc_->>'Value' eslargevalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'EsLarge';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABESLARGE';
delete from olapts.refreshhistory where tablename = 'ABESLARGE';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABESLARGE' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abeslarge - part a end', clock_timestamp();

--ChooseView
raise notice '% - Step abchooseview - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abchooseview;
CREATE TABLE olapts.abchooseview AS
select l.jsondoc_->>'Key' chooseviewkey_,
l.jsondoc_->>'Value' chooseviewvalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'ChooseView';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABCHOOSEVIEW';
delete from olapts.refreshhistory where tablename = 'ABCHOOSEVIEW';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABCHOOSEVIEW' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abchooseview - part a end', clock_timestamp();

--Riskcategorization
raise notice '% - Step abriskcategorization - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abriskcategorization;
CREATE TABLE olapts.abriskcategorization AS
select l.jsondoc_->>'Key' riskcategorizationkey_,
l.jsondoc_->>'Value' riskcategorizationvalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'Riskcategorization';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABRISKCATEGORIZATION';
delete from olapts.refreshhistory where tablename = 'ABRISKCATEGORIZATION';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABRISKCATEGORIZATION' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abriskcategorization - part a end', clock_timestamp();

--EnSoCreditCommittee
raise notice '% - Step abensocreditcommittee - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abensocreditcommittee;
CREATE TABLE olapts.abensocreditcommittee AS
select l.jsondoc_->>'Key' ensocreditcommitteekey_,
l.jsondoc_->>'Value' ensocreditcommitteevalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'EnSoCreditCommittee';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABENSOCREDITCOMMITTEE';
delete from olapts.refreshhistory where tablename = 'ABENSOCREDITCOMMITTEE';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABENSOCREDITCOMMITTEE' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abensocreditcommittee - part a end', clock_timestamp();

--EnSoParam
raise notice '% - Step abensoparam - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abensoparam;
CREATE TABLE olapts.abensoparam AS
select l.jsondoc_->>'Key' ensoparamkey_,
l.jsondoc_->>'Value' ensoparamvalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'EnSoParam';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABENSOPARAM';
delete from olapts.refreshhistory where tablename = 'ABENSOPARAM';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABENSOPARAM' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step ensoparam - part a end', clock_timestamp();

--END $$;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---- LeverageIndication ----

--DO $$
--DECLARE
--varprevsuccessdate TIMESTAMP ;
--BEGIN

-- If table exists in refresh history --
IF EXISTS (select from olapts.refreshhistory where tablename = 'ABLEVERAGEINDICATION') THEN
	raise notice '% - Step ableverageindication - part a start', clock_timestamp();
	insert into olapts.ableverageindication
		SELECT
		leverageindication.id_ AS id_,
		leverageindication.pkid_::varchar as pkid_,	
		(leverageindication.jsondoc_ ->> 'LeverageId')::numeric AS leverageId,
		(leverageindication.jsondoc_ ->> 'Active')::character varying AS active, 
		(leverageindication.jsondoc_->> 'CreatedDate')::timestamp AS createdDate,
		(leverageindication.jsondoc_->> 'CreatedUser') AS CreatedUser, --add 20/5
		(leverageindication.jsondoc_ ->> 'CreditCommitteeDate')::date AS creditCommitteeDate,	
		(leverageindication.jsondoc_->> 'EntityId')::numeric AS entityId,
		(leverageindication.jsondoc_ ->> 'EntityVersionId')::numeric AS entityVersionId,
		(leverageindication.jsondoc_ ->> 'HighLeverageCustomer') AS highleveragecustomer,
		(l3.jsondoc_ ->> 'Value') AS highleveragecustomerval,
		(leverageindication.jsondoc_->> 'LeverageFinancialIndication') AS leverageFinancialIndication,
		(l0.jsondoc_->> 'Value') AS leverageFinancialIndicationval,
		(leverageindication.jsondoc_ ->> 'LeverageFinancingReason') AS leverageFinancingReason,
		(l1.jsondoc_ ->> 'Value') AS leverageFinancingReasonval,
		(leverageindication.jsondoc_ ->> 'LeverageOwnedByaSponsor') AS leverageownedbyasponsor,--added 11/06/2024
		(l4.jsondoc_ ->> 'Values') AS leverageownedbyasponsorval,--added 11/06/2024
		(leverageindication.jsondoc_ ->> 'LeverageSaved')::integer AS leverageSaved,
		(leverageindication.jsondoc_ ->> 'LeverageSponsorName') AS leveragesponsorname,	--added 11/06/2024		
		(leverageindication.jsondoc_ ->> 'LeverageTypeReview') AS leverageTypeReview,	
		(l2.jsondoc_ ->> 'Value') AS leverageTypeReviewval,			
		leverageindication.wfid_::varchar,
		leverageindication.taskid_::varchar,
		leverageindication.versionid_::int4,
		leverageindication.isdeleted_::boolean,
		leverageindication.islatestversion_::boolean,
		leverageindication.baseversionid_::int4,
		leverageindication.contextuserid_::varchar,
		leverageindication.isvisible_::boolean,
		leverageindication.isvalid_::boolean,
		leverageindication.snapshotid_::int4,
		leverageindication.t_::varchar,
		leverageindication.createdby_::varchar,
		leverageindication.createddate_::timestamp,
		leverageindication.updatedby_::varchar,
		leverageindication.updateddate_::timestamp,
		leverageindication.fkid_entity,
		CASE WHEN leverageindication.updateddate_ > leverageindication.createddate_ THEN leverageindication.updatedby_ ELSE leverageindication.createdby_ END AS sourcepopulatedby_,
		GREATEST(leverageindication.createddate_, leverageindication.updateddate_) AS sourcepopulateddate_
		,current_setting('myvariables.popdate')::timestamp as populateddate_
		FROM madata.leverageindication
		LEFT JOIN madata.custom_lookup as l0 ON l0.t_ = 'LeverageFinancialIndication' and l0.jsondoc_->>'Key' = leverageindication.jsondoc_->>'LeverageFinancialIndication'		
		LEFT JOIN madata.custom_lookup as l1 ON l1.t_='LeverageReason' and l1.jsondoc_->>'Key' = leverageindication.jsondoc_->>'LeverageFinancingReason'		
		LEFT JOIN madata.custom_lookup as l2 ON l2.t_ = 'LeverageTypeReview' and l2.jsondoc_->>'Key' = leverageindication.jsondoc_->>'LeverageTypeReview'
		LEFT JOIN madata.custom_lookup as l3 ON l3.t_ = 'LeverageTypeReview' and l3.jsondoc_->>'Key' = leverageindication.jsondoc_->>'HighLeverageCustomer'
		LEFT JOIN madata.custom_lookup as l4 ON l4.t_ = 'LeverageOwnedBySponsor' and l4.jsondoc_->>'Key' = leverageindication.jsondoc_->>'LeverageOwnedByaSponsor' --added 11/06/2024
		WHERE
		GREATEST(leverageindication.updateddate_, leverageindication.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABLEVERAGEINDICATION')
		AND GREATEST(leverageindication.updateddate_, leverageindication.createddate_)::timestamp <= max_refreshhistory 
		AND leverageindication.t_ = 'LeverageIndication';
raise notice '% - Step ableverageindication - part a end', clock_timestamp();
ELSE
raise notice '% Step ableverageindication - part b start', clock_timestamp();
	DROP TABLE IF EXISTS olapts.ableverageindication;
	CREATE TABLE olapts.ableverageindication AS
		SELECT
		leverageindication.id_ AS id_,
		leverageindication.pkid_::varchar as pkid_,	
		(leverageindication.jsondoc_ ->> 'LeverageId')::numeric AS leverageId,
		(leverageindication.jsondoc_ ->> 'Active')::character varying AS active, 
		(leverageindication.jsondoc_->> 'CreatedDate')::timestamp AS createdDate,
		(leverageindication.jsondoc_->> 'CreatedUser') AS CreatedUser, --add 20/5
		(leverageindication.jsondoc_ ->> 'CreditCommitteeDate')::date AS creditCommitteeDate,	
		(leverageindication.jsondoc_->> 'EntityId')::numeric AS entityId,
		(leverageindication.jsondoc_ ->> 'EntityVersionId')::numeric AS entityVersionId,
		(leverageindication.jsondoc_ ->> 'HighLeverageCustomer') AS highleveragecustomer,
		(l3.jsondoc_ ->> 'Value') AS highleveragecustomerval,
		(leverageindication.jsondoc_->> 'LeverageFinancialIndication') AS leverageFinancialIndication,
		(l0.jsondoc_->> 'Value') AS leverageFinancialIndicationval,
		(leverageindication.jsondoc_ ->> 'LeverageFinancingReason') AS leverageFinancingReason,
		(l1.jsondoc_ ->> 'Value') AS leverageFinancingReasonval,
		(leverageindication.jsondoc_ ->> 'LeverageOwnedByaSponsor') AS leverageownedbyasponsor,--added 11/06/2024
		(l4.jsondoc_ ->> 'Values') AS leverageownedbyasponsorval,--added 11/06/2024		
		(leverageindication.jsondoc_ ->> 'LeverageSaved')::integer AS leverageSaved,
		(leverageindication.jsondoc_ ->> 'LeverageSponsorName') AS leveragesponsorname,	--added 11/06/2024		
		(leverageindication.jsondoc_ ->> 'LeverageTypeReview') AS leverageTypeReview,	
		(l2.jsondoc_ ->> 'Value') AS leverageTypeReviewval,		
		leverageindication.wfid_::varchar,
		leverageindication.taskid_::varchar,
		leverageindication.versionid_::int4,
		leverageindication.isdeleted_::boolean,
		leverageindication.islatestversion_::boolean,
		leverageindication.baseversionid_::int4,
		leverageindication.contextuserid_::varchar,
		leverageindication.isvisible_::boolean,
		leverageindication.isvalid_::boolean,
		leverageindication.snapshotid_::int4,
		leverageindication.t_::varchar,
		leverageindication.createdby_::varchar,
		leverageindication.createddate_::timestamp,
		leverageindication.updatedby_::varchar,
		leverageindication.updateddate_::timestamp,
		leverageindication.fkid_entity,
		CASE WHEN leverageindication.updateddate_ > leverageindication.createddate_ THEN leverageindication.updatedby_ ELSE leverageindication.createdby_ END AS sourcepopulatedby_,
		GREATEST(leverageindication.createddate_, leverageindication.updateddate_) AS sourcepopulateddate_
		,current_setting('myvariables.popdate')::timestamp as populateddate_
		FROM madata.leverageindication
		LEFT JOIN madata.custom_lookup as l0 ON l0.t_ = 'LeverageFinancialIndication' and l0.jsondoc_->>'Key' = leverageindication.jsondoc_->>'LeverageFinancialIndication'		
		LEFT JOIN madata.custom_lookup as l1 ON l1.t_='LeverageReason' and l1.jsondoc_->>'Key' = leverageindication.jsondoc_->>'LeverageFinancingReason'		
		LEFT JOIN madata.custom_lookup as l2 ON l2.t_ = 'LeverageTypeReview' and l2.jsondoc_->>'Key' = leverageindication.jsondoc_->>'LeverageTypeReview'
		LEFT JOIN madata.custom_lookup as l3 ON l3.t_ = 'LeverageTypeReview' and l3.jsondoc_->>'Key' = leverageindication.jsondoc_->>'HighLeverageCustomer'
		LEFT JOIN madata.custom_lookup as l4 ON l4.t_ = 'LeverageOwnedBySponsor' and l4.jsondoc_->>'Key' = leverageindication.jsondoc_->>'LeverageOwnedByaSponsor' --added 11/06/2024
		WHERE
		GREATEST(leverageindication.updateddate_, leverageindication.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABLEVERAGEINDICATION')
		AND GREATEST(leverageindication.updateddate_, leverageindication.createddate_)::timestamp <= max_refreshhistory 		
		AND leverageindication.t_ = 'LeverageIndication';
	raise notice '% - Step ableverageindication - part b end', clock_timestamp();
	
raise notice '% - Step ableverageindication_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.ableverageindication_idx;
DROP INDEX if exists olapts.ableverageindication_idx2;
CREATE INDEX IF NOT EXISTS ableverageindication_idx ON olapts.ableverageindication (id_);
CREATE INDEX IF NOT EXISTS ableverageindication_idx2 ON olapts.ableverageindication (pkid_,versionid_);
	
raise notice '% - Step ableverageindication - part a end', clock_timestamp();
END IF;

-- Create or update flag table -- 
raise notice '% Step ableverageindication - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.ableverageindicationflag;
CREATE TABLE IF NOT EXISTS olapts.ableverageindicationflag AS
	select
	id_,
	pkid_,
	wfid_ wfid_,
	taskid_ taskid_, 
	versionid_ versionid_,
	isdeleted_ isdeleted_,
	islatestversion_ islatestversion_,
	baseversionid_ baseversionid_,
	contextuserid_ contextuserid_,
	isvisible_ isvisible_,
	isvalid_ isvalid_,
	snapshotid_ snapshotid_,
	t_ t_,
	createdby_ createdby_,
	createddate_ createddate_,
	updatedby_ updatedby_,
	updateddate_ updateddate_,
	(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
	GREATEST(updateddate_, createddate_) as sourcepopulateddate_
	,current_setting('myvariables.popdate')::timestamp as populateddate_
	FROM madata.leverageindication
	where GREATEST(leverageindication.updateddate_, leverageindication.createddate_)::timestamp <= max_refreshhistory 	
	AND leverageindication.t_ = 'LeverageIndication';

raise notice '% - Step ableverageindicationflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.ableverageindicationflag_idx;
DROP INDEX if exists olapts.ableverageindicationflag_idx2;
CREATE INDEX IF NOT EXISTS ableverageindicationflag_idx ON olapts.ableverageindicationflag (id_);
CREATE INDEX IF NOT EXISTS ableverageindicationflag_idx2 ON olapts.ableverageindicationflag (pkid_,versionid_);
ANALYZE olapts.ableverageindicationflag ;

raise notice '% - Step ableverageindicationflag_idx - part a end', clock_timestamp(); 

-- Update refresh history -- 

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABLEVERAGEINDICATION';
delete from olapts.refreshhistory where tablename = 'ABLEVERAGEINDICATION';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABLEVERAGEINDICATION' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABLEVERAGEINDICATIONFLAG';
delete from olapts.refreshhistory where tablename = 'ABLEVERAGEINDICATIONFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABLEVERAGEINDICATIONFLAG' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step ableverageindication - part c end', clock_timestamp();
		

---- Reference data imports ----

-- LeverageFinancialIndication --
raise notice  '% - Step ableveragefinancialindication - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.ableveragefinancialindication;
CREATE TABLE olapts.ableveragefinancialindication AS
	SELECT
	l.jsondoc_->>'Key' ableveragefinancialindicationkey_,
	l.jsondoc_->>'Value' ableveragefinancialindicationvalue_,
	l.jsondoc_->>'Order' ableveragefinancialindicationorder_,
	isdeleted_,
	t_ t_,
	(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepoopulatedby_,
	GREATEST(updateddate_, createddate_) as sourcepopulateddate_
	from madata.v_lookup l
	where l.t_ = 'LeverageFinancialIndication';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABLEVERAGEFINANCIALINDICATION';
delete from olapts.refreshhistory where tablename = 'ABLEVERAGEFINANCIALINDICATION';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABLEVERAGEFINANCIALINDICATION' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step ableveragefinancialindication - part a end', clock_timestamp();

-- LeverageReason --
raise notice  '% - Step ableveragereason - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.ableveragereason;
CREATE TABLE olapts.ableveragereason AS
	SELECT
	l.jsondoc_->>'Key' ableveragereasonkey_,
	l.jsondoc_->>'Value' ableveragereasonvalue_,
	l.jsondoc_->>'Order' ableveragereasonorder_,
	isdeleted_,
	t_ t_,
	(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepoopulatedby_,
	GREATEST(updateddate_, createddate_) as sourcepopulateddate_
	from madata.v_lookup l
	where l.t_ = 'LeverageReason';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABLEVERAGEREASON';
delete from olapts.refreshhistory where tablename = 'ABLEVERAGEREASON';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABLEVERAGEREASON' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step ableveragereason - part a end', clock_timestamp();

--added 11/06/2024
-- LeverageOwnedByaSponsor --
raise notice  '% - Step ableverageownedbyasponsor - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.ableverageownedbyasponsor;
CREATE TABLE olapts.ableverageownedbyasponsor AS
	SELECT
	l.jsondoc_->>'Key' ableverageownedbyasponsorkey_,
	l.jsondoc_->>'Value' ableverageownedbyasponsorvalue_,
	l.jsondoc_->>'Order' ableverageownedbyasponsororder_,
	isdeleted_,
	t_ t_,
	(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepoopulatedby_,
	GREATEST(updateddate_, createddate_) as sourcepopulateddate_
	from madata.v_lookup l
	where l.t_ = 'LeverageOwnedByaSponsor';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABLEVERAGEOWNEDBYASPONSOR';
delete from olapts.refreshhistory where tablename = 'ABLEVERAGEOWNEDBYASPONSOR';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABLEVERAGEOWNEDBYASPONSOR' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step ableverageownedbyasponsor - part a end', clock_timestamp();


-- LeverageTypeReview --
raise notice  '% - Step ableveragetypereview - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.ableveragetypereview;
CREATE TABLE olapts.ableveragetypereview AS
	SELECT 
	l.jsondoc_->>'Key' ableveragetypereviewkey_,
	l.jsondoc_->>'Value' ableveragetypereviewvalue_,
	l.jsondoc_->>'Order' ableveragetyperevieworder_,
	isdeleted_,
	t_ t_,
	(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepoopulatedby_,
	GREATEST(updateddate_, createddate_) as sourcepopulateddate_
	from madata.v_lookup l 
	where l.t_ = 'LeverageTypeReview';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABLEVERAGETYPEREVIEW';
delete from olapts.refreshhistory where tablename = 'ABLEVERAGETYPEREVIEW';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABLEVERAGETYPEREVIEW' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step ableveragetypereview - part a end', clock_timestamp();

--END $$;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---luxemburgRatings---start

--DO $$
--DECLARE
--varprevsuccessdate TIMESTAMP ;
--BEGIN

IF EXISTS (select from olapts.refreshhistory where tablename = 'ABLUXEMBOURGRATINGS') THEN
raise notice '% - Step abluxembourgratings - part a start', clock_timestamp();
insert into olapts.abluxembourgratings
	SELECT
		mapinstance.id_ AS factmapinstanceid_,
		mapinstance.pkid_::varchar as pkid_,
		(mapinstance.jsondoc_ ->> 'EntityId')::numeric AS entityid,	
		(mapinstance.jsondoc_ ->> 'ModelId') AS modelid,
		(l0.jsondoc_ ->> 'Name') AS modelidval,
		(mapinstance.jsondoc_ ->> 'RmodelVersion') AS rmodelversion,
		(mapinstance.jsondoc_ ->> 'EntityVersionId')::numeric AS entityversionid,
		(mapinstance.jsondoc_->> 'Grade') AS grade,
		(l1.jsondoc_->> 'Grade') AS gradeval,
		(mapinstance.jsondoc_ ->> 'Score')::numeric AS score,
		(mapinstance.jsondoc_ ->> 'Pd')::numeric AS pd,
		(mapinstance.jsondoc_->> 'OriginationOfEntity') AS originationofentity,
		(l2.jsondoc_->> 'Value') AS originationofentityval,
		(mapinstance.jsondoc_->> 'Reason') AS reason,
		(l3.jsondoc_->> 'Value') AS reasonval,
		(mapinstance.jsondoc_ ->> 'Remarks') AS Remarks,
		case when mapinstance.t_ IN ('PdModelMidCorpLimitedData','PdModelMidCorporateFull','PdModelLargeCorp') then (l4.jsondoc_->> 'Value') 
             when mapinstance.t_ IN ('PdModelNonSystemic','PdModelMidCorpLimitedOver', 'PdModelMidCorpFullOverride','PdModelLargeCorpOverride') then (l5.jsondoc_->> 'Value') 
             when mapinstance.t_ IN ('PdModelShipping', 'PdModelShippingOverride', 'PdModeProjFinSlotAprOver', 'PdModeProjFinSlotAproach','PdModelRealEstateSlotting','PdModelRealEstateOverride') then (l6.jsondoc_->> 'Value')  
        end as SpecialGrade,
		mapinstance.wfid_::varchar,
		mapinstance.taskid_::varchar,
		mapinstance.versionid_::int4,
		mapinstance.isdeleted_::boolean,
		mapinstance.islatestversion_::boolean,
		mapinstance.baseversionid_::int4,
		mapinstance.contextuserid_::varchar,
		mapinstance.isvisible_::boolean,
		mapinstance.isvalid_::boolean,
		mapinstance.snapshotid_::int4,
		mapinstance.t_::varchar,
		mapinstance.createdby_::varchar,
		mapinstance.createddate_::timestamp,
		mapinstance.updatedby_::varchar,
		mapinstance.updateddate_::timestamp,
		mapinstance.fkid_entity,
		CASE WHEN mapinstance.updateddate_ > mapinstance.createddate_ THEN mapinstance.updatedby_ ELSE mapinstance.createdby_ END AS sourcepopulatedby_,
		GREATEST(mapinstance.createddate_, mapinstance.updateddate_) AS sourcepopulateddate_
		,current_setting('myvariables.popdate')::timestamp as populateddate_
	FROM
        madata.mapinstance
		left join madata.custom_lookup l0 on l0.t_ = 'MapModel'  and l0.jsondoc_->>'Id'  = mapinstance.jsondoc_ ->> 'ModelId'
		left join madata.custom_lookup l1 on l1.t_ = 'MapPdModelScale'  and l1.jsondoc_->>'Id'  = mapinstance.jsondoc_ ->> 'Grade'
		left join madata.custom_lookup l2 on l2.t_ = 'OriginationEntity'  and l2.jsondoc_->>'Key' = mapinstance.jsondoc_ ->> 'OriginationOfEntity'
		left join madata.custom_lookup l3 on l3.t_ = 'ReasonLuxembourg'  and l3.jsondoc_->>'Key' = mapinstance.jsondoc_ ->> 'Reason'
		left join madata.custom_lookup l4 on l4.t_ = 'LuxembourgRating2'  and l4.jsondoc_->>'Key' = mapinstance.jsondoc_ ->> 'SpecialGrade'
        left join madata.custom_lookup l5 on l5.t_ = 'LuxembourgRating'  and l5.jsondoc_->>'Key' = mapinstance.jsondoc_ ->> 'SpecialGrade'
        left join madata.custom_lookup l6 on l6.t_ = 'LuxembourgRating3'  and l6.jsondoc_->>'Key' = mapinstance.jsondoc_ ->> 'SpecialGrade'
	WHERE	GREATEST(mapinstance.updateddate_, mapinstance.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) 		
	from olapts.refreshhistory where tablename = 'ABLUXEMBOURGRATINGS')
	AND GREATEST(mapinstance.updateddate_, mapinstance.createddate_)::timestamp <= max_refreshhistory 
	AND mapinstance.t_ IN (
					  'PdModelNonSystemic',
					  'PdModelShipping',
					  'PdModelShippingOverride',
					  'PdModelMidCorpLimitedData',
					  'PdModelMidCorpLimitedOver',
					  'PdModelMidCorporateFull',
					  'PdModelMidCorpFullOverride',
					  'PdModeProjFinSlotAprOver',
					  'PdModeProjFinSlotAproach',
					  'PdModelRealEstateSlotting',
					  'PdModelRealEstateOverride',
					  'PdModelLargeCorp', 
					  'PdModelLargeCorpOverride');

raise notice '% - Step abluxembourgratings - part a end', clock_timestamp();
--------------------------------------------
ELSE
raise notice '% Step abluxembourgratings - part b start', clock_timestamp();
	DROP TABLE IF EXISTS olapts.abluxembourgratings;
	CREATE TABLE olapts.abluxembourgratings AS
	SELECT
		mapinstance.id_ AS factmapinstanceid_,
		mapinstance.pkid_::varchar as pkid_,
		(mapinstance.jsondoc_ ->> 'EntityId')::numeric AS entityid,	
		(mapinstance.jsondoc_ ->> 'ModelId') AS modelid,
		(l0.jsondoc_ ->> 'Name') AS modelidval,
		(mapinstance.jsondoc_ ->> 'RmodelVersion') AS rmodelversion,
		(mapinstance.jsondoc_ ->> 'EntityVersionId')::numeric AS entityversionid,
		(mapinstance.jsondoc_->> 'Grade') AS grade,
		(l1.jsondoc_->> 'Grade') AS gradeval,
		(mapinstance.jsondoc_ ->> 'Score')::numeric AS score,
		(mapinstance.jsondoc_ ->> 'Pd')::numeric AS pd,
		(mapinstance.jsondoc_->> 'OriginationOfEntity') AS originationofentity,
		(l2.jsondoc_->> 'Value') AS originationofentityval,
		(mapinstance.jsondoc_->> 'Reason') AS reason,
		(l3.jsondoc_->> 'Value') AS reasonval,
		(mapinstance.jsondoc_ ->> 'Remarks') AS Remarks,
		case when mapinstance.t_ IN ('PdModelMidCorpLimitedData','PdModelMidCorporateFull','PdModelLargeCorp') then (l4.jsondoc_->> 'Value') 
             when mapinstance.t_ IN ('PdModelNonSystemic','PdModelMidCorpLimitedOver', 'PdModelMidCorpFullOverride','PdModelLargeCorpOverride') then (l5.jsondoc_->> 'Value') 
             when mapinstance.t_ IN ('PdModelShipping', 'PdModelShippingOverride', 'PdModeProjFinSlotAprOver', 'PdModeProjFinSlotAproach','PdModelRealEstateSlotting','PdModelRealEstateOverride') then (l6.jsondoc_->> 'Value')  
        end as SpecialGrade,
		mapinstance.wfid_::varchar,
		mapinstance.taskid_::varchar,
		mapinstance.versionid_::int4,
		mapinstance.isdeleted_::boolean,
		mapinstance.islatestversion_::boolean,
		mapinstance.baseversionid_::int4,
		mapinstance.contextuserid_::varchar,
		mapinstance.isvisible_::boolean,
		mapinstance.isvalid_::boolean,
		mapinstance.snapshotid_::int4,
		mapinstance.t_::varchar,
		mapinstance.createdby_::varchar,
		mapinstance.createddate_::timestamp,
		mapinstance.updatedby_::varchar,
		mapinstance.updateddate_::timestamp,
		mapinstance.fkid_entity,
		CASE WHEN mapinstance.updateddate_ > mapinstance.createddate_ THEN mapinstance.updatedby_ ELSE mapinstance.createdby_ END AS sourcepopulatedby_,
		GREATEST(mapinstance.createddate_, mapinstance.updateddate_) AS sourcepopulateddate_
		,current_setting('myvariables.popdate')::timestamp as populateddate_
	FROM madata.mapinstance
		left join madata.custom_lookup l0 on l0.t_ = 'MapModel'  and l0.jsondoc_->>'Id'  = mapinstance.jsondoc_ ->> 'ModelId'
		left join madata.custom_lookup l1 on l1.t_ = 'MapPdModelScale'  and l1.jsondoc_->>'Id'  = mapinstance.jsondoc_ ->> 'Grade'
		left join madata.custom_lookup l2 on l2.t_ = 'OriginationEntity'  and l2.jsondoc_->>'Key' = mapinstance.jsondoc_ ->> 'OriginationOfEntity'
		left join madata.custom_lookup l3 on l3.t_ = 'ReasonLuxembourg'  and l3.jsondoc_->>'Key' = mapinstance.jsondoc_ ->> 'Reason'
		left join madata.custom_lookup l4 on l4.t_ = 'LuxembourgRating2'  and l4.jsondoc_->>'Key' = mapinstance.jsondoc_ ->> 'SpecialGrade'
        left join madata.custom_lookup l5 on l5.t_ = 'LuxembourgRating'  and l5.jsondoc_->>'Key' = mapinstance.jsondoc_ ->> 'SpecialGrade'
        left join madata.custom_lookup l6 on l6.t_ = 'LuxembourgRating3'  and l6.jsondoc_->>'Key' = mapinstance.jsondoc_ ->> 'SpecialGrade'
	WHERE	
		GREATEST(mapinstance.updateddate_, mapinstance.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABLUXEMBOURGRATINGS')
		AND GREATEST(mapinstance.updateddate_, mapinstance.createddate_)::timestamp <= max_refreshhistory 
		AND mapinstance.t_ IN (
					  'PdModelNonSystemic',
					  'PdModelShipping',
					  'PdModelShippingOverride',
					  'PdModelMidCorpLimitedData',
					  'PdModelMidCorpLimitedOver',
					  'PdModelMidCorporateFull',
					  'PdModelMidCorpFullOverride',
					  'PdModeProjFinSlotAprOver',
					  'PdModeProjFinSlotAproach',
					  'PdModelRealEstateSlotting',
					  'PdModelRealEstateOverride',
					  'PdModelLargeCorp', 
					  'PdModelLargeCorpOverride');
	
raise notice '% - Step abluxembourgratings - part b end', clock_timestamp();

raise notice '% - Step abluxembourgratings_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abluxembourgratings_idx;
DROP INDEX if exists olapts.abluxembourgratings_idx2;
CREATE INDEX IF NOT EXISTS abluxembourgratings_idx ON olapts.abluxembourgratings (factmapinstanceid_);
CREATE INDEX IF NOT EXISTS abluxembourgratings_idx2 ON olapts.abluxembourgratings (pkid_,versionid_);
	
raise notice '% - Step abluxembourgratings_idx - part a end', clock_timestamp();

END IF;

------------------------------------------------------------------------
raise notice '% step abluxembourgratings - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abluxembourgratingsflag;
CREATE TABLE IF NOT EXISTS olapts.abluxembourgratingsflag AS

select
id_,
pkid_,
wfid_ wfid_,
taskid_ taskid_, 
versionid_ versionid_,
isdeleted_ isdeleted_,
islatestversion_ islatestversion_,
baseversionid_ baseversionid_,
contextuserid_ contextuserid_,
isvisible_ isvisible_,
isvalid_ isvalid_,
snapshotid_ snapshotid_,
t_ t_,
createdby_ createdby_,
createddate_ createddate_,
updatedby_ updatedby_,
updateddate_ updateddate_,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_, createddate_) as sourcepopulateddate_
,current_setting('myvariables.popdate')::timestamp as populateddate_
FROM madata.mapinstance
where 
GREATEST(mapinstance.updateddate_, mapinstance.createddate_)::timestamp <= max_refreshhistory 
and 
mapinstance.t_ IN (
		  'PdModelNonSystemic',
		  'PdModelShipping',
		  'PdModelShippingOverride',
		  'PdModelMidCorpLimitedData',
		  'PdModelMidCorpLimitedOver',
		  'PdModelMidCorporateFull',
		  'PdModelMidCorpFullOverride',
		  'PdModeProjFinSlotAprOver',
		  'PdModeProjFinSlotAproach',
		  'PdModelRealEstateSlotting',
		  'PdModelRealEstateOverride',
		  'PdModelLargeCorp', 
		  'PdModelLargeCorpOverride');

----------------------------------------

raise notice '% - Step abluxembourgratingsflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abluxembourgratingsflag_idx;
DROP INDEX if exists olapts.abluxembourgratingsflag_idx2;
CREATE INDEX IF NOT EXISTS abluxembourgratingsflag_idx ON olapts.abluxembourgratingsflag (id_);
CREATE INDEX IF NOT EXISTS abluxembourgratingsflag_idx2 ON olapts.abluxembourgratingsflag (pkid_,versionid_);
ANALYZE olapts.abluxembourgratingsflag ;

raise notice '% - Step abluxembourgratingsflag_idx - part a end', clock_timestamp(); 

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABLUXEMBOURGRATINGS';
delete from olapts.refreshhistory where tablename = 'ABLUXEMBOURGRATINGS';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABLUXEMBOURGRATINGS' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABLUXEMBOURGRATINGSFLAG';
delete from olapts.refreshhistory where tablename = 'ABLUXEMBOURGRATINGSFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABLUXEMBOURGRATINGSFLAG' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abluxembourgratings - part c end', clock_timestamp();

-------------------REF Data--------------------------

--MapModel
raise notice '% - Step abmapmodel - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abmapmodel;
CREATE TABLE olapts.abmapmodel AS
select l.jsondoc_->>'Id' mapmodelkey_,
l.jsondoc_->>'Name' mapmodelvalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'MapModel';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABMAPMODEL';
delete from olapts.refreshhistory where tablename = 'ABMAPMODEL';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABMAPMODEL' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abmapmodel - part a end', clock_timestamp();

--MapPdModelScale
raise notice '% - Step abmappdmodelscale - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abmappdmodelscale;
CREATE TABLE olapts.abmappdmodelscale AS
select l.jsondoc_->>'Id' mappdmodelscalekey_,
l.jsondoc_->>'Grade' mappdmodelscalevalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'MapPdModelScale';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABMAPPDMODELSCALE';
delete from olapts.refreshhistory where tablename = 'ABMAPPDMODELSCALE';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABMAPPDMODELSCALE' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abmappdmodelscale - part a end', clock_timestamp();

--OriginationEntity
raise notice '% - Step aboriginationentity - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.aboriginationentity;
CREATE TABLE olapts.aboriginationentity AS
select l.jsondoc_->>'Key' originationentitykey_,
l.jsondoc_->>'Value' originationentityvalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'OriginationEntity';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABORIGINATIONENTITY';
delete from olapts.refreshhistory where tablename = 'ABORIGINATIONENTITY';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABORIGINATIONENTITY' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step aboriginationentity - part a end', clock_timestamp();

--ReasonLuxembourg
raise notice '% - Step abreasonluxembourg - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abreasonluxembourg;
CREATE TABLE olapts.abreasonluxembourg AS
select l.jsondoc_->>'Key' abreasonluxembourgkey_,
l.jsondoc_->>'Value' abreasonluxembourgvalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'ReasonLuxembourg';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABREASONLUXEMBOURG';
delete from olapts.refreshhistory where tablename = 'ABREASONLUXEMBOURG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABREASONLUXEMBOURG' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abreasonluxembourg - part a end', clock_timestamp();

--LuxembourgRating
raise notice '% - Step abluxembourgrating - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abluxembourgrating;
CREATE TABLE olapts.abluxembourgrating AS
select l.jsondoc_->>'Key' abluxembourgratingkey_,
l.jsondoc_->>'Value' abluxembourgratingvalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'LuxembourgRating';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABLUXEMBOURGRATING';
delete from olapts.refreshhistory where tablename = 'ABLUXEMBOURGRATING';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABLUXEMBOURGRATING' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abluxembourgrating - part a end', clock_timestamp();

--LuxembourgRating2
raise notice '% - Step abluxembourgrating2 - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abluxembourgrating2;
CREATE TABLE olapts.abluxembourgrating2 AS
select l.jsondoc_->>'Key' abluxembourgrating2key_,
l.jsondoc_->>'Value' abluxembourgrating2value,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'LuxembourgRating2';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABLUXEMBOURGRATING2';
delete from olapts.refreshhistory where tablename = 'ABLUXEMBOURGRATING2';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABLUXEMBOURGRATING2' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abluxembourgrating2 - part a end', clock_timestamp();

-------------------------------------------------------------------------
--LuxembourgRating3
raise notice '% - Step abluxembourgrating3 - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abluxembourgrating3;
CREATE TABLE olapts.abluxembourgrating3 AS
select l.jsondoc_->>'Key' abluxembourgrating3key_,
l.jsondoc_->>'Value' abluxembourgrating3value,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'LuxembourgRating3';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABLUXEMBOURGRATING3';
delete from olapts.refreshhistory where tablename = 'ABLUXEMBOURGRATING3';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABLUXEMBOURGRATING3' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abluxembourgrating3 - part a end', clock_timestamp();

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--END $$;

--LondonRatings
--DO $$
--DECLARE
--varprevsuccessdate TIMESTAMP ;
--BEGIN

IF EXISTS (select from olapts.refreshhistory where tablename = 'ABLLONDONRATINGS') THEN
raise notice '% - Step ablondonratings - part a start', clock_timestamp();
insert into olapts.ablondonratings
	SELECT
		mapinstance.id_ AS factmapinstanceid_,
		mapinstance.pkid_::varchar as pkid_,
		(mapinstance.jsondoc_ ->> 'EntityId')::numeric AS entityid,	
		(mapinstance.jsondoc_ ->> 'ModelId') AS modelid,
		(l0.jsondoc_ ->> 'Name') AS modelidval,
		(mapinstance.jsondoc_ ->> 'RmodelVersion') AS rmodelversion, --null
		(mapinstance.jsondoc_ ->> 'EntityVersionId')::numeric AS entityversionid,
		(mapinstance.jsondoc_->> 'Grade') AS grade,
		(l1.jsondoc_->> 'Grade') AS gradeval,
		(mapinstance.jsondoc_ ->> 'Score')::numeric AS score,
		(mapinstance.jsondoc_ ->> 'Pd')::numeric AS pd,
		(mapinstance.jsondoc_->> 'OriginationOfEntity') AS originationofentity,
		(l2.jsondoc_->> 'Value') AS originationofentityval,
		(mapinstance.jsondoc_->> 'Value') AS reason,
		(l3.jsondoc_->> 'Reason') AS reasonval,
		(mapinstance.jsondoc_ ->> 'Remarks') AS Remarks,
		case 
		when mapinstance.t_ IN ('PdModelLargeCorpLondon','PdModelMidCorpFullLondon')
			then (l4.jsondoc_->> 'Value') 
		when  mapinstance.t_ = 'PdModelNonSystemicLondon'
			then (l5.jsondoc_->> 'Value') 
		when  mapinstance.t_ IN ( 'PdModelProjFinLondon', 'PdModelRealEstateLondon', 'PdModelShippingLondon', 'PdModelProjFinOverrideLond')
			then (l6.jsondoc_->> 'Value')  
		end as SpecialGrade,
		mapinstance.wfid_::varchar,
		mapinstance.taskid_::varchar,
		mapinstance.versionid_::int4,
		mapinstance.isdeleted_::boolean,
		mapinstance.islatestversion_::boolean,
		mapinstance.baseversionid_::int4,
		mapinstance.contextuserid_::varchar,
		mapinstance.isvisible_::boolean,
		mapinstance.isvalid_::boolean,
		mapinstance.snapshotid_::int4,
		mapinstance.t_::varchar,
		mapinstance.createdby_::varchar,
		mapinstance.createddate_::timestamp,
		mapinstance.updatedby_::varchar,
		mapinstance.updateddate_::timestamp,
		mapinstance.fkid_entity,
		CASE WHEN mapinstance.updateddate_ > mapinstance.createddate_ THEN mapinstance.updatedby_ ELSE mapinstance.createdby_ END AS sourcepopulatedby_,
		GREATEST(mapinstance.createddate_, mapinstance.updateddate_) AS sourcepopulateddate_
		,current_setting('myvariables.popdate')::timestamp as populateddate_
	FROM madata.mapinstance
		left join madata.custom_lookup l0 on l0.t_ = 'MapModel'  and l0.jsondoc_->>'Id'  = mapinstance.jsondoc_ ->> 'ModelId'
		left join madata.custom_lookup l1 on l1.t_ = 'MapPdModelScale'  and l1.jsondoc_->>'Id'  = mapinstance.jsondoc_ ->> 'Grade'
		left join madata.custom_lookup l2 on l2.t_ = 'OriginationEntity'  and l2.jsondoc_->>'Key' = mapinstance.jsondoc_ ->> 'OriginationOfEntity'
		left join madata.custom_lookup l3 on l3.t_ = 'ReasonLuxembourg'  and l3.jsondoc_->>'Key' = mapinstance.jsondoc_ ->> 'Reason'
		left join madata.custom_lookup l4 on l4.t_ = 'LuxembourgRating2'  and l4.jsondoc_->>'Key' = mapinstance.jsondoc_ ->> 'SpecialGrade'
		left join madata.custom_lookup l5 on l5.t_ = 'LuxembourgRating'  and l5.jsondoc_->>'Key' = mapinstance.jsondoc_ ->> 'SpecialGrade'
		left join madata.custom_lookup l6 on l6.t_ = 'LuxembourgRating3'  and l6.jsondoc_->>'Key' = mapinstance.jsondoc_ ->> 'SpecialGrade'
	WHERE GREATEST(mapinstance.updateddate_, mapinstance.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABLLONDONRATINGS')
		AND GREATEST(mapinstance.updateddate_, mapinstance.createddate_)::timestamp <= max_refreshhistory 
		AND mapinstance.t_ IN (
							'PdModelLargeCorpLondon',
							'PdModelMidCorpFullLondon',
							'PdModelNonSystemicLondon', 
							'PdModelProjFinLondon',
							'PdModelRealEstateLondon',
							'PdModelShippingLondon',
							'PdModelProjFinOverrideLond'		
							);
raise notice '% - Step ablondonratings - part a end', clock_timestamp();
--------------------------------------------

ELSE
raise notice '% Step ablondonratings - part b start', clock_timestamp();
	DROP TABLE IF EXISTS olapts.ablondonratings;
	CREATE TABLE olapts.ablondonratings AS
	SELECT
		mapinstance.id_ AS factmapinstanceid_,
		mapinstance.pkid_::varchar as pkid_,
		(mapinstance.jsondoc_ ->> 'EntityId')::numeric AS entityid,	
		(mapinstance.jsondoc_ ->> 'ModelId') AS modelid,
		(l0.jsondoc_ ->> 'Name') AS modelidval,
		(mapinstance.jsondoc_ ->> 'RmodelVersion') AS rmodelversion,
		(mapinstance.jsondoc_ ->> 'EntityVersionId')::numeric AS entityversionid,
		(mapinstance.jsondoc_->> 'Grade') AS grade,
		(l1.jsondoc_->> 'Grade') AS gradeval,
		(mapinstance.jsondoc_ ->> 'Score')::numeric AS score,
		(mapinstance.jsondoc_ ->> 'Pd')::numeric AS pd,
		(mapinstance.jsondoc_->> 'OriginationOfEntity') AS originationofentity,
		(l2.jsondoc_->> 'Value') AS originationofentityval,
		(mapinstance.jsondoc_->> 'Value') AS reason,
		(l3.jsondoc_->> 'Reason') AS reasonval,
		(mapinstance.jsondoc_ ->> 'Remarks') AS Remarks,
		case 
		when mapinstance.t_ IN ('PdModelLargeCorpLondon','PdModelMidCorpFullLondon')
			then (l4.jsondoc_->> 'Value') 
		when  mapinstance.t_ = 'PdModelNonSystemicLondon'
			then (l5.jsondoc_->> 'Value') 
		when  mapinstance.t_ IN ( 'PdModelProjFinLondon', 'PdModelRealEstateLondon', 'PdModelShippingLondon', 'PdModelProjFinOverrideLond')
			then (l6.jsondoc_->> 'Value')  
		end as SpecialGrade,
		mapinstance.wfid_::varchar,
		mapinstance.taskid_::varchar,
		mapinstance.versionid_::int4,
		mapinstance.isdeleted_::boolean,
		mapinstance.islatestversion_::boolean,
		mapinstance.baseversionid_::int4,
		mapinstance.contextuserid_::varchar,
		mapinstance.isvisible_::boolean,
		mapinstance.isvalid_::boolean,
		mapinstance.snapshotid_::int4,
		mapinstance.t_::varchar,
		mapinstance.createdby_::varchar,
		mapinstance.createddate_::timestamp,
		mapinstance.updatedby_::varchar,
		mapinstance.updateddate_::timestamp,
		mapinstance.fkid_entity,
		CASE WHEN mapinstance.updateddate_ > mapinstance.createddate_ THEN mapinstance.updatedby_ ELSE mapinstance.createdby_ END AS sourcepopulatedby_,
		GREATEST(mapinstance.createddate_, mapinstance.updateddate_) AS sourcepopulateddate_
		,current_setting('myvariables.popdate')::timestamp as populateddate_
	FROM madata.mapinstance
		left join madata.custom_lookup l0 on l0.t_ = 'MapModel'  and l0.jsondoc_->>'Id'  = mapinstance.jsondoc_ ->> 'ModelId'
		left join madata.custom_lookup l1 on l1.t_ = 'MapPdModelScale'  and l1.jsondoc_->>'Id'  = mapinstance.jsondoc_ ->> 'Grade'
		left join madata.custom_lookup l2 on l2.t_ = 'OriginationEntity'  and l2.jsondoc_->>'Key' = mapinstance.jsondoc_ ->> 'OriginationOfEntity'
		left join madata.custom_lookup l3 on l3.t_ = 'ReasonLuxembourg'  and l3.jsondoc_->>'Key' = mapinstance.jsondoc_ ->> 'Reason'
		left join madata.custom_lookup l4 on l4.t_ = 'LuxembourgRating2'  and l4.jsondoc_->>'Key' = mapinstance.jsondoc_ ->> 'SpecialGrade'
		left join madata.custom_lookup l5 on l5.t_ = 'LuxembourgRating'  and l5.jsondoc_->>'Key' = mapinstance.jsondoc_ ->> 'SpecialGrade'
		left join madata.custom_lookup l6 on l6.t_ = 'LuxembourgRating3'  and l6.jsondoc_->>'Key' = mapinstance.jsondoc_ ->> 'SpecialGrade'
	WHERE GREATEST(mapinstance.updateddate_, mapinstance.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABLLONDONRATINGS')
		AND GREATEST(mapinstance.updateddate_, mapinstance.createddate_)::timestamp <= max_refreshhistory
		AND mapinstance.t_ IN (	'PdModelLargeCorpLondon',
							'PdModelMidCorpFullLondon',
							'PdModelNonSystemicLondon', 
							'PdModelProjFinLondon',
							'PdModelRealEstateLondon',
							'PdModelShippingLondon',
							'PdModelProjFinOverrideLond'		
							);
	
raise notice '% - Step ablondonratings - part b end', clock_timestamp();

raise notice '% - Step ablondonratings_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.ablondonratings_idx;
DROP INDEX if exists olapts.ablondonratings_idx2;
CREATE INDEX IF NOT EXISTS ablondonratings_idx ON olapts.ablondonratings (factmapinstanceid_);
CREATE INDEX IF NOT EXISTS ablondonratings_idx2 ON olapts.ablondonratings (pkid_,versionid_);
	
raise notice '% - Step ablondonratings_idx - part a end', clock_timestamp();

END IF;

------------------------------------------------------------------------
raise notice '% step ablondonratings - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.ablondonratingsflag;
CREATE TABLE  olapts.ablondonratingsflag AS

select
id_,
pkid_,
wfid_ wfid_,
taskid_ taskid_, 
versionid_ versionid_,
isdeleted_ isdeleted_,
islatestversion_ islatestversion_,
baseversionid_ baseversionid_,
contextuserid_ contextuserid_,
isvisible_ isvisible_,
isvalid_ isvalid_,
snapshotid_ snapshotid_,
t_ t_,
createdby_ createdby_,
createddate_ createddate_,
updatedby_ updatedby_,
updateddate_ updateddate_,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_, createddate_) as sourcepopulateddate_
,current_setting('myvariables.popdate')::timestamp as populateddate_
FROM madata.mapinstance
where 
GREATEST(mapinstance.updateddate_, mapinstance.createddate_)::timestamp <= max_refreshhistory 
and mapinstance.t_ IN (
				'PdModelLargeCorpLondon',
				'PdModelMidCorpFullLondon',
				'PdModelNonSystemicLondon', 
				'PdModelProjFinLondon',
				'PdModelRealEstateLondon',
				'PdModelShippingLondon',
				'PdModelProjFinOverrideLond'		
				);

----------------------------------------

raise notice '% - Step ablondonratingsflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.ablondonratingsflag_idx;
DROP INDEX if exists olapts.ablondonratingsflag_idx2;
CREATE INDEX IF NOT EXISTS ablondonratingsflag_idx ON olapts.ablondonratingsflag (id_);
CREATE INDEX IF NOT EXISTS ablondonratingsflag_idx2 ON olapts.ablondonratingsflag (pkid_,versionid_);
ANALYZE olapts.ablondonratingsflag ;

raise notice '% - Step ablondonratingsflag_idx - part a end', clock_timestamp(); 

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABLONDONRATINGS';
delete from olapts.refreshhistory where tablename = 'ABLONDONRATINGS';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABLONDONRATINGS' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABLONDONRATINGSFLAG';
delete from olapts.refreshhistory where tablename = 'ABLONDONRATINGSFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABLONDONRATINGSFLAG' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step ablondonratings - part c end', clock_timestamp();
----------------------------------------------------------------------------------

--LuxembourgRating2
raise notice '% - Step abluxembourgrating2 - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abluxembourgrating2;
CREATE TABLE olapts.abluxembourgrating2 AS
select l.jsondoc_->>'Key' abluxembourgrating2key_,
l.jsondoc_->>'Value' abluxembourgrating2value,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'LuxembourgRating2';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABLUXEMBOURGRATING2';
delete from olapts.refreshhistory where tablename = 'ABLUXEMBOURGRATING2';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABLUXEMBOURGRATING2' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abluxembourgrating2 - part a end', clock_timestamp();

-------------------------------------------------------------------------
--LuxembourgRating3
raise notice '% - Step abluxembourgrating3 - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abluxembourgrating3;
CREATE TABLE olapts.abluxembourgrating3 AS
select l.jsondoc_->>'Key' abluxembourgrating3key_,
l.jsondoc_->>'Value' abluxembourgrating3value,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'LuxembourgRating3';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABLUXEMBOURGRATING3';
delete from olapts.refreshhistory where tablename = 'ABLUXEMBOURGRATING3';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABLUXEMBOURGRATING3' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abluxembourgrating3 - part a end', clock_timestamp();

--END $$;

---- TeiresiasData ----

--DO $$
--DECLARE
--varprevsuccessdate TIMESTAMP ;
--BEGIN

-- If table exists in refresh history --
IF EXISTS (select from olapts.refreshhistory where tablename = 'ABTEIRESIASDATA') THEN
	raise notice '% - Step abteiresiasdata - part a start', clock_timestamp();
	insert into olapts.abteiresiasdata
		SELECT 
		teiresiasdata.id_ AS id_,
		teiresiasdata.pkid_::varchar as pkid_,	
		(teiresiasdata.jsondoc_ ->>'TeiresiasDataId')::numeric as TeiresiasDataId_,--null
		(teiresiasdata.jsondoc_ ->>'Active') as Active,
		(teiresiasdata.jsondoc_ ->>'DateUploaded')::timestamp as DateUploaded,
		(teiresiasdata.jsondoc_ ->>'EntityId')::numeric as EntityId,
		(teiresiasdata.jsondoc_ ->>'MasterId') as MasterId, --guid
		(teiresiasdata.jsondoc_ ->>'PaidDelinqYc1')::numeric as PaidDelinqYc1,
		(teiresiasdata.jsondoc_ ->>'PaidDelinqYc2')::numeric as PaidDelinqYc2,
		(teiresiasdata.jsondoc_ ->>'PaidDelinqYc3')::numeric as PaidDelinqYc13,
		(teiresiasdata.jsondoc_ ->>'PaidDelinqYc4')::numeric as PaidDelinqYc4,
		(teiresiasdata.jsondoc_ ->>'PaidDelinqYc5')::numeric as PaidDelinqYc5,
		(teiresiasdata.jsondoc_ ->>'Segment') as Segment,
		(teiresiasdata.jsondoc_ ->>'TaxId') as TaxId,
		(teiresiasdata.jsondoc_ ->>'TbScore') as TbScore,
		--manipulate date as latin/not latin/text dates are included at the same column
		(case when (teiresiasdata.jsondoc_ ->>'TbScoreDate') like '%πμ' 
		then to_char(to_date(replace(replace((teiresiasdata.jsondoc_ ->>'TbScoreDate'),'πμ','AM'),'μμ','PM'),'dd/mm/yyyy'), 'yyyy-mm-dd')
		 when (strpos((teiresiasdata.jsondoc_ ->>'TbScoreDate'),'μμ') <> 0 or strpos((teiresiasdata.jsondoc_ ->>'TbScoreDate'),'πμ') <> 0)
			then to_char(to_date(replace(replace((teiresiasdata.jsondoc_ ->>'TbScoreDate'),'πμ','AM'),'μμ','PM'),'dd/mm/yyyy'), 'yyyy-mm-dd')
		 when (strpos((teiresiasdata.jsondoc_ ->>'TbScoreDate'),'μμ') = 0 or strpos((teiresiasdata.jsondoc_ ->>'TbScoreDate'),'πμ') = 0)
			and (strpos((teiresiasdata.jsondoc_ ->>'TbScoreDate'),'AM') = 0 and strpos((teiresiasdata.jsondoc_ ->>'TbScoreDate'),'PM') = 0)
			then to_char(to_date(replace(replace((teiresiasdata.jsondoc_ ->>'TbScoreDate'),'πμ','AM'),'μμ','PM'),'dd/mm/yyyy'), 'yyyy-mm-dd')
			 else 
			 to_char(to_date(replace(replace((teiresiasdata.jsondoc_ ->>'TbScoreDate'),'πμ','AM'),'μμ','PM'),'mm/dd/yyyy'), 'yyyy-mm-dd')
		end)::timestamp as TbScoreDate,
		(teiresiasdata.jsondoc_ ->>'TotalDelinqYb1')::numeric as TotalDelinqYb1,
		(teiresiasdata.jsondoc_ ->>'TotalDelinqYb2')::numeric as TotalDelinqYb2,
		(teiresiasdata.jsondoc_ ->>'TotalDelinqYb3')::numeric as TotalDelinqYb3,
		(teiresiasdata.jsondoc_ ->>'TotalDelinqYb4')::numeric as TotalDelinqYb4,
		(teiresiasdata.jsondoc_ ->>'TotalDelinqYb5')::numeric as TotalDelinqYb5,
		(teiresiasdata.jsondoc_ ->>'TotalDelinqYc1')::numeric as TotalDelinqYc1,
		(teiresiasdata.jsondoc_ ->>'TotalDelinqYc2')::numeric as TotalDelinqYc2,
		(teiresiasdata.jsondoc_ ->>'TotalDelinqYc3')::numeric as TotalDelinqYc3,
		(teiresiasdata.jsondoc_ ->>'TotalDelinqYc4')::numeric as TotalDelinqYc4,
		(teiresiasdata.jsondoc_ ->>'TotalDelinqYc5')::numeric as TotalDelinqYc5,
		(teiresiasdata.jsondoc_->>'UserUploaded') as UserUploaded,		
		(teiresiasdata.jsondoc_ ->>'Year1')::int as Year1,
		(teiresiasdata.jsondoc_ ->>'Year2')::int as Year2,
		(teiresiasdata.jsondoc_ ->>'Year3')::int as Year3,
		(teiresiasdata.jsondoc_ ->>'Year4')::int as Year4,
		(teiresiasdata.jsondoc_ ->>'Year5')::int as Year5,
		(teiresiasdata.wfid_)::varchar,
        (teiresiasdata.taskid_)::varchar,
        (teiresiasdata.versionid_)::int4,
        (teiresiasdata.isdeleted_)::boolean,
        (teiresiasdata.islatestversion_)::boolean,
        (teiresiasdata.baseversionid_)::int4,
        (teiresiasdata.contextuserid_)::varchar,
        (teiresiasdata.isvisible_)::boolean,
        (teiresiasdata.isvalid_)::boolean,
        (teiresiasdata.snapshotid_)::int4,
        (teiresiasdata.t_)::varchar,
        (teiresiasdata.createdby_)::varchar,
        (teiresiasdata.createddate_)::timestamp,
        (teiresiasdata.updatedby_)::varchar,
        (teiresiasdata.updateddate_)::timestamp,
        (teiresiasdata.fkid_entity),
		CASE WHEN teiresiasdata.updateddate_ > teiresiasdata.createddate_ THEN teiresiasdata.updatedby_ ELSE teiresiasdata.createdby_ END AS sourcepopulatedby_
		,GREATEST(teiresiasdata.createddate_, teiresiasdata.updateddate_) AS sourcepopulateddate_
		,current_setting('myvariables.popdate')::timestamp as populateddate_
		FROM madata.teiresiasdata
		WHERE 
			GREATEST(teiresiasdata.updateddate_, teiresiasdata.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABTEIRESIASDATA')
			AND GREATEST(teiresiasdata.updateddate_, teiresiasdata.createddate_)::timestamp <= max_refreshhistory 	
			AND teiresiasdata.t_ = 'TeiresiasData';
	raise notice '% - Step abteiresiasdata - part a end', clock_timestamp();
ELSE
	raise notice '% Step teiresiasdata - part b start', clock_timestamp();
	DROP TABLE IF EXISTS olapts.abteiresiasdata;
	CREATE TABLE olapts.abteiresiasdata AS
		SELECT 
		teiresiasdata.id_ AS id_,
		teiresiasdata.pkid_::varchar as pkid_,	
		(teiresiasdata.jsondoc_ ->>'TeiresiasDataId')::numeric as TeiresiasDataId_,--null
		(teiresiasdata.jsondoc_ ->>'Active') as Active,
		(teiresiasdata.jsondoc_ ->>'DateUploaded')::timestamp as DateUploaded,
		(teiresiasdata.jsondoc_ ->>'EntityId')::numeric as EntityId,
		(teiresiasdata.jsondoc_ ->>'MasterId') as MasterId, --guid
		(teiresiasdata.jsondoc_ ->>'PaidDelinqYc1')::numeric as PaidDelinqYc1,
		(teiresiasdata.jsondoc_ ->>'PaidDelinqYc2')::numeric as PaidDelinqYc2,
		(teiresiasdata.jsondoc_ ->>'PaidDelinqYc3')::numeric as PaidDelinqYc13,
		(teiresiasdata.jsondoc_ ->>'PaidDelinqYc4')::numeric as PaidDelinqYc4,
		(teiresiasdata.jsondoc_ ->>'PaidDelinqYc5')::numeric as PaidDelinqYc5,
		(teiresiasdata.jsondoc_ ->>'Segment') as Segment,
		(teiresiasdata.jsondoc_ ->>'TaxId') as TaxId,
		(teiresiasdata.jsondoc_ ->>'TbScore') as TbScore,
		--manipulate date as latin/not latin/text dates are included at the same column
		(case when (teiresiasdata.jsondoc_ ->>'TbScoreDate') like '%πμ' 
		then to_char(to_date(replace(replace((teiresiasdata.jsondoc_ ->>'TbScoreDate'),'πμ','AM'),'μμ','PM'),'dd/mm/yyyy'), 'yyyy-mm-dd')
		 when (strpos((teiresiasdata.jsondoc_ ->>'TbScoreDate'),'μμ') <> 0 or strpos((teiresiasdata.jsondoc_ ->>'TbScoreDate'),'πμ') <> 0)
			then to_char(to_date(replace(replace((teiresiasdata.jsondoc_ ->>'TbScoreDate'),'πμ','AM'),'μμ','PM'),'dd/mm/yyyy'), 'yyyy-mm-dd')
		 when (strpos((teiresiasdata.jsondoc_ ->>'TbScoreDate'),'μμ') = 0 or strpos((teiresiasdata.jsondoc_ ->>'TbScoreDate'),'πμ') = 0)
			and (strpos((teiresiasdata.jsondoc_ ->>'TbScoreDate'),'AM') = 0 and strpos((teiresiasdata.jsondoc_ ->>'TbScoreDate'),'PM') = 0)
			then to_char(to_date(replace(replace((teiresiasdata.jsondoc_ ->>'TbScoreDate'),'πμ','AM'),'μμ','PM'),'dd/mm/yyyy'), 'yyyy-mm-dd')
			 else 
			 to_char(to_date(replace(replace((teiresiasdata.jsondoc_ ->>'TbScoreDate'),'πμ','AM'),'μμ','PM'),'mm/dd/yyyy'), 'yyyy-mm-dd')
		end)::timestamp as TbScoreDate,						
		(teiresiasdata.jsondoc_ ->>'TotalDelinqYb1')::numeric as TotalDelinqYb1,
		(teiresiasdata.jsondoc_ ->>'TotalDelinqYb2')::numeric as TotalDelinqYb2,
		(teiresiasdata.jsondoc_ ->>'TotalDelinqYb3')::numeric as TotalDelinqYb3,
		(teiresiasdata.jsondoc_ ->>'TotalDelinqYb4')::numeric as TotalDelinqYb4,
		(teiresiasdata.jsondoc_ ->>'TotalDelinqYb5')::numeric as TotalDelinqYb5,
		(teiresiasdata.jsondoc_ ->>'TotalDelinqYc1')::numeric as TotalDelinqYc1,
		(teiresiasdata.jsondoc_ ->>'TotalDelinqYc2')::numeric as TotalDelinqYc2,
		(teiresiasdata.jsondoc_ ->>'TotalDelinqYc3')::numeric as TotalDelinqYc3,
		(teiresiasdata.jsondoc_ ->>'TotalDelinqYc4')::numeric as TotalDelinqYc4,
		(teiresiasdata.jsondoc_ ->>'TotalDelinqYc5')::numeric as TotalDelinqYc5,
		(teiresiasdata.jsondoc_->>'UserUploaded') as UserUploaded,		
		(teiresiasdata.jsondoc_ ->>'Year1')::int as Year1,
		(teiresiasdata.jsondoc_ ->>'Year2')::int as Year2,
		(teiresiasdata.jsondoc_ ->>'Year3')::int as Year3,
		(teiresiasdata.jsondoc_ ->>'Year4')::int as Year4,
		(teiresiasdata.jsondoc_ ->>'Year5')::int as Year5,
		(teiresiasdata.wfid_)::varchar,
        (teiresiasdata.taskid_)::varchar,
        (teiresiasdata.versionid_)::int4,
        (teiresiasdata.isdeleted_)::boolean,
        (teiresiasdata.islatestversion_)::boolean,
        (teiresiasdata.baseversionid_)::int4,
        (teiresiasdata.contextuserid_)::varchar,
        (teiresiasdata.isvisible_)::boolean,
        (teiresiasdata.isvalid_)::boolean,
        (teiresiasdata.snapshotid_)::int4,
        (teiresiasdata.t_)::varchar,
        (teiresiasdata.createdby_)::varchar,
        (teiresiasdata.createddate_)::timestamp,
        (teiresiasdata.updatedby_)::varchar,
        (teiresiasdata.updateddate_)::timestamp,
        (teiresiasdata.fkid_entity),
		CASE WHEN teiresiasdata.updateddate_ > teiresiasdata.createddate_ THEN teiresiasdata.updatedby_ ELSE teiresiasdata.createdby_ END AS sourcepopulatedby_
		,GREATEST(teiresiasdata.createddate_, teiresiasdata.updateddate_) AS sourcepopulateddate_
		,current_setting('myvariables.popdate')::timestamp as populateddate_
		FROM madata.teiresiasdata
		WHERE 
			GREATEST(teiresiasdata.updateddate_, teiresiasdata.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABTEIRESIASDATA')
			AND GREATEST(teiresiasdata.updateddate_, teiresiasdata.createddate_)::timestamp <= max_refreshhistory 	
			AND teiresiasdata.t_ = 'TeiresiasData';
raise notice '% - Step abteiresiasdata - part b end', clock_timestamp();

--abteiresiasdata
raise notice '% - Step abteiresiasdata_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abteiresiasdata_idx;
DROP INDEX if exists olapts.abteiresiasdata_idx2;
CREATE INDEX IF NOT EXISTS abteiresiasdata_idx ON olapts.abteiresiasdata (id_);
CREATE INDEX IF NOT EXISTS abteiresiasdata_idx2 ON olapts.abteiresiasdata (pkid_,versionid_);
	
raise notice '% - Step abteiresiasdata_idx - part a end', clock_timestamp(); 
END IF;

raise notice '% step abteiresiasdata - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abteiresiasdataflag;
CREATE TABLE IF NOT EXISTS olapts.abteiresiasdataflag AS

select
id_,
pkid_,
wfid_ wfid_,
taskid_ taskid_, 
versionid_ versionid_,
isdeleted_ isdeleted_,
islatestversion_ islatestversion_,
baseversionid_ baseversionid_,
contextuserid_ contextuserid_,
isvisible_ isvisible_,
isvalid_ isvalid_,
snapshotid_ snapshotid_,
t_ t_,
createdby_ createdby_,
createddate_ createddate_,
updatedby_ updatedby_,
updateddate_ updateddate_,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_, createddate_) as sourcepopulateddate_
,current_setting('myvariables.popdate')::timestamp as populateddate_
FROM madata.teiresiasdata
where 
 GREATEST(teiresiasdata.updateddate_, teiresiasdata.createddate_)::timestamp <= max_refreshhistory 
and 
teiresiasdata.t_ = 'TeiresiasData'
;

raise notice '% - Step abteiresiasdataflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abteiresiasdataflag_idx;
DROP INDEX if exists olapts.abteiresiasdataflag_idx2;
CREATE INDEX IF NOT EXISTS abteiresiasdataflag_idx ON olapts.abteiresiasdataflag (id_);
CREATE INDEX IF NOT EXISTS abteiresiasdataflag_idx2 ON olapts.abteiresiasdataflag (pkid_,versionid_);
ANALYZE olapts.abteiresiasdataflag ;

raise notice '% - Step abteiresiasdataflag_idx - part a end', clock_timestamp(); 

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABTEIRESIASDATA';
delete from olapts.refreshhistory where tablename = 'ABTEIRESIASDATA';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABTEIRESIASDATA' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABTEIRESIASDATAFLAG';
delete from olapts.refreshhistory where tablename = 'ABTEIRESIASDATAFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABTEIRESIASDATAFLAG' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abteiresiasdata - part c end', clock_timestamp();
--END $$;

----------------------------------------------utp----------------------------------

--DO $$
--DECLARE
--varprevsuccessdate TIMESTAMP ;
--BEGIN

IF EXISTS (select from olapts.refreshhistory where tablename = 'ABUTP') THEN
raise notice '% - Step abutp - part a start', clock_timestamp();
insert into olapts.abutp
	select   utp.id_ AS id_,
		 utp.pkid_::varchar as pkid_,
		(utp.jsondoc_ ->>'UtpId')::numeric AS UtpId,
		(utp.jsondoc_ ->>'Active') AS Active,
		(utp.jsondoc_ ->>'AdverseId')::boolean AS AdverseId,
		(utp.jsondoc_ ->>'ArrearsId')::boolean AS ArrearsId,
		(utp.jsondoc_ ->>'AssessmentDate')::timestamp AS AssessmentDate,
		(utp.jsondoc_ ->>'BankId')::boolean AS BankId,
		(utp.jsondoc_ ->>'BorrowerExposuresId')::boolean AS BorrowerExposuresId,
		(utp.jsondoc_ ->>'BorrowerId')::boolean AS BorrowerId,
		(utp.jsondoc_ ->>'BorrowersId')::boolean AS BorrowersId,
		(utp.jsondoc_ ->>'BorrowerIncomeId')::boolean AS BorrowerIncomeId,	
		(utp.jsondoc_ ->>'BreachId')::boolean AS BreachId,
		(utp.jsondoc_ ->>'CdsId')::boolean AS CdsId,
		(utp.jsondoc_ ->>'ConnectedId')::boolean AS ConnectedId,
		(utp.jsondoc_ ->>'CreditCommittee') AS CreditCommittee,
		(l1.jsondoc_ ->>'Value') AS CreditCommitteeval,
		(utp.jsondoc_ ->>'CreditInstitutionId')::boolean AS CreditInstitutionId,
		(utp.jsondoc_ ->> 'DefaultDesignator')::boolean AS DefaultDesignator,
		(utp.jsondoc_ ->> 'DelayedId')::boolean AS DelayedId,
		(utp.jsondoc_ ->> 'DisActiveMarketId')::boolean AS DisActiveMarketId,
		(utp.jsondoc_ ->> 'DisappearanceId')::boolean AS DisappearanceId,		
		(utp.jsondoc_ ->> 'EbaId')::boolean AS ebaid,	--added 11/06/2024	
		(utp.jsondoc_ ->> 'EbitdaId')::boolean AS EbitdaId,
		(utp.jsondoc_ ->> 'EntityId')::numeric AS EntityId,
		(utp.jsondoc_ ->> 'EntityVersionId')::numeric AS EntityVersionId,
		(utp.jsondoc_ ->> 'ExpectationId')::boolean AS ExpectationId,
		(utp.jsondoc_ ->> 'FinancialId')::boolean AS FinancialId,
		(utp.jsondoc_ ->> 'FplMonthId')::boolean AS FplMonthId,
		(utp.jsondoc_ ->> 'FplmeasuresId')::boolean AS FplmeasuresId,
		(utp.jsondoc_ ->> 'FraudId')::boolean AS FraudId,
		(utp.jsondoc_ ->> 'IsdaId')::boolean AS IsdaId,
		(utp.jsondoc_ ->> 'LawsuitId')::boolean AS LawsuitId,
		(utp.jsondoc_ ->> 'LicenseId')::boolean AS LicenseId,
		(utp.jsondoc_ ->> 'LoanAccelaratedId')::boolean AS LoanAccelaratedId,
		(utp.jsondoc_ ->> 'LoanId')::boolean AS LoanId,
		(utp.jsondoc_ ->> 'LossId')::boolean AS LossId,
		(utp.jsondoc_ ->> 'MaterialId')::boolean AS MaterialId,
		(utp.jsondoc_ ->> 'ModificationsId')::boolean AS ModificationsId,
		(utp.jsondoc_ ->> 'MultipleId')::boolean AS MultipleId,
		(utp.jsondoc_ ->> 'Negativeid')::boolean AS Negativeid,
		(utp.jsondoc_ ->> 'NpvBiggerId')::boolean AS NpvBiggerId,		
		(utp.jsondoc_ ->> 'NpvId')::boolean AS NpvId, 	
		(utp.jsondoc_ ->> 'ObligorId')::boolean AS ObligorId,
		(utp.jsondoc_ ->> 'Origination') AS Origination,
		(utp.jsondoc_ ->> 'OutOfCourtId')::boolean AS OutOfCourtId,
		(utp.jsondoc_ ->> 'PostponementsId')::boolean AS PostponementsId,
		(utp.jsondoc_ ->> 'ProvisionId')::boolean AS  ProvisionId,
		(utp.jsondoc_ ->> 'ReductionId')::boolean AS ReductionId, 	
		(utp.jsondoc_ ->> 'RestrictedId')::boolean AS RestrictedId,
		(utp.jsondoc_ ->> 'RestructuringId')::boolean AS RestructuringId,
		(utp.jsondoc_ ->> 'SaleId')::boolean AS SaleId,
		(utp.jsondoc_ ->> 'ThirdpartyId')::boolean AS ThirdpartyId,
		(utp.jsondoc_ ->> 'UtpAssessmentDate')::timestamp AS UtpAssessmentDate,
		(utp.jsondoc_ ->> 'UtpAssessmentUser') AS UtpAssessmentUser,
		(utp.jsondoc_ ->> 'UtpAuthorizationDate')::date AS UtpAuthorizationDate,
		(utp.jsondoc_ ->> 'UtpAuthorizationSysDate')::timestamp AS UtpAuthorizationSysDate,
		(utp.jsondoc_ ->> 'UtpAuthorizedUser') AS UtpAuthorizedUser,
		(utp.jsondoc_ ->> 'UtpBoolHidden')::boolean AS UtpBoolHidden,
		(utp.jsondoc_ ->> 'UtpCatchTriggers')::integer AS UtpCatchTriggers,
		(utp.jsondoc_ ->> 'UtpComments') AS UtpComments,
		(utp.jsondoc_ ->>'UtpObligor') AS UtpObligor,
		(l2.jsondoc_ ->>'Value') AS UtpObligorval,
		(utp.jsondoc_ ->> 'WriteoffId')::boolean AS WriteoffId,
		utp.wfid_::varchar,
		utp.taskid_::varchar,
		utp.versionid_::int4,
		utp.isdeleted_::boolean,
		utp.islatestversion_::boolean,
		utp.baseversionid_::int4,
		utp.contextuserid_::varchar,
		utp.isvisible_::boolean,
		utp.isvalid_::boolean,
		utp.snapshotid_::int4,
		utp.t_::varchar,
		utp.createdby_::varchar,
		utp.createddate_::timestamp,
		utp.updatedby_::varchar,
		utp.updateddate_::timestamp,
		utp.fkid_entity,
		CASE WHEN utp.updateddate_ > utp.createddate_ THEN utp.updatedby_ ELSE utp.createdby_ END AS sourcepopulatedby_,
		GREATEST(utp.createddate_, utp.updateddate_) AS sourcepopulateddate_
		,current_setting('myvariables.popdate')::timestamp as populateddate_
	FROM madata.utp
		left join madata.custom_lookup l1 on l1.t_ = 'UtpCreditCommittee'  and l1.jsondoc_->>'Key'  = utp.jsondoc_ ->> 'CreditCommittee'
		left join madata.custom_lookup l2 on l2.t_ = 'TrueFalse'  and l2.jsondoc_->>'Key'  = utp.jsondoc_ ->> 'UtpObligor'	
	WHERE
		GREATEST(utp.updateddate_, utp.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABUTP')
		AND GREATEST(utp.updateddate_, utp.createddate_)::timestamp <= max_refreshhistory 
		AND utp.t_ = 'Utp'
	;
raise notice '% - Step abutp - part a end', clock_timestamp();
--------------------------------------------
ELSE
raise notice '% Step abutp - part b start', clock_timestamp();
	DROP TABLE IF EXISTS olapts.abutp;
	CREATE TABLE olapts.abutp AS
	select   utp.id_ AS id_,
		 utp.pkid_::varchar as pkid_,
		(utp.jsondoc_ ->>'UtpId')::numeric AS UtpId,
		(utp.jsondoc_ ->>'Active') AS Active,
		(utp.jsondoc_ ->>'AdverseId')::boolean AS AdverseId,
		(utp.jsondoc_ ->>'ArrearsId')::boolean AS ArrearsId,
		(utp.jsondoc_ ->>'AssessmentDate')::timestamp AS AssessmentDate,
		(utp.jsondoc_ ->>'BankId')::boolean AS BankId,
		(utp.jsondoc_ ->>'BorrowerExposuresId')::boolean AS BorrowerExposuresId,
		(utp.jsondoc_ ->>'BorrowerId')::boolean AS BorrowerId,
		(utp.jsondoc_ ->>'BorrowersId')::boolean AS BorrowersId,
		(utp.jsondoc_ ->>'BorrowerIncomeId')::boolean AS BorrowerIncomeId,	
		(utp.jsondoc_ ->>'BreachId')::boolean AS BreachId,
		(utp.jsondoc_ ->>'CdsId')::boolean AS CdsId,
		(utp.jsondoc_ ->>'ConnectedId')::boolean AS ConnectedId,
		(utp.jsondoc_ ->>'CreditCommittee') AS CreditCommittee,
		(l1.jsondoc_ ->>'Value') AS CreditCommitteeval,
		(utp.jsondoc_ ->>'CreditInstitutionId')::boolean AS CreditInstitutionId,
		(utp.jsondoc_ ->> 'DefaultDesignator')::boolean AS DefaultDesignator,
		(utp.jsondoc_ ->> 'DelayedId')::boolean AS DelayedId,
		(utp.jsondoc_ ->> 'DisActiveMarketId')::boolean AS DisActiveMarketId,
		(utp.jsondoc_ ->> 'DisappearanceId')::boolean AS DisappearanceId,
		(utp.jsondoc_ ->> 'EbaId')::boolean AS ebaid,	--added 11/06/2024		
		(utp.jsondoc_ ->> 'EbitdaId')::boolean AS EbitdaId,
		(utp.jsondoc_ ->> 'EntityId')::numeric AS EntityId,
		(utp.jsondoc_ ->> 'EntityVersionId')::numeric AS EntityVersionId,
		(utp.jsondoc_ ->> 'ExpectationId')::boolean AS ExpectationId,
		(utp.jsondoc_ ->> 'FinancialId')::boolean AS FinancialId,
		(utp.jsondoc_ ->> 'FplMonthId')::boolean AS FplMonthId,
		(utp.jsondoc_ ->> 'FplmeasuresId')::boolean AS FplmeasuresId,
		(utp.jsondoc_ ->> 'FraudId')::boolean AS FraudId,
		(utp.jsondoc_ ->> 'IsdaId')::boolean AS IsdaId,
		(utp.jsondoc_ ->> 'LawsuitId')::boolean AS LawsuitId,
		(utp.jsondoc_ ->> 'LicenseId')::boolean AS LicenseId,
		(utp.jsondoc_ ->> 'LoanAccelaratedId')::boolean AS LoanAccelaratedId,
		(utp.jsondoc_ ->> 'LoanId')::boolean AS LoanId,
		(utp.jsondoc_ ->> 'LossId')::boolean AS LossId,
		(utp.jsondoc_ ->> 'MaterialId')::boolean AS MaterialId,
		(utp.jsondoc_ ->> 'ModificationsId')::boolean AS ModificationsId,
		(utp.jsondoc_ ->> 'MultipleId')::boolean AS MultipleId,
		(utp.jsondoc_ ->> 'Negativeid')::boolean AS Negativeid,
		(utp.jsondoc_ ->> 'NpvBiggerId')::boolean AS NpvBiggerId,		
		(utp.jsondoc_ ->> 'NpvId')::boolean AS NpvId, 	
		(utp.jsondoc_ ->> 'ObligorId')::boolean AS ObligorId,
		(utp.jsondoc_ ->> 'Origination') AS Origination,
		(utp.jsondoc_ ->> 'OutOfCourtId')::boolean AS OutOfCourtId,
		(utp.jsondoc_ ->> 'PostponementsId')::boolean AS PostponementsId,
		(utp.jsondoc_ ->> 'ProvisionId')::boolean AS  ProvisionId,
		(utp.jsondoc_ ->> 'ReductionId')::boolean AS ReductionId, 	
		(utp.jsondoc_ ->> 'RestrictedId')::boolean AS RestrictedId,
		(utp.jsondoc_ ->> 'RestructuringId')::boolean AS RestructuringId,
		(utp.jsondoc_ ->> 'SaleId')::boolean AS SaleId,
		(utp.jsondoc_ ->> 'ThirdpartyId')::boolean AS ThirdpartyId,
		(utp.jsondoc_ ->> 'UtpAssessmentDate')::timestamp AS UtpAssessmentDate,
		(utp.jsondoc_ ->> 'UtpAssessmentUser') AS UtpAssessmentUser,
		(utp.jsondoc_ ->> 'UtpAuthorizationDate')::date AS UtpAuthorizationDate,
		(utp.jsondoc_ ->> 'UtpAuthorizationSysDate')::timestamp AS UtpAuthorizationSysDate,
		(utp.jsondoc_ ->> 'UtpAuthorizedUser') AS UtpAuthorizedUser,
		(utp.jsondoc_ ->> 'UtpBoolHidden')::boolean AS UtpBoolHidden,
		(utp.jsondoc_ ->> 'UtpCatchTriggers')::integer AS UtpCatchTriggers,
		(utp.jsondoc_ ->> 'UtpComments') AS UtpComments,
		(utp.jsondoc_ ->>'UtpObligor') AS UtpObligor,
		(l2.jsondoc_ ->>'Value') AS UtpObligorval,
		(utp.jsondoc_ ->> 'WriteoffId')::boolean AS WriteoffId,
		utp.wfid_::varchar,
		utp.taskid_::varchar,
		utp.versionid_::int4,
		utp.isdeleted_::boolean,
		utp.islatestversion_::boolean,
		utp.baseversionid_::int4,
		utp.contextuserid_::varchar,
		utp.isvisible_::boolean,
		utp.isvalid_::boolean,
		utp.snapshotid_::int4,
		utp.t_::varchar,
		utp.createdby_::varchar,
		utp.createddate_::timestamp,
		utp.updatedby_::varchar,
		utp.updateddate_::timestamp,
		utp.fkid_entity,
		CASE WHEN utp.updateddate_ > utp.createddate_ THEN utp.updatedby_ ELSE utp.createdby_ END AS sourcepopulatedby_,
		GREATEST(utp.createddate_, utp.updateddate_) AS sourcepopulateddate_
		,current_setting('myvariables.popdate')::timestamp as populateddate_
	FROM
		madata.utp
		left join madata.custom_lookup l1 on l1.t_ = 'UtpCreditCommittee'  and l1.jsondoc_->>'Key'  = utp.jsondoc_ ->> 'CreditCommittee'
		left join madata.custom_lookup l2 on l2.t_ = 'TrueFalse'  and l2.jsondoc_->>'Key'  = utp.jsondoc_ ->> 'UtpObligor'	
	WHERE	
		GREATEST(utp.updateddate_, utp.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABUTP')
		AND GREATEST(utp.updateddate_, utp.createddate_)::timestamp <= max_refreshhistory
		AND utp.t_ = 'Utp';	
raise notice '% - Step abutp - part b end', clock_timestamp();

--abutp

raise notice '% - Step abutp_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abutp_idx;
DROP INDEX if exists olapts.abutp_idx2;
CREATE INDEX IF NOT EXISTS abutp_idx ON olapts.abutp (id_);
CREATE INDEX IF NOT EXISTS abutp_idx2 ON olapts.abutp (pkid_,versionid_);

raise notice '% - Step abutp_idx - part a end', clock_timestamp(); 
END IF;

------------------------------------------------------------------------
raise notice '% step abutp - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abutpflag;
CREATE TABLE  olapts.abutpflag AS

select
id_,
pkid_,
wfid_ wfid_,
taskid_ taskid_, 
versionid_ versionid_,
isdeleted_ isdeleted_,
islatestversion_ islatestversion_,
baseversionid_ baseversionid_,
contextuserid_ contextuserid_,
isvisible_ isvisible_,
isvalid_ isvalid_,
snapshotid_ snapshotid_,
t_ t_,
createdby_ createdby_,
createddate_ createddate_,
updatedby_ updatedby_,
updateddate_ updateddate_,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_, createddate_) as sourcepopulateddate_
,current_setting('myvariables.popdate')::timestamp as populateddate_
FROM madata.utp
where 
GREATEST(utp.updateddate_, utp.createddate_)::timestamp <= max_refreshhistory 
and utp.t_ = 'Utp'
;

raise notice '% - Step abutpflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abutpflag_idx;
DROP INDEX if exists olapts.abutpflag_idx2;
CREATE INDEX IF NOT EXISTS abutpflag_idx ON olapts.abutpflag (id_);
CREATE INDEX IF NOT EXISTS abutpflag_idx2 ON olapts.abutpflag (pkid_,versionid_);
ANALYZE olapts.abutpflag ;

raise notice '% - Step abutpflag_idx - part a end', clock_timestamp(); 

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABUTP';
delete from olapts.refreshhistory where tablename = 'ABUTP';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABUTP' tablename,max_refreshhistory as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABUTPFLAG';
delete from olapts.refreshhistory where tablename = 'ABUTPFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABUTPFLAG' tablename,max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abutp - part c end', clock_timestamp();

----------------------------------------

-------------------REF Data--------------------------

--UtpCreditCommittee
raise notice '% - Step abutpcreditcommittee - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abutpcreditcommittee;
CREATE TABLE olapts.abutpcreditcommittee AS
select l.jsondoc_->>'Key' utpcreditcommitteekey_,
l.jsondoc_->>'Value' utpcreditcommitteevalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'UtpCreditCommittee';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABUTPCREDITCOMMITTEE';
delete from olapts.refreshhistory where tablename = 'ABUTPCREDITCOMMITTEE';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABUTPCREDITCOMMITTEE' tablename,max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abutpcreditcommittee - part a end', clock_timestamp();

--TrueFalse
raise notice '% - Step abtruefalse - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abtruefalse;
CREATE TABLE olapts.abtruefalse AS
select l.jsondoc_->>'Key' truefalsekey_,
l.jsondoc_->>'Value' truefalsevalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'TrueFalse';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABTRUEFALSE';
delete from olapts.refreshhistory where tablename = 'ABTRUEFALSE';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABTRUEFALSE' tablename,max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abtruefalse - part a end', clock_timestamp();

--END $$;

----------------------------------------------SpecialDelta----------------------------------
--DO $$
--DECLARE
--varprevsuccessdate TIMESTAMP ;
--BEGIN

IF EXISTS (select from olapts.refreshhistory where tablename = 'ABSPECIALDELTA') THEN
raise notice '% - Step abspecialdelta - part a start', clock_timestamp();
insert into olapts.abspecialdelta
	select   
		specialdelta.id_ AS id_,
		specialdelta.pkid_::varchar as pkid_,
		(specialdelta.jsondoc_ ->>'Active') AS Active,
		(specialdelta.jsondoc_->>'AuthorizationFlag')::boolean AS AuthorizationFlag,
		(specialdelta.jsondoc_->>'Comments') AS Comments,
		(specialdelta.jsondoc_->>'Deactivation')::boolean AS Deactivation,
		(specialdelta.jsondoc_ ->> 'EntityId') AS EntityId,
		(specialdelta.jsondoc_ ->> 'IsLux') AS IsLux,
		case 
			when (l1.jsondoc_ ->>'Value') is Null then 'OXI' 
			else (l1.jsondoc_ ->>'Value') 
		end AS IsLuxval,
		(specialdelta.jsondoc_ ->> 'IsVisible') AS IsVisible, 	
		(l2.jsondoc_ ->>'Value') as IsVisibleval, 				
		(specialdelta.jsondoc_ ->> 'OriginationOfEntity') AS OriginationOfEntity,
		(l3.jsondoc_->> 'Value') AS OriginationOfEntityval,
		(specialdelta.jsondoc_ ->> 'RatingReason') AS RatingReason,
		(l4.jsondoc_->> 'Value') AS RatingReasonval,
		(specialdelta.jsondoc_ ->>'RatingScenarioId') AS RatingScenarioId,
		(specialdelta.jsondoc_ ->>'SpecialDeltaSaved') AS SpecialDeltaSaved,
		(specialdelta.jsondoc_ ->> 'SpecialGrade') AS SpecialGrade,
		(l5.jsondoc_->> 'Value') AS SpecialGradeval,
		specialdelta.wfid_::varchar,
		specialdelta.taskid_::varchar,
		specialdelta.versionid_::int4,
		specialdelta.isdeleted_::boolean,
		specialdelta.islatestversion_::boolean,
		specialdelta.baseversionid_::int4,
		specialdelta.contextuserid_::varchar,
		specialdelta.isvisible_::boolean,
		specialdelta.isvalid_::boolean,
		specialdelta.snapshotid_::int4,
		specialdelta.t_::varchar,
		specialdelta.createdby_::varchar,
		specialdelta.createddate_::timestamp,
		specialdelta.updatedby_::varchar,
		specialdelta.updateddate_::timestamp,
		specialdelta.fkid_entity,
		CASE WHEN specialdelta.updateddate_ > specialdelta.createddate_ THEN specialdelta.updatedby_ ELSE specialdelta.createdby_ END AS sourcepopulatedby_,
		GREATEST(specialdelta.createddate_, specialdelta.updateddate_) AS sourcepopulateddate_
		,current_setting('myvariables.popdate')::timestamp as populateddate_
	FROM madata.specialdelta
		left join madata.custom_lookup l1 on l1.t_ = 'YesNoRm'  and l1.jsondoc_->>'Key'  = specialdelta.jsondoc_ ->> 'IsLux'
		left join madata.custom_lookup l2 on l2.t_ = 'YesNo'  and l2.jsondoc_->>'Key'  = specialdelta.jsondoc_ ->> 'IsVisible'	
		left join madata.custom_lookup l3 on l3.t_ = 'OriginationEntity'  and l3.jsondoc_->>'Key' = specialdelta.jsondoc_ ->> 'OriginationOfEntity'
		left join madata.custom_lookup l4 on l4.t_ = 'SpecialDeltaRatingReason'  and l4.jsondoc_->>'Key' = specialdelta.jsondoc_ ->> 'RatingReason'
		left join madata.custom_lookup l5 on l5.t_ = 'SpecialNonSystemicGrade'  and l5.jsondoc_->>'Key' = specialdelta.jsondoc_ ->> 'SpecialGrade'
	WHERE
		GREATEST(specialdelta.updateddate_, specialdelta.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABSPECIALDELTA')
		AND GREATEST(specialdelta.updateddate_, specialdelta.createddate_)::timestamp <= max_refreshhistory 
		AND specialdelta.t_ = 'SpecialDelta'
	;
raise notice '% - Step abspecialdelta - part a end', clock_timestamp();
--------------------------------------------

ELSE
raise notice '% Step abspecialdelta - part b start', clock_timestamp();
	DROP TABLE IF EXISTS olapts.abspecialdelta;
	CREATE TABLE olapts.abspecialdelta AS
	select   
		specialdelta.id_ AS id_,
		specialdelta.pkid_::varchar as pkid_,
		(specialdelta.jsondoc_ ->>'Active') AS Active,
		(specialdelta.jsondoc_->>'AuthorizationFlag')::boolean AS AuthorizationFlag,
		(specialdelta.jsondoc_->>'Comments') AS Comments,
		(specialdelta.jsondoc_->>'Deactivation')::boolean AS Deactivation,
		(specialdelta.jsondoc_ ->> 'EntityId') AS EntityId,
		(specialdelta.jsondoc_ ->> 'IsLux') AS IsLux,
		case 
			when (l1.jsondoc_ ->>'Value') is Null then 'OXI' 
			else (l1.jsondoc_ ->>'Value') 
		end AS IsLuxval,
		(specialdelta.jsondoc_ ->> 'IsVisible') AS IsVisible, 	
		(l2.jsondoc_ ->>'Value') as IsVisibleval, 				
		(specialdelta.jsondoc_ ->> 'OriginationOfEntity') AS OriginationOfEntity,
		(l3.jsondoc_->> 'Value') AS OriginationOfEntityval,
		(specialdelta.jsondoc_ ->> 'RatingReason') AS RatingReason,
		(l4.jsondoc_->> 'Value') AS RatingReasonval,
		(specialdelta.jsondoc_ ->>'RatingScenarioId') AS RatingScenarioId,
		(specialdelta.jsondoc_ ->>'SpecialDeltaSaved') AS SpecialDeltaSaved,
		(specialdelta.jsondoc_ ->> 'SpecialGrade') AS SpecialGrade,
		(l5.jsondoc_->> 'Value') AS SpecialGradeval,
		specialdelta.wfid_::varchar,
		specialdelta.taskid_::varchar,
		specialdelta.versionid_::int4,
		specialdelta.isdeleted_::boolean,
		specialdelta.islatestversion_::boolean,
		specialdelta.baseversionid_::int4,
		specialdelta.contextuserid_::varchar,
		specialdelta.isvisible_::boolean,
		specialdelta.isvalid_::boolean,
		specialdelta.snapshotid_::int4,
		specialdelta.t_::varchar,
		specialdelta.createdby_::varchar,
		specialdelta.createddate_::timestamp,
		specialdelta.updatedby_::varchar,
		specialdelta.updateddate_::timestamp,
		specialdelta.fkid_entity,
		CASE WHEN specialdelta.updateddate_ > specialdelta.createddate_ THEN specialdelta.updatedby_ ELSE specialdelta.createdby_ END AS sourcepopulatedby_,
		GREATEST(specialdelta.createddate_, specialdelta.updateddate_) AS sourcepopulateddate_
		,current_setting('myvariables.popdate')::timestamp as populateddate_
	FROM madata.specialdelta
		left join madata.custom_lookup l1 on l1.t_ = 'YesNoRm'  and l1.jsondoc_->>'Key'  = specialdelta.jsondoc_ ->> 'IsLux'
		left join madata.custom_lookup l2 on l2.t_ = 'YesNo'  and l2.jsondoc_->>'Key'  = specialdelta.jsondoc_ ->> 'IsVisible'	
		left join madata.custom_lookup l3 on l3.t_ = 'OriginationEntity'  and l3.jsondoc_->>'Key' = specialdelta.jsondoc_ ->> 'OriginationOfEntity'
		left join madata.custom_lookup l4 on l4.t_ = 'SpecialDeltaRatingReason'  and l4.jsondoc_->>'Key' = specialdelta.jsondoc_ ->> 'RatingReason'
		left join madata.custom_lookup l5 on l5.t_ = 'SpecialNonSystemicGrade'  and l5.jsondoc_->>'Key' = specialdelta.jsondoc_ ->> 'SpecialGrade'
	WHERE
		GREATEST(specialdelta.updateddate_, specialdelta.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABSPECIALDELTA')
		AND GREATEST(specialdelta.updateddate_, specialdelta.createddate_)::timestamp <= max_refreshhistory
		AND specialdelta.t_ = 'SpecialDelta'
	;
raise notice '% - Step abspecialdelta - part b end', clock_timestamp();

--abspecialdelta--
raise notice '% - Step abspecialdelta_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abspecialdelta_idx;
DROP INDEX if exists olapts.abspecialdelta_idx2;
CREATE INDEX IF NOT EXISTS abspecialdelta_idx ON olapts.abspecialdelta (id_);
CREATE INDEX IF NOT EXISTS abspecialdelta_idx2 ON olapts.abspecialdelta (pkid_,versionid_);

raise notice '% - Step abspecialdelta_idx - part a end', clock_timestamp(); 
END IF;
------------------------------------------------------------------------
raise notice '% step abspecialdelta - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abspecialdeltaflag;
CREATE TABLE  olapts.abspecialdeltaflag AS

select
id_,
pkid_,
wfid_ wfid_,
taskid_ taskid_, 
versionid_ versionid_,
isdeleted_ isdeleted_,
islatestversion_ islatestversion_,
baseversionid_ baseversionid_,
contextuserid_ contextuserid_,
isvisible_ isvisible_,
isvalid_ isvalid_,
snapshotid_ snapshotid_,
t_ t_,
createdby_ createdby_,
createddate_ createddate_,
updatedby_ updatedby_,
updateddate_ updateddate_,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_, createddate_) as sourcepopulateddate_
,current_setting('myvariables.popdate')::timestamp as populateddate_
FROM madata.specialdelta
where 
GREATEST(specialdelta.updateddate_, specialdelta.createddate_)::timestamp <=max_refreshhistory
and specialdelta.t_ = 'SpecialDelta'
;

raise notice '% - Step abspecialdeltaflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abspecialdeltaflag_idx;
DROP INDEX if exists olapts.abspecialdeltaflag_idx2;
CREATE INDEX IF NOT EXISTS abspecialdeltaflag_idx ON olapts.abspecialdeltaflag (id_);
CREATE INDEX IF NOT EXISTS abspecialdeltaflag_idx2 ON olapts.abspecialdeltaflag (pkid_,versionid_);
ANALYZE olapts.abspecialdeltaflag ;

raise notice '% - Step abspecialdeltaflag_idx - part a end', clock_timestamp(); 

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABSPECIALDELTA';
delete from olapts.refreshhistory where tablename = 'ABSPECIALDELTA';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABSPECIALDELTA' tablename,max_refreshhistory as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABSPECIALDELTAFLAG';
delete from olapts.refreshhistory where tablename = 'ABSPECIALDELTAFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABSPECIALDELTAFLAG' tablename,max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abspecialdelta - part c end', clock_timestamp();

----------------------------------------

-------------------REF Data--------------------------
--YesNoRM
raise notice '% - Step abyesnorm - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abyesnorm;
CREATE TABLE olapts.abyesnorm AS
select l.jsondoc_->>'Key' yesnormkey_,
l.jsondoc_->>'Value' yesnormvalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_
,current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'YesNoRm';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABYESNORM';
delete from olapts.refreshhistory where tablename = 'ABYESNORM';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABYESNORM' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abyesnorm - part a end', clock_timestamp();

--YesNo
raise notice '% - Step abyesno - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abyesno;
CREATE TABLE olapts.abyesno AS
select l.jsondoc_->>'Key' yesnokey_,
l.jsondoc_->>'Value' yesnovalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_
,current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'YesNo';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABYESNO';
delete from olapts.refreshhistory where tablename = 'ABYESNO';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABYESNO' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abyesno - part a end', clock_timestamp();

--OriginationEntity
raise notice '% - Step aboriginationentity - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.aboriginationentity;
CREATE TABLE olapts.aboriginationentity AS
select l.jsondoc_->>'Key' originationentitykey_,
l.jsondoc_->>'Value' originationentityvalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'OriginationEntity';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABORIGINATIONENTITY';
delete from olapts.refreshhistory where tablename = 'ABORIGINATIONENTITY';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABORIGINATIONENTITY' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step aboriginationentity - part a end', clock_timestamp();

--SpecialDeltaRatingReason 
raise notice '% - Step abspecialdeltaratingreason - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abspecialdeltaratingreason;
CREATE TABLE olapts.abspecialdeltaratingreason AS
select l.jsondoc_->>'Key' specialdeltaratingreasonkey_,
l.jsondoc_->>'Value' specialdeltaratingreasonvalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'SpecialDeltaRatingReason';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABSPECIALDELTARATINGREASON';
delete from olapts.refreshhistory where tablename = 'ABSPECIALDELTARATINGREASON';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABSPECIALDELTARATINGREASON' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abspecialdeltaratingreason - part a end', clock_timestamp();

--SpecialNonSystemicGrade 
raise notice '% - Step abspecialnonsystemicgrade - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abspecialnonsystemicgrade;
CREATE TABLE olapts.abspecialnonsystemicgrade AS
select l.jsondoc_->>'Key' specialnonsystemicgradekey_,
l.jsondoc_->>'Value' specialnonsystemicgradevalue,
isdeleted_,
t_ t_ ,
(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'SpecialNonSystemicGrade';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABSPECIALNONSYSTEMICGRADE';
delete from olapts.refreshhistory where tablename = 'ABSPECIALNONSYSTEMICGRADE';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABSPECIALNONSYSTEMICGRADE' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abspecialnonsystemicgrade - part a end', clock_timestamp();

--END $$; 

---------------ESG--------------------------------------
--DO $$
--DECLARE
--varprevsuccessdate TIMESTAMP ;

--BEGIN

---------------------------------------------- AbEsgAssessment ----------------------------------------------

-- If table exists in refresh history --
IF EXISTS (select from olapts.refreshhistory where tablename = 'abesgassessment') THEN
raise notice '% - Step abesgassessment - part a start', clock_timestamp();
insert into olapts.abesgassessment
	select esgassessment.id_ AS id_,
           esgassessment.pkid_,
           esgassessment.id_ AS esgassessmentid_,
           esgassessment.jsondoc_ ->> 'ApprovalDate'::text AS approvaldate,
           esgassessment.jsondoc_ ->> 'ApprovalUser'::text AS approvaluser,
           esgassessment.jsondoc_ ->> 'AssessmentDate'::text AS assessmentdate,
           esgassessment.jsondoc_ ->> 'AssessmentUser'::text AS assessmentuser,
           esgassessment.jsondoc_ ->> 'AssetsRecentValue'::text AS assetsrecentvalue,
          (esgassessment.jsondoc_ ->> 'AuthorizationFlag'::text)::boolean AS authorizationflag,
           esgassessment.jsondoc_ ->> 'CompanyNaceCode'::text AS companynacecode,
           esgassessment.jsondoc_ ->> 'CompanyName'::text AS companyname,
           esgassessment.jsondoc_ ->> 'CreditCommitteeDate'::text AS creditcommitteedate,
           esgassessment.jsondoc_ ->> 'EbiodiversityScore'::text AS ebiodiversityscore,
           esgassessment.jsondoc_ ->> 'EclimateGoalsTargetsScore'::text AS eclimategoalstargetsscore,
           esgassessment.jsondoc_ ->> 'EemissionsScore'::text AS eemissionsscore,
           esgassessment.jsondoc_ ->> 'EenergyScore'::text AS eenergyscore,
           esgassessment.jsondoc_ ->> 'EenvironmentalComplianceScore'::text AS eenvironmentalcompliancescore,
           esgassessment.jsondoc_ ->> 'EevaluationApproachScore'::text AS eevaluationapproachscore,
           esgassessment.jsondoc_ ->> 'EmanagementApproachScore'::text AS emanagementapproachscore,
           esgassessment.jsondoc_ ->> 'EmanagementSystemsScore'::text AS emanagementsystemsscore,
           esgassessment.jsondoc_ ->> 'EmaterialsScore'::text AS ematerialsscore,
           esgassessment.jsondoc_ ->> 'EmonitoringMechanismsScore'::text AS emonitoringmechanismsscore,
           esgassessment.jsondoc_ ->> 'EmployeesRecentValue'::text AS employeesrecentvalue,
           esgassessment.jsondoc_ ->> 'EntityId'::text AS entityid,
           esgassessment.jsondoc_ ->> 'EntityVersionId'::text AS entityversionid,
           esgassessment.jsondoc_ ->> 'EopportunitiesCleanTechScore'::text AS eopportunitiescleantechscore,
           esgassessment.jsondoc_ ->> 'EperformanceAssessmentScore'::text AS eperformanceassessmentscore,
           esgassessment.jsondoc_ ->> 'EphysicalRiskAssessmentScore'::text AS ephysicalriskassessmentscore,
           esgassessment.jsondoc_ ->> 'EpoliciesScore'::text AS epoliciesscore,
           esgassessment.jsondoc_ ->> 'EsgGrade'::text AS esggrade,
           esgassessment.jsondoc_ ->> 'EsgZonesOutcome'::text AS esgzonesoutcome,
           esgassessment.jsondoc_ ->> 'EsubGrade'::text AS esubgrade,
           esgassessment.jsondoc_ ->> 'EsubScore'::text AS esubscore,
           esgassessment.jsondoc_ ->> 'EsubZone'::text AS esubzone,
           esgassessment.jsondoc_ ->> 'EwasteScore'::text AS ewastescore,
           esgassessment.jsondoc_ ->> 'EwaterEffluentsScore'::text AS ewatereffluentsscore,
           esgassessment.jsondoc_ ->> 'ExpirationDate'::text AS expirationdate,
           esgassessment.jsondoc_ ->> 'GbusinessEthicsScore'::text AS gbusinessethicsscore,
           esgassessment.jsondoc_ ->> 'GcollectiveKnowledgeScore'::text AS gcollectiveknowledgescore,
           esgassessment.jsondoc_ ->> 'GcompositionDiversityScore'::text AS gcompositiondiversityscore,
           esgassessment.jsondoc_ ->> 'GperformanceEvaluationScore'::text AS gperformanceevaluationscore,
           esgassessment.jsondoc_ ->> 'GregulatoryComplianceScore'::text AS gregulatorycompliancescore,
           esgassessment.jsondoc_ ->> 'GremunerationScore'::text AS gremunerationscore,
           esgassessment.jsondoc_ ->> 'GrolesResponsibilitiesScore'::text AS grolesresponsibilitiesscore,
           esgassessment.jsondoc_ ->> 'GstakeholderEngagementScore'::text AS gstakeholderengagementscore,
           esgassessment.jsondoc_ ->> 'GstrategyRiskManagementScore'::text AS gstrategyriskmanagementscore,
           esgassessment.jsondoc_ ->> 'GsubGrade'::text AS gsubgrade,
           esgassessment.jsondoc_ ->> 'GsubScore'::text AS gsubscore,
           esgassessment.jsondoc_ ->> 'Gsubzone'::text AS gsubzone,
           esgassessment.jsondoc_ ->> 'GtransparencyScore'::text AS gtransparencyscore,
          (esgassessment.jsondoc_ ->> 'IsCompanyListedInStockExchange'::text)::boolean AS iscompanylistedinstockexchange,
           esgassessment.jsondoc_ ->> 'IsLatestApprovedAssessment'::text AS islatestapprovedassessment,
           esgassessment.jsondoc_ ->> 'NextReviewDate'::text AS nextreviewdate,
           esgassessment.jsondoc_ ->> 'OverrideAuthority'::text AS overrideauthority,
           l1.jsondoc_ ->> 'Value'::text AS overrideauthorityval,
           esgassessment.jsondoc_ ->> 'OverrideDriver'::text AS overridedriver,
           esgassessment.jsondoc_ ->> 'OverrideGrade'::text AS overridegrade,
           l2.jsondoc_ ->> 'Value'::text AS overridegradeval,
           esgassessment.jsondoc_ ->> 'OverrideProvider'::text AS overrideprovider,
           esgassessment.jsondoc_ ->> 'OverrideReason'::text AS overridereason,
           esgassessment.jsondoc_ ->> 'QuestionnaireDate'::text AS questionnairedate,
           esgassessment.jsondoc_ ->> 'QuestionnaireId'::text AS questionnaireid,
           esgassessment.jsondoc_ ->> 'QuestionnaireType'::text AS questionnairetype,
           esgassessment.jsondoc_ ->> 'QuestionnaireVersion'::text AS questionnaireversion,
           esgassessment.jsondoc_ ->> 'RatingFrom'::text AS ratingfrom,
           esgassessment.jsondoc_ ->> 'RevenuesRecentValue'::text AS revenuesrecentvalue,
           esgassessment.jsondoc_ ->> 'ScomplianceScore'::text AS scompliancescore,
           esgassessment.jsondoc_ ->> 'ScustomerHealthSafetyScore'::text AS scustomerhealthsafetyscore,
           esgassessment.jsondoc_ ->> 'ScustomerPrivacyScore'::text AS scustomerprivacyscore,
           esgassessment.jsondoc_ ->> 'SevaluationApproachScore'::text AS sevaluationapproachscore,
           esgassessment.jsondoc_ ->> 'SgoalsTargetsScore'::text AS sgoalstargetsscore,
           esgassessment.jsondoc_ ->> 'ShumanRightsScore'::text AS shumanrightsscore,
           esgassessment.jsondoc_ ->> 'SlaborManagementRelationsScore'::text AS slabormanagementrelationsscore,
           esgassessment.jsondoc_ ->> 'SlocalCommunitiesScore'::text AS slocalcommunitiesscore,
           esgassessment.jsondoc_ ->> 'SmanagementApproachScore'::text AS smanagementapproachscore,
           esgassessment.jsondoc_ ->> 'SmanagementSystemsScore'::text AS smanagementsystemsscore,
           esgassessment.jsondoc_ ->> 'SmonitoringMechanismsScore'::text AS smonitoringmechanismsscore,
           esgassessment.jsondoc_ ->> 'SoccupationalHealthSafetScore'::text AS soccupationalhealthsafetscore,
           esgassessment.jsondoc_ ->> 'SperformanceAssessmentScore'::text AS sperformanceassessmentscore,
           esgassessment.jsondoc_ ->> 'SpoliciesScore'::text AS spoliciesscore,
           esgassessment.jsondoc_ ->> 'SsubGrade'::text AS ssubgrade,
           esgassessment.jsondoc_ ->> 'SsubScore'::text AS ssubscore,
           esgassessment.jsondoc_ ->> 'SsubZone'::text AS ssubzone,
           esgassessment.jsondoc_ ->> 'StrainingEducationScore'::text AS strainingeducationscore,
           esgassessment.jsondoc_ ->> 'TotalEsgScore'::text AS totalesgscore,
           esgassessment.wfid_,
           esgassessment.taskid_,
           esgassessment.versionid_,
           esgassessment.isdeleted_,
           esgassessment.islatestversion_,
           esgassessment.baseversionid_,
           esgassessment.contextuserid_,
           esgassessment.isvisible_,
           esgassessment.isvalid_,
           esgassessment.snapshotid_,
           esgassessment.t_,
           esgassessment.createdby_,
           esgassessment.createddate_,
           esgassessment.updatedby_,
           esgassessment.updateddate_,
           esgassessment.fkid_entity,
           CASE WHEN esgassessment.updateddate_ > esgassessment.createddate_ THEN esgassessment.updatedby_ ELSE esgassessment.createdby_ END AS sourcepopulatedby_,
           GREATEST(esgassessment.createddate_, esgassessment.updateddate_) AS sourcepopulateddate_,
	       current_setting('myvariables.popdate')::timestamp as populateddate_
	FROM madata.esgassessment
	left join madata.custom_lookup l1 on l1.t_ = 'OverrideAuthority' and l1.jsondoc_->>'Key'  = esgassessment.jsondoc_ ->> 'OverrideAuthority'
	left join madata.custom_lookup l2 on l2.t_ = 'EsgOverrideGrade' and l2.jsondoc_ ->> 'Key' = esgassessment.jsondoc_ ->> 'OverrideGrade'
	WHERE
		 GREATEST(esgassessment.updateddate_, esgassessment.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) from olapts.refreshhistory where tablename = 'abesgassessment')
		 AND GREATEST(esgassessment.updateddate_, esgassessment.createddate_)::timestamp <= max_refreshhistory
		 AND esgassessment.t_ = 'EsgAssessment'
;
raise notice '% - Step abesgassessment - part a end', clock_timestamp();

-- If table doesn't exist in refresh history --
ELSE
raise notice '% Step abesgassessment - part b start', clock_timestamp();
	DROP TABLE IF EXISTS olapts.abesgassessment;
	CREATE TABLE olapts.abesgassessment AS
	select esgassessment.id_ AS id_,
           esgassessment.pkid_,
           esgassessment.id_ AS esgassessmentid_,
           esgassessment.jsondoc_ ->> 'ApprovalDate'::text AS approvaldate,
           esgassessment.jsondoc_ ->> 'ApprovalUser'::text AS approvaluser,
           esgassessment.jsondoc_ ->> 'AssessmentDate'::text AS assessmentdate,
           esgassessment.jsondoc_ ->> 'AssessmentUser'::text AS assessmentuser,
           esgassessment.jsondoc_ ->> 'AssetsRecentValue'::text AS assetsrecentvalue,
          (esgassessment.jsondoc_ ->> 'AuthorizationFlag'::text)::boolean AS authorizationflag,
           esgassessment.jsondoc_ ->> 'CompanyNaceCode'::text AS companynacecode,
           esgassessment.jsondoc_ ->> 'CompanyName'::text AS companyname,
           esgassessment.jsondoc_ ->> 'CreditCommitteeDate'::text AS creditcommitteedate,
           esgassessment.jsondoc_ ->> 'EbiodiversityScore'::text AS ebiodiversityscore,
           esgassessment.jsondoc_ ->> 'EclimateGoalsTargetsScore'::text AS eclimategoalstargetsscore,
           esgassessment.jsondoc_ ->> 'EemissionsScore'::text AS eemissionsscore,
           esgassessment.jsondoc_ ->> 'EenergyScore'::text AS eenergyscore,
           esgassessment.jsondoc_ ->> 'EenvironmentalComplianceScore'::text AS eenvironmentalcompliancescore,
           esgassessment.jsondoc_ ->> 'EevaluationApproachScore'::text AS eevaluationapproachscore,
           esgassessment.jsondoc_ ->> 'EmanagementApproachScore'::text AS emanagementapproachscore,
           esgassessment.jsondoc_ ->> 'EmanagementSystemsScore'::text AS emanagementsystemsscore,
           esgassessment.jsondoc_ ->> 'EmaterialsScore'::text AS ematerialsscore,
           esgassessment.jsondoc_ ->> 'EmonitoringMechanismsScore'::text AS emonitoringmechanismsscore,
           esgassessment.jsondoc_ ->> 'EmployeesRecentValue'::text AS employeesrecentvalue,
           esgassessment.jsondoc_ ->> 'EntityId'::text AS entityid,
           esgassessment.jsondoc_ ->> 'EntityVersionId'::text AS entityversionid,
           esgassessment.jsondoc_ ->> 'EopportunitiesCleanTechScore'::text AS eopportunitiescleantechscore,
           esgassessment.jsondoc_ ->> 'EperformanceAssessmentScore'::text AS eperformanceassessmentscore,
           esgassessment.jsondoc_ ->> 'EphysicalRiskAssessmentScore'::text AS ephysicalriskassessmentscore,
           esgassessment.jsondoc_ ->> 'EpoliciesScore'::text AS epoliciesscore,
           esgassessment.jsondoc_ ->> 'EsgGrade'::text AS esggrade,
           esgassessment.jsondoc_ ->> 'EsgZonesOutcome'::text AS esgzonesoutcome,
           esgassessment.jsondoc_ ->> 'EsubGrade'::text AS esubgrade,
           esgassessment.jsondoc_ ->> 'EsubScore'::text AS esubscore,
           esgassessment.jsondoc_ ->> 'EsubZone'::text AS esubzone,
           esgassessment.jsondoc_ ->> 'EwasteScore'::text AS ewastescore,
           esgassessment.jsondoc_ ->> 'EwaterEffluentsScore'::text AS ewatereffluentsscore,
           esgassessment.jsondoc_ ->> 'ExpirationDate'::text AS expirationdate,
           esgassessment.jsondoc_ ->> 'GbusinessEthicsScore'::text AS gbusinessethicsscore,
           esgassessment.jsondoc_ ->> 'GcollectiveKnowledgeScore'::text AS gcollectiveknowledgescore,
           esgassessment.jsondoc_ ->> 'GcompositionDiversityScore'::text AS gcompositiondiversityscore,
           esgassessment.jsondoc_ ->> 'GperformanceEvaluationScore'::text AS gperformanceevaluationscore,
           esgassessment.jsondoc_ ->> 'GregulatoryComplianceScore'::text AS gregulatorycompliancescore,
           esgassessment.jsondoc_ ->> 'GremunerationScore'::text AS gremunerationscore,
           esgassessment.jsondoc_ ->> 'GrolesResponsibilitiesScore'::text AS grolesresponsibilitiesscore,
           esgassessment.jsondoc_ ->> 'GstakeholderEngagementScore'::text AS gstakeholderengagementscore,
           esgassessment.jsondoc_ ->> 'GstrategyRiskManagementScore'::text AS gstrategyriskmanagementscore,
           esgassessment.jsondoc_ ->> 'GsubGrade'::text AS gsubgrade,
           esgassessment.jsondoc_ ->> 'GsubScore'::text AS gsubscore,
           esgassessment.jsondoc_ ->> 'Gsubzone'::text AS gsubzone,
           esgassessment.jsondoc_ ->> 'GtransparencyScore'::text AS gtransparencyscore,
          (esgassessment.jsondoc_ ->> 'IsCompanyListedInStockExchange'::text)::boolean AS iscompanylistedinstockexchange,
           esgassessment.jsondoc_ ->> 'IsLatestApprovedAssessment'::text AS islatestapprovedassessment,
           esgassessment.jsondoc_ ->> 'NextReviewDate'::text AS nextreviewdate,
           esgassessment.jsondoc_ ->> 'OverrideAuthority'::text AS overrideauthority,
           l1.jsondoc_ ->> 'Value'::text AS overrideauthorityval,
           esgassessment.jsondoc_ ->> 'OverrideDriver'::text AS overridedriver,
           esgassessment.jsondoc_ ->> 'OverrideGrade'::text AS overridegrade,
           l2.jsondoc_ ->> 'Value'::text AS overridegradeval,
           esgassessment.jsondoc_ ->> 'OverrideProvider'::text AS overrideprovider,
           esgassessment.jsondoc_ ->> 'OverrideReason'::text AS overridereason,
           esgassessment.jsondoc_ ->> 'QuestionnaireDate'::text AS questionnairedate,
           esgassessment.jsondoc_ ->> 'QuestionnaireId'::text AS questionnaireid,
           esgassessment.jsondoc_ ->> 'QuestionnaireType'::text AS questionnairetype,
           esgassessment.jsondoc_ ->> 'QuestionnaireVersion'::text AS questionnaireversion,
           esgassessment.jsondoc_ ->> 'RatingFrom'::text AS ratingfrom,
           esgassessment.jsondoc_ ->> 'RevenuesRecentValue'::text AS revenuesrecentvalue,
           esgassessment.jsondoc_ ->> 'ScomplianceScore'::text AS scompliancescore,
           esgassessment.jsondoc_ ->> 'ScustomerHealthSafetyScore'::text AS scustomerhealthsafetyscore,
           esgassessment.jsondoc_ ->> 'ScustomerPrivacyScore'::text AS scustomerprivacyscore,
           esgassessment.jsondoc_ ->> 'SevaluationApproachScore'::text AS sevaluationapproachscore,
           esgassessment.jsondoc_ ->> 'SgoalsTargetsScore'::text AS sgoalstargetsscore,
           esgassessment.jsondoc_ ->> 'ShumanRightsScore'::text AS shumanrightsscore,
           esgassessment.jsondoc_ ->> 'SlaborManagementRelationsScore'::text AS slabormanagementrelationsscore,
           esgassessment.jsondoc_ ->> 'SlocalCommunitiesScore'::text AS slocalcommunitiesscore,
           esgassessment.jsondoc_ ->> 'SmanagementApproachScore'::text AS smanagementapproachscore,
           esgassessment.jsondoc_ ->> 'SmanagementSystemsScore'::text AS smanagementsystemsscore,
           esgassessment.jsondoc_ ->> 'SmonitoringMechanismsScore'::text AS smonitoringmechanismsscore,
           esgassessment.jsondoc_ ->> 'SoccupationalHealthSafetScore'::text AS soccupationalhealthsafetscore,
           esgassessment.jsondoc_ ->> 'SperformanceAssessmentScore'::text AS sperformanceassessmentscore,
           esgassessment.jsondoc_ ->> 'SpoliciesScore'::text AS spoliciesscore,
           esgassessment.jsondoc_ ->> 'SsubGrade'::text AS ssubgrade,
           esgassessment.jsondoc_ ->> 'SsubScore'::text AS ssubscore,
           esgassessment.jsondoc_ ->> 'SsubZone'::text AS ssubzone,
           esgassessment.jsondoc_ ->> 'StrainingEducationScore'::text AS strainingeducationscore,
           esgassessment.jsondoc_ ->> 'TotalEsgScore'::text AS totalesgscore,
           esgassessment.wfid_,
           esgassessment.taskid_,
           esgassessment.versionid_,
           esgassessment.isdeleted_,
           esgassessment.islatestversion_,
           esgassessment.baseversionid_,
           esgassessment.contextuserid_,
           esgassessment.isvisible_,
           esgassessment.isvalid_,
           esgassessment.snapshotid_,
           esgassessment.t_,
           esgassessment.createdby_,
           esgassessment.createddate_,
           esgassessment.updatedby_,
           esgassessment.updateddate_,
           esgassessment.fkid_entity,
           CASE WHEN esgassessment.updateddate_ > esgassessment.createddate_ THEN esgassessment.updatedby_ ELSE esgassessment.createdby_ END AS sourcepopulatedby_,
           GREATEST(esgassessment.createddate_, esgassessment.updateddate_) AS sourcepopulateddate_,
	       current_setting('myvariables.popdate')::timestamp as populateddate_
	FROM madata.esgassessment
	left join madata.custom_lookup l1 on l1.t_ = 'OverrideAuthority' and l1.jsondoc_->>'Key'  = esgassessment.jsondoc_ ->> 'OverrideAuthority'
	left join madata.custom_lookup l2 on l2.t_ = 'EsgOverrideGrade' and l2.jsondoc_ ->> 'Key' = esgassessment.jsondoc_ ->> 'OverrideGrade'
	WHERE
		 GREATEST(esgassessment.updateddate_, esgassessment.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) from olapts.refreshhistory where tablename = 'abesgassessment')
		 AND GREATEST(esgassessment.updateddate_, esgassessment.createddate_)::timestamp <= max_refreshhistory
		 AND esgassessment.t_ = 'EsgAssessment'
;
raise notice '% - Step abesgassessment - part b end', clock_timestamp();

--AbEsgAssessment Indexes--
raise notice '% - Step abesgassessment_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abesgassessment_idx;
DROP INDEX if exists olapts.abesgassessment_idx2;
CREATE INDEX IF NOT EXISTS abesgassessment_idx ON olapts.abesgassessment (id_);
CREATE INDEX IF NOT EXISTS abesgassessment_idx2 ON olapts.abesgassessment (pkid_,versionid_);
REINDEX TABLE olapts.abesgassessment;
ANALYZE olapts.abesgassessment ;	

raise notice '% - Step abesgassessment_idx - part a end', clock_timestamp(); 
END IF;
------------------------------------------------------------------------
-- Create or update flag table -- 
raise notice '% step abesgassessment - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abesgassessmentflag;
CREATE TABLE  olapts.abesgassessmentflag AS
select id_,
       pkid_,
       wfid_ wfid_,
       taskid_ taskid_, 
       versionid_ versionid_,
       isdeleted_ isdeleted_,
       islatestversion_ islatestversion_,
       baseversionid_ baseversionid_,
       contextuserid_ contextuserid_,
       isvisible_ isvisible_,
       isvalid_ isvalid_,
       snapshotid_ snapshotid_,
       t_ t_,
       createdby_ createdby_,
       createddate_ createddate_,
       updatedby_ updatedby_,
       updateddate_ updateddate_,
       (case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
       GREATEST(updateddate_, createddate_) as sourcepopulateddate_,
       current_setting('myvariables.popdate')::timestamp as populateddate_
FROM madata.esgassessment
where GREATEST(esgassessment.updateddate_, esgassessment.createddate_)::timestamp <=max_refreshhistory
      and esgassessment.t_ = 'EsgAssessment'
;

--abesgassessmentflag Indexes--

raise notice '% - Step abesgassessmentflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abesgassessmentflag_idx;
DROP INDEX if exists olapts.abesgassessmentflag_idx2;
CREATE INDEX IF NOT EXISTS abesgassessmentflag_idx ON olapts.abesgassessmentflag (id_);
CREATE INDEX IF NOT EXISTS abesgassessmentflag_idx2 ON olapts.abesgassessmentflag (pkid_,versionid_);
REINDEX TABLE olapts.abesgassessmentflag;
ANALYZE olapts.abesgassessmentflag ;

raise notice '% - Step abesgassessmentflag_idx - part a end', clock_timestamp(); 

-- Update refresh history -- 

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'abesgassessment';
delete from olapts.refreshhistory where tablename = 'abesgassessment';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'abesgassessment' tablename,max_refreshhistory as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'abesgassessmentflag';
delete from olapts.refreshhistory where tablename = 'abesgassessmentflag';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'abesgassessmentflag' tablename,max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abesgassessment - part c end', clock_timestamp();

--END $$;

-------------------REF Data--------------------------

--DO $$
--DECLARE
--varprevsuccessdate TIMESTAMP ;
--BEGIN

--AbOverrideAuthority
raise notice '% - Step AbOverrideAuthority - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.aboverrideauthority;
CREATE TABLE olapts.aboverrideauthority AS
select l.jsondoc_->>'Key' overrideauthoritykey_,
       l.jsondoc_->>'Value' overrideauthorityvalue,
       isdeleted_,
       t_ t_ ,
       (case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
       GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
       current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'OverrideAuthority';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'aboverrideauthority';
delete from olapts.refreshhistory where tablename = 'aboverrideauthority';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'aboverrideauthority' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step aboverrideauthority - part a end', clock_timestamp();

--AbOverrideGrade
raise notice '% - Step AbEsgOverrideGrade - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abesgoverridegrade;
CREATE TABLE olapts.abesgoverridegrade AS
select l.jsondoc_->>'Key' overridegradekey_,
       l.jsondoc_->>'Value' overridegradevalue,
       isdeleted_,
       t_ t_ ,
       (case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
       GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
       current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'EsgOverrideGrade';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'abesgoverridegrade';
delete from olapts.refreshhistory where tablename = 'abesgoverridegrade';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'abesgoverridegrade' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abesgoverridegrade - part a end', clock_timestamp();

--END $$;

---------------------------------------------- AbEsgQuestion ----------------------------------------------

--DO $$
--DECLARE
--varprevsuccessdate TIMESTAMP ;

--BEGIN

-- If table exists in refresh history --
IF EXISTS (select from olapts.refreshhistory where tablename = 'abesgquestion') THEN
raise notice '% - Step abesgquestion - part a start', clock_timestamp();
insert into olapts.abesgquestion
	select esgquestion.id_ AS id_,
		   esgquestion.pkid_::varchar as pkid_,
		   esgquestion.id_ AS esgquestionid_,
    	   esgquestion.jsondoc_ ->> 'Comments'::text AS comments,
    	   esgquestion.jsondoc_ ->> 'EsgMasterId'::text AS esgmasterid,
    	   esgquestion.jsondoc_ ->> 'Factor'::text AS factor,
    	   esgquestion.jsondoc_ ->> 'FactorEy'::text AS factorey,
    	   esgquestion.jsondoc_ ->> 'Pillar'::text AS pillar,
    	   esgquestion.jsondoc_ ->> 'PillarEy'::text AS pillarey,
    	   esgquestion.jsondoc_ ->> 'PreviousReferenceYear'::text AS previousreferenceyear,
    	   esgquestion.jsondoc_ ->> 'PreviousValue'::text AS previousvalue,
    	   esgquestion.jsondoc_ ->> 'QuestionDescr'::text AS questiondescr,
    	   esgquestion.jsondoc_ ->> 'QuestionDescrEy'::text AS questiondescrey,
    	   esgquestion.jsondoc_ ->> 'QuestionId'::text AS questionid,
    	   esgquestion.jsondoc_ ->> 'QuestionIdEy'::text AS questionidey,
    	   esgquestion.jsondoc_ ->> 'ReferenceYear'::text AS referenceyear,
    	   esgquestion.jsondoc_ ->> 'SubFactor'::text AS subfactor,
    	   esgquestion.jsondoc_ ->> 'SubFactorEy'::text AS subfactorey,
    	   esgquestion.jsondoc_ ->> 'Value'::text AS value,
		   esgquestion.wfid_::varchar,
		   esgquestion.taskid_::varchar,
		   esgquestion.versionid_::int4,
		   esgquestion.isdeleted_::boolean,
		   esgquestion.islatestversion_::boolean,
		   esgquestion.baseversionid_::int4,
		   esgquestion.contextuserid_::varchar,
		   esgquestion.isvisible_::boolean,
		   esgquestion.isvalid_::boolean,
		   esgquestion.snapshotid_::int4,
		   esgquestion.t_::varchar,
		   esgquestion.createdby_::varchar,
		   esgquestion.createddate_::timestamp,
		   esgquestion.updatedby_::varchar,
		   esgquestion.updateddate_::timestamp,
		   esgquestion.fkid_esgmaster,
		   CASE WHEN esgquestion.updateddate_ > esgquestion.createddate_ THEN esgquestion.updatedby_ ELSE esgquestion.createdby_ END AS sourcepopulatedby_,
		   GREATEST(esgquestion.createddate_, esgquestion.updateddate_) AS sourcepopulateddate_,
		   current_setting('myvariables.popdate')::timestamp as populateddate_
	FROM madata.esgquestion
	WHERE
		GREATEST(esgquestion.updateddate_, esgquestion.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) from olapts.refreshhistory where tablename = 'abesgquestion')
		AND GREATEST(esgquestion.updateddate_, esgquestion.createddate_)::timestamp <= max_refreshhistory
		AND esgquestion.t_ = 'EsgQuestion'
	;
raise notice '% - Step abesgquestion - part a end', clock_timestamp();

-- If table doesn't exist in refresh history --
ELSE
raise notice '% Step abesgquestion - part b start', clock_timestamp();
	DROP TABLE IF EXISTS olapts.abesgquestion;
	CREATE TABLE olapts.abesgquestion AS
	select esgquestion.id_ AS id_,
		   esgquestion.pkid_::varchar as pkid_,
		   esgquestion.id_ AS esgquestionid_,
    	   esgquestion.jsondoc_ ->> 'Comments'::text AS comments,
    	   esgquestion.jsondoc_ ->> 'EsgMasterId'::text AS esgmasterid,
    	   esgquestion.jsondoc_ ->> 'Factor'::text AS factor,
    	   esgquestion.jsondoc_ ->> 'FactorEy'::text AS factorey,
    	   esgquestion.jsondoc_ ->> 'Pillar'::text AS pillar,
    	   esgquestion.jsondoc_ ->> 'PillarEy'::text AS pillarey,
    	   esgquestion.jsondoc_ ->> 'PreviousReferenceYear'::text AS previousreferenceyear,
    	   esgquestion.jsondoc_ ->> 'PreviousValue'::text AS previousvalue,
    	   esgquestion.jsondoc_ ->> 'QuestionDescr'::text AS questiondescr,
    	   esgquestion.jsondoc_ ->> 'QuestionDescrEy'::text AS questiondescrey,
    	   esgquestion.jsondoc_ ->> 'QuestionId'::text AS questionid,
    	   esgquestion.jsondoc_ ->> 'QuestionIdEy'::text AS questionidey,
    	   esgquestion.jsondoc_ ->> 'ReferenceYear'::text AS referenceyear,
    	   esgquestion.jsondoc_ ->> 'SubFactor'::text AS subfactor,
    	   esgquestion.jsondoc_ ->> 'SubFactorEy'::text AS subfactorey,
    	   esgquestion.jsondoc_ ->> 'Value'::text AS value,
		   esgquestion.wfid_::varchar,
		   esgquestion.taskid_::varchar,
		   esgquestion.versionid_::int4,
		   esgquestion.isdeleted_::boolean,
		   esgquestion.islatestversion_::boolean,
		   esgquestion.baseversionid_::int4,
		   esgquestion.contextuserid_::varchar,
		   esgquestion.isvisible_::boolean,
		   esgquestion.isvalid_::boolean,
		   esgquestion.snapshotid_::int4,
		   esgquestion.t_::varchar,
		   esgquestion.createdby_::varchar,
		   esgquestion.createddate_::timestamp,
		   esgquestion.updatedby_::varchar,
		   esgquestion.updateddate_::timestamp,
		   esgquestion.fkid_esgmaster,
		   CASE WHEN esgquestion.updateddate_ > esgquestion.createddate_ THEN esgquestion.updatedby_ ELSE esgquestion.createdby_ END AS sourcepopulatedby_,
		   GREATEST(esgquestion.createddate_, esgquestion.updateddate_) AS sourcepopulateddate_,
		   current_setting('myvariables.popdate')::timestamp as populateddate_
	FROM madata.esgquestion
	WHERE
		GREATEST(esgquestion.updateddate_, esgquestion.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) from olapts.refreshhistory where tablename = 'abesgquestion')
		AND GREATEST(esgquestion.updateddate_, esgquestion.createddate_)::timestamp <= max_refreshhistory
		AND esgquestion.t_ = 'EsgQuestion'
	;
raise notice '% - Step abesgquestion - part b end', clock_timestamp();

--AbEsgQuestion Indexes--

raise notice '% - Step abesgquestion_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abesgquestion_idx;
DROP INDEX if exists olapts.abesgquestion_idx2;
CREATE INDEX IF NOT EXISTS abesgquestion_idx ON olapts.abesgquestion (id_);
CREATE INDEX IF NOT EXISTS abesgquestion_idx2 ON olapts.abesgquestion (pkid_,versionid_);
REINDEX TABLE olapts.abesgquestion;
ANALYZE olapts.abesgquestion ;	

raise notice '% - Step abesgquestion_idx - part a end', clock_timestamp(); 
END IF;

-- Create or update flag table -- 

raise notice '% step abesgquestion - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abesgquestionflag;
CREATE TABLE  olapts.abesgquestionflag AS

select id_,
       pkid_,
       wfid_ wfid_,
       taskid_ taskid_, 
       versionid_ versionid_,
       isdeleted_ isdeleted_,
       islatestversion_ islatestversion_,
       baseversionid_ baseversionid_,
       contextuserid_ contextuserid_,
       isvisible_ isvisible_,
       isvalid_ isvalid_,
       snapshotid_ snapshotid_,
       t_ t_,
       createdby_ createdby_,
       createddate_ createddate_,
       updatedby_ updatedby_,
       updateddate_ updateddate_,
       (case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
       GREATEST(updateddate_, createddate_) as sourcepopulateddate_,
       current_setting('myvariables.popdate')::timestamp as populateddate_
FROM madata.esgquestion
where 
     GREATEST(esgquestion.updateddate_, esgquestion.createddate_)::timestamp <=max_refreshhistory
     and esgquestion.t_ = 'EsgQuestion'
;

--AbEsgQuestionFlag Indexes--

raise notice '% - Step abesgquestionflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abesgquestionflag_idx;
DROP INDEX if exists olapts.abesgquestionflag_idx2;
CREATE INDEX IF NOT EXISTS abesgquestionflag_idx ON olapts.abesgquestionflag (id_);
CREATE INDEX IF NOT EXISTS abesgquestionflag_idx2 ON olapts.abesgquestionflag (pkid_,versionid_);
REINDEX TABLE olapts.abesgquestionflag;
ANALYZE olapts.abesgquestionflag ;

raise notice '% - Step abesgquestionflag_idx - part a end', clock_timestamp(); 

-- Update refresh history -- 

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'abesgquestion';
delete from olapts.refreshhistory where tablename = 'abesgquestion';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'abesgquestion' tablename,max_refreshhistory as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'abesgquestionflag';
delete from olapts.refreshhistory where tablename = 'abesgquestionflag';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'abesgquestionflag' tablename,max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abesgquestion - part c end', clock_timestamp();

--END $$;

---------------------------------------------- AbEsgOverallAssessment ----------------------------------------------

--DO $$
--DECLARE
--varprevsuccessdate TIMESTAMP ;

--BEGIN

-- If table exists in refresh history --
IF EXISTS (select from olapts.refreshhistory where tablename = 'abesgoverallassessment') THEN
raise notice '% - Step abesgoverallassessment - part a start', clock_timestamp();
insert into olapts.abesgoverallassessment
	select esgoverallassessment.id_ AS id_,
           esgoverallassessment.pkid_,
           esgoverallassessment.id_ AS esgoverallassessmentid_,
           esgoverallassessment.jsondoc_ ->> 'AssessmentDate'::text AS assessmentdate,
           esgoverallassessment.jsondoc_ ->> 'AssessmentUser'::text AS assessmentuser,
           esgoverallassessment.jsondoc_ ->> 'EntityId'::text AS entityid,
           esgoverallassessment.jsondoc_ ->> 'EntityVersionId'::text AS entityversionid,
           esgoverallassessment.jsondoc_ ->> 'EsOutcome'::text AS esoutcome,
           l1.jsondoc_ ->> 'Value'::text AS esoutcomeval,
           esgoverallassessment.jsondoc_ ->> 'EsgObligorOutcome'::text AS esgobligoroutcome,
           l2.jsondoc_ ->> 'Value'::text AS esgobligoroutcomeval,
           esgoverallassessment.jsondoc_ ->> 'EsgOverallOutcome'::text AS esgoveralloutcome,
           esgoverallassessment.jsondoc_ ->> 'IsLatestApprovedAssessment'::text AS islatestapprovedassessment,
           esgoverallassessment.jsondoc_ ->> 'IsSustainable'::text AS issustainable,
           l3.jsondoc_ ->> 'Value'::text AS issustainableval,
           esgoverallassessment.jsondoc_ ->> 'LoanApplication'::text AS loanapplication,
           esgoverallassessment.jsondoc_ ->> 'LoanSubApplication'::text AS loansubapplication,
           esgoverallassessment.wfid_,
           esgoverallassessment.taskid_,
           esgoverallassessment.versionid_,
           esgoverallassessment.statusid_,
           esgoverallassessment.isdeleted_,
           esgoverallassessment.islatestversion_,
           esgoverallassessment.baseversionid_,
           esgoverallassessment.contextuserid_,
           esgoverallassessment.isvisible_,
           esgoverallassessment.isvalid_,
           esgoverallassessment.snapshotid_,
           esgoverallassessment.t_,
           esgoverallassessment.createdby_,
           esgoverallassessment.createddate_,
           esgoverallassessment.updatedby_,
           esgoverallassessment.updateddate_,
           esgoverallassessment.fkid_entity,
	       CASE WHEN esgoverallassessment.updateddate_ > esgoverallassessment.createddate_ THEN esgoverallassessment.updatedby_ ELSE esgoverallassessment.createdby_ END AS sourcepopulatedby_,
           GREATEST(esgoverallassessment.createddate_, esgoverallassessment.updateddate_) AS sourcepopulateddate_,
	       current_setting('myvariables.popdate')::timestamp as populateddate_
	FROM madata.esgoverallassessment
	LEFT JOIN madata.custom_lookup l1 ON l1.t_::text = 'EsgRiskCategorization'::text AND (l1.jsondoc_ ->> 'Key'::text) = (esgoverallassessment.jsondoc_ ->> 'EsOutcome'::text)
    LEFT JOIN madata.custom_lookup l2 ON l2.t_::text = 'EsgClassification'::text AND (l2.jsondoc_ ->> 'Key'::text) = (esgoverallassessment.jsondoc_ ->> 'EsgObligorOutcome'::text)
    LEFT JOIN madata.custom_lookup l3 ON l3.t_::text = 'EsgYesNo'::text AND (l3.jsondoc_ ->> 'Key'::text) = (esgoverallassessment.jsondoc_ ->> 'IsSustainable'::text)
	WHERE
		 GREATEST(esgoverallassessment.updateddate_, esgoverallassessment.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) from olapts.refreshhistory where tablename = 'abesgoverallassessment')
		 AND GREATEST(esgoverallassessment.updateddate_, esgoverallassessment.createddate_)::timestamp <= max_refreshhistory
		 AND esgoverallassessment.t_ = 'EsgOverallAssessment'
	;
raise notice '% - Step abesgoverallassessment - part a end', clock_timestamp();

-- If table doesn't exist in refresh history --
ELSE
raise notice '% Step abesgoverallassessment - part b start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abesgoverallassessment;
CREATE TABLE olapts.abesgoverallassessment AS
	select esgoverallassessment.id_ AS id_,
           esgoverallassessment.pkid_,
           esgoverallassessment.id_ AS esgoverallassessmentid_,
           esgoverallassessment.jsondoc_ ->> 'AssessmentDate'::text AS assessmentdate,
           esgoverallassessment.jsondoc_ ->> 'AssessmentUser'::text AS assessmentuser,
           esgoverallassessment.jsondoc_ ->> 'EntityId'::text AS entityid,
           esgoverallassessment.jsondoc_ ->> 'EntityVersionId'::text AS entityversionid,
           esgoverallassessment.jsondoc_ ->> 'EsOutcome'::text AS esoutcome,
           l1.jsondoc_ ->> 'Value'::text AS esoutcomeval,
           esgoverallassessment.jsondoc_ ->> 'EsgObligorOutcome'::text AS esgobligoroutcome,
           l2.jsondoc_ ->> 'Value'::text AS esgobligoroutcomeval,
           esgoverallassessment.jsondoc_ ->> 'EsgOverallOutcome'::text AS esgoveralloutcome,
           esgoverallassessment.jsondoc_ ->> 'IsLatestApprovedAssessment'::text AS islatestapprovedassessment,
           esgoverallassessment.jsondoc_ ->> 'IsSustainable'::text AS issustainable,
           l3.jsondoc_ ->> 'Value'::text AS issustainableval,
           esgoverallassessment.jsondoc_ ->> 'LoanApplication'::text AS loanapplication,
           esgoverallassessment.jsondoc_ ->> 'LoanSubApplication'::text AS loansubapplication,
           esgoverallassessment.wfid_,
           esgoverallassessment.taskid_,
           esgoverallassessment.versionid_,
           esgoverallassessment.statusid_,
           esgoverallassessment.isdeleted_,
           esgoverallassessment.islatestversion_,
           esgoverallassessment.baseversionid_,
           esgoverallassessment.contextuserid_,
           esgoverallassessment.isvisible_,
           esgoverallassessment.isvalid_,
           esgoverallassessment.snapshotid_,
           esgoverallassessment.t_,
           esgoverallassessment.createdby_,
           esgoverallassessment.createddate_,
           esgoverallassessment.updatedby_,
           esgoverallassessment.updateddate_,
           esgoverallassessment.fkid_entity,
	       CASE WHEN esgoverallassessment.updateddate_ > esgoverallassessment.createddate_ THEN esgoverallassessment.updatedby_ ELSE esgoverallassessment.createdby_ END AS sourcepopulatedby_,
           GREATEST(esgoverallassessment.createddate_, esgoverallassessment.updateddate_) AS sourcepopulateddate_,
	       current_setting('myvariables.popdate')::timestamp as populateddate_
	FROM madata.esgoverallassessment
	LEFT JOIN madata.custom_lookup l1 ON l1.t_::text = 'EsgRiskCategorization'::text AND (l1.jsondoc_ ->> 'Key'::text) = (esgoverallassessment.jsondoc_ ->> 'EsOutcome'::text)
    LEFT JOIN madata.custom_lookup l2 ON l2.t_::text = 'EsgClassification'::text AND (l2.jsondoc_ ->> 'Key'::text) = (esgoverallassessment.jsondoc_ ->> 'EsgObligorOutcome'::text)
    LEFT JOIN madata.custom_lookup l3 ON l3.t_::text = 'EsgYesNo'::text AND (l3.jsondoc_ ->> 'Key'::text) = (esgoverallassessment.jsondoc_ ->> 'IsSustainable'::text)
	WHERE
		 GREATEST(esgoverallassessment.updateddate_, esgoverallassessment.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) from olapts.refreshhistory where tablename = 'abesgoverallassessment')
		 AND GREATEST(esgoverallassessment.updateddate_, esgoverallassessment.createddate_)::timestamp <= max_refreshhistory
		 AND esgoverallassessment.t_ = 'EsgOverallAssessment'
;
raise notice '% - Step abesgoverallassessment - part b end', clock_timestamp();

--AbEsgOverallAssessment Indexes--
raise notice '% - Step abesgoverallassessment_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abesgoverallassessment_idx;
DROP INDEX if exists olapts.abesgoverallassessment_idx2;
CREATE INDEX IF NOT EXISTS abesgoverallassessment_idx ON olapts.abesgoverallassessment (id_);
CREATE INDEX IF NOT EXISTS abesgoverallassessment_idx2 ON olapts.abesgoverallassessment (pkid_,versionid_);
REINDEX TABLE olapts.abesgoverallassessment;
ANALYZE olapts.abesgoverallassessment ;	

raise notice '% - Step abesgoverallassessment_idx - part a end', clock_timestamp(); 
END IF;

-- Create or update flag table -- 

raise notice '% step abesgoverallassessment - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abesgoverallassessmentflag;
CREATE TABLE  olapts.abesgoverallassessmentflag AS

select id_,
       pkid_,
       wfid_ wfid_,
       taskid_ taskid_, 
       versionid_ versionid_,
       isdeleted_ isdeleted_,
       islatestversion_ islatestversion_,
       baseversionid_ baseversionid_,
       contextuserid_ contextuserid_,
       isvisible_ isvisible_,
       isvalid_ isvalid_,
       snapshotid_ snapshotid_,
       t_ t_,
       createdby_ createdby_,
       createddate_ createddate_,
       updatedby_ updatedby_,
       updateddate_ updateddate_,
       (case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
       GREATEST(updateddate_, createddate_) as sourcepopulateddate_,
       current_setting('myvariables.popdate')::timestamp as populateddate_
FROM madata.esgoverallassessment
where 
     GREATEST(esgoverallassessment.updateddate_, esgoverallassessment.createddate_)::timestamp <=max_refreshhistory
     and esgoverallassessment.t_ = 'EsgOverallAssessment'
;

----- AbEsgOverallAssessmentFlag Indexes -----

raise notice '% - Step abesgoverallassessmentflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abesgoverallassessmentflag_idx;
DROP INDEX if exists olapts.abesgoverallassessmentflag_idx2;
CREATE INDEX IF NOT EXISTS abesgoverallassessmentflag_idx ON olapts.abesgoverallassessmentflag (id_);
CREATE INDEX IF NOT EXISTS abesgoverallassessmentflag_idx2 ON olapts.abesgoverallassessmentflag (pkid_,versionid_);
REINDEX TABLE olapts.abesgoverallassessmentflag;
ANALYZE olapts.abesgoverallassessmentflag ;

raise notice '% - Step abesgoverallassessmentflag_idx - part a end', clock_timestamp(); 

-- Update refresh history -- 

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'abesgoverallassessment';
delete from olapts.refreshhistory where tablename = 'abesgoverallassessment';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'abesgoverallassessment' tablename,max_refreshhistory as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'abesgoverallassessmentflag';
delete from olapts.refreshhistory where tablename = 'abesgoverallassessmentflag';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'abesgoverallassessmentflag' tablename,max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abesgoverallassessment - part c end', clock_timestamp();

--END $$;

-------------------REF Data--------------------------

--DO $$
--DECLARE
--varprevsuccessdate TIMESTAMP ;
--BEGIN

--AbEsgRiskCategorization

raise notice '% - Step AbEsgRiskCategorization - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abesgriskcategorization;
CREATE TABLE olapts.abesgriskcategorization AS
select l.jsondoc_->>'Key' esgriskcategorizationkey_,
       l.jsondoc_->>'Value' esgriskcategorizationvalue,
       isdeleted_,
       t_ t_ ,
       (case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
       GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
       current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'EsgRiskCategorization';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'abesgriskcategorization';
delete from olapts.refreshhistory where tablename = 'abesgriskcategorization';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'abesgriskcategorization' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abesgriskcategorization - part a end', clock_timestamp();

--AbEsgClassification

raise notice '% - Step AbEsgClassification - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abesgclassification;
CREATE TABLE olapts.abesgclassification AS
select l.jsondoc_->>'Key' esgclassificationkey_,
       l.jsondoc_->>'Value' esgclassificationvalue,
       isdeleted_,
       t_ t_ ,
       (case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
       GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
       current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'EsgClassification';

-- Update refresh history -- 

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'abesgclassification';
delete from olapts.refreshhistory where tablename = 'abesgclassification';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'abesgclassification' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abesgclassification - part a end', clock_timestamp();

--AbEsgYesNo

raise notice '% - Step AbEsgYesNo - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abesgyesno;
CREATE TABLE olapts.abesgyesno AS
select l.jsondoc_->>'Key' esgyesnokey_,
       l.jsondoc_->>'Value' esgyesnovalue,
       isdeleted_,
       t_ t_ ,
       (case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
       GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
       current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'EsgYesNo';

-- Update refresh history -- 

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'abesgyesno';
delete from olapts.refreshhistory where tablename = 'abesgyesno';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'abesgyesno' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abesgyesno - part a end', clock_timestamp();

--END $$;

---------------END--ESG---------------------------------

---------------------------------------------- ABMODELSTARTUPGR ----------------------------------------------

-- Custom MAP Models - New Rating model 1: "StartUp Gr" (Id: PdModelStartUpGr)

IF EXISTS (select from olapts.refreshhistory where tablename = 'ABMODELSTARTUPGR') THEN
raise notice '% - Step abmodelstartupgr - part a start', clock_timestamp();
insert into olapts.abmodelstartupgr
SELECT mi.id_ factmappdinstanceid_,
       mi.pkid_::varchar as pkid_,
       (mi.jsondoc_->>'AttractivenessSubmitted') attractivenesssubmitted,
       l1.jsondoc_->>'Value' attractivenesssubmittedval,
       mi.jsondoc_->>'AttractivenessSubmittedScore' attractivenesssubmittedscore,
       mi.jsondoc_->>'AttractivenessSubmittedWeight' attractivenesssubmittedweight,
       (mi.jsondoc_->>'AvailabilityAssets') availabilityassets,
       l2.jsondoc_->>'Value' availabilityassetsval,
       mi.jsondoc_->>'AvailabilityAssetsScore' availabilityassetsscore,
       mi.jsondoc_->>'AvailabilityAssetsWeight' availabilityassetsweight,
       (mi.jsondoc_->>'AvailableFinancialData') availablefinancialdata,
       l3.jsondoc_->>'Value' availablefinancialdataval,
       mi.jsondoc_->>'AvailableFinancialDataScore' availablefinancialdatascore,
       mi.jsondoc_->>'AvailableFinancialDataWeight' availablefinancialdataweight,
       (mi.jsondoc_->>'CollateralPledgedCash') collateralpledgedcash,
       l4.jsondoc_->>'Value' collateralpledgedcashval,
       mi.jsondoc_->>'CollateralPledgedCashScore' collateralpledgedcashscore,
       mi.jsondoc_->>'CollateralPledgedCashWeight' collateralpledgedcashweight,
       (mi.jsondoc_->>'CompetenceExperience') competenceexperience,
       l5.jsondoc_->>'Value' competenceexperienceval,
       mi.jsondoc_->>'CompetenceExperienceScore' competenceexperiencescore,
       mi.jsondoc_->>'CompetenceExperienceWeight' competenceexperienceweight,
       (mi.jsondoc_->>'CreditHistory') credithistory,
       l6.jsondoc_->>'Value' credithistoryval,
       mi.jsondoc_->>'CreditHistoryScore' credithistoryscore,
       mi.jsondoc_->>'CreditHistoryWeight' credithistoryweight,
       (mi.jsondoc_->>'ElementsCompanyCapital') elementscompanycapital,
       l7.jsondoc_->>'Value' elementscompanycapitalval,
       mi.jsondoc_->>'ElementsCompanyCapitalScore' elementscompanycapitalscore,
       mi.jsondoc_->>'ElementsCompanyCapitalWeight' elementscompanycapitalweight,
       mi.jsondoc_->>'FixedTermScore' fixedtermscore,
       mi.jsondoc_->>'FixedTermWeight' fixedtermweight,
       mi.jsondoc_->>'Grade' grade,
       mi.jsondoc_->>'GreekState' greekstate,
       (mi.jsondoc_->>'GreekStateCompanies') greekstatecompanies,
       l8.jsondoc_->>'Value' greekstatecompaniesval,
       mi.jsondoc_->>'GreekStateCompaniesScore' greekstatecompaniesscore,
       mi.jsondoc_->>'GreekStateCompaniesWeight' greekstatecompaniesweight,
       mi.jsondoc_->>'GreekStateWeight' greekstateweight,
       (mi.jsondoc_->>'IndustryAttractivenes') industryattractivenes,
       l9.jsondoc_->>'Value' industryattractivenesval,
       mi.jsondoc_->>'IndustryAttractivenesScore' industryattractivenesscore,
       mi.jsondoc_->>'IndustryAttractivenesWeight' industryattractivenesweight,
       mi.jsondoc_->>'Other' other,
       mi.jsondoc_->>'OtherWeight' otherweight,
       mi.jsondoc_->>'Pd' pd,
       mi.jsondoc_->>'QualitativeData' qualitativedata,
       mi.jsondoc_->>'QualitativeDataWeight' qualitativedataweight,
       mi.jsondoc_->>'Quantitative' quantitative,
       mi.jsondoc_->>'QuantitativeWeight' quantitativeweight,
       mi.jsondoc_->>'Score' score,
       (mi.jsondoc_->>'StrongParentCompany') strongparentcompany,
       l10.jsondoc_->>'Value' strongparentcompanyval,
       mi.jsondoc_->>'StrongParentCompanyScore' strongparentcompanyscore,
       mi.jsondoc_->>'StrongParentCompanyWeight' strongparentcompanyweight,
       (mi.jsondoc_->>'Token')::varchar token,
       mi.wfid_::varchar ,
       mi.taskid_::varchar ,
       mi.versionid_::int4 ,
	   mi.statusid_::int4,
       mi.isdeleted_::boolean ,
       mi.islatestversion_::boolean ,
       mi.baseversionid_::int4 ,
       mi.contextuserid_::varchar ,
       mi.isvisible_::boolean ,
       mi.isvalid_::boolean ,
       mi.snapshotid_::int4 ,
       mi.t_::varchar ,
       mi.createdby_::varchar ,
       mi.createddate_::timestamp ,
       mi.updatedby_::varchar ,
       mi.updateddate_::timestamp ,
       mi.fkid_entity,
       (case when mi.updateddate_>mi.createddate_ then mi.updatedby_ else mi.createdby_ end) as sourcepopulatedby_,
       GREATEST(mi.updateddate_,mi.createddate_) as sourcepopulateddate_ ,
       current_setting('myvariables.popdate')::timestamp as populateddate_
from madata.mapinstance mi
left join madata.custom_lookup l1 on l1.t_ = 'AttractivenessSubmittedSu' and l1.jsondoc_->>'Id' = mi.jsondoc_ ->> 'AttractivenessSubmitted'
left join madata.custom_lookup l2 on l2.t_ = 'AvailabilityAssetsSu' and l2.jsondoc_->>'Id' = mi.jsondoc_ ->> 'AvailabilityAssets'
left join madata.custom_lookup l3 on l3.t_ = 'AvailableFinancialDataSu' and l3.jsondoc_->>'Id' = mi.jsondoc_ ->> 'AvailableFinancialData'
left join madata.custom_lookup l4 on l4.t_ = 'CollateralPledgedCashSu' and l4.jsondoc_->>'Id' = mi.jsondoc_ ->> 'CollateralPledgedCash'
left join madata.custom_lookup l5 on l5.t_ = 'CompetenceExperienceSu' and l5.jsondoc_->>'Id' = mi.jsondoc_ ->> 'CompetenceExperience'
left join madata.custom_lookup l6 on l6.t_ = 'CreditHistorySu' and l6.jsondoc_->>'Id' = mi.jsondoc_ ->> 'CreditHistory'
left join madata.custom_lookup l7 on l7.t_ = 'ElementsCompanyCapitalSu' and l7.jsondoc_->>'Id' = mi.jsondoc_ ->> 'ElementsCompanyCapital'
left join madata.custom_lookup l8 on l8.t_ = 'GreekStateCompaniesSu' and l8.jsondoc_->>'Id' = mi.jsondoc_ ->> 'GreekStateCompanies'
left join madata.custom_lookup l9 on l9.t_ = 'IndustryAttractivenesSu' and l9.jsondoc_->>'Id' = mi.jsondoc_ ->> 'IndustryAttractivenes'
left join madata.custom_lookup l10 on l10.t_ = 'StrongParentCompanySu' and l10.jsondoc_->>'Id' = mi.jsondoc_ ->> 'StrongParentCompany'
where GREATEST(mi.updateddate_,mi.createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABMODELSTARTUPGR')
      and GREATEST(mi.updateddate_,mi.createddate_)::timestamp <=  max_refreshhistory
      and mi.t_ = 'PdModelStartUpGr'
;

raise notice '% - Step abmodelstartupgr - part a end', clock_timestamp();
ELSE
raise notice '% - Step abmodelstartupgr - part b start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abmodelstartupgr;
CREATE TABLE olapts.abmodelstartupgr AS
SELECT mi.id_ factmappdinstanceid_,
       mi.pkid_::varchar as pkid_,
       (mi.jsondoc_->>'AttractivenessSubmitted') attractivenesssubmitted,
       l1.jsondoc_->>'Value' attractivenesssubmittedval,
       mi.jsondoc_->>'AttractivenessSubmittedScore' attractivenesssubmittedscore,
       mi.jsondoc_->>'AttractivenessSubmittedWeight' attractivenesssubmittedweight,
       (mi.jsondoc_->>'AvailabilityAssets') availabilityassets,
       l2.jsondoc_->>'Value' availabilityassetsval,
       mi.jsondoc_->>'AvailabilityAssetsScore' availabilityassetsscore,
       mi.jsondoc_->>'AvailabilityAssetsWeight' availabilityassetsweight,
       (mi.jsondoc_->>'AvailableFinancialData') availablefinancialdata,
       l3.jsondoc_->>'Value' availablefinancialdataval,
       mi.jsondoc_->>'AvailableFinancialDataScore' availablefinancialdatascore,
       mi.jsondoc_->>'AvailableFinancialDataWeight' availablefinancialdataweight,
       (mi.jsondoc_->>'CollateralPledgedCash') collateralpledgedcash,
       l4.jsondoc_->>'Value' collateralpledgedcashval,
       mi.jsondoc_->>'CollateralPledgedCashScore' collateralpledgedcashscore,
       mi.jsondoc_->>'CollateralPledgedCashWeight' collateralpledgedcashweight,
       (mi.jsondoc_->>'CompetenceExperience') competenceexperience,
       l5.jsondoc_->>'Value' competenceexperienceval,
       mi.jsondoc_->>'CompetenceExperienceScore' competenceexperiencescore,
       mi.jsondoc_->>'CompetenceExperienceWeight' competenceexperienceweight,
       (mi.jsondoc_->>'CreditHistory') credithistory,
       l6.jsondoc_->>'Value' credithistoryval,
       mi.jsondoc_->>'CreditHistoryScore' credithistoryscore,
       mi.jsondoc_->>'CreditHistoryWeight' credithistoryweight,
       (mi.jsondoc_->>'ElementsCompanyCapital') elementscompanycapital,
       l7.jsondoc_->>'Value' elementscompanycapitalval,
       mi.jsondoc_->>'ElementsCompanyCapitalScore' elementscompanycapitalscore,
       mi.jsondoc_->>'ElementsCompanyCapitalWeight' elementscompanycapitalweight,
       mi.jsondoc_->>'FixedTermScore' fixedtermscore,
       mi.jsondoc_->>'FixedTermWeight' fixedtermweight,
       mi.jsondoc_->>'Grade' grade,
       mi.jsondoc_->>'GreekState' greekstate,
       (mi.jsondoc_->>'GreekStateCompanies') greekstatecompanies,
       l8.jsondoc_->>'Value' greekstatecompaniesval,
       mi.jsondoc_->>'GreekStateCompaniesScore' greekstatecompaniesscore,
       mi.jsondoc_->>'GreekStateCompaniesWeight' greekstatecompaniesweight,
       mi.jsondoc_->>'GreekStateWeight' greekstateweight,
       (mi.jsondoc_->>'IndustryAttractivenes') industryattractivenes,
       l9.jsondoc_->>'Value' industryattractivenesval,
       mi.jsondoc_->>'IndustryAttractivenesScore' industryattractivenesscore,
       mi.jsondoc_->>'IndustryAttractivenesWeight' industryattractivenesweight,
       mi.jsondoc_->>'Other' other,
       mi.jsondoc_->>'OtherWeight' otherweight,
       mi.jsondoc_->>'Pd' pd,
       mi.jsondoc_->>'QualitativeData' qualitativedata,
       mi.jsondoc_->>'QualitativeDataWeight' qualitativedataweight,
       mi.jsondoc_->>'Quantitative' quantitative,
       mi.jsondoc_->>'QuantitativeWeight' quantitativeweight,
       mi.jsondoc_->>'Score' score,
       (mi.jsondoc_->>'StrongParentCompany') strongparentcompany,
       l10.jsondoc_->>'Value' strongparentcompanyval,
       mi.jsondoc_->>'StrongParentCompanyScore' strongparentcompanyscore,
       mi.jsondoc_->>'StrongParentCompanyWeight' strongparentcompanyweight,
       (mi.jsondoc_->>'Token')::varchar token,
       mi.wfid_::varchar ,
       mi.taskid_::varchar ,
       mi.versionid_::int4 ,
	   mi.statusid_::int4,	   
       mi.isdeleted_::boolean ,
       mi.islatestversion_::boolean ,
       mi.baseversionid_::int4 ,
       mi.contextuserid_::varchar ,
       mi.isvisible_::boolean ,
       mi.isvalid_::boolean ,
       mi.snapshotid_::int4 ,
       mi.t_::varchar ,
       mi.createdby_::varchar ,
       mi.createddate_::timestamp ,
       mi.updatedby_::varchar ,
       mi.updateddate_::timestamp ,
       mi.fkid_entity,
       (case when mi.updateddate_>mi.createddate_ then mi.updatedby_ else mi.createdby_ end) as sourcepopulatedby_,
       GREATEST(mi.updateddate_,mi.createddate_) as sourcepopulateddate_ ,
       current_setting('myvariables.popdate')::timestamp as populateddate_
from madata.mapinstance mi
left join madata.custom_lookup l1 on l1.t_ = 'AttractivenessSubmittedSu' and l1.jsondoc_->>'Id' = mi.jsondoc_ ->> 'AttractivenessSubmitted'
left join madata.custom_lookup l2 on l2.t_ = 'AvailabilityAssetsSu' and l2.jsondoc_->>'Id' = mi.jsondoc_ ->> 'AvailabilityAssets'
left join madata.custom_lookup l3 on l3.t_ = 'AvailableFinancialDataSu' and l3.jsondoc_->>'Id' = mi.jsondoc_ ->> 'AvailableFinancialData'
left join madata.custom_lookup l4 on l4.t_ = 'CollateralPledgedCashSu' and l4.jsondoc_->>'Id' = mi.jsondoc_ ->> 'CollateralPledgedCash'
left join madata.custom_lookup l5 on l5.t_ = 'CompetenceExperienceSu' and l5.jsondoc_->>'Id' = mi.jsondoc_ ->> 'CompetenceExperience'
left join madata.custom_lookup l6 on l6.t_ = 'CreditHistorySu' and l6.jsondoc_->>'Id' = mi.jsondoc_ ->> 'CreditHistory'
left join madata.custom_lookup l7 on l7.t_ = 'ElementsCompanyCapitalSu' and l7.jsondoc_->>'Id' = mi.jsondoc_ ->> 'ElementsCompanyCapital'
left join madata.custom_lookup l8 on l8.t_ = 'GreekStateCompaniesSu' and l8.jsondoc_->>'Id' = mi.jsondoc_ ->> 'GreekStateCompanies'
left join madata.custom_lookup l9 on l9.t_ = 'IndustryAttractivenesSu' and l9.jsondoc_->>'Id' = mi.jsondoc_ ->> 'IndustryAttractivenes'
left join madata.custom_lookup l10 on l10.t_ = 'StrongParentCompanySu' and l10.jsondoc_->>'Id' = mi.jsondoc_ ->> 'StrongParentCompany'
where GREATEST(mi.updateddate_,mi.createddate_) > (select COALESCE(max(asofdate),to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABMODELSTARTUPGR')
      and GREATEST(mi.updateddate_,mi.createddate_)::timestamp <=  max_refreshhistory
      and mi.t_ = 'PdModelStartUpGr'
;

raise notice '% - Step abmodelstartupgr - part b end', clock_timestamp();

--abmodelstartupgr
raise notice '% - Step abmodelstartupgr_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abmodelstartupgr_idx;
DROP INDEX if exists olapts.abmodelstartupgr_idx2;
CREATE INDEX IF NOT EXISTS abmodelstartupgr_idx ON olapts.abmodelstartupgr (factmappdinstanceid_,pkid_,wfid_);
CREATE INDEX IF NOT EXISTS abmodelstartupgr_idx2 ON olapts.abmodelstartupgr (pkid_,versionid_,sourcepopulateddate_,wfid_) include(isvisible_,isvalid_,isdeleted_,islatestversion_,createdby_,createddate_,updatedby_,updateddate_,sourcepopulatedby_);	

raise notice '% - Step abmodelstartupgr_idx - part a end', clock_timestamp(); 
END IF;

raise notice '% - Step abmodelstartupgr - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abmodelstartupgrflag;
CREATE TABLE olapts.abmodelstartupgrflag AS
select id_,
       pkid_,
       wfid_ wfid_ ,
       taskid_ taskid_ ,
       versionid_ versionid_ ,
       isdeleted_::boolean isdeleted_ ,
       islatestversion_::boolean islatestversion_ ,
       baseversionid_ baseversionid_ ,
       contextuserid_ contextuserid_ ,
       isvisible_::boolean isvisible_ ,
       isvalid_::boolean isvalid_ ,
       snapshotid_ snapshotid_ ,
       t_ t_ ,
       createdby_ createdby_ ,
       createddate_ createddate_ ,
       updatedby_ updatedby_ ,
       updateddate_ updateddate_ ,
       (case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
       GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
       current_setting('myvariables.popdate')::timestamp as populateddate_ 
FROM madata.mapinstance mi
where GREATEST(updateddate_,createddate_)::timestamp <=  max_refreshhistory
      and mi.t_ = 'PdModelStartUpGr'
;

----- abmodelstartupgrflag Indexes -----

raise notice '% - Step abmodelstartupgrflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.abmodelstartupgrflag_idx;
DROP INDEX if exists olapts.abmodelstartupgrflag_idx2;
CREATE INDEX IF NOT EXISTS abmodelstartupgrflag_idx ON olapts.abmodelstartupgrflag (id_,wfid_);
CREATE INDEX IF NOT EXISTS abmodelstartupgrflag_idx2 ON olapts.abmodelstartupgrflag (pkid_,versionid_,sourcepopulateddate_) include(isvisible_,isvalid_,isdeleted_,islatestversion_,createdby_,createddate_,updatedby_,updateddate_,sourcepopulatedby_,wfid_);
REINDEX TABLE olapts.abmodelstartupgrflag;
ANALYZE olapts.abmodelstartupgrflag ;

raise notice '% - Step abmodelstartupgrflag_idx - part a end', clock_timestamp(); 

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABMODELSTARTUPGR';
delete from olapts.refreshhistory where tablename = 'ABMODELSTARTUPGR';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABMODELSTARTUPGR' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABMODELSTARTUPGRFLAG';
delete from olapts.refreshhistory where tablename = 'ABMODELSTARTUPGRFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABMODELSTARTUPGRFLAG' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abmodelstartupgr - part c end', clock_timestamp();
-- End Custom MAP Models - New Rating model 1: "StartUp Gr" (Id: PdModelStartUpGr)

--END $$; 

-------------------REF Data--------------------------

--DO $$
--DECLARE
--varprevsuccessdate TIMESTAMP ;
--BEGIN

--AbAttractivenessSubmittedSu

raise notice '% - Step abattractivenesssubmittedsu - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abattractivenesssubmittedsu;
CREATE TABLE olapts.abattractivenesssubmittedsu AS
select l.jsondoc_->>'Id' attractivenesssubmittedsukey_,
       l.jsondoc_->>'Value' attractivenesssubmittedsuvalue,
	   l.jsondoc_->>'Score' attractivenesssubmittedsuscore,
       isdeleted_::boolean,
       t_ t_ ,
       (case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
       GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
       current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'AttractivenessSubmittedSu';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABATTRACTIVENESSSUBMITTEDSU';
delete from olapts.refreshhistory where tablename = 'ABATTRACTIVENESSSUBMITTEDSU';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABATTRACTIVENESSSUBMITTEDSU' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abattractivenesssubmittedsu - part a end', clock_timestamp();

--AbAvailabilityAssetsSu

raise notice '% - Step AbAvailabilityAssetsSu - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abavailabilityassetssu;
CREATE TABLE olapts.abavailabilityassetssu AS
select l.jsondoc_->>'Id' availabilityassetssukey_,
       l.jsondoc_->>'Value' availabilityassetssuvalue,
	   l.jsondoc_->>'Score' availabilityassetssuscore,
       isdeleted_::boolean,
       t_ t_ ,
       (case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
       GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
       current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'AvailabilityAssetsSu';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABAVAILABILITYASSETSSU';
delete from olapts.refreshhistory where tablename = 'ABAVAILABILITYASSETSSU';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABAVAILABILITYASSETSSU' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abavailabilityassetssu - part a end', clock_timestamp();

--AbAvailableFinancialDataSu

raise notice '% - Step AbAvailableFinancialDataSu - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abavailablefinancialdatasu;
CREATE TABLE olapts.abavailablefinancialdatasu AS
select l.jsondoc_->>'Id' availablefinancialdatasukey_,
       l.jsondoc_->>'Value' availablefinancialdatasuvalue,
	   l.jsondoc_->>'Score' availablefinancialdatasuscore,
       isdeleted_::boolean,
       t_ t_ ,
       (case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
       GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
       current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'AvailableFinancialDataSu';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABAVAILABLEFINANCIALDATASU';
delete from olapts.refreshhistory where tablename = 'ABAVAILABLEFINANCIALDATASU';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABAVAILABLEFINANCIALDATASU' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abavailablefinancialdatasu - part a end', clock_timestamp();

--AbCollateralPledgedCashSu

raise notice '% - Step AbCollateralPledgedCashSu - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abcollateralpledgedcashsu;
CREATE TABLE olapts.abcollateralpledgedcashsu AS
select l.jsondoc_->>'Id' collateralpledgedcashsukey_,
       l.jsondoc_->>'Value' collateralpledgedcashsuvalue,
	   l.jsondoc_->>'Score' collateralpledgedcashsuscore,
       isdeleted_::boolean,
       t_ t_ ,
       (case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
       GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
       current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'CollateralPledgedCashSu';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABCOLLATERALPLEDGEDCASHSU';
delete from olapts.refreshhistory where tablename = 'ABCOLLATERALPLEDGEDCASHSU';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABCOLLATERALPLEDGEDCASHSU' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abcollateralpledgedcashsu - part a end', clock_timestamp();

--AbCompetenceExperienceSu

raise notice '% - Step AbCompetenceExperienceSu - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abcompetenceexperiencesu;
CREATE TABLE olapts.abcompetenceexperiencesu AS
select l.jsondoc_->>'Id' competenceexperiencesukey_,
       l.jsondoc_->>'Value' competenceexperiencesuvalue,
	   l.jsondoc_->>'Score' competenceexperiencesuscore,
       isdeleted_::boolean,
       t_ t_ ,
       (case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
       GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
       current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'CompetenceExperienceSu';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABCOMPETENCEEXPERIENCESU';
delete from olapts.refreshhistory where tablename = 'ABCOMPETENCEEXPERIENCESU';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABCOMPETENCEEXPERIENCESU' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abcompetenceexperiencesu - part a end', clock_timestamp();

--AbCreditHistorySu

raise notice '% - Step AbCreditHistorySu - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abcredithistorysu;
CREATE TABLE olapts.abcredithistorysu AS
select l.jsondoc_->>'Id' credithistorysukey_,
       l.jsondoc_->>'Value' credithistorysuvalue,
	   l.jsondoc_->>'Score' credithistorysuscore,
       isdeleted_::boolean,
       t_ t_ ,
       (case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
       GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
       current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'CreditHistorySu';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABCREDITHISTORYSU';
delete from olapts.refreshhistory where tablename = 'ABCREDITHISTORYSU';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABCREDITHISTORYSU' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abcredithistorysu - part a end', clock_timestamp();

--AbElementsCompanyCapitalSu

raise notice '% - Step AbElementsCompanyCapitalSu - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abelementscompanycapitalsu;
CREATE TABLE olapts.abelementscompanycapitalsu AS
select l.jsondoc_->>'Id' elementscompanycapitalsukey_,
       l.jsondoc_->>'Value' elementscompanycapitalsuvalue,
	   l.jsondoc_->>'Score' elementscompanycapitalsuscore,
       isdeleted_::boolean,
       t_ t_ ,
       (case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
       GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
       current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'ElementsCompanyCapitalSu';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABELEMENTSCOMPANYCAPITALSU';
delete from olapts.refreshhistory where tablename = 'ABELEMENTSCOMPANYCAPITALSU';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABELEMENTSCOMPANYCAPITALSU' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abelementscompanycapitalsu - part a end', clock_timestamp();

--AbGreekStateCompaniesSu

raise notice '% - Step AbGreekStateCompaniesSu - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abgreekstatecompaniessu;
CREATE TABLE olapts.abgreekstatecompaniessu AS
select l.jsondoc_->>'Id' greekstatecompaniessukey_,
       l.jsondoc_->>'Value' greekstatecompaniessuvalue,
	   l.jsondoc_->>'Score' greekstatecompaniessuscore,
       isdeleted_::boolean,
       t_ t_ ,
       (case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
       GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
       current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'GreekStateCompaniesSu';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABGREEKSTATECOMPANIESSU';
delete from olapts.refreshhistory where tablename = 'ABGREEKSTATECOMPANIESSU';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABGREEKSTATECOMPANIESSU' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abgreekstatecompaniessu - part a end', clock_timestamp();

--AbIndustryAttractivenesSu

raise notice '% - Step AbIndustryAttractivenesSu - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abindustryattractivenessu;
CREATE TABLE olapts.abindustryattractivenessu AS
select l.jsondoc_->>'Id' industryattractivenessukey_,
       l.jsondoc_->>'Value' industryattractivenessuvalue,
	   l.jsondoc_->>'Score' industryattractivenessuscore,
       isdeleted_::boolean,
       t_ t_ ,
       (case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
       GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
       current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'IndustryAttractivenesSu';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABINDUSTRYATTRACTIVENESSU';
delete from olapts.refreshhistory where tablename = 'ABINDUSTRYATTRACTIVENESSU';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABINDUSTRYATTRACTIVENESSU' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abindustryattractivenessu - part a end', clock_timestamp();

--AbStrongParentCompanySu

raise notice '% - Step AbStrongParentCompanySu - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abstrongparentcompanysu;
CREATE TABLE olapts.abstrongparentcompanysu AS
select l.jsondoc_->>'Id' strongparentcompanysukey_,
       l.jsondoc_->>'Value' strongparentcompanysuvalue,
	   l.jsondoc_->>'Score' strongparentcompanysuscore,
       isdeleted_::boolean,
       t_ t_ ,
       (case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
       GREATEST(updateddate_,createddate_) as sourcepopulateddate_,
       current_setting('myvariables.popdate')::timestamp as populateddate_ 
from madata.v_lookup l
where l.t_ = 'StrongParentCompanySu';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABSTRONGPARENTCOMPANYSU';
delete from olapts.refreshhistory where tablename = 'ABSTRONGPARENTCOMPANYSU';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABSTRONGPARENTCOMPANYSU' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abstrongparentcompanysu - part a end', clock_timestamp();

--END $$;

---------------END--StartUp---------------------------------

------------------Start OperatingRisk----------------------------------------------------------------------------------------------------------------------------

---- OperatingRiskAssessment ---- --added 11/06/2024

--DO $$
--DECLARE
--varprevsuccessdate TIMESTAMP ;
--BEGIN

-- If table exists in refresh history --
IF EXISTS (select from olapts.refreshhistory where tablename = 'ABOPERATINGRISKASSESSMENT') THEN
	raise notice '% - Step aboperatingriskassessment - part a start', clock_timestamp();
	insert into olapts.aboperatingriskassessment
		SELECT
		operatingriskassessment.id_ AS id_,
		operatingriskassessment.pkid_::varchar as pkid_,	
		(operatingriskassessment.jsondoc_->> 'OprId') AS oprid,
		(operatingriskassessment.jsondoc_->> 'ApprovalDate')::timestamp AS approvaldate, 		
		(operatingriskassessment.jsondoc_->> 'ApprovalUser') AS approvaluser,	
		(operatingriskassessment.jsondoc_->> 'ApproverComments') AS approvercomments, 	
		(operatingriskassessment.jsondoc_->> 'AssessmentDate')::timestamp AS assessmentdate,
		(operatingriskassessment.jsondoc_->> 'AssessmentUser') AS assessmentuser,
		(operatingriskassessment.jsondoc_->> 'AuthorizationFlag')::boolean AS authorizationflag,		
		(operatingriskassessment.jsondoc_->> 'Comments') AS comments,		
		(operatingriskassessment.jsondoc_->> 'CreditCommittee') AS creditcommittee,	
		(l0.jsondoc_->> 'Value') AS creditcommitteeval,	
		(operatingriskassessment.jsondoc_->> 'CreditCommitteeDate')::timestamp AS creditcommitteedate,		
		(operatingriskassessment.jsondoc_->> 'EntityId')::numeric AS entityId,		
		(operatingriskassessment.jsondoc_->> 'EntityVersionId')::numeric AS entityVersionId,		
		(operatingriskassessment.jsondoc_->> 'IsLatestApproved')::boolean AS islatestapproved,		
		(operatingriskassessment.jsondoc_->> 'OperatingRiskFlag') AS operatingriskflag,		
		(l1.jsondoc_->> 'Value') AS operatingriskflagval,		
		operatingriskassessment.wfid_::varchar,
		operatingriskassessment.taskid_::varchar,
		operatingriskassessment.versionid_::int4,
		operatingriskassessment.isdeleted_::boolean,
		operatingriskassessment.islatestversion_::boolean,
		operatingriskassessment.baseversionid_::int4,
		operatingriskassessment.contextuserid_::varchar,
		operatingriskassessment.isvisible_::boolean,
		operatingriskassessment.isvalid_::boolean,
		operatingriskassessment.snapshotid_::int4,
		operatingriskassessment.t_::varchar,
		operatingriskassessment.createdby_::varchar,
		operatingriskassessment.createddate_::timestamp,
		operatingriskassessment.updatedby_::varchar,
		operatingriskassessment.updateddate_::timestamp,
		operatingriskassessment.fkid_entity,
		CASE WHEN operatingriskassessment.updateddate_ > operatingriskassessment.createddate_ THEN operatingriskassessment.updatedby_ ELSE operatingriskassessment.createdby_ END AS sourcepopulatedby_,
		GREATEST(operatingriskassessment.createddate_, operatingriskassessment.updateddate_) AS sourcepopulateddate_
		,current_setting('myvariables.popdate')::timestamp as populateddate_
		FROM madata.operatingriskassessment
		LEFT JOIN madata.custom_lookup as l0 ON l0.t_ = 'CreditCommittee' and l0.jsondoc_->>'Key' = operatingriskassessment.jsondoc_->>'CreditCommittee'		
		LEFT JOIN madata.custom_lookup as l1 ON l1.t_ = 'OprOperatingRiskFlag' and l1.jsondoc_->>'Key' = operatingriskassessment.jsondoc_->>'OperatingRiskFlag'		
		WHERE
		GREATEST(operatingriskassessment.updateddate_, operatingriskassessment.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABOPERATINGRISKASSESSMENT')
		AND GREATEST(operatingriskassessment.updateddate_, operatingriskassessment.createddate_)::timestamp <= max_refreshhistory 
		AND operatingriskassessment.t_ = 'OperatingRiskAssessment';
raise notice '% - Step aboperatingriskassessment - part a end', clock_timestamp();
ELSE
raise notice '% Step aboperatingriskassessment - part b start', clock_timestamp();
	DROP TABLE IF EXISTS olapts.aboperatingriskassessment;
	CREATE TABLE olapts.aboperatingriskassessment AS
		SELECT
		operatingriskassessment.id_ AS id_,
		operatingriskassessment.pkid_::varchar as pkid_,	
		(operatingriskassessment.jsondoc_->> 'OprId') AS oprid,
		(operatingriskassessment.jsondoc_->> 'ApprovalDate')::timestamp AS approvaldate, 		
		(operatingriskassessment.jsondoc_->> 'ApprovalUser') AS approvaluser,	
		(operatingriskassessment.jsondoc_->> 'ApproverComments') AS approvercomments, 	
		(operatingriskassessment.jsondoc_->> 'AssessmentDate')::timestamp AS assessmentdate,
		(operatingriskassessment.jsondoc_->> 'AssessmentUser') AS assessmentuser,
		(operatingriskassessment.jsondoc_->> 'AuthorizationFlag')::boolean AS authorizationflag,		
		(operatingriskassessment.jsondoc_->> 'Comments') AS comments,		
		(operatingriskassessment.jsondoc_->> 'CreditCommittee') AS creditcommittee,	
		(l0.jsondoc_->> 'Value') AS creditcommitteeval,	
		(operatingriskassessment.jsondoc_->> 'CreditCommitteeDate')::timestamp AS creditcommitteedate,		
		(operatingriskassessment.jsondoc_->> 'EntityId')::numeric AS entityId,		
		(operatingriskassessment.jsondoc_->> 'EntityVersionId')::numeric AS entityVersionId,		
		(operatingriskassessment.jsondoc_->> 'IsLatestApproved')::boolean AS islatestapproved,		
		(operatingriskassessment.jsondoc_->> 'OperatingRiskFlag') AS operatingriskflag,		
		(l1.jsondoc_->> 'Value') AS operatingriskflagval,		
		operatingriskassessment.wfid_::varchar,
		operatingriskassessment.taskid_::varchar,
		operatingriskassessment.versionid_::int4,
		operatingriskassessment.isdeleted_::boolean,
		operatingriskassessment.islatestversion_::boolean,
		operatingriskassessment.baseversionid_::int4,
		operatingriskassessment.contextuserid_::varchar,
		operatingriskassessment.isvisible_::boolean,
		operatingriskassessment.isvalid_::boolean,
		operatingriskassessment.snapshotid_::int4,
		operatingriskassessment.t_::varchar,
		operatingriskassessment.createdby_::varchar,
		operatingriskassessment.createddate_::timestamp,
		operatingriskassessment.updatedby_::varchar,
		operatingriskassessment.updateddate_::timestamp,
		operatingriskassessment.fkid_entity,
		CASE WHEN operatingriskassessment.updateddate_ > operatingriskassessment.createddate_ THEN operatingriskassessment.updatedby_ ELSE operatingriskassessment.createdby_ END AS sourcepopulatedby_,
		GREATEST(operatingriskassessment.createddate_, operatingriskassessment.updateddate_) AS sourcepopulateddate_
		,current_setting('myvariables.popdate')::timestamp as populateddate_
		FROM madata.operatingriskassessment
		LEFT JOIN madata.custom_lookup as l0 ON l0.t_ = 'CreditCommittee' and l0.jsondoc_->>'Key' = operatingriskassessment.jsondoc_->>'CreditCommittee'		
		LEFT JOIN madata.custom_lookup as l1 ON l1.t_ = 'OprOperatingRiskFlag' and l1.jsondoc_->>'Key' = operatingriskassessment.jsondoc_->>'OperatingRiskFlag'		
		WHERE
		GREATEST(operatingriskassessment.updateddate_, operatingriskassessment.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABOPERATINGRISKASSESSMENT')
		AND GREATEST(operatingriskassessment.updateddate_, operatingriskassessment.createddate_)::timestamp <= max_refreshhistory 		
		AND operatingriskassessment.t_ = 'OperatingRiskAssessment';
	raise notice '% - Step aboperatingriskassessment - part b end', clock_timestamp();
	
raise notice '% - Step aboperatingriskassessment_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.aboperatingriskassessment_idx;
DROP INDEX if exists olapts.aboperatingriskassessment_idx2;
CREATE INDEX IF NOT EXISTS aboperatingriskassessment_idx ON olapts.aboperatingriskassessment (id_);
CREATE INDEX IF NOT EXISTS aboperatingriskassessment_idx2 ON olapts.aboperatingriskassessment (pkid_,versionid_);
	
raise notice '% - Step aboperatingriskassessment - part a end', clock_timestamp();
END IF;

-- Create or update flag table -- 
raise notice '% Step aboperatingriskassessment - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.aboperatingriskassessmentflag;
CREATE TABLE IF NOT EXISTS olapts.aboperatingriskassessmentflag AS
	select
	id_,
	pkid_,
	wfid_ wfid_,
	taskid_ taskid_, 
	versionid_ versionid_,
	isdeleted_ isdeleted_,
	islatestversion_ islatestversion_,
	baseversionid_ baseversionid_,
	contextuserid_ contextuserid_,
	isvisible_ isvisible_,
	isvalid_ isvalid_,
	snapshotid_ snapshotid_,
	t_ t_,
	createdby_ createdby_,
	createddate_ createddate_,
	updatedby_ updatedby_,
	updateddate_ updateddate_,
	(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
	GREATEST(updateddate_, createddate_) as sourcepopulateddate_
	,current_setting('myvariables.popdate')::timestamp as populateddate_
	FROM madata.operatingriskassessment
	where GREATEST(operatingriskassessment.updateddate_, operatingriskassessment.createddate_)::timestamp <= max_refreshhistory 	
	AND operatingriskassessment.t_ = 'OperatingRiskAssessment';

raise notice '% - Step aboperatingriskassessmentflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.aboperatingriskassessmentflag_idx;
DROP INDEX if exists olapts.aboperatingriskassessmentflag_idx2;
CREATE INDEX IF NOT EXISTS aboperatingriskassessmentflag_idx ON olapts.aboperatingriskassessmentflag (id_);
CREATE INDEX IF NOT EXISTS aboperatingriskassessmentflag_idx2 ON olapts.aboperatingriskassessmentflag (pkid_,versionid_);
ANALYZE olapts.aboperatingriskassessmentflag ;

raise notice '% - Step aboperatingriskassessmentflag_idx - part a end', clock_timestamp(); 

-- Update refresh history -- 

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABOPERATINGRISKASSESSMENT';
delete from olapts.refreshhistory where tablename = 'ABOPERATINGRISKASSESSMENT';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABOPERATINGRISKASSESSMENT' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABOPERATINGRISKASSESSMENTFLAG';
delete from olapts.refreshhistory where tablename = 'ABOPERATINGRISKASSESSMENTFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABOPERATINGRISKASSESSMENTFLAG' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step aboperatingriskassessment - part c end', clock_timestamp();
		

---- Reference data imports ----

-- CreditCommittee --
raise notice  '% - Step abcreditcommittee - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.abcreditcommittee;
CREATE TABLE olapts.abcreditcommittee AS
	SELECT
	l.jsondoc_->>'Key' abcreditcommitteekey_,
	l.jsondoc_->>'Value' abcreditcommitteevalue_,
	l.jsondoc_->>'Order' abcreditcommitteeorder_,
	isdeleted_,
	t_ t_,
	(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepoopulatedby_,
	GREATEST(updateddate_, createddate_) as sourcepopulateddate_
	from madata.v_lookup l
	where l.t_ = 'CreditCommittee';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABCREDITCOMMITTEE';
delete from olapts.refreshhistory where tablename = 'ABCREDITCOMMITTEE';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABCREDITCOMMITTEE' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step abcreditcommittee - part a end', clock_timestamp();

-- OperatingRiskFlag --
raise notice  '% - Step aboperatingriskflag - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.aboperatingriskflag;
CREATE TABLE olapts.aboperatingriskflag AS
	SELECT
	l.jsondoc_->>'Key' aboperatingriskflagkey_,
	l.jsondoc_->>'Value' aboperatingriskflagvalue_,
	l.jsondoc_->>'Order' aboperatingriskflagorder_,
	isdeleted_,
	t_ t_,
	(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepoopulatedby_,
	GREATEST(updateddate_, createddate_) as sourcepopulateddate_
	from madata.v_lookup l
	where l.t_ = 'OprOperatingRiskFlag';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABOPERATINGRISKFLAG';
delete from olapts.refreshhistory where tablename = 'ABOPERATINGRISKFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABOPERATINGRISKFLAG' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step ABOPERATINGRISKFLAG - part a end', clock_timestamp();


--END $$;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------

---- OperatingRiskAssessmentTrigger ----

--DO $$
--DECLARE
--varprevsuccessdate TIMESTAMP ;
--BEGIN

-- If table exists in refresh history --
IF EXISTS (select from olapts.refreshhistory where tablename = 'ABOPERATINGRISKASSESSMENTTRIGGER') THEN
	raise notice '% - Step aboperatingriskassessmenttrigger - part a start', clock_timestamp();
	insert into olapts.aboperatingriskassessmenttrigger
		SELECT
		operatingriskassessmenttrigger.id_ AS id_,
		operatingriskassessmenttrigger.pkid_::varchar as pkid_,	
		(operatingriskassessmenttrigger.jsondoc_->> 'OprMasterId') AS oprmasterid,		
		(operatingriskassessmenttrigger.jsondoc_->> 'OperatingRiskNonTriggers') AS operatingrisknontriggers, 
		(l0.jsondoc_->> 'Value') AS operatingrisknontriggersval,	
		(operatingriskassessmenttrigger.jsondoc_->> 'OperatingRiskSaved')::numeric AS operatingrisksaved,	
		(operatingriskassessmenttrigger.jsondoc_->> 'OperatingRiskTriggers') AS operatingrisktriggers, 
		(l1.jsondoc_->> 'Value') AS operatingrisktriggersval,			
		(operatingriskassessmenttrigger.jsondoc_->> 'OprId') AS oprid,		
		 operatingriskassessmenttrigger.wfid_::varchar,
		 operatingriskassessmenttrigger.taskid_::varchar,
		 operatingriskassessmenttrigger.versionid_::int4,
		 operatingriskassessmenttrigger.isdeleted_::boolean,
		 operatingriskassessmenttrigger.islatestversion_::boolean,
		 operatingriskassessmenttrigger.baseversionid_::int4,
		 operatingriskassessmenttrigger.contextuserid_::varchar,
		 operatingriskassessmenttrigger.isvisible_::boolean,
		 operatingriskassessmenttrigger.isvalid_::boolean,
		 operatingriskassessmenttrigger.snapshotid_::int4,
		 operatingriskassessmenttrigger.t_::varchar,
		 operatingriskassessmenttrigger.createdby_::varchar,
		 operatingriskassessmenttrigger.createddate_::timestamp,
		 operatingriskassessmenttrigger.updatedby_::varchar,
		 operatingriskassessmenttrigger.updateddate_::timestamp,
		 operatingriskassessmenttrigger.fkid_operatingriskassessment,
		CASE WHEN operatingriskassessmenttrigger.updateddate_ > operatingriskassessmenttrigger.createddate_ THEN operatingriskassessmenttrigger.updatedby_ ELSE operatingriskassessmenttrigger.createdby_ END AS sourcepopulatedby_,
		GREATEST(operatingriskassessmenttrigger.createddate_, operatingriskassessmenttrigger.updateddate_) AS sourcepopulateddate_
		,current_setting('myvariables.popdate')::timestamp as populateddate_
		FROM madata.operatingriskassessmenttrigger
		LEFT JOIN madata.custom_lookup as l0 ON l0.t_ = 'OprOperatingRiskNonTriggers' and l0.jsondoc_->>'Key' = operatingriskassessmenttrigger.jsondoc_->>'OperatingRiskNonTriggers'		
		LEFT JOIN madata.custom_lookup as l1 ON l1.t_ = 'OprOperatingRiskTriggers' and l1.jsondoc_->>'Key' = operatingriskassessmenttrigger.jsondoc_->>'OperatingRiskTriggers'		
		WHERE
		GREATEST(operatingriskassessmenttrigger.updateddate_, operatingriskassessmenttrigger.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABOPERATINGRISKASSESSMENTTRIGGER')
		AND GREATEST(operatingriskassessmenttrigger.updateddate_, operatingriskassessmenttrigger.createddate_)::timestamp <= max_refreshhistory 
		AND operatingriskassessmenttrigger.t_ = 'OperatingRiskAssessmentTrigger';
raise notice '% - Step aboperatingriskassessmenttrigger - part a end', clock_timestamp();
ELSE
raise notice '% Step aboperatingriskassessmenttrigger - part b start', clock_timestamp();
	DROP TABLE IF EXISTS olapts.aboperatingriskassessmenttrigger;
	CREATE TABLE olapts.aboperatingriskassessmenttrigger AS
		SELECT
		operatingriskassessmenttrigger.id_ AS id_,
		operatingriskassessmenttrigger.pkid_::varchar as pkid_,	
		(operatingriskassessmenttrigger.jsondoc_->> 'OprMasterId') AS oprmasterid,		
		(operatingriskassessmenttrigger.jsondoc_->> 'OperatingRiskNonTriggers') AS operatingrisknontriggers, 
		(l0.jsondoc_->> 'Value') AS operatingrisknontriggersval,	
		(operatingriskassessmenttrigger.jsondoc_->> 'OperatingRiskSaved')::numeric AS operatingrisksaved,	
		(operatingriskassessmenttrigger.jsondoc_->> 'OperatingRiskTriggers') AS operatingrisktriggers, 
		(l1.jsondoc_->> 'Value') AS operatingrisktriggersval,			
		(operatingriskassessmenttrigger.jsondoc_->> 'OprId') AS oprid,		
		 operatingriskassessmenttrigger.wfid_::varchar,
		 operatingriskassessmenttrigger.taskid_::varchar,
		 operatingriskassessmenttrigger.versionid_::int4,
		 operatingriskassessmenttrigger.isdeleted_::boolean,
		 operatingriskassessmenttrigger.islatestversion_::boolean,
		 operatingriskassessmenttrigger.baseversionid_::int4,
		 operatingriskassessmenttrigger.contextuserid_::varchar,
		 operatingriskassessmenttrigger.isvisible_::boolean,
		 operatingriskassessmenttrigger.isvalid_::boolean,
		 operatingriskassessmenttrigger.snapshotid_::int4,
		 operatingriskassessmenttrigger.t_::varchar,
		 operatingriskassessmenttrigger.createdby_::varchar,
		 operatingriskassessmenttrigger.createddate_::timestamp,
		 operatingriskassessmenttrigger.updatedby_::varchar,
		 operatingriskassessmenttrigger.updateddate_::timestamp,
		 operatingriskassessmenttrigger.fkid_operatingriskassessment,
		CASE WHEN operatingriskassessmenttrigger.updateddate_ > operatingriskassessmenttrigger.createddate_ THEN operatingriskassessmenttrigger.updatedby_ ELSE operatingriskassessmenttrigger.createdby_ END AS sourcepopulatedby_,
		GREATEST(operatingriskassessmenttrigger.createddate_, operatingriskassessmenttrigger.updateddate_) AS sourcepopulateddate_
		,current_setting('myvariables.popdate')::timestamp as populateddate_				
		FROM madata.operatingriskassessmenttrigger
		LEFT JOIN madata.custom_lookup as l0 ON l0.t_ = 'OprOperatingRiskNonTriggers' and l0.jsondoc_->>'Key' = operatingriskassessmenttrigger.jsondoc_->>'OperatingRiskNonTriggers'		
		LEFT JOIN madata.custom_lookup as l1 ON l1.t_ = 'OprOperatingRiskTriggers' and l1.jsondoc_->>'Key' = operatingriskassessmenttrigger.jsondoc_->>'OperatingRiskTriggers'		
		WHERE
		GREATEST(operatingriskassessmenttrigger.updateddate_, operatingriskassessmenttrigger.createddate_) > (select COALESCE(max(asofdate), to_timestamp(0)) from olapts.refreshhistory where tablename = 'ABOPERATINGRISKASSESSMENTTRIGGER')
		AND GREATEST(operatingriskassessmenttrigger.updateddate_, operatingriskassessmenttrigger.createddate_)::timestamp <= max_refreshhistory 		
		AND operatingriskassessmenttrigger.t_ = 'OperatingRiskAssessmentTrigger';
	raise notice '% - Step aboperatingriskassessmenttrigger - part b end', clock_timestamp();
	
raise notice '% - Step aboperatingriskassessmenttrigger_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.aboperatingriskassessmenttrigger_idx;
DROP INDEX if exists olapts.aboperatingriskassessmenttrigger_idx2;
CREATE INDEX IF NOT EXISTS aboperatingriskassessmenttrigger_idx ON olapts.aboperatingriskassessmenttrigger (id_);
CREATE INDEX IF NOT EXISTS aboperatingriskassessmenttrigger_idx2 ON olapts.aboperatingriskassessmenttrigger (pkid_,versionid_);
	
raise notice '% - Step aboperatingriskassessmenttrigger - part a end', clock_timestamp();
END IF;

-- Create or update flag table -- 
raise notice '% Step aboperatingriskassessmenttrigger - part c start', clock_timestamp();
DROP TABLE IF EXISTS olapts.aboperatingriskassessmenttriggerflag;
CREATE TABLE IF NOT EXISTS olapts.aboperatingriskassessmenttriggerflag AS
	select
	id_,
	pkid_,
	wfid_ wfid_,
	taskid_ taskid_, 
	versionid_ versionid_,
	isdeleted_ isdeleted_,
	islatestversion_ islatestversion_,
	baseversionid_ baseversionid_,
	contextuserid_ contextuserid_,
	isvisible_ isvisible_,
	isvalid_ isvalid_,
	snapshotid_ snapshotid_,
	t_ t_,
	createdby_ createdby_,
	createddate_ createddate_,
	updatedby_ updatedby_,
	updateddate_ updateddate_,
	(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepopulatedby_,
	GREATEST(updateddate_, createddate_) as sourcepopulateddate_
	,current_setting('myvariables.popdate')::timestamp as populateddate_
	FROM madata.operatingriskassessmenttrigger
	where GREATEST(operatingriskassessmenttrigger.updateddate_, operatingriskassessmenttrigger.createddate_)::timestamp <= max_refreshhistory 	
	AND operatingriskassessmenttrigger.t_ = 'OperatingRiskAssessmentTrigger';

raise notice '% - Step aboperatingriskassessmenttriggerflag_idx - part a start', clock_timestamp(); 
DROP INDEX if exists olapts.aboperatingriskassessmenttriggerflag_idx;
DROP INDEX if exists olapts.aboperatingriskassessmenttriggerflag_idx2;
CREATE INDEX IF NOT EXISTS aboperatingriskassessmenttriggerflag_idx ON olapts.aboperatingriskassessmenttriggerflag (id_);
CREATE INDEX IF NOT EXISTS aboperatingriskassessmenttriggerflag_idx2 ON olapts.aboperatingriskassessmenttriggerflag (pkid_,versionid_);
ANALYZE olapts.aboperatingriskassessmenttriggerflag ;

raise notice '% - Step aboperatingriskassessmenttriggerflag_idx - part a end', clock_timestamp(); 

-- Update refresh history -- 

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABOPERATINGRISKASSESSMENTTRIGGER';
delete from olapts.refreshhistory where tablename = 'ABOPERATINGRISKASSESSMENTTRIGGER';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABOPERATINGRISKASSESSMENTTRIGGER' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABOPERATINGRISKASSESSMENTTRIGGERFLAG';
delete from olapts.refreshhistory where tablename = 'ABOPERATINGRISKASSESSMENTTRIGGERFLAG';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABOPERATINGRISKASSESSMENTTRIGGERFLAG' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step aboperatingriskassessmenttrigger - part c end', clock_timestamp();
		

---- Reference data imports ----

-- OperatingRiskNonTriggers --
raise notice  '% - Step aboperatingrisknontriggers - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.aboperatingrisknontriggers;
CREATE TABLE olapts.aboperatingrisknontriggers AS
	SELECT
	l.jsondoc_->>'Key' aboperatingrisknontriggerskey_,
	l.jsondoc_->>'Value' aboperatingrisknontriggersvalue_,
	l.jsondoc_->>'Order' aboperatingrisknontriggersorder_,
	isdeleted_,
	t_ t_,
	(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepoopulatedby_,
	GREATEST(updateddate_, createddate_) as sourcepopulateddate_
	from madata.v_lookup l
	where l.t_ = 'OprOperatingRiskNonTriggers';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABOPERATINGRISKNONTRIGGERS';
delete from olapts.refreshhistory where tablename = 'ABOPERATINGRISKNONTRIGGERS';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABOPERATINGRISKNONTRIGGERS' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step aboperatingrisknontriggers - part a end', clock_timestamp();

-- OperatingRiskTriggers --
raise notice  '% - Step aboperatingrisktriggers - part a start', clock_timestamp();
DROP TABLE IF EXISTS olapts.aboperatingrisktriggers;
CREATE TABLE olapts.aboperatingrisktriggers AS
	SELECT
	l.jsondoc_->>'Key' aboperatingrisktriggerskey_,
	l.jsondoc_->>'Value' aboperatingrisktriggersvalue_,
	l.jsondoc_->>'Order' aboperatingrisktriggersorder_,
	isdeleted_,
	t_ t_,
	(case when updateddate_>createddate_ then updatedby_ else createdby_ end) as sourcepoopulatedby_,
	GREATEST(updateddate_, createddate_) as sourcepopulateddate_
	from madata.v_lookup l
	where l.t_ = 'OprOperatingRiskTriggers';

select COALESCE(max(asofdate),to_timestamp(0)) into varprevsuccessdate from olapts.refreshhistory where tablename = 'ABOPERATINGRISKTRIGGERS';
delete from olapts.refreshhistory where tablename = 'ABOPERATINGRISKTRIGGERS';
insert into olapts.refreshhistory(tablename,asofdate,prevsuccessdate) select 'ABOPERATINGRISKTRIGGERS' tablename, max_refreshhistory as  asofdate,varprevsuccessdate;

raise notice '% - Step aboperatingrisktriggers - part a end', clock_timestamp();


--END $$;

---------------END--OperatingRisk---------------------------------

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--ANALYZE START--
ANALYZE olapts.abpdmodelbcategory ;
ANALYZE olapts.abcorrectiveactionsmaster ;
ANALYZE olapts.abcorrectiveactions ;
ANALYZE olapts.abebadefinition ;
ANALYZE olapts.abespolicy ;
ANALYZE olapts.ableverageindication ;
ANALYZE olapts.abluxembourgratings ;
ANALYZE olapts.ablondonratings ;
ANALYZE olapts.abteiresiasdata ;
ANALYZE olapts.abutp ;	
ANALYZE olapts.abspecialdelta ;	
ANALYZE olapts.abesgassessment;
ANALYZE olapts.abesgquestion;
ANALYZE olapts.abmodelstartupgr;
ANALYZE olapts.abesgoverallassessment;
ANALYZE olapts.aboperatingriskassessment; 
ANALYZE olapts.aboperatingrisktriggers;
--ANALYZE END--

-- RESET parameters tuning
RESET maintenance_work_mem;
RESET work_mem;
RESET effective_cache_size;
RESET effective_io_concurrency;
RESET enable_partitionwise_join;
RESET enable_partitionwise_aggregate;
RESET max_parallel_workers_per_gather;
RESET max_parallel_maintenance_workers;
RESET max_parallel_workers;
RESET random_page_cost;
RESET seq_page_cost;
RESET enable_seqscan;
RESET parallel_leader_participation;
RESET default_statistics_target;
RESET force_parallel_mode;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET jit;
RESET jit_above_cost;
RESET jit_optimize_above_cost;
RESET jit_inline_above_cost;

--------------

pl_status:=TRUE;
RETURN pl_status;
	
end;
$BODY$;

ALTER FUNCTION olapts.populate_olap_uni() OWNER TO olap;

-------------------------------------------------------------
--Check if the function was created 
-------------------------------------------------------------
--select * from olapts.populate_olap_uni()

