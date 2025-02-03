--==========================================
--ALTER TABLE---abmapinstance------dev----------------------
--==========================================
	
ALTER TABLE olapts.abmapinstance
ADD COLUMN fkid_teiresiasdata character varying;	

--=================================================
--CREATE BACKUP OF OLD abmapinstance----------------
--=================================================
create table olapts.abmapinstance_backup as
select * from olapts.abmapinstance

--=================================================
------CHECK DATA-----------------------------------
--=================================================
select * from olapts.abmapinstance_backup limit 100


--=================================================
-------DROP OLD abmapinstance-----------------------
--=================================================
--DROP TABLE IF EXISTS olapts.abmapinstance;

--=================================================
--CREATE NEW abmapinstance--------------------------
--=================================================


CREATE TABLE IF NOT EXISTS olapts.abmapinstance
(
    factmappdinstanceid_ character varying COLLATE pg_catalog."default",
    pkid_ character varying COLLATE pg_catalog."default",
    alerts text COLLATE pg_catalog."default",
    errors text COLLATE pg_catalog."default",
    grade text COLLATE pg_catalog."default",
    pd numeric,
    score numeric,
	---------------
   --competitionlevel integer,
   --operatingyears integer,
   --numberofproducts integer,
   --geographicalcoverage integer,
   --exports boolean,
   --sectorprogress integer,
   --customerprogress integer,
   --organizationchart integer,
   --production integer,
   --distribution integer,
   --management integer,
   --succesionstatus integer,
   --managerability integer,
   --miss integer,
   --buildingcondition integer,
   --machineryequipmentage integer,
   --technologylevel integer,
   --marketinglevel integer,
   --productsandservices integer,
   --coopyearsalpha integer,
   --ownershipdeliq integer,
   --willinglesstodiscloseinform integer,
   --inabilitytopayinter integer,
	------------------
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
	fkid_teiresiasdata character varying COLLATE pg_catalog."default",
    sourcepopulatedby_ character varying COLLATE pg_catalog."default",
    sourcepopulateddate_ timestamp without time zone,
    populateddate_ timestamp without time zone
)


--=============================================================================
--INSERT INTO olapts.abmapinstance data from olapts.abmapinstance_backup
--=============================================================================

INSERT INTO olapts.abmapinstance(
	factmappdinstanceid_, pkid_, alerts, errors, grade, pd, score, 
	--competitionlevel, 
	--operatingyears, numberofproducts, geographicalcoverage, exports, sectorprogress, 
	--customerprogress, organizationchart, production, distribution, management,
	--succesionstatus, managerability, miss, buildingcondition, machineryequipmentage, 
	--technologylevel, marketinglevel, productsandservices, coopyearsalpha, ownershipdeliq, 
	--willinglesstodiscloseinform, inabilitytopayinter,
	wfid_, taskid_, versionid_, isdeleted_,
	islatestversion_, baseversionid_, contextuserid_, isvisible_, isvalid_, snapshotid_, t_, 
	createdby_, createddate_, updatedby_, updateddate_, fkid_entity, fkid_teiresiasdata,
	sourcepopulatedby_,
	sourcepopulateddate_, populateddate_)
	
	select
	
	factmappdinstanceid_, pkid_, alerts, errors, grade, pd, score, 
	--competitionlevel, 
	--operatingyears, numberofproducts, geographicalcoverage, exports, sectorprogress, 
	--customerprogress, organizationchart, production, distribution, management,
	--succesionstatus, managerability, miss, buildingcondition, machineryequipmentage, 
	--technologylevel, marketinglevel, productsandservices, coopyearsalpha, ownershipdeliq, 
	--willinglesstodiscloseinform, inabilitytopayinter,
	wfid_, taskid_, versionid_, isdeleted_,
	islatestversion_, baseversionid_, contextuserid_, isvisible_, isvalid_, snapshotid_, t_, 
	createdby_, createddate_, updatedby_, updateddate_, fkid_entity, fkid_teiresiasdata,
	sourcepopulatedby_,
	sourcepopulateddate_, populateddate_
	from olapts.abmapinstance_backup

	--=================================================
	--CHECK DATA--------------------------------
	--=================================================
	--select * from olapts.abmapinstance
	
	--=================================================
	--DROP BACKUP TABLE--------------------------------
	--=================================================
	--DROP TABLE olapts.abmapinstance_backup
	
	--=================================================
	--Run Ralph Script--------------------------------
	--=================================================