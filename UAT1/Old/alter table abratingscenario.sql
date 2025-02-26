--==============================================================================================================================
-- ΕΚΤΕΛΕΣΗ SCRIPT ΣΤΑΔΙΑΚΑ
-- Θα χρειαστεί να τρέξει πρώτα το Α' ΜΕΡΟΣ, μετά μόνη της η γραμμή 39 (αφού βγεί από σχόλιο) και τέλος το Β' ΜΕΡΟΣ
--==============================================================================================================================

------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------Α' ΜΕΡΟΣ--------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

--==========================================
--ALTER TABLE-------------------------------
--==========================================

ALTER TABLE olapts.abratingscenario
ADD COLUMN revivalid text,
ADD COLUMN expirationdate date,
ADD COLUMN originalgrade date;

--=================================================
--CREATE BACKUP OF OLD abratingscenario------------
--=================================================

create table olapts.abratingscenario_backup as
select * from olapts.abratingscenario;

--=================================================
------CHECK DATA-----------------------------------
--=================================================
select * from olapts.abratingscenario_backup limit 100;

------------------------------------------------------------------------------------------------------------------------
----------------------------------ΕΚΤΕΛΕΣΗ ΓΡΑΜΜΗΣ 39 ΜΕΤΑ ΤΗΝ ΕΠΙΤΥΧΗ ΕΚΤΕΛΕΣΗ ΤΟΥ Α ΜΕΡΟΥΣ----------------------------
------------------------------------------------------------------------------------------------------------------------

--=================================================
-------DROP OLD abratingscenario-------------------
--=================================================
--***uncomment row 39 after the backup of table***
--DROP TABLE IF EXISTS olapts.abratingscenario;

------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------Β' ΜΕΡΟΣ--------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

--=================================================
--CREATE NEW abratingscenario----------------------
--=================================================
CREATE TABLE IF NOT EXISTS olapts.abratingscenario
(
    factratingscenarioid_ character varying COLLATE pg_catalog."default",
    pkid_ character varying COLLATE pg_catalog."default",
    ratingscenarioid text COLLATE pg_catalog."default",
    versionid_ integer,
    entityid text COLLATE pg_catalog."default",
    originalfinancialcontext text COLLATE pg_catalog."default",
    financialcontext text COLLATE pg_catalog."default",
    financialid text COLLATE pg_catalog."default",
    financialversionid integer,
    peeranalysis_version_match text COLLATE pg_catalog."default",
    entity_version_match text COLLATE pg_catalog."default",
    projection_version_match text COLLATE pg_catalog."default",
    stmts_versions_ text COLLATE pg_catalog."default",
    name text COLLATE pg_catalog."default",
	originalgrade text COLLATE pg_catalog."default",--added field
    scenariotyperef text COLLATE pg_catalog."default",
    amuser text COLLATE pg_catalog."default",
    ratingstatus text COLLATE pg_catalog."default",
    nextreviewdate date,
    creditcommitteedate date,
    modelid text COLLATE pg_catalog."default",
    isprimary boolean,
    finalgrade text COLLATE pg_catalog."default",
    finalscore numeric,
    modelgrade text COLLATE pg_catalog."default",
    modelpd text COLLATE pg_catalog."default",
    approveddate timestamp without time zone,
    mastergrade text COLLATE pg_catalog."default",
    statementcount text COLLATE pg_catalog."default",
    lateststatementid text COLLATE pg_catalog."default",
    overridegrade text COLLATE pg_catalog."default",
    overridepd text COLLATE pg_catalog."default",
    approveid text COLLATE pg_catalog."default",
    approver text COLLATE pg_catalog."default",
    islatestapprovedscenario boolean,
    approvalstatus text COLLATE pg_catalog."default",
    configversion text COLLATE pg_catalog."default",
    isapproved boolean,
    modelinputschanged text COLLATE pg_catalog."default",
    masteroverridegrade text COLLATE pg_catalog."default",
    masteroverridepd text COLLATE pg_catalog."default",
    selectedfinancialid text COLLATE pg_catalog."default",
    masterpd text COLLATE pg_catalog."default",
    modelversion text COLLATE pg_catalog."default",
    overlaypd text COLLATE pg_catalog."default",
    overlayrating text COLLATE pg_catalog."default",
    projectionid text COLLATE pg_catalog."default",
    proposedrating text COLLATE pg_catalog."default",
    usedfinancial boolean,
    parentid text COLLATE pg_catalog."default",
    parentname text COLLATE pg_catalog."default",
    extendedreviewdate timestamp without time zone,
	expirationdate date, --added field
	revivalid text,  --added field
    wfid_ character varying COLLATE pg_catalog."default",
    taskid_ character varying COLLATE pg_catalog."default",
    snapshotid_ integer,
    contextuserid_ character varying COLLATE pg_catalog."default",
    createdby_ character varying COLLATE pg_catalog."default",
    createddate_ timestamp without time zone,
    updatedby_ character varying COLLATE pg_catalog."default",
    updateddate_ timestamp without time zone,
    isvalid_ boolean,
    isdeleted_ boolean,
    isvisible_ boolean,
    islatestversion_ boolean,
    t_ character varying COLLATE pg_catalog."default",
    baseversionid_ integer,
    statusid_ integer,
    sourcepopulatedby_ character varying COLLATE pg_catalog."default",
    sourcepopulateddate_ timestamp without time zone
);

--=============================================================================
--INSERT INTO olapts.abratingscenario data from olapts.abratingscenario_backup
--=============================================================================
INSERT INTO olapts.abratingscenario(
       factratingscenarioid_, pkid_, ratingscenarioid, versionid_, entityid, originalfinancialcontext, 
       financialcontext, financialid, financialversionid, peeranalysis_version_match, entity_version_match, 
       projection_version_match, stmts_versions_, name, originalgrade, scenariotyperef, amuser, ratingstatus, 
       nextreviewdate, creditcommitteedate, modelid, isprimary, finalgrade, finalscore, modelgrade, modelpd, 
       approveddate, mastergrade, statementcount, lateststatementid, overridegrade, overridepd, approveid, 
       approver, islatestapprovedscenario, approvalstatus, configversion, isapproved, modelinputschanged, 
       masteroverridegrade, masteroverridepd, selectedfinancialid, masterpd, modelversion, overlaypd, 
       overlayrating, projectionid, proposedrating, usedfinancial, parentid, parentname, extendedreviewdate, 
       expirationdate, revivalid, wfid_, taskid_, snapshotid_, contextuserid_, createdby_, createddate_, 
       updatedby_, updateddate_, isvalid_, isdeleted_, isvisible_, islatestversion_, t_, baseversionid_, 
       statusid_, sourcepopulatedby_, sourcepopulateddate_)

select
	factratingscenarioid_, pkid_, ratingscenarioid, versionid_, entityid, originalfinancialcontext, 
    financialcontext, financialid, financialversionid, peeranalysis_version_match, entity_version_match, 
    projection_version_match, stmts_versions_, name, originalgrade, scenariotyperef, amuser, ratingstatus, 
    nextreviewdate, creditcommitteedate, modelid, isprimary, finalgrade, finalscore, modelgrade, modelpd, 
    approveddate, mastergrade, statementcount, lateststatementid, overridegrade, overridepd, approveid, 
    approver, islatestapprovedscenario, approvalstatus, configversion, isapproved, modelinputschanged, 
    masteroverridegrade, masteroverridepd, selectedfinancialid, masterpd, modelversion, overlaypd, 
    overlayrating, projectionid, proposedrating, usedfinancial, parentid, parentname, extendedreviewdate, 
    expirationdate, revivalid, wfid_, taskid_, snapshotid_, contextuserid_, createdby_, createddate_, 
    updatedby_, updateddate_, isvalid_, isdeleted_, isvisible_, islatestversion_, t_, baseversionid_, 
    statusid_, sourcepopulatedby_, sourcepopulateddate_
from olapts.abratingscenario_backup;

	
	--=================================================
	--CHECK DATA---------------------------------------
	--=================================================
	--select * from olapts.abratingscenario limit 100;
	
	--=================================================
	--DROP BACKUP TABLE--------------------------------
	--=================================================
	--DROP TABLE olapts.abratingscenario_backup;
