--==============================================================================================================================
-- ΕΚΤΕΛΕΣΗ SCRIPT ΣΤΑΔΙΑΚΑ
-- Θα χρειαστεί να τρέξει πρώτα το Α' ΜΕΡΟΣ, μετά μόνη της η γραμμή 36 (αφού βγεί από σχόλιο) και τέλος το Β' ΜΕΡΟΣ
--==============================================================================================================================

------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------Α' ΜΕΡΟΣ--------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

--==========================================
--ALTER TABLE-------------------------------
--==========================================
ALTER TABLE olapts.abentityrating
ADD COLUMN originalmodelgrade text;

--=================================================
--CREATE BACKUP OF OLD abentityrating--------------
--=================================================
create table olapts.abentityrating_backup as
select * from olapts.abentityrating;

--=================================================
------CHECK DATA-----------------------------------
--=================================================
select * from olapts.abentityrating_backup limit 100;

------------------------------------------------------------------------------------------------------------------------
----------------------------------ΕΚΤΕΛΕΣΗ ΓΡΑΜΜΗΣ 36 ΜΕΤΑ ΤΗΝ ΕΠΙΤΥΧΗ ΕΚΤΕΛΕΣΗ ΤΟΥ Α ΜΕΡΟΥΣ----------------------------
------------------------------------------------------------------------------------------------------------------------

--=================================================
-------DROP OLD abentityrating---------------------
--=================================================

--***uncomment row 36 after the backup of table***
--DROP TABLE IF EXISTS olapts.abentityrating;


------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------Β' ΜΕΡΟΣ--------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

--=================================================
--CREATE NEW abentityrating------------------------
--=================================================
CREATE TABLE IF NOT EXISTS olapts.abentityrating
(
    factentityratingid_ character varying COLLATE pg_catalog."default",
    pkid_ character varying COLLATE pg_catalog."default",
    approvalstatus text COLLATE pg_catalog."default",
    approveddate timestamp without time zone,
    approvedgrade text COLLATE pg_catalog."default",
    approvedpd numeric,
    approveid text COLLATE pg_catalog."default",
    approver text COLLATE pg_catalog."default",
    cascadegrade text COLLATE pg_catalog."default",
    cascadenote text COLLATE pg_catalog."default",
    cascadepd numeric,
    cascadereason text COLLATE pg_catalog."default",
    cascadeuserid text COLLATE pg_catalog."default",
    comments text COLLATE pg_catalog."default",
    configversion integer,
    defaultcomment text COLLATE pg_catalog."default",
    defaultdate date,
    defaultgrade text COLLATE pg_catalog."default",
    defaultpd numeric,
    defaultreason text COLLATE pg_catalog."default",
    entityid numeric,
    finalgrade text COLLATE pg_catalog."default",
    finalpd numeric,
    finalscore numeric,
    id numeric,
    isapproved boolean,
    isdefault boolean,
    isoutofdate boolean,
    latestapprovedscenarioid text COLLATE pg_catalog."default",
    masterapprovedgrade text COLLATE pg_catalog."default",
    masterapprovedpd numeric,
    mastercascadegrade text COLLATE pg_catalog."default",
    mastercascadepd numeric,
    masterfinalgrade text COLLATE pg_catalog."default",
    masterfinalpd numeric,
    mastergrade text COLLATE pg_catalog."default",
    masteroverlaypd numeric,
    masteroverlayrating text COLLATE pg_catalog."default",
    masteroverridegrade text COLLATE pg_catalog."default",
    masteroverridepd numeric,
    masterpd numeric,
    mastersourcegrade text COLLATE pg_catalog."default",
    mastersourcepd numeric,
    modelgrade text COLLATE pg_catalog."default",
    modelpd numeric,
	originalmodelgrade text, --added field
    outofdatereason text COLLATE pg_catalog."default",
    overlaypd numeric,
    overlayrating text COLLATE pg_catalog."default",
    overridegrade text COLLATE pg_catalog."default",
    overridepd numeric,
    sourceentityid numeric,
    sourceentityratingversionid integer,
    sourcegrade text COLLATE pg_catalog."default",
    sourcelongname text COLLATE pg_catalog."default",
    sourcepd numeric,
    transientapprovedgrade text COLLATE pg_catalog."default",
    transientapprovedpd numeric,
    transientgrade text COLLATE pg_catalog."default",
    transientpd numeric,
    wfid_ character varying COLLATE pg_catalog."default",
    taskid_ character varying COLLATE pg_catalog."default",
    versionid_ integer,
    statusid_ integer,
    isdeleted_ boolean,
    islatestversion_ boolean,
    isvisible_ boolean,
    isvalid_ boolean,
    baseversionid_ integer,
    snapshotid_ integer,
    contextuserid_ character varying COLLATE pg_catalog."default",
    t_ character varying COLLATE pg_catalog."default",
    createdby_ character varying COLLATE pg_catalog."default",
    createddate_ timestamp without time zone,
    updatedby_ character varying COLLATE pg_catalog."default",
    updateddate_ timestamp without time zone,
    sourcepopulatedby_ character varying COLLATE pg_catalog."default",
    sourcepopulateddate_ timestamp without time zone,
    populateddate_ timestamp without time zone
);

--=============================================================================
--INSERT INTO olapts.abentityrating data from olapts.abentityrating_backup
--=============================================================================
INSERT INTO olapts.abentityrating(
	factentityratingid_, pkid_, approvalstatus, approveddate, approvedgrade, approvedpd, approveid, approver, cascadegrade, cascadenote, cascadepd, 
	cascadereason, cascadeuserid, comments, configversion, defaultcomment, defaultdate, defaultgrade, defaultpd, defaultreason, entityid, finalgrade, 
	finalpd, finalscore, id, isapproved, isdefault, isoutofdate, latestapprovedscenarioid, masterapprovedgrade, masterapprovedpd, mastercascadegrade, 
	mastercascadepd, masterfinalgrade, masterfinalpd, mastergrade, masteroverlaypd, masteroverlayrating, masteroverridegrade, masteroverridepd, masterpd, 
	mastersourcegrade, mastersourcepd, modelgrade, modelpd,originalmodelgrade, outofdatereason, overlaypd, overlayrating, overridegrade, overridepd, sourceentityid, sourceentityratingversionid,
	sourcegrade, sourcelongname, sourcepd, transientapprovedgrade, transientapprovedpd, transientgrade, transientpd, wfid_, taskid_, versionid_, statusid_, isdeleted_, islatestversion_, 
	isvisible_, isvalid_, baseversionid_, snapshotid_, contextuserid_, t_, createdby_, createddate_, updatedby_, updateddate_, sourcepopulatedby_, sourcepopulateddate_, populateddate_
)

select
	factentityratingid_, pkid_, approvalstatus, approveddate, approvedgrade, approvedpd, approveid, approver, cascadegrade, cascadenote, cascadepd, 
	cascadereason, cascadeuserid, comments, configversion, defaultcomment, defaultdate, defaultgrade, defaultpd, defaultreason, entityid, finalgrade, 
	finalpd, finalscore, id, isapproved, isdefault, isoutofdate, latestapprovedscenarioid, masterapprovedgrade, masterapprovedpd, mastercascadegrade, 
	mastercascadepd, masterfinalgrade, masterfinalpd, mastergrade, masteroverlaypd, masteroverlayrating, masteroverridegrade, masteroverridepd, masterpd, 
	mastersourcegrade, mastersourcepd, modelgrade, modelpd,originalmodelgrade, outofdatereason, overlaypd, overlayrating, overridegrade, overridepd, sourceentityid, sourceentityratingversionid,
	sourcegrade, sourcelongname, sourcepd, transientapprovedgrade, transientapprovedpd, transientgrade, transientpd, wfid_, taskid_, versionid_, statusid_, isdeleted_, islatestversion_, 
	isvisible_, isvalid_, baseversionid_, snapshotid_, contextuserid_, t_, createdby_, createddate_, updatedby_, updateddate_, sourcepopulatedby_, sourcepopulateddate_, populateddate_
from olapts.abentityrating_backup;

	
	--=================================================
	--CHECK DATA---------------------------------------
	--=================================================
	--select * from olapts.abentityrating limit 100;
	
	--=================================================
	--DROP BACKUP TABLE--------------------------------
	--=================================================
	--DROP TABLE olapts.abentityrating_backup;
	