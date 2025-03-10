--==========================================
--ALTER TABLE-------------------------------
--==========================================
	
ALTER TABLE olapts.aboperatingriskassessment
ADD COLUMN nontriggers text,
ADD COLUMN nontriggersval text,
ADD COLUMN triggers text,
ADD COLUMN triggersval text;


--=================================================
--CREATE BACKUP OF OLD aboperatingriskassessment----------------
--=================================================
create table olapts.aboperatingriskassessment_backup as
select * from olapts.aboperatingriskassessment;


--=================================================
------CHECK DATA-----------------------------------
--=================================================
select * from olapts.aboperatingriskassessment_backup limit 100;


--=================================================
-------DROP OLD aboperatingriskassessment-----------------------
--=================================================
--DROP TABLE IF EXISTS olapts.aboperatingriskassessment;

--=================================================
--CREATE NEW aboperatingriskassessment--------------------------
--=================================================
CREATE TABLE IF NOT EXISTS olapts.aboperatingriskassessment
(
    id_ character varying COLLATE pg_catalog."default",
    pkid_ character varying COLLATE pg_catalog."default",
    oprid text COLLATE pg_catalog."default",
    approvaldate timestamp without time zone,
    approvaluser text COLLATE pg_catalog."default",
    approvercomments text COLLATE pg_catalog."default",
    assessmentdate timestamp without time zone,
    assessmentuser text COLLATE pg_catalog."default",
    authorizationflag boolean,
    comments text COLLATE pg_catalog."default",
    creditcommittee text COLLATE pg_catalog."default",
    creditcommitteeval text COLLATE pg_catalog."default",
    creditcommitteedate timestamp without time zone,
    entityid numeric,
    entityversionid numeric,
    islatestapproved boolean,
	nontriggers text COLLATE pg_catalog."default",
	nontriggersval text COLLATE pg_catalog."default",
    operatingriskflag text COLLATE pg_catalog."default",
    operatingriskflagval text COLLATE pg_catalog."default",
	triggers text COLLATE pg_catalog."default",
	triggersval text COLLATE pg_catalog."default",
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
--INSERT INTO olapts.aboperatingriskassessment data from olapts.aboperatingriskassessment_backup
--=============================================================================

INSERT INTO olapts.aboperatingriskassessment(
	id_, 
	pkid_,
	oprid,
	approvaldate,
	approvaluser,
	approvercomments,
	assessmentdate,
	assessmentuser, 
	authorizationflag,
	comments,
	creditcommittee, 
	creditcommitteeval, 
	creditcommitteedate, 
	entityid, 
	entityversionid, 
	islatestapproved,
	nontriggers,
	nontriggersval,
	operatingriskflag, 
	operatingriskflagval,
	triggers ,
	triggersval ,
	wfid_,
	taskid_, 
	versionid_, 
	isdeleted_, 
	islatestversion_,
	baseversionid_,
	contextuserid_, 
	isvisible_, 
	isvalid_,
	snapshotid_, 
	t_,
	createdby_, 
	createddate_, 
	updatedby_, 
	updateddate_, 
	fkid_entity,
	sourcepopulatedby_, 
	sourcepopulateddate_, 
	populateddate_
	)
	
	select 
		id_, 
	pkid_,
	oprid,
	approvaldate,
	approvaluser,
	approvercomments,
	assessmentdate,
	assessmentuser, 
	authorizationflag,
	comments,
	creditcommittee, 
	creditcommitteeval, 
	creditcommitteedate, 
	entityid, 
	entityversionid, 
	islatestapproved,
	nontriggers,
	nontriggersval,
	operatingriskflag, 
	operatingriskflagval,
	triggers ,
	triggersval ,
	wfid_,
	taskid_, 
	versionid_, 
	isdeleted_, 
	islatestversion_,
	baseversionid_,
	contextuserid_, 
	isvisible_, 
	isvalid_,
	snapshotid_, 
	t_,
	createdby_, 
	createddate_, 
	updatedby_, 
	updateddate_, 
	fkid_entity,
	sourcepopulatedby_, 
	sourcepopulateddate_, 
	populateddate_
	from olapts.aboperatingriskassessment_backup
	;
	
	--=================================================
	--CHECK DATA--------------------------------
	--=================================================
	--select * from olapts.aboperatingriskassessment
	
	--=================================================
	--DROP BACKUP TABLE--------------------------------
	--=================================================
	--DROP TABLE olapts.aboperatingriskassessment_backup
	
	--=================================================
	--Run Ralph Script--------------------------------
	--=================================================
	