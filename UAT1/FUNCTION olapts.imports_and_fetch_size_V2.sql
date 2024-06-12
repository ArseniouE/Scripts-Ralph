CREATE OR REPLACE FUNCTION olapts.imports_and_fetch_size()
    RETURNS boolean
    LANGUAGE 'plpgsql'
    VOLATILE
    PARALLEL UNSAFE
    COST 100
    
AS $BODY$
declare
 pl_status boolean:=FALSE;
begin

--IMPORT FOREIGN SCHEMA tenant LIMIT TO (addedaccount) FROM SERVER matenantserver INTO madata;
--IMPORT FOREIGN SCHEMA tenant LIMIT TO (ratingoverride) FROM SERVER matenantserver INTO madata;
--IMPORT FOREIGN SCHEMA tenant LIMIT TO (peeranalysis) FROM SERVER matenantserver INTO madata;
--IMPORT FOREIGN SCHEMA tenant LIMIT TO (entityofficer) FROM SERVER matenantserver INTO madata;
--IMPORT FOREIGN SCHEMA tenant LIMIT TO (projectionstatement) FROM SERVER matenantserver INTO madata;
--IMPORT FOREIGN SCHEMA tenant LIMIT TO (ratingscenarioblockdata) FROM SERVER matenantserver INTO madata;
--IMPORT FOREIGN SCHEMA tenant LIMIT TO (utp) FROM SERVER matenantserver INTO madata;
--IMPORT FOREIGN SCHEMA tenant LIMIT TO (leverageindication) FROM SERVER matenantserver INTO madata;
--IMPORT FOREIGN SCHEMA tenant LIMIT TO (teiresiasdata) FROM SERVER matenantserver INTO madata;
--IMPORT FOREIGN SCHEMA tenant LIMIT TO (espolicy) FROM SERVER matenantserver INTO madata;
--IMPORT FOREIGN SCHEMA tenant LIMIT TO (ebadefinition) FROM SERVER matenantserver INTO madata;
--IMPORT FOREIGN SCHEMA tenant LIMIT TO (correctiveactions) FROM SERVER matenantserver INTO madata;
--IMPORT FOREIGN SCHEMA tenant LIMIT TO (correctiveactionsmaster) FROM SERVER matenantserver INTO madata;
--IMPORT FOREIGN SCHEMA tenant LIMIT TO (specialdelta) FROM SERVER matenantserver INTO madata;
--IMPORT FOREIGN SCHEMA tenant LIMIT TO (esgassessment) FROM SERVER matenantserver INTO madata;
--IMPORT FOREIGN SCHEMA tenant LIMIT TO (esgquestion) FROM SERVER matenantserver INTO madata;
--IMPORT FOREIGN SCHEMA tenant LIMIT TO (esgoverallassessment) FROM SERVER matenantserver INTO madata;
--IMPORT FOREIGN SCHEMA tenant LIMIT TO (operatingriskassessment) FROM SERVER matenantserver INTO madata;

-- Θα πρέπει να γίνει ενα DO για καθε import. Να αλλαξει το LIMIT TO (projectionstatement) και το ΑND table_name = 'correctiveactionsmaster' με το όνομα του 

--addedaccount
DO $$
DECLARE
    schema_exists BOOLEAN;
BEGIN
    -- Check if the foreign schema has been imported
    SELECT EXISTS (
        SELECT *
        FROM information_schema.tables
        WHERE
        table_name = 'addedaccount'  -- Check for a specific table in the schema
    ) INTO schema_exists;
    
    -- If the schema doesn't exist, import it
    IF NOT schema_exists THEN
        BEGIN
            IMPORT FOREIGN SCHEMA tenant LIMIT TO (addedaccount) FROM SERVER matenantserver INTO madata;
            RAISE NOTICE 'Schema imported successfully.';
        EXCEPTION
            WHEN others THEN
                RAISE NOTICE 'Error importing schema: %', SQLERRM;
        END;
    ELSE
        RAISE NOTICE 'Schema already exists.';
    END IF;
END $$;

--ratingoverride
DO $$
DECLARE
    schema_exists BOOLEAN;
BEGIN
    -- Check if the foreign schema has been imported
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE
        table_name = 'ratingoverride'  -- Check for a specific table in the schema
    ) INTO schema_exists;
    
    -- If the schema doesn't exist, import it
    IF NOT schema_exists THEN
        BEGIN
            IMPORT FOREIGN SCHEMA tenant LIMIT TO (ratingoverride) FROM SERVER matenantserver INTO madata;
            RAISE NOTICE 'Schema imported successfully.';
        EXCEPTION
            WHEN others THEN
                RAISE NOTICE 'Error importing schema: %', SQLERRM;
        END;
    ELSE
        RAISE NOTICE 'Schema already exists.';
    END IF;
END $$;

--peeranalysis
DO $$
DECLARE
    schema_exists BOOLEAN;
BEGIN
    -- Check if the foreign schema has been imported
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE
        table_name = 'peeranalysis'  -- Check for a specific table in the schema
    ) INTO schema_exists;
    
    -- If the schema doesn't exist, import it
    IF NOT schema_exists THEN
        BEGIN
            IMPORT FOREIGN SCHEMA tenant LIMIT TO (peeranalysis) FROM SERVER matenantserver INTO madata;
            RAISE NOTICE 'Schema imported successfully.';
        EXCEPTION
            WHEN others THEN
                RAISE NOTICE 'Error importing schema: %', SQLERRM;
        END;
    ELSE
        RAISE NOTICE 'Schema already exists.';
    END IF;
END $$;

--entityofficer
DO $$
DECLARE
    schema_exists BOOLEAN;
BEGIN
    -- Check if the foreign schema has been imported
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE
        table_name = 'entityofficer'  -- Check for a specific table in the schema
    ) INTO schema_exists;
    
    -- If the schema doesn't exist, import it
    IF NOT schema_exists THEN
        BEGIN
            IMPORT FOREIGN SCHEMA tenant LIMIT TO (entityofficer) FROM SERVER matenantserver INTO madata;
            RAISE NOTICE 'Schema imported successfully.';
        EXCEPTION
            WHEN others THEN
                RAISE NOTICE 'Error importing schema: %', SQLERRM;
        END;
    ELSE
        RAISE NOTICE 'Schema already exists.';
    END IF;
END $$;

--projectionstatement
DO $$
DECLARE
    schema_exists BOOLEAN;
BEGIN
    -- Check if the foreign schema has been imported
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE
        table_name = 'projectionstatement'  -- Check for a specific table in the schema
    ) INTO schema_exists;
    
    -- If the schema doesn't exist, import it
    IF NOT schema_exists THEN
        BEGIN
            IMPORT FOREIGN SCHEMA tenant LIMIT TO (projectionstatement) FROM SERVER matenantserver INTO madata;
            RAISE NOTICE 'Schema imported successfully.';
        EXCEPTION
            WHEN others THEN
                RAISE NOTICE 'Error importing schema: %', SQLERRM;
        END;
    ELSE
        RAISE NOTICE 'Schema already exists.';
    END IF;
END $$;

--ratingscenarioblockdata
DO $$
DECLARE
    schema_exists BOOLEAN;
BEGIN
    -- Check if the foreign schema has been imported
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE
        table_name = 'ratingscenarioblockdata'  -- Check for a specific table in the schema
    ) INTO schema_exists;
    
    -- If the schema doesn't exist, import it
    IF NOT schema_exists THEN
        BEGIN
            IMPORT FOREIGN SCHEMA tenant LIMIT TO (ratingscenarioblockdata) FROM SERVER matenantserver INTO madata;
            RAISE NOTICE 'Schema imported successfully.';
        EXCEPTION
            WHEN others THEN
                RAISE NOTICE 'Error importing schema: %', SQLERRM;
        END;
    ELSE
        RAISE NOTICE 'Schema already exists.';
    END IF;
END $$;

--utp
DO $$
DECLARE
    schema_exists BOOLEAN;
BEGIN
    -- Check if the foreign schema has been imported
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE
        table_name = 'utp'  -- Check for a specific table in the schema
    ) INTO schema_exists;
    
    -- If the schema doesn't exist, import it
    IF NOT schema_exists THEN
        BEGIN
            IMPORT FOREIGN SCHEMA tenant LIMIT TO (utp) FROM SERVER matenantserver INTO madata;
            RAISE NOTICE 'Schema imported successfully.';
        EXCEPTION
            WHEN others THEN
                RAISE NOTICE 'Error importing schema: %', SQLERRM;
        END;
    ELSE
        RAISE NOTICE 'Schema already exists.';
    END IF;
END $$;

--leverageindication
DO $$
DECLARE
    schema_exists BOOLEAN;
BEGIN
    -- Check if the foreign schema has been imported
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE
        table_name = 'leverageindication'  -- Check for a specific table in the schema
    ) INTO schema_exists;
    
    -- If the schema doesn't exist, import it
    IF NOT schema_exists THEN
        BEGIN
            IMPORT FOREIGN SCHEMA tenant LIMIT TO (leverageindication) FROM SERVER matenantserver INTO madata;
            RAISE NOTICE 'Schema imported successfully.';
        EXCEPTION
            WHEN others THEN
                RAISE NOTICE 'Error importing schema: %', SQLERRM;
        END;
    ELSE
        RAISE NOTICE 'Schema already exists.';
    END IF;
END $$;

--teiresiasdata
DO $$
DECLARE
    schema_exists BOOLEAN;
BEGIN
    -- Check if the foreign schema has been imported
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE
        table_name = 'teiresiasdata'  -- Check for a specific table in the schema
    ) INTO schema_exists;
    
    -- If the schema doesn't exist, import it
    IF NOT schema_exists THEN
        BEGIN
            IMPORT FOREIGN SCHEMA tenant LIMIT TO (teiresiasdata) FROM SERVER matenantserver INTO madata;
            RAISE NOTICE 'Schema imported successfully.';
        EXCEPTION
            WHEN others THEN
                RAISE NOTICE 'Error importing schema: %', SQLERRM;
        END;
    ELSE
        RAISE NOTICE 'Schema already exists.';
    END IF;
END $$;

--espolicy
DO $$
DECLARE
    schema_exists BOOLEAN;
BEGIN
    -- Check if the foreign schema has been imported
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE
        table_name = 'espolicy'  -- Check for a specific table in the schema
    ) INTO schema_exists;
    
    -- If the schema doesn't exist, import it
    IF NOT schema_exists THEN
        BEGIN
            IMPORT FOREIGN SCHEMA tenant LIMIT TO (espolicy) FROM SERVER matenantserver INTO madata;
            RAISE NOTICE 'Schema imported successfully.';
        EXCEPTION
            WHEN others THEN
                RAISE NOTICE 'Error importing schema: %', SQLERRM;
        END;
    ELSE
        RAISE NOTICE 'Schema already exists.';
    END IF;
END $$;

--ebadefinition
DO $$
DECLARE
    schema_exists BOOLEAN;
BEGIN
    -- Check if the foreign schema has been imported
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE
        table_name = 'ebadefinition'  -- Check for a specific table in the schema
    ) INTO schema_exists;
    
    -- If the schema doesn't exist, import it
    IF NOT schema_exists THEN
        BEGIN
            IMPORT FOREIGN SCHEMA tenant LIMIT TO (ebadefinition) FROM SERVER matenantserver INTO madata;
            RAISE NOTICE 'Schema imported successfully.';
        EXCEPTION
            WHEN others THEN
                RAISE NOTICE 'Error importing schema: %', SQLERRM;
        END;
    ELSE
        RAISE NOTICE 'Schema already exists.';
    END IF;
END $$;

--correctiveactions
DO $$
DECLARE
    schema_exists BOOLEAN;
BEGIN
    -- Check if the foreign schema has been imported
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE
        table_name = 'correctiveactions'  -- Check for a specific table in the schema
    ) INTO schema_exists;
    
    -- If the schema doesn't exist, import it
    IF NOT schema_exists THEN
        BEGIN
            IMPORT FOREIGN SCHEMA tenant LIMIT TO (correctiveactions) FROM SERVER matenantserver INTO madata;
            RAISE NOTICE 'Schema imported successfully.';
        EXCEPTION
            WHEN others THEN
                RAISE NOTICE 'Error importing schema: %', SQLERRM;
        END;
    ELSE
        RAISE NOTICE 'Schema already exists.';
    END IF;
END $$;

--correctiveactionsmaster
DO $$
DECLARE
    schema_exists BOOLEAN;
BEGIN
    -- Check if the foreign schema has been imported
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE
        table_name = 'correctiveactionsmaster'  -- Check for a specific table in the schema
    ) INTO schema_exists;
    
    -- If the schema doesn't exist, import it
    IF NOT schema_exists THEN
        BEGIN
            IMPORT FOREIGN SCHEMA tenant LIMIT TO (correctiveactionsmaster) FROM SERVER matenantserver INTO madata;
            RAISE NOTICE 'Schema imported successfully.';
        EXCEPTION
            WHEN others THEN
                RAISE NOTICE 'Error importing schema: %', SQLERRM;
        END;
    ELSE
        RAISE NOTICE 'Schema already exists.';
    END IF;
END $$;

--specialdelta
DO $$
DECLARE
    schema_exists BOOLEAN;
BEGIN
    -- Check if the foreign schema has been imported
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE
        table_name = 'specialdelta'  -- Check for a specific table in the schema
    ) INTO schema_exists;
    
    -- If the schema doesn't exist, import it
    IF NOT schema_exists THEN
        BEGIN
            IMPORT FOREIGN SCHEMA tenant LIMIT TO (specialdelta) FROM SERVER matenantserver INTO madata;
            RAISE NOTICE 'Schema imported successfully.';
        EXCEPTION
            WHEN others THEN
                RAISE NOTICE 'Error importing schema: %', SQLERRM;
        END;
    ELSE
        RAISE NOTICE 'Schema already exists.';
    END IF;
END $$;

--esgassessment
DO $$
DECLARE
    schema_exists BOOLEAN;
BEGIN
    -- Check if the foreign schema has been imported
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE
        table_name = 'esgassessment'  -- Check for a specific table in the schema
    ) INTO schema_exists;
    
    -- If the schema doesn't exist, import it
    IF NOT schema_exists THEN
        BEGIN
            IMPORT FOREIGN SCHEMA tenant LIMIT TO (esgassessment) FROM SERVER matenantserver INTO madata;
            RAISE NOTICE 'Schema imported successfully.';
        EXCEPTION
            WHEN others THEN
                RAISE NOTICE 'Error importing schema: %', SQLERRM;
        END;
    ELSE
        RAISE NOTICE 'Schema already exists.';
    END IF;
END $$;

--esgquestion
DO $$
DECLARE
    schema_exists BOOLEAN;
BEGIN
    -- Check if the foreign schema has been imported
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE
        table_name = 'esgquestion'  -- Check for a specific table in the schema
    ) INTO schema_exists;
    
    -- If the schema doesn't exist, import it
    IF NOT schema_exists THEN
        BEGIN
            IMPORT FOREIGN SCHEMA tenant LIMIT TO (esgquestion) FROM SERVER matenantserver INTO madata;
            RAISE NOTICE 'Schema imported successfully.';
        EXCEPTION
            WHEN others THEN
                RAISE NOTICE 'Error importing schema: %', SQLERRM;
        END;
    ELSE
        RAISE NOTICE 'Schema already exists.';
    END IF;
END $$;

--esgoverallassessment
DO $$
DECLARE
    schema_exists BOOLEAN;
BEGIN
    -- Check if the foreign schema has been imported
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE
        table_name = 'esgoverallassessment'  -- Check for a specific table in the schema
    ) INTO schema_exists;
    
    -- If the schema doesn't exist, import it
    IF NOT schema_exists THEN
        BEGIN
            IMPORT FOREIGN SCHEMA tenant LIMIT TO (esgoverallassessment) FROM SERVER matenantserver INTO madata;
            RAISE NOTICE 'Schema imported successfully.';
        EXCEPTION
            WHEN others THEN
                RAISE NOTICE 'Error importing schema: %', SQLERRM;
        END;
    ELSE
        RAISE NOTICE 'Schema already exists.';
    END IF;
END $$;

--added 11/06/2024
--operatingriskassessment
DO $$
DECLARE
    schema_exists BOOLEAN;
BEGIN
    -- Check if the foreign schema has been imported
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE
        table_name = 'operatingriskassessment'  -- Check for a specific table in the schema
    ) INTO schema_exists;
    
    -- If the schema doesn't exist, import it
    IF NOT schema_exists THEN
        BEGIN
            IMPORT FOREIGN SCHEMA tenant LIMIT TO (operatingriskassessment) FROM SERVER matenantserver INTO madata;
            RAISE NOTICE 'Schema imported successfully.';
        EXCEPTION
            WHEN others THEN
                RAISE NOTICE 'Error importing schema: %', SQLERRM;
        END;
    ELSE
        RAISE NOTICE 'Schema already exists.';
    END IF;
END $$;

--added 11/06/2024
--operatingriskassessmenttrigger 
DO $$
DECLARE
    schema_exists BOOLEAN;
BEGIN
    -- Check if the foreign schema has been imported
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE
        table_name = 'operatingriskassessmenttrigger'  -- Check for a specific table in the schema
    ) INTO schema_exists;
    
    -- If the schema doesn't exist, import it
    IF NOT schema_exists THEN
        BEGIN
            IMPORT FOREIGN SCHEMA tenant LIMIT TO (operatingriskassessmenttrigger) FROM SERVER matenantserver INTO madata;
            RAISE NOTICE 'Schema imported successfully.';
        EXCEPTION
            WHEN others THEN
                RAISE NOTICE 'Error importing schema: %', SQLERRM;
        END;
    ELSE
        RAISE NOTICE 'Schema already exists.';
    END IF;
END $$;

DO $$
BEGIN
    -- Check if the option 'fetch_size' and 'use_remote_estimate' is already set for the foreign table
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_foreign_table
        WHERE ftrelid = 'madata.historicalstatement'::regclass
        AND ftoptions::text ILIKE '%fetch_size%'
		AND ftoptions::text ILIKE '%use_remote_estimate%'
    ) THEN
        RAISE NOTICE 'fetch_size and use_remote_estimate is already set for the foreign table.';
    ELSE
        -- Option 'fetch_size' is not set, so set it
        ALTER FOREIGN TABLE madata.historicalstatement
        OPTIONS (fetch_size '100', use_remote_estimate 'on');
        RAISE NOTICE 'fetch_size is set to 100 and use_remote_estimate on for the foreign table.';
    END IF;
END $$;

DO $$
BEGIN
    -- Check if the option 'fetch_size' and 'use_remote_estimate' is already set for the foreign table
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_foreign_table
        WHERE ftrelid = 'madata.ratingscenarioblockdata'::regclass
        AND ftoptions::text ILIKE '%fetch_size%'
		AND ftoptions::text ILIKE '%use_remote_estimate%'
    ) THEN
        RAISE NOTICE 'fetch_size and use_remote_estimate is already set for the foreign table.';
    ELSE
        -- Option 'fetch_size' is not set, so set it
        ALTER FOREIGN TABLE madata.ratingscenarioblockdata
        OPTIONS (fetch_size '100', use_remote_estimate 'on');
        RAISE NOTICE 'fetch_size is set to 100 and use_remote_estimate on for the foreign table.';
    END IF;
END $$;


pl_status:=TRUE;
RETURN pl_status;
	
end;
$BODY$;