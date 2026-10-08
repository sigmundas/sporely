BEGIN READ ONLY;
SELECT jsonb_build_object('phase','before','timeout',current_setting('statement_timeout'),'source',(SELECT source FROM pg_settings WHERE name='statement_timeout')); SET LOCAL statement_timeout='121s'; SELECT jsonb_build_object('phase','local_probe','timeout',current_setting('statement_timeout'),'source',(SELECT source FROM pg_settings WHERE name='statement_timeout')); ROLLBACK; BEGIN READ ONLY; SELECT jsonb_build_object('phase','after_rollback','timeout',current_setting('statement_timeout'),'source',(SELECT source FROM pg_settings WHERE name='statement_timeout'));
ROLLBACK;
