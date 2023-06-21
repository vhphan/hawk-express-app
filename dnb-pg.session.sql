-- query to check location of postgres config file

SELECT name, setting FROM pg_settings WHERE name = 'config_file';

