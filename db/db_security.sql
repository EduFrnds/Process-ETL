-- Criação de usuários e permissões para camada layer_gold e seus dados gold_data.

CREATE ROLE gold_data_acess;

GRANT USAGE ON SCHEMA layer_gold TO gold_data_acess;
GRANT SELECT ON ALL TABLES IN SCHEMA layer_gold TO gold_data_acess;

ALTER DEFAULT PRIVILEGES IN SCHEMA layer_gold GRANT SELECT ON TABLES TO gold_data_acess;

REVOKE USAGE ON SCHEMA layer_gold FROM PUBLIC;
REVOKE SELECT ON ALL TABLES IN SCHEMA layer_gold FROM PUBLIC;

CREATE USER analista_dados WITH PASSWORD 'projeto_etl';

GRANT gold_data_acess TO analista_dados;